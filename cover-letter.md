# Rework paging and read reconciliation around explicit replica frontiers

Currently, due to subtle bugs in logic, paged reads can return wrong results when replicas disagree, or when a replica resumes a page from a cached querier. Results can lose rows, repeat rows, return deleted rows, return a static-only row which should not exist, or fail assertions.

This series adds a randomized test which sniffs these defects, and fixes them by changing five rules of the read path.

Initially, I put this test in a loop of: run the test until failure, let Claude Opus analyze the failure, write a fix, restart the test.
This loop got to 38 patches (sic!) before the test stopped failing. (And those failures and fixes were pretty much independent, at least in the sense that the test, which was starting from the same seed each time, was failing on a later and later iteration every time, without regressing prior cases).

This series is a rewrite of that into something more coherent.

## Test infrastructure

The first five commits extract some logic to separate functions to make the read path testable, and then add the test:
- The replica's page driver moves into `replica::read_data_page()` and `replica::read_mutation_page()`. The coordinator's page decisions move into `service/read_page_resolution.{hh,cc}`. Neither move changes behavior.
- `test/lib/read_model` computes the complete answer of a SELECT over a small history of writes, independently from the database's read path. It serves as the test oracle.
- `test/lib/paged_read` runs a query through the production CQL layer, pager, coordinator decisions and replica page driver, over simulated replicas which hold various parts of the write history. The harness controls replica placement, page size, limits, cluster features, querier cache evictions, repairs and the order of replies.
- `test_general` in `paged_read_test` draws random cases, checks every page of a read against the model, and shrinks a failing case to C++ code which then can be used to replay it.

Later commits add checks to the harness as the new paging rules need them. The fixed test cases named `test_witness*` come from shrunk failures.

## New paging rules

### 1. A page from a cached querier must equal a page from a fresh querier for the same read command

When a page continued a partition with a cached querier, the replica rebuilt the partition's state (partition tombstone, static row, open range tombstone) and there were subtle bugs for each piece of that rebuilt state. The querier cache could also reuse a reader whose position or ranges did not quite match the next page request.

Now a cached querier has a different resume path: it unpops the saved partition state into its reader. The reader then feeds the compactor the same fragments which a new reader would produce at the resume position. The querier cache also reuses a querier only if the next page starts exactly where its reader stopped, and asks for exactly what the reader's slice has left. Also, only fragments after the page's start count against the tombstone limit, so every page makes progress regardless of how low that limit is. (In practice it is never low enough for this to matter, but it makes the readers more amenable
to testing).

The paged_read_test re-reads every cached-querier page with a new querier and fails if the two results differ.

This rule needs no new cluster-level changes.

### 2. The digest covers exactly the partitions of the result

Normally, when a partition has live static columns, they are emitted (if selected)
as extra columns added to emitted clustering rows.
But when there are no live clustering rows, a special row ("static-only row") with a null
clustering key is emitted instead.

But, when a query selected no static column, two replicas could disagree about
the static-only row and still send matching digests. The coordinator could not detect this.

A new digest algorithm, `xxHash_without_empty_partitions`, fixes this by
leaving the partition keys of empty partitions out of the digest.
A static-only row whose static columns are not selected does not affect the digest directly.
The only way it affects the digest is by adding the partition itself to the digest.
In the preexisting digest algorithm, an empty partition is added to the digest
if it has some tombstones, which makes it indistinguishable from a partition with
only a static-only row. In the new algorithm, an empty partition is not added
to the digest, so they are different.

By the way, if I understand correctly, this seems like the original intent of the digest
algorithm. The fact that tombstone-only partitions are included in the digest seems
like an oversight.

### 3. Every reply states how far the replica read

The coordinator used to guess how far each replica read from indirect signals:
the last_position field of the data reply, the short-read flag, the row count, the limits,
the last row in the reply. Those guesses were wrong in many cases which would cause several subtle bugs.

Now a replica can explicitly report a frontier (`query::read_frontier`):
the position where its reader stopped, or no position if it reached the end of the requested ranges.
The reply holds all of the replica's data before the frontier.
A mutation reply also lists "skips": partitions which the replica left early because of PER PARTITION LIMIT.
DISTINCT is also now expressed as a per-partition limit of one,
so a coordinator can ask for more rows of one partition of a DISTINCT query.

### 4. The coordinator combines replica replies up to the common frontier, then resumes from there

When digests differed, the coordinator estimated from row counts whether the merged data was enough to fill a page, and retried with larger limits when it was not. These estimates were wrong in many ways.

Now each reconciliation round takes the common frontier of the replies: the earliest stop (or skip, if after the merge the combined replies don't fulfill the per-partition limit of that partition).
It keeps only the merged data before the common frontier, and computes the repair mutations from it. If the page needs more rows and may not end short, the next round starts from that point. A round now never repeats with larger limits. Progress follows from rule 1, because every replica reads past the start of its page.

When digests match, the coordinator also uses the frontiers. If it cannot tell whether the data reply is enough to fill the page correctly according to the limits, it treats the result as a mismatch and goes to the reconciliation path.

### 5. A static-only row decision is delayed to the end of its partition

Whether a partition yields a static-only row, or a DISTINCT row, depends on the whole partition. But a page can stop inside a partition. The replicas and the coordinator treated that stop as the partition's end. A page could return a static-only row, and the next page could return a live row of the same partition. A DISTINCT query could also skip a partition whose first live row came after the cursor.

Now the coordinator returns a row of such a partition only when the data before the common frontier is enough to seal the decision. Otherwise the page returns no row of the partition. The pager records this in a new paging state field, `partition_row_pending`. When it is set, the next page continues the unfinished partition from the cursor. (Before the patch, a DISTINCT query skipped to the next partition instead.) Unless the query restricts the clustering key, the next page also sets `always_return_static_content`, so that the replicas can return the pending static-only row.

## Smaller fixes

These are independent of the rules above:
- `frozen_mutation_consumer_adaptor` no longer feeds range tombstone changes to a consumer which asked to stop. Before, the adaptor could feed the consumer a range tombstone change after it asked to stop, which moved the page's cursor past a live row.
- `partition_slice_builder` keeps the per-partition row limit when it starts from an existing slice. Before, PER PARTITION LIMIT would be lost on reversed reads in the legacy format.
- The pager counts PER PARTITION LIMIT rows of the cursor's partition, not of the page's last partition. Before, a page which ended short without a row of the cursor's partition could make the next page either drop rows within the limit or return rows beyond it.
- `result_view::calculate_last_position()` reacts to a page without a partition with `on_internal_error()` instead of `SCYLLA_ASSERT`.

## Compatibility

Rules 2 to 5 change what replicas send, or how the coordinator interprets it. They apply only when every node has the new `READ_FRONTIERS` cluster feature. Wire changes:
- In `query::result` and the `read_digest` verb, `wire_position` replaces `last_position`. The coordinator interprets it as a last_position (as used by the legacy logic) or as a frontier stop position,
depending on what it asked for.
- `reconcilable_result` gains new fields for the stop position and the list of skips.
- The paging state gets the `partition_row_pending` flag.
- The new digest algorithm is requested only with the feature. The verbs already carry the algorithm.

Without the feature, the coordinator and the pager run the old logic, which is (hopefully) unchanged.
Its known defects stay present in mixed-version clusters.
Rule 1 and the smaller fixes apply regardless of the feature. The only change made specifically to the old logic converts a `SCYLLA_ASSERT` into an internal error.

## Testing

The randomized test, `test_general`, runs a small number of iterations by default.
In preparation for this series, it was run for millions of iterations.
With `READ_FRONTIERS`, every run must match the model's complete answer. Without it, the test only checks a weaker contract: a run must avoid internal errors other than the known one, and must pass the checks of the replica replies and the repairs.
There are also a bunch of fixed-scenario tests demonstrating some bugs fixed by each patch.
