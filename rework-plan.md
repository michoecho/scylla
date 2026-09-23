<!-- Copyright (C) 2026 ScyllaDB -->
<!-- SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1 -->

# Paging rework: plan

This plan redoes the paging and reconciliation fixes from scratch. The first
attempt is on branch `paging-fix-series-v1`, and [progress.md](progress.md)
records it. That attempt fixed about forty defects one at a time, each driven
by a failing random case. The fixes worked, but many of them patched symptoms
of the same few causes. This rework removes those causes directly. It uses the
first attempt only as a record of lessons and of failing cases, not as code to
restore.

## Starting point

Branch `worktree/paging-rework` starts from `f4c5fca223` ("Local patches"),
which is upstream master `5f352afcf4` plus local tooling. On top of it are:
- the extraction of the coordinator's page decisions into
  [service/read_page_resolution.hh](service/read_page_resolution.hh), and of
  the replica's page driver into `read_data_page()` and `read_mutation_page()`
  in [replica/querier.hh](replica/querier.hh). Neither changes behavior;
- the tests which record the current behavior of both, in
  [test/boost/read_page_resolution_test.cc](test/boost/read_page_resolution_test.cc);
- the randomized harness: the answer model in
  [test/lib/read_model.hh](test/lib/read_model.hh), the simulated cluster in
  [test/lib/paged_read.hh](test/lib/paged_read.hh), and `test_general` in
  [test/boost/paged_read_test.cc](test/boost/paged_read_test.cc).

The baseline fails `test_general` often. That is expected until the rework is
done.

## Five causes, five changes

The defects of the first attempt come from five causes. Each change below
removes one. The order at the end says which change depends on which.

### D. A resumed cached reader must behave exactly like a fresh one

Cause: when a page continues a partition with a cached querier, the compactor
rebuilds the partition's state in several separate branches of
`start_new_page()`: the partition tombstone, the static row, the open range
tombstone. Each branch had its own bug. The cache check also accepted readers
whose position or ranges did not match the page.

Change:
- One resume path. It feeds the compactor the same fragments which a fresh
  reader would produce at the resume position.
- A strict reuse check. The saved position, including its bound weight, and
  the reader's ranges must match what the page asks for. A reader's slice is
  fixed when it is created, so a page with other ranges needs a new reader.
- Limits count only fragments strictly after the page's start. Re-sent state
  never counts, so a page cannot stop on what it re-sent.

Not obvious:
- Every page must still consume at least one fragment after its start, unless
  its range is exhausted. Otherwise the paging state repeats. This guarantee is
  what bounds a scan of tombstones, and change B depends on it.
- The open range tombstone must be reopened at the resume position, not at the
  position of the last fragment. The static row changes that position.
- A direct check belongs in the harness: for every page read with a cached
  querier, a fresh reader must return the same result. That turns the whole
  class into one assertion.
- This change needs no cluster feature. It can go upstream alone.

### A. Every reply states its frontier

Cause: the coordinator deduces how far each replica read from indirect
signals: the last position, the short-read flag, the row count, the limits,
and, for mutation pages, the content. Each deduction failed somewhere. The last
position is the position of the last fragment the reader consumed. It lags
behind what the replica read, and it is present or absent for reasons which
vary by code path and version.

Change: every reply (data, digest and mutation) carries a frontier F. It means
"I read every fragment of the range strictly before F, and nothing at or after
F". Reaching the end of the range is written as the range's end, not as a
missing value. The replica computes F when it decides to stop.

Not obvious:
- The per-partition limit makes a replica skip the rest of a partition and go
  on. The reply must also list these skip points, one per partition it left
  early. DISTINCT is a per-partition limit of one. Express it as
  `partition_row_limit = 1` in the slice instead of the hard-coded cap in the
  compactor, so that a continuation can ask for more rows of one partition.
- A replica which consumed a whole partition reports the `partition_end`
  region. Today the position can stay at the static row, which the pager reads
  as "partition done" even when a decision about the partition is pending.
- The multishard path (`read_page()` in
  [replica/multishard_query.cc](replica/multishard_query.cc)) computes its own
  cursor. It must follow the same definition. The harness does not cover that
  path yet.
- This is a wire change behind a cluster feature. Share the feature with change
  E, since both change what replicas send.

### E. The digest covers exactly the returned result

Cause: when a query selects no static column, the digest ignores static cells.
Two replicas can then disagree about a static-only row with matching digests.
No coordinator logic can detect it.

Change: a new digest algorithm which covers the liveness of each partition's
static row, behind the same cluster feature as change A.

Not obvious:
- Matching digests from replicas with different frontiers are still sound,
  if the page is trimmed to the smaller frontier. A replica which read further
  cannot have a live row beyond the other's frontier, or the digests would
  differ.
- With the feature off, this defect is unfixable. The harness needs a contract
  for that configuration; see "Harness" below.

### B. Trim to the common frontier, then continue

Cause: when reconciliation might lack data, the resolver estimates where the
page ends from row counts, and retries with larger limits. The estimates were
wrong in several ways, and the retries could fail to grow.

Change: one rule. Take the smallest frontier among the replies used, and the
smallest skip point in each partition. Keep only the merged data before that
point, and set the cursor to it. If the page needs more rows and may not end
short, start another round from that point. Never repeat a round with larger
limits.

Not obvious:
- Progress follows from change D's guarantee that every page moves past its
  start. No estimate is needed, so none can be wrong.
- Unpaged reads may not end short, because `query::result_merger` drops the
  ranges after a short partial result. They loop inside the coordinator.
- Conversion to the client's rows applies the limits after trimming. The
  resolver must never predict where conversion stops.
- The data reply after a digest match is also trimmed to the smallest
  frontier. This replaces today's lowering of the last position.
- Repair mutations computed from trimmed data are still safe: a repair write
  merges by timestamp, so it cannot make a replica lose data.
- The retry code in `resolve_mutation_page()` and the
  `got_incomplete_information*` family go away.

### C. A static-only row is decided at its partition's end

Cause: whether a partition yields a static-only row depends on the whole
partition, but a page can stop in the middle of it. The replica already
decides the row at the partition's end, in
`mutation_querier::consume_end_of_partition()`. But it treats a page stop as a
partition end, and the coordinator and the pager place the row at the static
row's position, before all clustering rows.

Change:
- Every result row has a decision position, after which no data can change
  whether the row exists. For a clustering row, it is the row itself. For a
  static-only row, it is the partition's end. For a DISTINCT row, it is the
  static row if the merged static row is live, otherwise the first live
  clustering row, otherwise the partition's end.
- Change B keeps a row only if its decision position is before the common
  frontier.
- A replica which stops inside a partition never decides its static-only row.

Not obvious:
- The paging state needs a new field that says that no row of the cursor's
  partition has been returned yet. The position cannot express it.
  `rows_fetched_for_last_partition` cannot either: the pager maintains it only
  under PER PARTITION LIMIT, and older coordinators write 0 there. The new
  field must default to "decided", which fails in the safer direction.
- When that field is set, the next page asks for static content
  (`always_return_static_content`). Asking for it otherwise returns a spurious
  static-only row.
- Cursor encoding: "after all clustering rows" means the rows are done and the
  row is pending. `partition_end` means the partition is done. Master's pager
  already moves on for any region other than `clustered`. An older pager given
  a pending cursor loses the row silently. So this also needs a cluster
  feature.
- The internal pagers of `select_statement`, for filtering and aggregation,
  must carry the same state.

## Order

1. D. Replica only, no feature. Removes about ten defects of the first attempt.
2. A with E, under one cluster feature. The only wire change besides C's field.
3. B. Coordinator only; depends on A.
4. C. Depends on A and B; adds the paging-state field and its feature.

Small independent defects of the first attempt can go in at any point, as
separate commits:
- `partition_slice_builder` drops the per-partition limit when it starts from
  an existing slice. It breaks PER PARTITION LIMIT on reversed reads in the
  legacy format.
- `frozen_mutation_consumer_adaptor` flushes range tombstone changes into a
  consumer which already asked to stop. That moves the converted cursor past a
  live row.

## Mixed-version clusters

Until every node has the features, the coordinator must use the old logic.
Keep that path as master's code, not as a patched variant. Otherwise two
designs need maintenance. The known defects then remain in mixed clusters, and
that is the price. The harness should draw the feature per case and apply the
contract below.

## Harness

Needed before or with the changes:
- A contract for configurations which are unfixable by design (features off).
  The first attempt grew six guards for them, one failing case at a time.
  Decide the contract up front instead. One option: in such configurations,
  check only that each page is a prefix of the answer after dropping rows
  that the digests cannot decide, that the query ends, and that no row
  repeats. A lost row can admit another row under LIMIT, and a replica can
  invent a row, so the contract must allow both.
- The cached-versus-fresh check of change D.
- A test that `storage_proxy`'s executor calls the extracted decisions the way
  the harness does. The harness copies the executor's control flow, and
  nothing checks that the copy stays true.

Known gaps, which could hide defects:
- The multishard read path, which change A touches.
- A pager kept across pages, compound clustering keys, equal timestamps.
- Consistency levels whose participants change between pages.
- A case which both keeps queriers and applies repairs.
- No check of the per-partition limit of results, and none that the data and
  digest replies of the same replica agree.
- A shrunk case keeps its schedule seed, so the shrinker rarely removes a
  replica.

## Acceptance

- `test_general` passes a large campaign in the full-feature configuration,
  and the contract of the other configurations.
- Every shrunk witness recorded in [progress.md](progress.md) passes as a fixed
  case. Run them only at the end, as a check of the harness's reach, as the
  first attempt's change of plan intended. Some witnesses name options which
  the new code may not have; adapt them.
- The cqlpy tests of paging, static rows, DISTINCT, limits, filtering and the
  tombstone limit pass. So do the cluster paging tests, and a mixed-version
  test of the features.
- A performance check of unpaged reads, which change B may send through more
  rounds.

## Progress

- D: commit `ac0a69d94a`.
- E: commit `632b5c6a20`. The feature is `READ_FRONTIERS`; the harness
  option is `read_options::read_frontiers`.
- A: commits `246a4d8218` and `e9d25290a3`. Replicas report
  `query::read_frontier` in every reply, and the harness checks it against a
  read of what it covers. No coordinator uses it yet. The DISTINCT limit
  gives way to an explicit per-partition limit in the slice, but
  `select_statement` does not set one yet.
- Known cost of A: the skips list one position per partition which a read
  left at the per-partition limit, so a scan with PER PARTITION LIMIT sends
  about one more key per partition.
- On 20000 runs of `test_general` with seed 1, after E and A: 840 failures.
