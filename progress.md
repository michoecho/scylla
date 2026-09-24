<!-- Copyright (C) 2026 ScyllaDB -->
<!-- SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1 -->

# Paging test redesign: progress

> **Superseded.** This file records the first attempt, on branch
> `paging-fix-series-v1`. All commit IDs below refer to that branch. The
> rework on `worktree/paging-rework` follows [rework-plan.md](rework-plan.md)
> and uses this file only as a record of lessons and failing cases.

This file tracks the implementation of [paging-test-redesign.md](paging-test-redesign.md). Stage 1 of that plan is complete: the baseline coordinator decisions and the shared replica page driver are extracted, without behavior changes. Stage 2 is in progress: the small-history model, the simulated replicas and the coordinator loop through the real pager are done. A first random campaign found seven baseline defects, which are listed below with shrunk witnesses.

The plan changed after that campaign; see [Change of plan](#change-of-plan). The work no longer follows the generator families and the per-defect tracking of the design. It completes the harness first, and then builds a single general test. Part 1, the harness features, is complete. In part 2, the general test exists. The user then replaced the large campaign with a test-and-fix loop. It has ported thirteen historical fixes, folded a fourteenth into a fix of its own, and made twenty-two fixes of its own, including the first fix of defect 7, for which the historical series has none. It has also added five harness guards and widened three of them, all for the digest blind spot which no fix can reach, and stopped drawing one cluster feature disabled. Every historical fix is now ported except `c05317d472`. The campaign of 1000000 histories passes: 10000000 runs, seed 1.

## Handoff (2026-09-16, second session)

Read [Change of plan](#change-of-plan) and [Test-and-fix loop](#test-and-fix-loop-2026-09-15) first.

### Current task

The user's procedure:
1. Run the general test until its first failure.
2. Match the failure with a known defect: one of the seven defects of the first campaign (see [Simulated replicas and the coordinator loop](#simulated-replicas-and-the-coordinator-loop-commit-916e357dc9)), or a commit of the historical series. The series is `git log --reverse f4c5fca2231c..f72c59d9f1`; commit `a9c234ce01` reverted it.
3. Port the historical fix, and repeat.

Stop and report to the user when a failure matches no known defect, or when it matches a defect whose fix is already ported.

### Next step

The campaign of 1000000 histories is exhausted at seed 1: all 10000000 runs
pass. The loop has no failure to work on.

Another seed, or a larger campaign, is the same experiment again. It draws from
the same distribution, so it can only reach defects whose probability per case
is below about 1e-7, which is where the yield already was: the last three
failures came at runs 1042912, 4889480 and 1320331 of a 10000000-run campaign.
It is cheap to start and it may still find something, but it is not where the
remaining coverage is.

The remaining coverage is in the generator's own reach. The list under
[Limits of the harness which could hide known defects](#limits-of-the-harness-which-could-hide-known-defects)
names what no seed can reach: a pager kept for the whole query, compound
clustering keys, equal timestamps, consistency levels whose participants change
between pages, a case which both keeps queriers and applies repairs, and the
two missing checks, the per-partition limit of results and agreement between
the data and digest replies of the same replica. Closing one of those changes
what the campaign can find; reseeding does not. Ask the user which to take.

A clean campaign of 10000000 runs takes about 25 minutes, which is longer than
the Bash tool's 10-minute limit, so the harness moves it to the background.
Give the inner `timeout` at least 3000 seconds, or it kills the run before it
finishes and the output has no `runs failed` line.

The loop ran three campaign sizes: 1000 histories until run 9165, 100000 until
run 462012, and 1000000 since. Commit `fea24c1141` also changed the random
stream, by drawing one fewer number, so the run numbers before and after it do
not line up.

The user approved the first decision of this session: extend the harness guard
for run 41999 rather than add a row count to the digest reply, because the
guard is needed either way and the count would not remove it. Run 342978 then
showed that the guard's rule was too narrow, and it was widened; see
[Fixed: the stop can be anywhere in the page which lost an undecidable row](#fixed-the-stop-can-be-anywhere-in-the-page-which-lost-an-undecidable-row).

Runs 71682, 204283, 255025, 290482, 462012, 1015676 and 1031066 matched no
known defect. They were fixed on the standing instruction to fix a defect
rather than skip it, and each is reported below with its mechanism, so the user
can still reject one. Two of them are worth the user's attention:

- `26ce55b3c3` and the earlier fixes of the loop made the pager continue a
  partition it had not continued before, which exposed two stale assumptions in
  the querier cache and the pager. See
  [Fixed: a cursor which leaves no clustering row of its partition](#fixed-a-cursor-which-leaves-no-clustering-row-of-its-partition)
  and [Fixed: a cached querier reused for a page which asks for no clustering row](#fixed-a-cached-querier-reused-for-a-page-which-asks-for-no-clustering-row).
- The fix of run 1031066 changes what `query::result::last_position()` means on
  a data page, and it changes seven recorded expectations of
  `read_page_resolution_test`. The new meaning is the one
  `multishard_query.cc` already used and the one
  `get_or_calculate_last_position()` assumes. It is the largest change of the
  session; see
  [Fixed: a replica which stopped with a full page is invisible to the decision](#fixed-a-replica-which-stopped-with-a-full-page-is-invisible-to-the-decision).

The only historical fix which is still unported is `c05317d472`. No failure has
asked for it yet.

Earlier notes, still current: runs 15 to 41999 are all settled; see the table
of the loop and the sections below.

### State of the tree

- Branch `empty_page_reconciliation`. The working tree is clean.
- The ports are commits `7906af0cae`, `f36598b781`, `d4b9783a82`, `b3c4783cb6`, `d16948f00f`, `fb723fc211`, `39470b8470`, `5f87c41a12`, `ff7e4b5f75`, the port of `3a76dd076f`, `4b67350dcc`, `eb9a7c55da`, `7441658ffb` and `ba68d49f49`; see the table of the loop.
- The fixes of the earlier sessions are `7a5e055ff4`, `b87d5d98fa`, `6b8fd06244`, `017d6a5fbb`, `d54f3f04ce`, `a8c0c701bc`, `c75bac876f`, `0122c5bebd`, `e220178129`, `acc78d64a3`, `ef1d8c09e4`, `16ba381860`, `0037fdf1cf`, `62523a18cf`, `3ad746dfe9`, `770a9823c8` and `26ce55b3c3`.
- The twelve commits of this session, oldest first:
  - `10a5beec1e` lets a case without `STATIC_ROW_DIGEST` stop where it lost an undecidable static-only row, for run 41999. It changes no production code.
  - `2815d890a5` keeps the per-partition row limit when `partition_slice_builder` starts from a slice, for run 71682. It changes [partition_slice_builder.cc](partition_slice_builder.cc) only.
  - `7f79df5827` checks the page's last partition against the replicas which went past it, for run 204283. It changes [service/read_page_resolution.cc](service/read_page_resolution.cc) only.
  - `2f2fd58ccb` moves the pager on from a cursor which leaves no clustering row of its partition, for run 255025. It changes [service/pager/query_pagers.cc](service/pager/query_pagers.cc) and [service/pager/query_pager.hh](service/pager/query_pager.hh).
  - `de8c71184b` ends a page whose limit the partition that `start_new_page()` closed already filled, for run 290482. It changes `consume_page()` in [replica/querier.hh](replica/querier.hh) only.
  - `4aecc3a67f` widens the guard of `10a5beec1e` to the whole page which lost the row, for run 342978. It changes no production code.
  - `b9e82c0ae5` stops counting a re-emitted range tombstone change against the tombstone limit twice, for run 462012. It changes `query_result_builder` in [mutation/mutation_partition.cc](mutation/mutation_partition.cc) only.
  - `4310838df4` rejects a cached querier for a page which asks for no clustering row, for run 1015676. It changes `clustering_position_matches()` in [replica/querier.cc](replica/querier.cc) only.
  - `c78d37d317` makes a data page report a cursor only where it stopped, for run 1031066. It changes `read_data_page()` in [replica/querier.cc](replica/querier.cc), `earliest_stop()` in [service/read_page_resolution.cc](service/read_page_resolution.cc), and seven expectations of [test/boost/read_page_resolution_test.cc](test/boost/read_page_resolution_test.cc).
  - `fea24c1141` stops the general test drawing `empty_replica_pages` disabled, for run 1320331, at the user's decision. It changes no production code, and it changes the random stream.
  - `7c87481e96` reconciles an unpaged read whose replicas stopped apart, for run 1042912 of the new stream. It changes `decide_digest_page()` in [service/read_page_resolution.cc](service/read_page_resolution.cc) only.
  - `4b527e28da` widens the invented-DISTINCT-row guard to any live static cell of a replica's view, for run 4889480. It adds `read_model::live_static_cell_partitions()` and changes no production code.
- `ninja dev-build` succeeds. `./test.py test/boost` runs 3905 tests which pass. Its 114 failures are all object-storage (`_gcs`, `_gs`) and `aws_kms` tests, which need a container image Podman cannot pull on this machine: "current system boot ID differs from cached boot ID". They fail for the environment, not for any change here.
- The 31 tests of `read_page_resolution_test` and the 8 fixed tests of `paged_read_test` pass, as do `mutation_test`, `database_test`, `querier_cache_test`, `mutation_query_test`, `multishard_query_test`, `storage_proxy_test`, `cql_query_test` and `sstable_conforms_to_mutation_source_test`.
- The cqlpy tests of `test_tombstone_limit.py`, `test_paging.py`, `test_static.py`, `test_limit.py`, `test_distinct.py`, `test_filtering.py` and `cassandra_tests/validation/operations/select_limit_test.py` pass. `test_tombstone_limit.py` was checked for discrimination: raising the limit by 1000000 in `check_tombstone_limit()` makes 15 of its 17 tests fail. No other cqlpy test and no cluster test was run.

### Historical fixes not ported yet

Notes from their commit messages and diffs:
- `c05317d472` needs the slice option of `ff088ca0b3`, which is now ported.
- `1d62bea32f` is now ported; see the table. Run 9165 asked for it: its data replica alone reached the consistency level, and the digest reply which arrived before the decision moved the cursor behind the row which the page returned. The port leaves out the commit's error injection, `storage_proxy::digest_read_wait_for_all_responses`, which only served `test/cluster/test_paging.py`.
- `70c0b43291` is now ported; see the table and [Fixed: a short page whose cursor is behind its last row](#fixed-a-short-page-whose-cursor-is-behind-its-last-row). It had been ported and reverted earlier, after its trace came out byte-identical on the run-900 witness. Run 900 was a harness defect, so that measurement did not cover the defect this commit actually fixes.
- `776bbaf665` is folded into the fix of run 6630, at the user's request, so it needs no separate port. Its cap of a partition's row count, at one for DISTINCT and at the per-partition limit otherwise, is one part of counting the rows which conversion returns.
- `f72c59d9f1` only documents defect 7. Commit `6b8fd06244` fixes it instead; see below.

### How to port a historical fix

- Read the message and the production diff: `git show <commit> -- mutation/ query/ replica/ service/ idl/ gms/`. Leave out its tests. `test/cluster/test_paging.py` no longer exists, and the general test serves as the regression test.
- Where the revert restored a file unchanged, the diff applies directly: `git show <commit> -- <paths> > <patch>`, then `git apply --check <patch>` and `git apply <patch>`. This worked for `mutation/` and `query/`.
- The code of `data_read_resolver` and of `abstract_read_executor::reconcile()` now lives in `service/read_page_resolution.cc`, in `mutation_page_resolver` and `resolve_mutation_page()`. Its logger is `rplogger`, not `slogger`. Port the hunks of `storage_proxy.cc` there by hand.
- A port can need a helper which an earlier reverted commit added, such as `query::result::mark_as_short_read()` from `cbd6afe6cb`.
- The tests of `read_page_resolution_test` characterize the baseline, including its defects. When a port changes a recorded behavior, update the expectation and its comment, and say so in the commit message.
- After a port, build, run `pytest test/boost/read_page_resolution_test.cc test/boost/paged_read_test.cc`, and rerun the loop's command. After a change on the replica side, also run `pytest` on `database_test.cc`, `querier_cache_test.cc` and `multishard_query_test.cc`, which are all in `combined_tests`.
- Before trusting a match, read the trace of the shrunk case and work out the mechanism. Check that the new failure is not the defect of the port just made.
- Commit each port on its own. The subject names the component, as the historical subject does. The body starts with "Port the fix of `<commit>`, which a9c234ce01 reverted.", explains the defect briefly, and describes the shrunk witness. Do not add a Co-Authored-By trailer. Then add a row to the table of the loop.

### Limits of the harness which could hide known defects

Ask the user before closing any of these.

- The replicas which count toward CL are the same on every page, and the decision waits for all of them. Consistency levels whose participants change between pages, such as QUORUM without all replicas, are excluded, because their complete answer is not defined.
- A case cannot both keep queriers and apply repairs.
- A shrunk case keeps its schedule seed, whose choices depend on the number of replicas. So the shrinker often cannot remove replicas.
- Deferred: a pager kept for the whole query, compound clustering keys, and equal timestamps.
- Missing checks: the per-partition limit of results, and agreement between the data and digest replies of the same replica.
- No test shows that `storage_proxy`'s executor uses the extracted decisions as the harness does.
- A case with `empty_replica_pages = false` still gets replicas which report `last_position`; only the coordinator ignores it. A real cluster without the feature has a replica which does not report it at all. The harness therefore models the coordinator's blindness, not the replica's silence.

### Practical notes

- Build: `ninja build/dev/test/boost/combined_tests`. It takes 1 to 2 minutes after a change to the harness, and longer after a change to a widely included production header.
- Fixed tests: `pytest test/boost/paged_read_test.cc`. `test_general` skips itself unless `SCYLLA_PAGED_READ_CAMPAIGN` sets the number of histories.
- The cqlpy tests are not expensive. After `ninja build/dev/scylla`, `pytest test/cqlpy/test_tombstone_limit.py` runs its 17 tests in about 2 seconds against a node which the framework starts. Run it after a change to the tombstone limit. When a suite passes that fast, check that it discriminates before trusting it: build with the behavior deliberately broken and confirm the tests fail. For `check_tombstone_limit()`, adding a large constant to the limit works; replacing the condition with `true` does not build.
- `SCYLLA_PAGED_READ_STOP_AT_FAILURE=1` stops `test_general` after its first failure. Seastar options go after the `--`, so the logger flag is `-- ... --logger-log-level testlog=debug`; before the `--`, Boost.Test claims the name and rejects it. `read_page_resolution=trace` logs each reconciled result, which shows what the merge saw.
- The runs are deterministic for a given seed and code. The seastar test runner prints `random-seed=<seed>` at startup.
- Each report starts with `Shrunk from:`. The first `read_case` after it is the original case. The second one is the shrunk case, followed by its CQL, the expected and actual rows, and the trace of each page.
- The Bash tool stops a command after 10 minutes. A campaign of 1000000 histories which finds a failure near run 1300000 takes about 3 minutes, shrinking included.
- To work out a failure, copy its shrunk case into a scratch `SEASTAR_THREAD_TEST_CASE` in `paged_read_test.cc`, inside the suite, and call `run_and_check()`. Vary one option at a time to find which ones the defect needs. The shrinker already removed the options it could, so each one which remains is load-bearing. Do not shrink further by hand before reproducing: removing what looks redundant often makes the case pass.
- clangd can report stale errors when a header and a source file are edited in the same step. The build decides.
- To find the failures which need a feature, print the `read_options` of each shrunk case and look for the feature in them. Before trusting such a failure, read its trace: a harness error would show up there.

## Test-and-fix loop (2026-09-15)

The user's procedure: run the general test until its first failure, match the failure with a known defect, port the historical fix, and repeat. Stop and report a failure which matches no known defect, or which matches a defect whose fix is already ported.

Each iteration reruns seed 1. The runs are deterministic for a seed, and the fixes do not change the random cases.

```
SCYLLA_PAGED_READ_CAMPAIGN=100000 SCYLLA_PAGED_READ_STOP_AT_FAILURE=1 build/dev/test/boost/combined_tests --run_test=paged_read_test/test_general -- -c1 -m1G --overprovisioned --unsafe-bypass-fsync 1 --kernel-page-cache 1 --collectd 0 --random-seed 1
```

The campaign ran 1000 histories until run 9165 was fixed, then 100000 until run
462012 exhausted it. It now runs 1000000, which is 10000000 runs and about 25
minutes when it finds nothing. Commit `fea24c1141` changed the random stream,
by drawing one fewer number, so the run numbers before and after it do not line
up; the table marks the ones which follow it.

| First failing run | Failure | Historical fix | Port |
| --- | --- | --- | --- |
| 15 | A reconciled page is short, without a partition and without a cursor (defect 2) | `f7e42a7d68` | `7906af0cae` |
| 45 | A dead static row stops a data page, and the pager skips the partition (defect 1) | `7cf6143064` | `f36598b781` |
| 91 | A range tombstone beyond a replica's stop moves the cursor past a live row (defect 5) | `ac6608e490` | `d4b9783a82` |
| 96 | A DISTINCT partition whose page stops inside it is dropped (defect 6) | `64e4ca281b`, `3504bb07fd`, `ff088ca0b3`, `87a340bd41` | `b3c4783cb6`, `d16948f00f`, `fb723fc211`, `39470b8470` |
| 271 | A reconciled page emits the static-only row of a partition in which a replica stopped (defect 3) | `aed19a5f1c` | `5f87c41a12` |
| 274 | A reconciled cursor at the end of a partition repeats the paging state | None; see below | `7a5e055ff4` |
| 282 | A data page stops at a range tombstone at its own start, and the paging state repeats | `5cf935471f` | `4b67350dcc` |
| 303 | An unpaged read retries reconciliation without enlarging its row limit | None; see below | `b87d5d98fa` |
| 304 | Digests miss an unselected static-only row (defect 7) | None; `f72c59d9f1` only documents it | `6b8fd06244` |
| 310 | A stale cursor of a cached querier repeats the paging state | `cbd6afe6cb`, `3a76dd076f` | `ff7e4b5f75` and the port of `3a76dd076f` |
| 900 | A harness defect: the repeated-state key omits the undecided-partition flag | None; not a production defect | The repeat-key commit |
| 952 | Defect 7 with `static_row_digest` disabled | `6b8fd06244`, already ported; unfixable without the feature | The static-only check commit |
| 952 | A short page returns a row from beyond its own cursor, so the next page repeats it | `70c0b43291` | The trimming commit |
| 1012 | A static-only row deferred by a short page is lost when the next page is empty | None; the re-emission of `ff088ca0b3` does not reach a reader at the end of its stream | The end-of-stream commit |
| 1999 | A DISTINCT row which only a static cell establishes, with `static_row_digest` disabled | None; defect 7 again, which the harness guard did not cover | The DISTINCT guard commit |
| 3725 | Defect 7 with `static_row_digest` disabled, where `LIMIT` admits another row in place of the lost one | None; defect 7 again, which the row filter of the guard could not absorb | The raised-limit commit |
| 5178 | A static-only row deferred by a short page is lost when the next page starts in a later partition | None; the end-of-stream commit covered only a reader with no fragment left | The partition-start commit |
| 6630 | An unpaged read returns a row of a later partition instead of the first live row of the answer | None; `776bbaf665` caps the same count, but not for this query | The conversion-count commit, which folds `776bbaf665` in |
| 9165 | A digest reply which arrives after the consistency level moves the cursor behind the page's last row, so the next page repeats it | `1d62bea32f` | `7441658ffb` |
| 11440 | A resumed querier reopens a range tombstone after all clustering rows, so the page returns deleted rows as live | None; the defect predates the series | `acc78d64a3` |
| 12373 | Conversion stops at a limit, but the adaptor flushes the close of a later range tombstone, so the cursor skips a live row | None | The frozen-mutation commit |
| 13570 | A DISTINCT page whose replicas disagree about the first live row returns a partition which has none | None; the retry could not ask a replica for more rows of a DISTINCT partition | `ba68d49f49` |
| 16564 | A page which continues a partition with a cached querier returns no partition, so the merge loses the partition tombstone | None | `16ba381860` |
| 17998 | An unpaged read reconciles to a static-only row which no replica decided, and loses the live row beyond a replica's stop | None | `0037fdf1cf` |
| 24804 | A reconciliation round with a single reply returns it unchanged, so the page keeps an undecided static-only row | None | `62523a18cf` |
| 25518 | A page which consumes a partition without reaching a clustering position reports its static row, and the pager skips the partition | `7cf6143064`, already ported; it covers only stopping on the static row | `3ad746dfe9` |
| 32081 | A spurious DISTINCT row which only a static cell establishes, with `static_row_digest` disabled | None; defect 7 again, which the DISTINCT guard covers only in the other direction | `770a9823c8`, the invented-row guard |
| 38672 | A cached data querier which stopped before a row is reused for a page which starts after it, and the row is returned twice | None | `26ce55b3c3`, the querier-position commit |
| 41999 | Defect 7 with `static_row_digest` disabled, where the lost row also ends the query, so a live row of a later partition is lost | None; defect 7 again, which the guard covers only for the lost row itself | `10a5beec1e`, the early-stop guard |
| 71682 | A reversed read in the legacy format ignores PER PARTITION LIMIT | None; the defect predates the series | `2815d890a5` |
| 204283 | A replica which left the page's last partition at its per-partition limit holds a tombstone the merge never sees, so a deleted row is returned as live | None | `7f79df5827` |
| 255025 | A cursor at the end of the query's clustering ranges makes the pager continue the partition with no range, and the paging state repeats | None; the fix of run 274 covers only a cursor after all clustering rows | `2f2fd58ccb` |
| 290482 | A page which re-emits a retained static row returns rows of the next partition past its row limit | None | `de8c71184b` |
| 342978 | Defect 7 with `static_row_digest` disabled, where the lost row is not the one the answer stops at | None; defect 7 again, whose guard had too narrow a rule | `4aecc3a67f`, the widened guard |
| 462012 | A re-emitted range tombstone change is counted against the tombstone limit twice, so a page which read nothing new ends where it started | None | `b9e82c0ae5` |
| 1015676 | A page which asks for no clustering row reuses a cached querier and returns the rows the querier's own ranges admit | None | `4310838df4` |
| 1031066 | A replica which stopped with a full page reports no stop the decision can see, so the page ends its range and loses what only that replica holds | None | `c78d37d317` |
| 1320331 | A replica which stopped with a full page, with `empty_replica_pages` disabled, where the coordinator may not use a cursor at all | None; a blind spot of the cluster feature | `fea24c1141`, which stops drawing the feature disabled |
| 1042912, new stream | An unpaged read accepts a page whose range another replica stopped inside, and loses what only that replica holds beyond the stop | None | `7c87481e96` |
| 4889480, new stream | A DISTINCT row which a replica invents from a static cell on a page which continues the partition after its rows | None; defect 7 again, which the guard covered only over the query's whole range | `4b527e28da`, the widened invented-row guard |

Other changes:
- `64e4ca281b`, `3504bb07fd` and `ff088ca0b3` came without a failure of their own. The fix of `87a340bd41` builds on them, and the user chose to port the whole chain. Their diffs applied unchanged, except that the port of `64e4ca281b` passes the page's slice in `read_data_page()` and `read_mutation_page()` of `replica/querier.cc`, instead of in `table.cc`.
- `cql_test_env` enables all supported features, so the harness's pager sets `defer_undecided_static_only_row`. The harness cannot disable `DEFERRED_STATIC_ONLY_ROWS`.
- The port of `aed19a5f1c` changed no recorded behavior of `read_page_resolution_test`.
- The port of `ac6608e490` adds `query::result::mark_as_short_read()`, which came from `cbd6afe6cb`.
- The port of `f7e42a7d68` removes the extra reversal. `test_short_replica_trims_a_reversed_page` now expects the rows which the short replica examined.
- Commit `5f10d19f85` fixes two flaky CL-scheduling tests of `read_page_resolution_test`. They read the late reply after the reply which reached CL, and the read could yield.
- Commit `bda71f868d` adds `SCYLLA_PAGED_READ_STOP_AT_FAILURE`.

Verification: after each port, `read_page_resolution_test` and `paged_read_test` pass. After the port of `7cf6143064`, `database_test`, `querier_cache_test` and `multishard_query_test` pass too. After the ports of `64e4ca281b`, `ff088ca0b3` and `87a340bd41`, these three and `mutation_query_test` pass. After all ports, `mutation_test` and `perf_row_cache_reads` build, whose calls of the compactor and the querier the port of `64e4ca281b` changed, and the 78 tests of `mutation_test` pass. After the port of `5cf935471f`, `read_page_resolution_test`, `paged_read_test`, `database_test`, `querier_cache_test`, `multishard_query_test` and `mutation_query_test` pass. After the fix of defect 7 and the port of `cbd6afe6cb`, the 30 tests of `read_page_resolution_test`, the 8 fixed tests of `paged_read_test`, the 88 tests of `mutation_test` and `mutation_query_test`, and the 85 tests of `database_test`, `querier_cache_test` and `multishard_query_test` pass. After the port of `3a76dd076f`, the 31 tests of `read_page_resolution_test`, the 8 fixed tests of `paged_read_test` and the 85 tests of `database_test`, `querier_cache_test` and `multishard_query_test` pass. After the port of `1d62bea32f`, those 39 tests and the 3 tests of `storage_proxy_test` pass; the port changes no replica code. After the fixes of runs 11440 and 12373, the 39 tests and the 173 tests of `mutation_test`, `database_test`, `querier_cache_test`, `mutation_query_test` and `multishard_query_test` pass, and `ninja dev-build` succeeds. After the fixes of runs 17998, 24804 and 25518, those 212 tests and the 3 tests of `storage_proxy_test` pass, `ninja dev-build` succeeds, and the cqlpy tests of the tombstone limit, of paging, of static rows and of mutation fragments pass. After the guard of run 32081, which changes only the harness, the 31 tests of `read_page_resolution_test` and the 8 fixed tests of `paged_read_test` pass. No cluster test was run.

### Fixed: a DISTINCT page whose replicas disagree about the first live row

Run 13570, fixed by commit `ba68d49f49`.

Shrunk witness:

```
placed_history{
    {range_deletion{1, bound{1, true}, bound{3, true}, 12}, 0b10},
    {regular_cell_write{1, 5, regular_column::v1, 4, 10, lifetime::expiring}, 0b10},
    {row_marker_write{1, 2, 6, lifetime::expiring}, 0b1},
    {range_deletion{1, bound{4, true}, std::nullopt, 11}, 0b1},
}
select_query{.distinct = true, .select_v1 = false, .select_v2 = false}
read_options{.replica_count = 2, .page_size = 2}
```

`SELECT DISTINCT pk, s FROM ks.cf`. The answer is empty: partition 1 has no live
static cell, and both of its rows are dead in the merge. Row 2 is a row marker
at timestamp 6 on replica 0, under replica 1's deletion of [1, 3] at timestamp
12. Row 5 is a cell at timestamp 10 on replica 1, under replica 0's deletion of
[4, +inf) at timestamp 11. The client receives one row for partition 1.

The trace of the single page:
1. Replica 0's data page returns row 2, the first live row it sees. Replica 1's
   digest reports row 5. The digests differ, so the coordinator reconciles.
2. In the mutation round, the compactor of each replica applies a per-partition
   limit of one, which a DISTINCT query implies. Replica 0 returns row 2 and
   stops there, so its deletion of [4, +inf) never reaches the coordinator.
   Replica 1 returns its deletion of [1, 3] and row 5.
3. The merge has row 2 dead and row 5 live, so the page returns partition 1.

The coordinator has to notice that replica 0 stopped inside partition 1, before
the row which the merge chose, and read the partition again with a larger
limit. Two things stop it, and I measured both:

- `got_incomplete_information()` runs its in-partition check only when
  `original_per_partition_limit < query::max_rows_if_set`. A DISTINCT query
  carries its limit in the `distinct` slice option, not in
  `partition_row_limit`, so the check is skipped. `reached_partition_end`
  compares a replica's row count against `cmd.slice.partition_row_limit()` for
  the same reason, and reports that every replica finished the partition.
  Using `_effective_partition_row_limit` in both places makes the resolver ask
  for a retry.
- The retry then cannot make progress. It raises
  `retry_cmd->slice.partition_row_limit`, but the replica's compactor sets
  `_partition_row_limit` to one whenever the slice has the `distinct` option,
  whatever `partition_row_limit` says. Every round returns the same one row, and
  the read fails after the harness's sixteenth round.

The historical series has no fix for this. Its DISTINCT chain (`64e4ca281b`,
`3504bb07fd`, `ff088ca0b3`, `87a340bd41`) is about a page which stops inside a
partition, not about a retry which needs more of one.

#### Why the compactor caps a DISTINCT partition at one row

[mutation/mutation_compactor.hh](mutation/mutation_compactor.hh), in the query
constructor of `compact_mutation_state`:

```cpp
, _partition_row_limit(_slice.options.contains(query::partition_slice::option::distinct) ? 1 : slice.partition_row_limit())
```

`_partition_row_limit` feeds `_current_partition_limit = std::min(_row_limit,
_partition_row_limit)`, which `consume(clustering_row)` counts against to set
`_stop`. So `slice.partition_row_limit()` is not read at all when the option is
set: a larger value is discarded, not clamped.

Two callers set the option, and both mean "one row per partition":
- [cql3/statements/select_statement.cc](cql3/statements/select_statement.cc),
  from `SELECT DISTINCT`.
- [replica/table.cc](replica/table.cc), in the view-update base read, as
  `need_static && !need_regular`. That read is local, so it never reconciles.

The cap predates the per-partition limit. Before `01b18063ea` ("query: Add per
partition row limit", 2016) the compactor held a `const bool _is_distinct` and
did `_partition_limit = _is_distinct ? 1 : _limit`. That commit added
`partition_row_limit` and folded the two into the ternary above, leaving
DISTINCT's limit hardcoded instead of routing it through the new field.

#### The fix

The first idea was to drop the `distinct` option from the retry command. The
user and I rejected it. It would turn off the DISTINCT guard of
`drop_undecided_static_row()`, which reads the round command, so a retry round
would drop the static row of a partition whose static row settles its DISTINCT
answer. The pager does recover the row -- `_may_leave_partition_undecided` is
true for DISTINCT, the stop is a clustering position, so the next page re-reads
the partition with `always_return_static_content` -- but the extra page is pure
waste, and defeating that guard is exactly what the exemption exists to prevent.
It would also force `_effective_partition_row_limit` and `find_short_partitions`
to take their per-partition limit from the original command rather than the
round command, because the round command would lose the option while conversion
keeps it.

Instead, make the compactor prefer a limit which the slice sets explicitly:

```cpp
, _partition_row_limit(_slice.options.contains(query::partition_slice::option::distinct)
        && slice.partition_row_limit() == query::partition_max_rows
    ? 1 : slice.partition_row_limit())
```

The two channels never compete, so this is unambiguous:
- CQL rejects `PER PARTITION LIMIT` with `SELECT DISTINCT`, so a user DISTINCT
  query always leaves `partition_row_limit` unset.
- `query::max_rows`, `query::partition_max_rows` and the `partition_slice`
  constructor's default are all `UINT64_MAX`, so "unset" has one spelling.
- The view-update read of `table.cc` passes `query::max_rows`, so it still gets
  one row per partition.

Today's behavior is therefore preserved exactly, and the retry round's numeric
limit becomes the only thing which changes. The round command keeps `distinct`,
so `drop_undecided_static_row()` keeps its guard, and
`_effective_partition_row_limit` stays one in every round, which is what
conversion applies.

Commit `ba68d49f49` does this. The helper is
`query::effective_partition_row_limit()` in
[query/query-request.hh](query/query-request.hh). It replaces the ternary in
the compactor, in `may_need_paging()` of
[service/pager/query_pagers.cc](service/pager/query_pagers.cc), and in
`mutation_page_resolver`.

In `mutation_page_resolver` the limit splits in two, because a retry round
raises the round command's limit while conversion keeps applying the original
one:
- `reached_partition_end` compares a replica's row count against the limit
  which this round's replicas applied, `effective_partition_row_limit(cmd.slice)`.
  It used the raw `cmd.slice.partition_row_limit()`, which is unset in round 1
  while the replica applied one, so every replica reported that it finished the
  partition. A retry round raises it, and the comparison has to follow, or a
  replica which finished a partition reports that it did not and the rounds
  repeat.
- `got_incomplete_information()` and `find_short_partitions()` take the
  effective limit of the *original* command, which is what conversion applies.
  `resolve_mutation_page()` now computes it once and passes it in place of the
  raw field, so `_effective_partition_row_limit` is that argument and the
  special case which computed it is gone.
- The retry grows from `effective_partition_row_limit(cmd.slice)`, because
  doubling an unset limit returns `max_rows`, which reads back as unset.

Note: CQL also rejects clustering restrictions with DISTINCT, so
`has_ck_selector(default_row_ranges)` is false and `_may_return_static_only_rows`
is true for any DISTINCT query on a table with static columns.

Verification: the 31 tests of `read_page_resolution_test`, the 8 fixed tests of
`paged_read_test` and the 176 tests of `mutation_test`, `database_test`,
`querier_cache_test`, `mutation_query_test`, `multishard_query_test` and
`storage_proxy_test` pass. `ninja dev-build` succeeds. The campaign reaches run
16564.

### Fixed: a resumed page which drops a partition tombstone

Run 16564, fixed by commit `16ba381860`. It matched no known defect.

Shrunk witness:

```
placed_history{
    {partition_deletion{1, 12}, 0b1},
    {row_marker_write{1, 4, 6, lifetime::expired}, 0b11},
    {row_marker_write{1, 5, 5, lifetime::permanent}, 0b10},
}
select_query{.select_s = false}
read_options{.replica_count = 2, .page_size = 2, .page_size_in_bytes = 44, .querier_cache = true}
```

`SELECT pk, ck, v1, v2 FROM ks.cf`. The answer is empty: replica 0's deletion of
partition 1 at timestamp 12 covers both rows. The client receives row 5.

The defect needs the querier cache. A scratch case which varies one option at a
time passes with `querier_cache = false`, and with `page_size_in_bytes` unset.

What happens:
1. Page 0 stops inside partition 1, at row 4. Both replicas' mutation rounds
   return partition 1; replica 0's carries its partition tombstone, so the merge
   returns no row.
2. Page 1 continues partition 1 from `(4, +inf)`. Its mutation round reuses
   replica 0's cached querier, which is positioned inside partition 1 after row
   4. Replica 0 has nothing left there, so its page returns **0 partitions**.
3. Replica 1 returns row 5. The merge has no partition tombstone, so row 5 is
   live, and the page returns it.

A fresh reader for the same page would emit the partition-start fragment of
partition 1, and its tombstone with it, so the page would return one partition
with no rows. The querier cache is a cache, so it must not change what a page
returns. That is the defect.

`start_new_page()` in [mutation/mutation_compactor.hh](mutation/mutation_compactor.hh)
already re-emits the state which a partition-start fragment would carry: it
re-emits a retained static row and reopens an active range tombstone. It does
not re-emit the partition tombstone. Nothing else does either:
`partition_is_not_empty()`, which emits `consume_new_partition()` and the
partition tombstone, runs only when a fragment reaches the consumer, and this
page has none.

#### The fix

Re-emit the partition tombstone in `start_new_page()`, exactly as `consume()`
does for a partition-start fragment: through `partition_is_not_empty()`, and
only when the tombstone is not purgeable, which is the condition `consume()`
applies. `reemit_partition_tombstone` also joins the condition which increments
`_stats.total_partitions`, or `live_partitions` exceeds it and
`dead_partitions()` underflows.

That alone breaks run **4909**, which passes today:

```
placed_history{
    {partition_deletion{4, 8}, 0b1},
    {range_deletion{1, bound{3, false}, bound{4, true}, 2}, 0b1},
    {row_deletion{1, 3, 7}, 0b1},
    {static_cell_write{2, std::nullopt, 3, lifetime::permanent}, 0b1},
    {range_deletion{4, bound{1, true}, bound{6, true}, 4}, 0b1},
    {regular_cell_write{3, 2, regular_column::v2, 8, 1, lifetime::permanent}, 0b1},
}
select_query{.select_v1 = false, .select_v2 = false}
read_options{.replica_count = 1, .page_size = 3, .page_size_in_bytes = 3, .tombstone_limit = 1, .querier_cache = true, .schedule_seed = 1533966746}
```

Page 3 stops inside partition 4, at row 6. Page 4 continues it and re-emits the
partition tombstone. With a tombstone limit of one, the page reaches the limit
on that tombstone, returns no row, and reports the cursor it started from. The
paging state repeats. Both `tombstone_limit` and `page_size_in_bytes` are
load-bearing: the byte budget is what makes page 3 stop inside partition 4.

Two earlier fixes in [mutation/mutation_partition.cc](mutation/mutation_partition.cc)
handle the same shape by counting a fragment at the page's start against the
tombstone limit without stopping on it: a dead static row, and a range tombstone
change. **Counting without stopping is not enough here.**
`consume_end_of_partition()` calls `check_tombstone_limit()` again, which is
what keeps those two exemptions from unbounding a scan. The page then ends at
the end of the continued partition, which for an empty one is the position the
page started from, and the state still repeats.

So the re-emitted tombstone is not counted either. The previous page counted it
already, so it is not new information. `query_result_builder` counts the
partitions it starts -- the object is made for one page, at
[replica/querier.cc:528](replica/querier.cc#L528) -- and skips the tombstone of
the first one, which is the continued partition whenever `start_new_page()`
re-emitted one. A page which starts at a fresh partition instead loses one unit
of the limit; from the second partition on the limit bounds a scan of deleted
partitions as before.

This needs no hook on the `CompactedFragmentsConsumer` concept and no flag on
`query::result::builder`. Both were considered and are unnecessary.

Verification: runs 16564 and 4909 pass. The campaign reaches run 17998. The 217
tests of `read_page_resolution_test`, `paged_read_test`, `mutation_test`,
`database_test`, `querier_cache_test`, `mutation_query_test`,
`multishard_query_test` and `storage_proxy_test` pass. `ninja dev-build`
succeeds. The 17 tests of [test/cqlpy/test_tombstone_limit.py](test/cqlpy/test_tombstone_limit.py)
pass; those tests do cover this path, because all four of its
`test_partition_tombstone_*` cases fail against a build whose
`consume(tombstone)` never counts.

### Fixed: an invented DISTINCT row on a page which continues a partition

Run 4889480 of the stream after `fea24c1141`, settled by commit `4b527e28da`.

Shrunk witness:

```
placed_history{
    {range_deletion{2, std::nullopt, bound{4, false}, 14}, 0b1},
    {partition_deletion{4, 11}, 0b10},
    {regular_cell_write{4, 1, regular_column::v1, 6, 8, lifetime::expiring}, 0b1},
    {static_cell_write{4, 0, 4, lifetime::permanent}, 0b1},
    {static_cell_write{1, 9, 12, lifetime::expiring}, 0b10},
}
select_query{.distinct = true, .select_s = false, .select_v1 = false, .select_v2 = false}
read_options{.replica_count = 2, .page_size = 3, .page_size_in_bytes = 516, .static_row_digest = false}
```

`SELECT DISTINCT pk FROM ks.cf`. The answer is partition 1 alone. Replica 0
holds a live static cell and a live row of partition 4; replica 1 holds a
deletion of that partition which postdates both. Page 0 stops inside partition
4 on its byte limit, which leaves the partition undecided, and page 1 continues
it after the row, with `always_return_static_content`. Replica 0 answers with
the partition's static-only row. Without `STATIC_ROW_DIGEST` the digests cannot
see the disagreement, and the page returns a partition the answer does not
have.

The guard of `770a9823c8` asked which partitions of a replica's own view have a
DISTINCT row that rests on a static cell alone, over the query's whole range.
Partition 4 does not qualify there, because replica 0 holds a live row of it. A
page restricts the range, and a page which continues a partition after its live
clustering rows asks for a range in which the static cell alone establishes the
row.

The guard now asks the wider question of a replica's view:
`read_model::live_static_cell_partitions()`, which is every partition the
replica holds a live static cell in. The narrower question still serves the
complete answer, where a row which a live clustering row establishes is
decided, because an earlier page returned it.

### Fixed: an unpaged read whose replicas stopped apart

Run 1042912 of the stream after `fea24c1141`, fixed by commit `7c87481e96`.

Shrunk witness:

```
placed_history{
    {range_deletion{1, bound{2, false}, bound{4, false}, 21}, 0b1},
    {regular_cell_write{2, 1, regular_column::v1, 6, 13, lifetime::permanent}, 0b10},
    {static_cell_write{1, 1, 23, lifetime::permanent}, 0b10},
    {row_marker_write{4, 3, 7, lifetime::permanent}, 0b1},
}
select_query{.select_s = false, .select_v1 = false, .select_v2 = false, .limit = 1}
read_options{.replica_count = 2, .page_size = 3, .static_row_digest = false,
             .querier_cache = true, .schedule_seed = 3135680056}
```

Matching digests say that two replicas agree, but only over the part of the
range each of them examined. A paged read handles a replica which examined less
by ending the page at the earliest stop and marking it short. An unpaged read
cannot: nothing pages from its cursor, and the merger of a multi-partition read
drops the partitions after a short result. So it accepted the data reply
whatever the other replicas reported.

`SELECT pk, ck FROM ks.cf LIMIT 1` is unpaged, because `may_need_paging()`
returns false for a row limit of one. The first range holds partitions 1 and 2,
in that order. Replica 1 fills its row limit of one with partition 1's
static-only row and stops there, never reaching partition 2. Replica 0 holds no
live row of either and reaches the end of the range. The digests match, the
coordinator accepts replica 0's empty page, and the read goes on to a later
range. Partition 1's row is the answer and is undecidable without
`STATIC_ROW_DIGEST`, but partition 2's row is decidable, and it is lost too.

The fix reconciles instead, which reads what the replicas actually hold.
Replicas which stopped together examined the same range, so their match stands
and the page is accepted as before; only a reply which stopped apart from the
data reply's own stop forces the reconciliation.

### Settled: a replica which stopped without a cursor, with empty_replica_pages disabled

Run 1320331, settled by commit `fea24c1141`. The user chose to stop drawing the
feature disabled, over extending the guard, which would have been a licence so
wide that it would not name which rows can go missing.

Shrunk witness:

```
placed_history{
    {regular_cell_write{4, 3, regular_column::v1, 6, 4, lifetime::permanent}, 0b1},
    {range_deletion{2, std::nullopt, bound{0, true}, 3}, 0b10},
    {row_marker_write{3, 2, 1, lifetime::permanent}, 0b1},
    {static_cell_write{2, 3, 5, lifetime::permanent}, 0b1},
}
select_query{.select_s = false, .select_v1 = false, .select_v2 = false}
read_options{.replica_count = 2, .page_size = 1, .empty_replica_pages = false,
             .empty_replica_mutation_pages = false, .native_reverse_queries = false,
             .static_row_digest = false, .schedule_seed = 2125198832}
```

`SELECT pk, ck FROM ks.cf` with one row per page. The answer is the static-only
row of partition 2, row 3 of partition 4 and row 2 of partition 3. The client
receives only row 2 of partition 3.

Page 0's first range holds partitions 2 and 4. Replica 0 holds both; replica 1
holds only a range deletion of partition 2. Replica 0 fills its row limit of
one with partition 2's static-only row and stops at that partition's end,
without reaching partition 4. Replica 1 reaches the end of the range with no
row. Both replies are data replies, and the digests match, because without
`STATIC_ROW_DIGEST` the static-only row does not reach the digest. The
coordinator takes replica 1's page, which is the first to arrive, and the range
ends. Row 3 of partition 4, which only replica 0 holds, is never read.

This is the shape which `c78d37d317` fixed. That fix reads the stop from the
reply's cursor, and `decide_digest_page()` uses the cursor only when
`EMPTY_REPLICA_PAGES` is enabled: `last_position()` is a `[[version 5.1]]`
field of `idl/result.idl.hh`, and a cluster without the feature has a replica
which does not send it.

The options were to extend the harness guard, to choose the data reply which
covers least instead of the first to arrive, or to stop drawing the feature
disabled. The user chose the last, which loses the coverage of the pre-5.1
protocol.

The middle option is still available if that coverage is wanted back. When the
digests match, the replies agree about content and differ only in extent, so
the coordinator may keep the one which covers least, and it can tell that a
reply stopped even without a cursor: a page whose row count equals the
command's row limit is full, and a full page stopped. That needs no new field,
and it narrows the blind spot rather than closing it, because two full pages of
different extents stay indistinguishable without cursors.

### Fixed: a replica which stopped with a full page is invisible to the decision

Run 1031066, fixed by commit `c78d37d317`. This is the largest change of the
session, because it changes what `query::result::last_position()` means on a
data page.

Shrunk witness:

```
placed_history{
    {row_marker_write{3, 2, 8, lifetime::expiring}, 0b1},
    {static_cell_write{2, 1, 9, lifetime::permanent}, 0b1},
    {regular_cell_write{1, 5, regular_column::v2, 8, 7, lifetime::permanent}, 0b1},
    {regular_cell_write{2, 5, regular_column::v1, std::nullopt, 2, lifetime::permanent}, 0b10},
    {row_marker_write{4, 3, 4, lifetime::permanent}, 0b1},
}
select_query{.select_s = false, .select_v1 = false, .select_v2 = false}
read_options{.replica_count = 2, .page_size = 1, .static_row_digest = false,
             .querier_cache = true, .schedule_seed = 3230499980}
```

A digest match says that two replicas agree, but only over the part of the
range each of them examined. `earliest_stop()` decided that a digest reply had
stopped when the reply was short, or when its position preceded the data
reply's. A replica which stopped with a *full* page is neither: its page is not
short, and its position can lie after the data reply's.

Page 1's first range holds partitions 1, 2 and 4. The digest replica stops at
the end of partition 2, having filled a row limit of one with that partition's
static-only row. The data replica holds data only in partition 2, reaches the
end of the range with no live row, and its digest matches. The page ends the
range, and row 3 of partition 4 is lost.

The position itself is the signal, and `read_page()` of
[replica/multishard_query.cc](replica/multishard_query.cc) already used it that
way: it sets a cursor only where the page reaches a limit or stops short.
`read_data_page()` set one whenever the querier which read last had a position,
so a page which exhausted its range reported its last examined position, which
is not a stop. The fix aligns `read_data_page()`, and lets `earliest_stop()`
take the earliest position any reply reports, the data reply's own included.
`get_or_calculate_last_position()` covers the callers which need a cursor for a
page which reported none; that is what it is for.

Seven tests of `read_page_resolution_test` recorded the cursor of an exhausted
page. Their expectations are now no cursor. That also makes the page driver and
the multishard producer agree in every case of the comparison corpus, so
`producer_case` records one cursor per case instead of two.

### Fixed: a cached querier reused for a page which asks for no clustering row

Run 1015676, fixed by commit `4310838df4`.

Shrunk witness:

```
placed_history{
    {range_deletion{1, bound{2, true}, std::nullopt, 15}, 0b1},
    {row_marker_write{1, 3, 9, lifetime::expired}, 0b11},
    {row_marker_write{3, 5, 8, lifetime::expiring}, 0b11},
    {regular_cell_write{2, 2, regular_column::v2, 1, 16, lifetime::permanent}, 0b1},
    {regular_cell_write{1, 5, regular_column::v1, 4, 5, lifetime::permanent}, 0b11},
}
select_query{.select_s = false, .select_v2 = false}
read_options{.replica_count = 2, .extra_replicas = 1, .page_size = 3,
             .page_size_in_bytes = 25, .querier_cache = true, .schedule_seed = 3651344462}
```

`clustering_position_matches()` treated an empty set of clustering ranges for
the partition as proof that the querier matches, on the grounds that such a
page is the last of a query with clustering restrictions and expects nothing
more.

The fix of run 274 made that false. Since then a page continues a partition
with no clustering range, to read the partition's static-only row, which an
earlier page left undecided. The saved querier's reader carries the wider
ranges of the page which made it, and the page's slice does not filter what the
reader returns, so the page returns clustering rows it did not ask for.

Partition 1 holds a live row at 5 and, on one replica only, a range deletion of
[2, +inf) which covers it, so the answer has no row of partition 1. Page 1
reaches the end of the partition's rows without deciding its static-only row,
and page 2 continues the partition with no clustering range. The replica
without the deletion reuses its querier, returns row 5, and the page returns
it.

### Fixed: a re-emitted range tombstone change counted twice

Run 462012, fixed by commit `b9e82c0ae5`.

Shrunk witness:

```
placed_history{
    {range_deletion{2, bound{0, true}, bound{5, true}, 7}, 0b10},
    {partition_deletion{2, 17}, 0b101},
    {static_cell_write{1, 6, 16, lifetime::permanent}, 0b1},
    {regular_cell_write{2, 1, regular_column::v2, 9, 9, lifetime::permanent}, 0b10},
    {row_marker_write{2, 4, 18, lifetime::permanent}, 0b110},
    {regular_cell_write{3, 4, regular_column::v1, 5, 14, lifetime::expiring}, 0b1},
    {row_marker_write{2, 5, 11, lifetime::permanent}, 0b10},
}
select_query{.select_v2 = false, .filter = {predicate{column::v2, comparison::lt, 4}}}
read_options{.replica_count = 3, .page_size = 2, .tombstone_limit = 1,
             .querier_cache = true, .schedule_seed = 3864528926}
```

A page which continues a partition starts at the previous page's stop, and
`start_new_page()` re-emits the range tombstone which was active there, as a
change at that same position. `query_result_builder` already refused to stop on
such a change, but it still counted it against the tombstone limit, and the
previous page counted it too.

A page can therefore reach the limit having read nothing new.
`consume_end_of_partition()` stops the page on the limit, marks it short, and
the page's position is still the one it started from, because no fragment moved
it. The next page asks for the same position and stops at the same place, so
the paging state repeats.

The fix does not count the change in the page's first partition, which is the
one a page can continue. That is the rule the re-emitted partition tombstone
already followed, and it costs the same: a page whose first partition is a
fresh one loses one unit of the limit.

### Fixed: the stop can be anywhere in the page which lost an undecidable row

Run 342978, settled by commit `4aecc3a67f`, which widens the guard of
`10a5beec1e`.

Shrunk witness:

```
placed_history{
    {regular_cell_write{3, 2, regular_column::v1, 5, 3, lifetime::permanent}, 0b10},
    {static_cell_write{2, 3, 1, lifetime::permanent}, 0b10},
    {regular_cell_write{4, 1, regular_column::v2, 5, 2, lifetime::expiring}, 0b11},
    {range_deletion{2, bound{1, true}, bound{5, false}, 16}, 0b1},
}
select_query{.distinct = true, .select_s = false, .select_v1 = false, .select_v2 = false}
read_options{.replica_count = 2, .page_size = 2, .static_row_digest = false}
```

The first rule asked for an undecidable row between the last row the read
returned and the first one it did not, which is where the lost row sits only
when the page holds a single row. A page holds as many rows as its size, and
losing any one of them shortens the page. The lost row can therefore be
anywhere in the page, before the rows the page did return.

`SELECT DISTINCT pk FROM ks.cf` with two rows per page. The answer is
partitions 2, 4 and 3. Partition 2's row rests on a live static cell which only
replica 1 holds, so the coordinator accepts replica 0's page of partition 4
alone. One row does not fill the page, the query ends, and partition 3 is lost
as well.

The guard now allows the stop wherever the read's last page covers an
undecidable row of the answer. That span runs from just after the last row of
the page before it to the first row the read did not return. Only the last page
can be the short one, because the query ends with it.

### Fixed: a page which re-emits a static row exceeds its row limit

Run 290482, fixed by commit `de8c71184b`.

Shrunk witness:

```
placed_history{
    {regular_cell_write{4, 2, regular_column::v1, 1, 4, lifetime::permanent}, 0b1},
    {partition_deletion{3, 5}, 0b1},
    {row_deletion{2, 5, 7}, 0b1},
    {static_cell_write{2, 3, 2, lifetime::permanent}, 0b1},
}
select_query{.select_s = false}
read_options{.replica_count = 4, .extra_replicas = 3, .page_size = 1,
             .querier_cache = true, .schedule_seed = 2947302135}
```

`consume_page()` calls `start_new_page()`, which closes a partition the
previous page left open and re-emits its static row. That row can be the
partition's static-only row, which counts as one row of the page, and it can
fill the page's row limit. The page is done then, but `consume_page()` consumed
the reader all the same.

The compactor's own stop condition does not catch this. It compares the number
of rows it took from the partition with the limit which is left, and that limit
is already zero, so the comparison never holds, and subtracting the partition's
rows from it wraps around.

Partition 2 holds a live static cell and a dead row; partition 4 holds a live
row. With one row per page, page 0 stops inside partition 2, and page 1 asks
the replica to continue it. The replica re-emits the static row, counts the
static-only row against a row limit of one, and then returns the row of
partition 4 as well.

### Fixed: a cursor which leaves no clustering row of its partition

Run 255025, fixed by commit `2f2fd58ccb`.

Shrunk witness:

```
placed_history{
    {row_marker_write{1, 2, 13, lifetime::permanent}, 0b101},
    {range_deletion{1, bound{2, false}, bound{6, false}, 14}, 0b110},
    {range_deletion{2, bound{1, true}, bound{2, false}, 1}, 0b101},
    {regular_cell_write{1, 1, regular_column::v2, 1, 3, lifetime::expiring}, 0b1},
    {static_cell_write{1, 3, 2, lifetime::permanent}, 0b10},
}
select_query{.ck_end = bound{3, false}, .select_v1 = false, .select_v2 = false}
read_options{.replica_count = 3, .extra_replicas = 1, .page_size = 3,
             .page_size_in_bytes = 37, .querier_cache = true, .schedule_seed = 2743198667}
```

The fix of run 274 made the pager move on from a cursor after all clustering
rows. It did not cover a cursor which the query's own clustering ranges leave
nothing after, such as the end bound of the last range. For that cursor the
pager continued the partition with an empty set of ranges. A replica then
returns the partition's static row and nothing else, a mutation page which
holds no live row is short, and the reconciled result reports the same cursor,
so the paging state repeats.

The pager now trims the clustering ranges before the decision, and treats a
cursor which leaves none like a cursor after all clustering rows. A page which
continues a partition with no row left reads the rest of it, so it decides the
partition, which is what `continued_from_end_of_rows` now says; it reads a flag
which `fetch_page_result()` sets instead of re-deriving it from the previous
cursor.

`SELECT pk, ck, s FROM ks.cf WHERE ck < 3` over three replicas, with three rows
per page and a byte limit which one row fills. Page 2 ends at the end bound of
the clustering range, before ck 3. Page 3 asks for the rows of that partition
after ck 3, of which the query admits none, and reports the same paging state.

### Fixed: a replica which left the page's last partition at its per-partition limit

Run 204283, fixed by commit `7f79df5827`.

Shrunk witness:

```
placed_history{
    {range_deletion{2, bound{1, true}, bound{5, false}, 16}, 0b1},
    {regular_cell_write{2, 3, regular_column::v2, 2, 8, lifetime::permanent}, 0b10},
    {regular_cell_write{2, 5, regular_column::v1, 0, 7, lifetime::permanent}, 0b1},
    {range_deletion{2, bound{4, false}, bound{6, false}, 11}, 0b10},
}
select_query{.distinct = true, .select_s = false, .select_v1 = false, .select_v2 = false, .limit = 1}
read_options{.replica_count = 2, .page_size = 5, .static_row_digest = false}
```

A replica which stops inside a partition at the per-partition limit skips to
the next partition. It has not examined the rest of that partition, so it can
hold a newer tombstone for a row which another replica returned from there.

`got_incomplete_information()` caught this for every partition but the page's
last one, where it called `got_incomplete_information_across_partitions()`
instead. That function cannot serve the case: it compares the last position
each replica sent across the whole range, which lies in a later partition once
the replica has moved on, and it excuses a replica which reached the end of the
range altogether.

The fix checks the page's last partition on its own, for the replicas which
went past it. Only the per-partition limit can have stopped those inside it, so
a retry with a larger limit moves them. A replica which stopped in the
partition and went no further is left to the across-partitions comparison,
which can trim the page where short reads are allowed; raising the
per-partition limit would not move a replica which stopped on its memory limit.

`SELECT DISTINCT pk FROM ks.cf LIMIT 1` over two replicas. Both hold a row of
partition 2 which the other's range tombstone deletes, so the partition has no
live row, and only partition 4 belongs to the answer. DISTINCT gives each
replica a per-partition limit of one, so replica 1 stops after its own row of
partition 2 and goes on to partition 4, never sending the tombstone which
deletes replica 0's row.

### Fixed: a builder which starts from a slice drops the per-partition limit

Run 71682, fixed by commit `2815d890a5`. The defect predates the historical
series: `partition_slice_builder`'s constructor which takes an existing slice
never carried `partition_row_limit`.

Shrunk witness:

```
placed_history{
    {regular_cell_write{1, 5, regular_column::v2, 9, 17, lifetime::permanent}, 0b1},
    {regular_cell_write{1, 3, regular_column::v2, 9, 13, lifetime::permanent}, 0b1},
}
select_query{.partitions = std::vector<int32_t>{1}, .reversed = true, .per_partition_limit = 1}
read_options{.replica_count = 1, .page_size = 5, .native_reverse_queries = false,
             .schedule_seed = 695175667}
```

Both production callers rebuild a slice for a reversed read:
`legacy_reverse_slice_to_native_reverse_slice()` and `reverse_slice()`. The
first runs on the replica, in `query::reversed()`, when a reversed read arrives
in the legacy wire format. PER PARTITION LIMIT is dropped there, and the
replica returns every row of each partition.

`SELECT ... WHERE pk = 1 ORDER BY ck DESC PER PARTITION LIMIT 1` in the legacy
reversed format returns both rows.

### Fixed: defect 7 which truncates the query, with the feature disabled

Run 41999, settled by commit `10a5beec1e`, which
[the widened guard](#fixed-the-stop-can-be-anywhere-in-the-page-which-lost-an-undecidable-row)
later corrected. It is defect 7 again, with the static-row digest feature
disabled, which is the blind spot no coordinator decision can reach. What is
new is the consequence: the lost row also ends the query, so a live row of a
later partition is lost too.

Shrunk witness:

```
placed_history{
    {static_cell_write{2, 0, 3, lifetime::permanent}, 0b10},
    {row_marker_write{2, 4, 8, lifetime::expired}, 0b1},
    {row_marker_write{4, 1, 1, lifetime::permanent}, 0b10},
}
select_query{.select_s = false}
read_options{.replica_count = 2, .page_size = 1, .static_row_digest = false}
```

`SELECT pk, ck, v1, v2 FROM ks.cf` with one row per page. Replica 0 holds only
the expired row 4 of partition 2. Replica 1 holds the live static cell of
partition 2 and the live row 1 of partition 4. The answer is the static-only
row of partition 2 and row 1 of partition 4. The client receives nothing.

The trace of page 0, which is the only page:
1. Replica 0's data reply has no row and is not short, with its cursor at
   partition 2, row 4, which is where its own data ends.
2. Replica 1's digest reply stopped on the row limit, after all rows of
   partition 2, having produced the static-only row.
3. The query selects no static column and the feature is off, so the digest
   does not cover the static cell, and the digests match. The coordinator
   accepts replica 0's reply.
4. The page has no row and is not short, which is fewer rows than the limit, so
   the pager declares the query exhausted. Row 1 of partition 4, which no page
   ever reached, is lost with it.

With `static_row_digest` enabled the same case passes: the digests differ, the
reconciled page returns the static-only row, and page 1 returns row 1 of
partition 4. That is measured, not inferred.

The user chose to extend the guard, over the row-count option, because the
guard is needed either way: the count narrows the blind spot but does not close
it, so the cases the count cannot catch would still have to pass the guard, and
the guard's wording would be the same. The count remains available as extra
production hardening; see
[What a row count in the digest reply would and would not catch](#what-a-row-count-in-the-digest-reply-would-and-would-not-catch).

### What a row count in the digest reply would and would not catch

A page's row count is `max(live clustering rows, 1)` per retained partition,
which `mutation_querier::consume_end_of_stream()` in
[mutation/mutation_partition.cc](mutation/mutation_partition.cc) adds. The old
`xxHash` digest covers, per partition the reader entered, the partition key,
which `result::builder::add_partition()` in
[query/query-result-writer.hh](query/query-result-writer.hh) feeds before the
partition writer saves its rollback point, so `retract()` keeps it; the static
row's tombstone and cells, but only when the query selects a static column; and
the key, row tombstone and regular cells of every live clustering row. Dead
rows feed neither the digest nor the count.

One direction therefore holds. Matching digests pin down the partition keys the
replica entered and its live clustering rows, so the only freedom left in the
count is the `max(..., 1)` term of a partition with no live clustering row.
That term is one exactly for a retained static-only row. A count which differs
under matching digests is therefore a static-only row disagreement, and not
something else.

The converse does not hold, which is what matters. The count is a number, and
two replicas can disagree about static-only rows without disagreeing about how
many they have. Measured witness:

```
placed_history{
    {row_marker_write{1, 1, 1, lifetime::expired}, 0b11},
    {row_marker_write{2, 1, 2, lifetime::expired}, 0b11},
    {static_cell_write{1, 7, 3, lifetime::permanent}, 0b01},
    {static_cell_write{2, 8, 4, lifetime::permanent}, 0b10},
}
select_query{.select_s = false}
read_options{.replica_count = 2, .page_size = 2, .static_row_digest = false}
```

Both replicas enter both partitions, through the dead row which both hold.
Replica 0 holds the live static cell of partition 1 and replica 1 that of
partition 2, so each returns one static-only row, and each digest is the two
partition keys. The digests match, the counts are both one, and the coordinator
accepts the data replica's page: the client gets the static-only row of
partition 1 and never the one of partition 2. With `static_row_digest` enabled
the same case returns both rows.

So the count catches a difference in the *number* of static-only rows on a
page, while the static-row digest catches a difference in *which* partitions
have a live static row. The count would catch the shapes of runs 304, 952 and
41999, in which one replica has a static-only row the other does not, and it
would leave the shape above. It is a narrower check, not a replacement.

### Fixed: a cached querier reused for a page which starts after its position

Run 38672, fixed by commit `26ce55b3c3`.

Shrunk witness:

```
placed_history{
    {row_marker_write{2, 5, 8, lifetime::permanent}, 0b1},
    {regular_cell_write{2, 1, regular_column::v1, 6, 7, lifetime::permanent}, 0b1},
    {range_deletion{1, std::nullopt, bound{4, true}, 4}, 0b1},
    {range_deletion{2, bound{0, false}, bound{5, false}, 6}, 0b1},
}
select_query{.select_v2 = false}
read_options{.replica_count = 3, .extra_replicas = 2, .page_size = 1, .tombstone_limit = 1, .querier_cache = true, .schedule_seed = 4178336223}
```

`SELECT pk, ck, s, v1 FROM ks.cf` with one row per page, a tombstone limit of
one and the querier cache. Only replica 0 holds data: rows 1 and 5 of partition
2, a deletion of `(-inf, 4]` of partition 1, and a deletion of `(0, 5)` of
partition 2, which covers row 1 but not row 5. The answer is row 1 and row 5 of
partition 2. The client receives row 1, row 5 and row 5 again.

The trace:
1. Page 1 returns row 1 of partition 2, with the cursor at that row.
2. Page 2 asks for `(1, +inf)` of partition 2. Replica 0's data page stops on
   the tombstone limit at the end of the range deletion, whose exclusive end
   bound is the position before row 5, so its cursor is `ck 5 (weight -1)`, and
   it saves its data querier there. The reconciliation round returns row 5,
   from the mutation querier, which is at a different position, and the page
   returns it with the cursor at `ck 5 (weight 0)`.
3. Page 3 asks for `(5, +inf)` of partition 2. Replica 0 reuses the data
   querier which page 2 saved, at the position before row 5, and it returns row
   5 again.

Fix: `clustering_position_matches()` in
[replica/querier.cc](replica/querier.cc) compared the saved position's *key*
with the start bound of the page's first clustering range, and ignored the
bound weight, so a querier which stopped before row 5 passed the check for a
page which starts after row 5. The reader's own slice is the one it was made
with, so nothing filtered the row out. It now also requires the weight to be
`equal` or `after_all_prefixed`: the exclusive bound means the page resumes
after the key, so the querier has to have consumed the key. A rejected querier
makes a fresh reader for the page's range, which is correct.

Notes:
- The page's cursor is not at fault. Page 2's cursor is the cursor of the
  reconciled page, which legitimately went further than replica 0's data page.
  A replica's saved querier and the page's cursor can always disagree, and this
  check is the mechanism which has to notice.
- Reversed queriers are never cached: `insert_querier()` drops them, with a
  reference to #3159. The reversed branch of the check is therefore unreached,
  and the weight condition is stated in the position's own domain, where the
  same reasoning holds for either direction.
- The pager does express a weight of -1, as an *inclusive* start bound at the
  key; page 1 of the run-10533 case shows it. Such a page could reuse a querier
  which stopped before the key, and the check still rejects it because it
  demands an exclusive bound. That is a missed reuse, not a wrong answer, and
  it predates this fix.
- Run 310 was also a cached-querier defect, and it is not this one: there a
  stale cursor repeated the paging state. This failure repeats a row.

Verification: run 38672 passes and the campaign reaches run 41999. The 134
tests of `read_page_resolution_test`, `paged_read_test`, `querier_cache_test`,
`database_test`, `multishard_query_test` and `mutation_query_test` pass.
`ninja dev-build` succeeds. The 51 tests of
[test/cqlpy/test_paging.py](test/cqlpy/test_paging.py),
[test/cqlpy/test_static.py](test/cqlpy/test_static.py),
[test/cqlpy/test_tombstone_limit.py](test/cqlpy/test_tombstone_limit.py) and
[test/cqlpy/test_select_from_mutation_fragments.py](test/cqlpy/test_select_from_mutation_fragments.py)
pass. No cluster test was run.

### Fixed: a DISTINCT row which a replica invents from a static cell

Run 32081, settled by the harness guard of commit `770a9823c8`, which the user
approved. It changes no production code.

Shrunk witness:

```
placed_history{
    {static_cell_write{2, 3, 1, lifetime::permanent}, 0b1},
    {static_cell_write{2, std::nullopt, 4, lifetime::permanent}, 0b10},
}
select_query{.distinct = true, .select_s = false, .select_v1 = false, .select_v2 = false}
read_options{.replica_count = 2, .page_size = 3, .static_row_digest = false}
```

`SELECT DISTINCT pk FROM ks.cf`. Replica 0 holds a live static cell of
partition 2, at timestamp 1. Replica 1 holds a null static cell at timestamp 4,
which deletes it. The merged static row is dead, so the answer is empty. The
client received the row of partition 2.

The query selects no static column and the case disables `STATIC_ROW_DIGEST`,
so the digests cover only the partition keys. They match, and the coordinator
returns replica 0's page, which has the row. No fix can decide the row, which
is the class of runs 952, 1999 and 3725.

The guard of run 1999, `read_model::static_row_only_distinct_partitions()`,
covers one direction of this blind spot: a DISTINCT row which only a live
static cell establishes, which the read can lose. Run 32081 is the other
direction, a row which the read invents: a replica whose own view establishes
the row from a live static cell serves a page with it, although the complete
answer has no row of the partition.

Guard: `check()` collects the partitions of both directions. A replica's writes
are one view of the data, so the same function answers the question about it,
and `replica_history()` builds that view. Only partitions which the complete
answer has no row of join the set from the replica side: a partition which the
answer has a row of is decided, because every replica returns a row of it,
whether a live static cell or a live clustering row establishes that row.

Rejected alternative: dropping the live-static-cell test of
`static_row_only_distinct_partitions()`, so that every partition without a live
clustering row counts. That is what the earlier handoff recommended. It is
broader than the reachable case: it also skips partitions which no replica has
a static cell of, where nothing can go wrong.

The guard applies only where `undecidable_static_only_rows()` holds, which
needs the feature disabled and no static column selected. The same witness with
the feature enabled is checked in full and passes, because the digests then
cover the static row, the coordinator reconciles the page, and the merge finds
the static row dead.

Known gap, left open: an invented row also takes a slot under a `LIMIT`, and
`losable` counts only the undecidable rows of the complete answer, which an
invented row is not. Run 32081 has no limit. Run 3725 arose from run 1999 the
same way, so the campaign will show this if it matters.

After the guard, run 32081 passes and the loop reaches run 38672. The 31 tests
of `read_page_resolution_test` and the 8 fixed tests of `paged_read_test` pass.

### Fixed: an unpaged read which returns an undecided static-only row

Run 17998, fixed by commit `0037fdf1cf`. It matched no known defect.

Shrunk witness:

```
placed_history{
    {regular_cell_write{1, 3, regular_column::v2, 1, 3, lifetime::permanent}, 0b1},
    {static_cell_write{1, 3, 7, lifetime::expiring}, 0b10},
    {regular_cell_write{1, 4, regular_column::v1, 3, 9, lifetime::permanent}, 0b1},
    {range_deletion{1, bound{2, true}, std::nullopt, 6}, 0b10},
}
select_query{.select_s = false, .select_v1 = false, .limit = 1}
read_options{.replica_count = 2, .page_size = 100}
```

`SELECT pk, ck, v2 FROM ks.cf LIMIT 1`. Replica 0 holds rows 3 and 4; replica 1
holds a live static cell and a deletion of `[2, +inf)` at timestamp 6. The
deletion kills row 3, at timestamp 3, but not row 4, at timestamp 9. The answer
is row 4. The client received a static-only row.

`LIMIT 1` makes `may_need_paging()` return false, so the read is unpaged. Its
command carries neither `allow_short_read` nor `defer_undecided_static_only_row`
(`options=0x8103` without bit 8 and bit 15, and the trace prints "no short
reads").

The trace of the single page:
1. Replica 0's data page returns row 3, which is live on replica 0 alone.
   Replica 1's digest differs, so the coordinator reconciles.
2. Each replica's mutation round returns one row under the row limit of one.
   Replica 0 returns row 3 and stops there, so row 4 never reaches the
   coordinator. Replica 1 returns its static row and its deletion.
3. The merge has row 3 dead and the static row live, so the page returned the
   static-only row.

The static-only row is undecided: replica 0 stopped inside the partition,
before its end, so the partition may still hold a live clustering row -- and it
does. `drop_undecided_static_row()` is the guard for exactly this, and it
returns early because the command does not set
`defer_undecided_static_only_row`. An unpaged read has no next page to defer
the row to, so dropping it is not the fix; the read has to retry with a larger
limit.

`got_incomplete_information()` did not ask for that retry. It compares each
replica's stop with the position of the page's last row, and a static-only row
has no position of its own: `get_reconciled_last_position()` returns a position
before every clustering row of the partition, which lies before every replica
stop.

Fix: the resolver reports incomplete information where the page's last row is a
static-only row and a replica stopped inside its partition, before its end.
`has_undecided_static_only_row()` decides whether a reconciled partition
carries such a row, which is the same test as `drop_undecided_static_row()`
with the same DISTINCT exemption: a live static row establishes a DISTINCT
row whatever clustering rows follow. The retry doubles the row limit, the
second round reads two rows, and the page returns row 4.

The same rule applies to the per-partition limit. A replica can stop inside a
partition at that limit and read on past it, so its overall stop says nothing
about the partition. `got_incomplete_information_in_partition()` therefore
raises the per-partition limit of the retry where such a replica did not reach
the end of a partition whose page row is a static-only row. No failure has
asked for this part; it is the same rule, in the place which the deferring path
does not cover either, because `drop_undecided_static_row()` only looks at the
earliest stop's partition.

The flag is false where the page has no room for the partition's row, which is
where the row limit or the partition limit ended the page before it.

After the fix, run 17998 passes and the loop reaches run 24804. The 31 tests of
`read_page_resolution_test` and the 8 fixed tests of `paged_read_test` pass,
with no recorded behavior changed.

### Fixed: a single reply which stopped early skipped the resolution

Run 24804, fixed by commit `62523a18cf`. It matched no known defect.

Shrunk witness:

```
placed_history{
    {regular_cell_write{2, 2, regular_column::v1, 2, 4, lifetime::expired}, 0b1},
    {regular_cell_write{2, 4, regular_column::v1, 0, 6, lifetime::expiring}, 0b1},
    {static_cell_write{2, 9, 5, lifetime::permanent}, 0b1},
}
select_query{.partitions = std::vector<int32_t>{1, 2}, .select_s = false, .select_v1 = false, .select_v2 = false}
read_options{.replica_count = 2, .extra_replicas = 1, .page_size = 3, .page_size_in_bytes = 7, .schedule_seed = 408707876}
```

`SELECT pk, ck FROM ks.cf WHERE pk IN (1, 2)`, with a page of 3 rows and 7
bytes. Only replica 0 holds partition 2: a live static cell, a row 2 whose only
cell has expired, and a live row 4. The answer is row 4. The client received the
static-only row of partition 2 and then row 4.

The trace of page 0, range `pk 2`:
1. Replica 0's data page returns row 4 and is short on the size limit.
   Replica 1 has nothing, so the digests differ.
2. The harness reconciles with one target, which models a LOCAL consistency
   level dropping the replicas of other datacenters. Replica 0's mutation page
   stops on the size limit after row 2, so it returns the static row and the
   dead row 2, and row 4 does not reach the coordinator.
3. The page returned the static-only row of partition 2, with the cursor at
   row 2. Page 1 then returned row 4.

`resolve()` short-circuited a single reply: "if there is a result only from one
node there is nothing to reconcile". Nothing to merge is not nothing to do.
Conversion to a data result does not know where the replica stopped, so it
returns the static-only row of a partition whose rest the replica never
examined, and `drop_undecided_static_row()` never runs.

Fix: take the short-circuit only for a reply which reached the end of its
range, which leaves nothing undecided. A reply which stopped early goes through
the resolution, which drops the undecided static row, marks the page short and
bounds its cursor at the replica's stop. Page 0 then returns nothing with the
cursor at row 2 and the partition undecided, page 1 returns row 4, and page 2
returns nothing because the pager has fetched a row of the partition.

The resolution of a single reply computes no repair differences, because a
partition's only version equals the reconciled one. It costs an unfreeze, a
difference and a freeze more than the short-circuit, on a path which a digest
mismatch has already made expensive. It also counts the page's rows with
`converted_live_row_count()` instead of trusting the replica's count, which is
the correction of run 6630.

After the fix, run 24804 passes and the loop reaches run 25518. The 31 tests of
`read_page_resolution_test` and the 8 fixed tests of `paged_read_test` pass,
with no recorded behavior changed.

### Fixed: a cursor which reports the static row of a consumed partition

Run 25518, fixed by commit `3ad746dfe9`. It is defect 1 again, whose historical
fix `7cf6143064` is ported as `f36598b781`.

Shrunk witness:

```
placed_history{
    {static_cell_write{2, 5, 14, lifetime::permanent}, 0b1},
    {range_deletion{2, bound{2, true}, std::nullopt, 10}, 0b1},
    {static_cell_write{2, std::nullopt, 5, lifetime::permanent}, 0b10},
    {regular_cell_write{3, 1, regular_column::v1, 9, 18, lifetime::permanent}, 0b10},
}
select_query{.select_s = false}
read_options{.replica_count = 2, .page_size = 5, .tombstone_limit = 1}
```

`SELECT pk, ck, v1, v2 FROM ks.cf` with a tombstone limit of one. Partition 2
holds a live static cell and a deletion of `[2, +inf)` on replica 0, and a
static cell which a null overwrote on replica 1. Partition 3 holds a live row 1
on replica 1. The answer is the static-only row of partition 2 and row 1 of
partition 3. The client received only the row of partition 3.

The trace of page 0:
1. Replica 0 counts the range deletion against the tombstone limit and stops at
   the end of partition 2, with its cursor after all of the partition's
   clustering rows.
2. Replica 1 counts its dead static cell against the limit, and also stops at
   the end of partition 2. Its cursor is the partition's static row.
3. The digests match, so the page is accepted with the earliest cursor, which
   is replica 1's. The page has no row.
4. Page 1 read the range after partition 2, and returned row 1 of partition 3.
   The static-only row of partition 2 is lost.

The cursor comes from the compactor's position, which is the position of the
last fragment it consumed. Replica 1's partition 2 holds only a static row, so
the position stayed in the static-row region although the page consumed the
whole partition. The pager reads a cursor outside the clustering region as the
end of the partition -- the page returned the partition's static-only row, so
the next page moves past the partition. `7cf6143064` stopped a data page from
stopping on a dead static row for this reason, and let it stop at the end of the
partition instead, which reports the same position.

Fix: `consume_end_of_partition()` moves the position to the end of the
partition's rows when the consumer did not cut the partition and the partition
delivered no clustering fragment. That is where the page stands. Page 0's
cursor is then after all of partition 2's clustering rows, the pager records the
partition as undecided and continues it for its static-only row, and page 1
returns the row.

A partition which delivered a clustering fragment keeps the position of that
fragment, so no recorded cursor changes. The first version of the fix moved the
position for every fully consumed partition, which is just as accurate but
changed the cursor of 11 tests of `read_page_resolution_test`.

The two ends of the convention now agree for the new position:
`ring_position_matches()` in [replica/querier.cc](replica/querier.cc) expects an
inclusive start bound for a cursor in the clustering region, which is what the
pager builds for a partition it continues, and `clustering_position_matches()`
accepts the empty clustering ranges which that page carries. So a cached
querier of such a page is still reusable.

After the fix, run 25518 passes and the loop reaches run 32081. The 31 tests of
`read_page_resolution_test`, the 8 fixed tests of `paged_read_test` and the 173
tests of `mutation_test`, `database_test`, `querier_cache_test`,
`mutation_query_test` and `multishard_query_test` pass, with no recorded
behavior changed. `ninja dev-build` succeeds, and the cqlpy tests of the
tombstone limit, of paging, of static rows and of mutation fragments pass.

### Fixed: a consumer fed after it asked to stop

Shrunk witness of run 12373:

```
placed_history{
    {regular_cell_write{4, 2, regular_column::v2, 4, 14, lifetime::permanent}, 0b1},
    {row_deletion{2, 4, 21}, 0b10},
    {regular_cell_write{3, 2, regular_column::v1, 6, 19, lifetime::permanent}, 0b1},
    {range_deletion{4, bound{2, true}, bound{5, false}, 8}, 0b1},
    {regular_cell_write{4, 4, regular_column::v1, 3, 10, lifetime::permanent}, 0b10},
    {regular_cell_write{2, 4, regular_column::v2, 2, 17, lifetime::permanent}, 0b1},
}
select_query{.per_partition_limit = 3}
read_options{.replica_count = 2, .page_size = 1}
```

`SELECT pk, ck, s, v1, v2 FROM ks.cf PER PARTITION LIMIT 3`. The answer is rows
2 and 4 of partition 4, then row 2 of partition 3. The client does not receive
row 4 of partition 4.

The replicas differ, so the first page reconciles. Partition 4 of the reconciled
result holds a range deletion of [2, 5) at timestamp 8, and rows 2 and 4, which
are newer than it. `to_data_query_result()` converts under the page's limit of
one row per partition, so its compactor stops after row 2.

`frozen_mutation_consumer_adaptor` buffers a frozen mutation's range tombstones
in a `range_tombstone_change_generator`, and flushes the changes before each new
fragment. `on_end_of_partition()` flushed the rest unconditionally, so the
compactor received the close of the deletion, at the position before row 5,
after it had already asked to stop. The close moved the compactor's `_last_pos`,
which becomes the page's cursor, so the next page continued from row 5 and never
returned row 4.

`mutation::consume()` already skips the same flush when its consumer stops, and
a replica's own reads go through a `mutation_reader`, which stops where its
consumer stops. Only a read which reconciles was affected.

### Fixed: a range tombstone reopened after the static row

Shrunk witness of run 11440:

```
placed_history{
    {range_deletion{4, bound{2, true}, bound{2, true}, 18}, 0b1},
    {range_deletion{4, bound{0, true}, std::nullopt, 12}, 0b1},
    {regular_cell_write{4, 3, regular_column::v1, 6, 10, lifetime::expiring}, 0b10},
    {static_cell_write{4, 3, 11, lifetime::permanent}, 0b1},
}
select_query{.partitions = std::vector<int32_t>{4}, .select_s = false, .select_v1 = false}
read_options{.replica_count = 2, .page_size = 2, .page_size_in_bytes = 279, .querier_cache = true}
```

`SELECT pk, ck, v2 FROM ks.cf WHERE pk = 4`. The answer is the static-only row of
partition 4: the deletion of [0, +inf) at timestamp 12 deletes the only cell of
row 3, which replica 1 wrote at timestamp 10. The client receives row 3 instead.

`start_new_page()` re-emits the static row which the previous page retained, and
then reopens the range tombstone which was active at the page's stop.
`consume(static_row)` sets `_last_pos` to the static row, and the reopen took its
position from `_last_pos` afterwards, so the tombstone reopened after all
clustering rows and covered nothing. Replica 0's second mutation page therefore
carried the static row but no tombstone, and the merge saw row 3 as live.

The ordering predates the series which `a9c234ce01` reverted: `f4c5fca2231c` has
the same two statements in the same order. `ff088ca0b3` only widened the
condition under which the static row is re-emitted, which is what brought the
two into contact more often.

The case needs a replica which resumes a cached querier inside a partition, with
a range tombstone open and a static row retained. A page which starts in a later
partition, or one whose reader has nothing left, re-emits the static row at a
partition start or end, where no range tombstone is open.

### Fixed: a page which ends past a replica stop, estimated with an inflated row count

Shrunk witness of run 6630:

```
placed_history{
    {row_marker_write{4, 3, 8, lifetime::permanent}, 0b1},
    {regular_cell_write{3, 2, regular_column::v2, 4, 5, lifetime::permanent}, 0b10},
    {static_cell_write{1, 4, 4, lifetime::permanent}, 0b1},
    {row_deletion{2, 4, 7}, 0b10},
    {regular_cell_write{2, 4, regular_column::v2, 6, 2, lifetime::permanent}, 0b1},
}
select_query{.ck_end = bound{6, true}, .select_v1 = false, .limit = 1}
read_options{.replica_count = 2, .page_size = 0}
```

`SELECT pk, ck, s, v2 FROM ks.cf WHERE ck <= 6 LIMIT 1 ALLOW FILTERING`. The read is unpaged, so it allows no short reads. The ring order is partition 1, 2, 4, 3; the earlier handoff said 1, 4, 2, 3, which was wrong. The answer is row 3 of partition 4. The client receives row 2 of partition 3.

The trace of the single page:
1. Replica 0 holds partition 1's static cell, row 4 of partition 2, which is live on it alone, and row 3 of partition 4. With a row limit of 1 it stops at row 4 of partition 2, and never reaches partition 4.
2. Replica 1 holds the deletion of row 4 of partition 2, and row 2 of partition 3. It reads through partition 3.
3. The digests differ, so the coordinator reconciles. After the merge, row 4 of partition 2 is dead, so the only live row of the merge is row 2 of partition 3, which lies beyond replica 0's stop.
4. The accepted page returns that row, is not short, and its cursor is partition 3, row 2. `LIMIT 1` then ends the query.

`got_incomplete_information()` should have asked for another round, because replica 0 stopped before the row which the page returns. It estimates where the page ends by subtracting each partition's live row count from the row limit, and compares the replica stops against that estimate. Partition 1 comes first, and `mutation_partition::live_row_count()` returns 1 for it, because it counts a live static row as a row of a partition which has no live clustering row. Conversion does not return that row: the compactor emits a static-only row only for a query which does not restrict clustering keys, and this query restricts `ck <= 6`. So the estimate ended the page at partition 1, before its first clustering row, while conversion ran on to partition 3. Both replica stops are later than that estimate, so the resolver reported complete information.

A trace of the resolver on the witness confirms it:

```
last_reconciled_position = pk 1 / before all clustered rows
reconciled_last          = pk 3 / ck 2
replica 0 stop = pk 2 / ck 4, earlier than last_reconciled_position: false
replica 1 stop = pk 3 / ck 2, earlier than last_reconciled_position: false
```

The same witness without partition 1's static cell passes: the estimate then lands on row 2 of partition 3, replica 0's stop is earlier than it, and the retry round returns the right answer.

This is the class of run 952. The port of `70c0b43291` fixed the trimming path, which compares the stops against the end of the reconciled result. The check of a read which allows no short reads still compared them against the estimate.

Fix, at the user's request: the resolver counts the rows which conversion returns, in `converted_live_row_count()`. A reconciled partition holds the rows which the replicas returned, and their reads already restricted those to the query's clustering ranges. Its live rows are therefore the rows which conversion returns, except that a live static row counts only where conversion returns a static-only row, and that conversion returns at most `_effective_partition_row_limit` rows of a partition.

That limit is the cap of `776bbaf665`, which the user asked to fold in rather than port separately: one row for a DISTINCT query, whose slice does not limit the rows of a partition, and the per-partition limit otherwise. Like that commit, this one caps the count where reconciliation merges a partition, where trimming counts its rows again, and in the position which the estimate derives.

Rejected alternative: comparing the stops against the end of the reconciled result, as the trimming does. It is safe, but over-conservative. An unpaged read would then retry with doubled limits whenever a replica stopped before the end of the merge, even when the row limit would have ended the page earlier.

After the fix, run 6630 passes and the loop reaches run 9165. The 31 tests of `read_page_resolution_test` and the 8 fixed tests of `paged_read_test` pass, with no recorded behavior changed, and so do the 95 tests of `database_test`, `querier_cache_test`, `multishard_query_test` and `mutation_query_test`.

### Fixed without a historical fix: a retry which does not enlarge its limit

Shrunk witness of run 303:

```
placed_history{
    {regular_cell_write{1, 1, regular_column::v1, 9, 7, lifetime::permanent}, 0b100},
    {static_cell_write{4, 0, 5, lifetime::permanent}, 0b1},
    {regular_cell_write{1, 3, regular_column::v2, 4, 11, lifetime::permanent}, 0b10},
    {row_deletion{1, 1, 12}, 0b1},
}
select_query{.select_s = false, .select_v1 = false, .select_v2 = false, .limit = 1}
read_options{.replica_count = 3, .page_size = 0}
```

The answer is row 3 of partition 1. The query is unpaged, so it allows no short reads. The harness fails the read after 16 reconciliation rounds:
1. Each replica reads mutations with a row limit of 1. Replica 0 returns the dead row 1 of partition 1 and the static-only row of partition 4. Replica 1 returns row 3 of partition 1. Replica 2 returns row 1 of partition 1, which is live on replica 2 alone, and stops at its limit.
2. After the merge, row 1 is dead. Two live rows remain: row 3 of partition 1 and the static-only row of partition 4.
3. The page's last row is row 3. Replica 2 stopped before it. Short reads are not allowed, so `got_incomplete_information()` asks for another round.
4. The retry limit is t²/l + 1, with t = 1 requested row and l = 2 live rows. So it stays 1, and every round repeats the first one.

The estimate assumes that the merged result has at most t live rows. Here the static-only row of a later partition raises l above t.

Master (`5f352afcf4`) has the same retry calculation in `abstract_read_executor::reconcile()`, and the same stop comparison for a replica which stops on a row. It has no cap on rounds, so a real read would retry until its timeout. I derived this from the code. I did not run the case on the code before the ports. The historical series has no fix for it: `f7e42a7d68` avoids a larger retry only for replicas whose mutation has no rows.

Fix, at the user's request (commit `b87d5d98fa`): each retry round at least doubles the limits which it estimates. This applies to the row limit, the partition limit and the per-partition limit. Retries can therefore read up to twice as many rows as the estimate asks for. The fix changes one recorded behavior: `test_retry_without_live_rows_disallows_short_reads` now expects a retry row limit of 4 instead of 3. After the fix, run 303 passes, and `read_page_resolution_test` and `paged_read_test` pass.

### Fixed without a historical fix: a digest which misses a static-only row

This is defect 7. Shrunk witness of run 304:

```
placed_history{
    {static_cell_write{4, 0, 5, lifetime::permanent}, 0b1},
    {row_marker_write{4, 4, 3, lifetime::expired}, 0b1111},
}
select_query{.partitions = std::vector<int32_t>{2, 3, 4}, .select_s = false, .select_v1 = false, .select_v2 = false}
read_options{.replica_count = 4, .page_size = 3, .schedule_seed = 2345716740}
```

Only replica 0 holds the live static cell of partition 4. All replicas hold the dead row 4. Replicas 1 and 3 get data requests and return nothing. The query selects no static column, so replica 0's digest does not cover the static cell, and all digests match. The page has no row, but the answer is the static-only row of partition 4.

`f72c59d9f1` only documents this. The message of `c05317d472` says that a fix needs a digest which covers static-only rows, and so a new digest algorithm behind a cluster feature. The user asked for a real fix rather than a skip, and allowed new RPC fields, so I wrote that fix.

Fix (commit `6b8fd06244`):
- `query::digest_algorithm::xxHash_with_static_row` hashes like `xxHash`, and additionally feeds a discriminator and the static row's liveness for every partition, whether or not the query selects a static column. `mutation_querier::query_static_row()` does this outside its `static_columns.empty()` guard, which is what hid the row before. Replicas which disagree about a static-only row therefore get different digests, and the coordinator reconciles the page.
- The `STATIC_ROW_DIGEST` cluster feature gates it. A digest is only comparable with digests of the same algorithm, so `storage_proxy::digest_algorithm()` asks for the new one only when every node supports it. The algorithm already travels in the `read_data` and `read_digest` verbs, so no verb changed.
- The harness draws `static_row_digest` per case, prints it when disabled, and shrinks toward enabling it.

After the fix, run 304 passes, and the loop reaches run 310. `read_page_resolution_test`, `paged_read_test`, `mutation_test`, `mutation_query_test`, `database_test`, `querier_cache_test` and `multishard_query_test` pass. The other direction of the defect, a spurious static-only row, has the same cause and the same fix; the harness reached it too (see part 1, step 3).

Open point: the digest of a partition now depends on its static-row liveness for every query, so a cluster in which only some nodes have the feature must not compare the two algorithms. The feature gate ensures that. The cqlpy and cluster tests were not run.

### Fixed: an incomplete repeated-state key

Run 900 was a harness defect, not a production defect. Its witness:

```
placed_history{
    {static_cell_write{2, std::nullopt, 11, lifetime::permanent}, 0b10},
    {regular_cell_write{3, 5, regular_column::v1, 8, 1, lifetime::expiring}, 0b1},
}
select_query{.select_s = false, .select_v1 = false}
read_options{.replica_count = 2, .page_size = 2, .page_size_in_bytes = 4}
```

The answer is row 5 of partition 3. Pages 0 and 1 both return nothing with the cursor at partition 2, after all rows, and the harness reported a repeated paging state:
1. The digests differ, so the coordinator reconciles. Replica 0 returns partition 3 with row 5. Replica 1 returns partition 2, which holds only a static cell tombstone: no live static row, no clustering row and no range tombstone.
2. `get_replica_last_position()` gives the after-all-rows sentinel for a partition without rows and range tombstones, so replica 1's stop is partition 2, after all rows. It is the earliest stop.
3. Trimming keeps the stop partition and drops the partitions after it, so partition 3 and its live row are dropped. Partition 2 contributes nothing live, so the page has no rows.
4. The page's cursor is the stop. The trace prints it as `{position: clustered, ckp{}, 1}`; weight 1 is `after_all_prefixed`, which `bound_view::top()` builds, so this is the after-all-rows sentinel.

The two pages are not the same state. Page 0 leaves partition 2 undecided, so page 1 continues it for its static-only row, which is what `7a5e055ff4` asks for. Page 1 continued the partition from the end of its rows, so it decided the partition and cleared the flag. Page 2 would therefore have moved past partition 2 and returned row 5.

The flag is a real paging-state field. `idl/paging_state.idl.hh` carries it to the client, and `query_pagers.cc` decides `has_ck` from it for a cursor after all rows. The harness's `describe()` of a paging state omitted it, so the two states compared equal.

Fix: `describe()` now appends ", partition undecided" when the flag is set. After it, run 900 passes and the loop reaches run 952. The 31 tests of `read_page_resolution_test` and the 8 fixed tests of `paged_read_test` pass.

Lesson for the harness: the repeated-state key must cover every part of the paging state which decides the next page. It still omits the query id, which is intended, and the last replicas and the read-repair decision, which the harness does not vary.

### Fixed: defect 7 with the static-row digest feature disabled

Shrunk witness of run 952:

```
placed_history{
    {regular_cell_write{1, 5, regular_column::v2, std::nullopt, 8, lifetime::permanent}, 0b1},
    {regular_cell_write{2, 4, regular_column::v1, 0, 15, lifetime::permanent}, 0b1},
    {static_cell_write{1, 4, 5, lifetime::expiring}, 0b10},
}
select_query{.select_s = false, .select_v1 = false}
read_options{.replica_count = 2, .page_size = 1, .static_row_digest = false, .schedule_seed = 3637446295}
```

The answer is the static-only row of partition 1 and row 4 of partition 2. The client receives only row 4, so the rows are not a prefix of the answer. On the first range of page 0, replica 0 returns no row with its cursor at partition 1, row 5, and replica 1 returns the static-only row of partition 1. The digests match, so the coordinator accepts replica 0's cursor and the page never emits the static-only row.

This is defect 7, whose fix `6b8fd06244` is already ported. The fix gates the static-row digest behind the `STATIC_ROW_DIGEST` cluster feature, and this case draws the feature off. With it off, the digests cannot express the disagreement, so the coordinator has no way to detect it. The defect is unfixable in that configuration by design; that is why the fix needed a new digest algorithm and a feature.

The harness draws `static_row_digest` off with probability 1/4 (`paged_read_test.cc`), and the shrinker prefers to turn it back on, so a shrunk case keeps it off only when the failure needs it. The options were to stop drawing the feature off, losing the coverage of the old digest algorithm; to accept the answer of such a case as undefined for a static-only row which no selected column covers; or to skip only this defect's shape, which the change of plan argues against.

Taken: the second option, which the previous session recommended. `check()` now takes the `read_case`. When `undecidable_static_only_rows()` holds, which is a case that disables `static_row_digest` and selects no static column, the prefix and equality comparisons drop the static-only rows of both the expected and the actual answer. Every other row still has to match exactly, so the coverage of the old digest algorithm stays. `read_model::is_static_only_row()` decides which rows those are; a DISTINCT row also has no clustering key, but a live clustering row can establish it, so it is not one.

This states the real contract of the old algorithm. Without the feature, a digest covers a partition's static cells only when the query selects them, and covers the partition's key even for a partition which returns nothing, so the two replicas are indistinguishable to the coordinator. No coordinator decision can recover the row.

After it, the feature-off witness passes and the shrinker reaches the defect below at the same run number. The 8 fixed tests of `paged_read_test`, the 31 of `read_page_resolution_test` and the 12 of `read_model_test` pass.

### Fixed: a static-only row lost when the next page starts in a later partition

Shrunk witness of run 5178:

```
placed_history{
    {static_cell_write{1, 4, 2, lifetime::permanent}, 0b1},
    {regular_cell_write{4, 4, regular_column::v1, 1, 4, lifetime::permanent}, 0b1},
    {range_deletion{1, bound{2, false}, bound{4, true}, 6}, 0b1},
}
select_query{.select_s = false, .select_v1 = false, .select_v2 = false}
read_options{.replica_count = 2, .page_size = 1, .querier_cache = true}
```

`SELECT pk, ck FROM ks.cf`. Only replica 0 holds anything. The answer is the static-only row of partition 1 and row 4 of partition 4. The client receives only row 4 of partition 4. The feature `static_row_digest` is enabled and the query has no limit, so neither guard of the harness applies.

The trace:
1. Page 0 has a row limit of 1. Replica 0's data page returns one row with its cursor at partition 1, after row 4. The digests differ, so the coordinator reconciles, and the accepted page has 0 rows, is short, and its cursor is partition 1 after row 4, with the partition undecided. `drop_undecided_static_row()` did its job: the replica stopped inside the partition.
2. Page 1 continues partition 1 with `specific=[{pk 1 : (ck 4, +inf)}]`. Replica 0 reuses its cached querier, which has nothing left in partition 1 and returns row 4 of partition 4 instead. Partition 1's retained static row is never re-emitted.
3. Page 2 is empty and the query ends.

This is the defect of run 1012 again. The end-of-stream commit `d54f3f04ce` fixed it only for a reader with no fragment left. Here the reader has one, the partition start of partition 4, so `consume_page()` passed `reader_at_end = false`, `continues_current_partition` stayed false, and `reemit_static_row` never fired.

Fix: both cases mean that the page has left the partition which the compactor is positioned in behind. The compactor sits inside a partition, so a reused reader can only emit a partition-start fragment for a later partition, and `consume_page()` already reports a reader with nothing left as a partition start. So `continues_current_partition` now tests the region for `partition_start` instead of the `reader_at_end` flag, which made that parameter redundant and removed it again. The specific-range test is unchanged, and still distinguishes a partition which the page continues from one it has moved past (run 752).

The close at the end of `start_new_page()` now also runs when a fragment does follow. That is required, not incidental: the reader emits no partition-end fragment for the partition which the page reopened, so without the close the consumer would still have it open when partition 4's partition start arrives.

After the fix, run 5178 passes and the loop reaches run 6630. The 225 tests of `database_test`, `read_model_test`, `read_page_resolution_test`, `paged_read_test`, `mutation_test`, `querier_cache_test`, `mutation_query_test` and `multishard_query_test` pass, with no recorded behavior changed.

### Fixed: an undecidable row which frees a slot under a limit

Shrunk witness of run 3725:

```
placed_history{
    {range_deletion{1, bound{2, false}, std::nullopt, 3}, 0b10},
    {static_cell_write{1, 9, 10, lifetime::expiring}, 0b1},
    {static_cell_write{4, 0, 2, lifetime::permanent}, 0b10},
    {row_marker_write{3, 5, 8, lifetime::permanent}, 0b1},
}
select_query{.select_s = false, .limit = 2}
read_options{.replica_count = 2, .page_size = 2, .static_row_digest = false, .schedule_seed = 3538202229}
```

`SELECT pk, ck, v1, v2 FROM ks.cf LIMIT 2`. The answer is the static-only rows of partitions 1 and 4. The client receives the static-only row of partition 4 and row 5 of partition 3. On the first range, replica 0 returns partition 1's static-only row and replica 1 returns nothing; the digests match, so the coordinator accepts the empty result. That is the undecidable configuration of defect 7.

The row filter of the guard could not absorb it. It drops the static-only rows of both answers, which leaves an empty expected answer and row 5 of partition 3 on the actual side. The lost row freed a slot under `LIMIT 2`, so a row which the complete answer never reaches entered the page. A limit couples the undecidable rows to the decidable ones, so dropping rows from both sides cannot express it.

Fix, at the user's request and in the harness only: a case in the undecidable configuration whose query has a row limit or a partition limit is compared with the answer of the same query with those limits raised by the number of undecidable rows. That number is how many rows the read can lose, counted over the answer of the query without limits, so the raised answer covers everything such a read can return. An undecidable row is the only row of its partition, which is why a lost one frees a slot under a partition limit as well. `check()` takes the schema now, so it can evaluate that answer itself.

The comparison with the raised answer accepts any prefix of it, because the harness does not know how many rows the read actually lost. The other limits are unaffected: a per-partition limit cannot admit another row of the same partition, since an undecidable row is its partition's only one. A case which is not in this configuration, or whose query has neither limit, is still compared for equality with the complete answer.

Known weakening: a read which returns no rows at all now passes in this configuration. The stronger check, that the decidable rows of the original limit must all appear, holds only if such a read can lose undecidable rows but never invent them. A spurious static-only row also consumes a slot, and the same digest blindness allows it, so that check was not added.

After it, run 3725 passes and the loop reaches run 5178. The 13 tests of `read_model_test`, the 31 of `read_page_resolution_test` and the 8 fixed tests of `paged_read_test` pass.

### Fixed: a DISTINCT row which only a live static cell establishes

Run 1999 was defect 7 reaching a DISTINCT query. The case disables `static_row_digest` and selects no static column, and partition 1's DISTINCT row rests only on a live static cell of one replica, while the other replica has a range deletion and no live clustering row. With the old digest algorithm the two replicas are indistinguishable, so no coordinator decision can recover the row, exactly as in [Fixed: defect 7 with the static-row digest feature disabled](#fixed-defect-7-with-the-static-row-digest-feature-disabled).

The guard of `017d6a5fbb` did not cover it. `undecidable_static_only_rows()` holds for the case, but `read_model::is_static_only_row()` returns false for every DISTINCT row, because a live clustering row can establish one. Here no live clustering row does.

Fix, in the harness only: `read_model::static_row_only_distinct_partitions()` returns the partitions whose DISTINCT row only a live static cell establishes. It resolves the history and applies the same live-row test as `partition_answer()`, which is the `candidates.empty() && s` shape of its `q.distinct` branch. A DISTINCT query has no clustering restriction, so every row of a partition counts. `check()` exempts those rows beside the static-only rows when `undecidable_static_only_rows()` holds. A DISTINCT row which a live clustering row establishes is decidable, and the harness still demands it.

After it, run 1999 passes and the loop reaches run 3725. The 13 tests of `read_model_test`, the 31 of `read_page_resolution_test` and the 8 fixed tests of `paged_read_test` pass.

### Fixed: a short page whose cursor is behind its last row

Run 952 failed on a duplicate row, with the `static_row_digest` feature enabled. Shrunk witness:

```
placed_history{
    {regular_cell_write{2, 4, regular_column::v1, 0, 15, lifetime::permanent}, 0b1},
    {regular_cell_write{1, 3, regular_column::v1, 1, 4, lifetime::expired}, 0b10},
    {static_cell_write{1, 4, 5, lifetime::expiring}, 0b10},
}
select_query{.select_s = false, .select_v1 = false}
read_options{.replica_count = 2, .page_size = 1}
```

`SELECT pk, ck, v2 FROM ks.cf`. The answer is the static-only row of partition 1 and row 4 of partition 2. The client receives row 4 of partition 2, then the static-only row of partition 1, then row 4 of partition 2 again. So the rows are not a prefix of the answer, and they are not the answer.

The trace of page 0 shows the mechanism. Replica 0 holds row 4 of partition 2; replica 1 holds partition 1, whose only clustering row has an expired cell, and a live static cell. With a row limit of 1:
1. Replica 0 returns row 4 of partition 2, not short, cursor partition 2 row 4. Replica 1 returns a digest, not short, cursor partition 1 row 3.
2. The digests differ, so the coordinator reconciles. Both replicas return one partition and one row.
3. The accepted page has 1 row and is **short**, with the cursor at partition 1, row 3. The row it returns is row 4 of partition 2, which is *after* that cursor in ring order.
4. The paging state is partition 1, row 3, partition undecided. Page 1 resumes there and returns partition 1's static-only row. Page 2 resumes after partition 1 and returns row 4 of partition 2 a second time.

The page emitted a row from beyond its own cursor. The cursor is the earliest replica stop, which is correct as a bound on what the replicas examined, but conversion filled the page from the data replica past that bound instead of trimming the row away. A page must not return a row which its cursor does not cover, or the next page returns it again.

This is the shape which the design calls "conversion fills in P before the common replica stop in Q". `70c0b43291` fixes exactly it, and its port is the fix taken here.

The previous session's two guesses about the mechanism were both wrong, and the trace corrects them:
- The trimming does compare partition keys first. `reconciliation_position::less_compare` compares the decorated keys and only falls back to the position within a partition. The loop which drops partitions also compares keys.
- `70c0b43291` was not measured against this witness. The earlier session ported it, saw a byte-identical trace on the run-900 witness, and reverted it. Run 900 turned out to be a harness defect, so that measurement said nothing about this one.

The real gate is the *reference position* which the trimming compares the stop against, not the comparator. `got_incomplete_information()` walks the reconciled partitions in query order and subtracts each partition's live row count from the remaining row limit. It stops at the partition which reaches the limit and passes that partition's last position as `last_reconciled_position`. With a row limit of 1:
1. Partition 1 is first in query order. Its clustering row is dead, but its static row is live, and `mutation_partition::live_row_count()` returns 1 for a partition whose only live content is its static row. So `row_count < rows_left` is `1 < 1`, which is false.
2. The loop therefore takes its else branch on the very first partition and passes partition 1's own last position as `last_reconciled_position`.
3. `earliest_replica_stop` is partition 1, row 3, which is *not* earlier than that, so the trimming did not run and `_trimmed_to_replica_stop` stayed false. Partition 2 and its row survived.
4. `_earliest_replica_stop` is set regardless of trimming, so the tail of `resolve_mutation_page()` still lowered the cursor to it, because the converted cursor in partition 2 is later. The page was marked short with a row beyond its own cursor.

So `last_reconciled_position` is an estimate of where conversion will stop, derived from live row counts. When that estimate is earlier than the result's true end, the trimming compares against the wrong bound and leaves rows which the cursor does not cover. That is the disagreement which `70c0b43291` describes.

Fix (the port of `70c0b43291`): compare the stop against `get_reconciled_last_position(rp.front())`, the end of the reconciled result, instead of against `last_reconciled_position`. `rp` is in descending query order, so `rp.front()` is its last partition. Trimming then runs whenever a replica stopped before the end of what the merge produced, and the page can no longer return a row after its cursor. When the counts agree, conversion stops at or before the stop anyway, so the extra trimming removes only rows which the page does not return.

After the fix, run 952 passes and the loop reaches run 1012. The 31 tests of `read_page_resolution_test` and the 8 fixed tests of `paged_read_test` pass, with no recorded behavior changed.

### Fixed: a static-only row lost after a short page inside its partition

Run 1012 failed on a missing row. The rows are a prefix of the answer on every page, so only the whole-answer check failed. Shrunk witness:

```
placed_history{
    {regular_cell_write{4, 4, regular_column::v2, std::nullopt, 6, lifetime::permanent}, 0b1},
    {static_cell_write{4, 3, 4, lifetime::permanent}, 0b1},
}
select_query{.partitions = std::vector<int32_t>{4}, .select_v2 = false}
read_options{.replica_count = 2, .page_size = 5, .page_size_in_bytes = 3, .querier_cache = true}
```

`SELECT pk, ck, s, v1 FROM ks.cf WHERE pk = 4`. Only replica 0 holds partition 4, which has a live static cell and one clustering row whose only cell is `v2`, which the query does not select. The answer is the static-only row of partition 4. The client receives nothing.

The trace:
1. Page 0: replica 0's data page is short after row 4, cursor partition 4 row 4. Replica 1 returns an empty digest. The digests differ, so the coordinator reconciles. Replica 0's mutation page is short with 1 row; replica 1 returns nothing. The accepted page has 0 rows, is short, and its cursor is partition 4, row 4, with the partition undecided. The static row was dropped as undecided, which is `drop_undecided_static_row()` doing its job: the replica stopped inside the partition, so its static-only row is not decided yet.
2. Page 1 continues partition 4 after row 4. Replica 0 reuses its cached querier and returns 0 rows, not short, cursor partition 4 row 4. Replica 1 has no usable cached querier and returns an empty digest. The digests now match, so the coordinator accepts the empty page and the query ends as exhausted.

The static-only row is never returned. Page 0 correctly deferred it. Page 1 reached the end of the partition without a live clustering row, which is exactly the condition under which the row must be emitted, but replica 0's reused querier produced an empty page, which carries no static row to emit. The digests then match and the query ends.

The cause is in the replica, not in the coordinator. A trace of `consume_page()` on the witness shows page 1's read on the cached querier with the static row still held and `always_return_static_content` set, but with `peek()` returning no fragment: the reader is restricted to `(ck 4, +inf)` of partition 4 and has nothing left, so it is at the end of its stream. `consume_page()` mapped that to `partition_region::partition_start`, and `start_new_page()` re-emits a retained static row only for a `clustered` or `partition_end` region. So the row which `ff088ca0b3` retains for the end of the partition was retained and then dropped.

Fix: `consume_page()` now tells `start_new_page()` that the reader has no fragment left, and `start_new_page()` decides from the page's slice what that means. The page reaches the end of the partition which the compactor is positioned in exactly when the slice has a specific range for that partition's key, which the pager sets for a partition it continues. The region becomes `partition_end` then, which re-emits the static row and counts the partition, and the call closes the partition afterwards, because the reader emits no partition-end fragment to close it. Without such a range the page has moved past the partition, and the region stays at the partition start, which emits nothing and reopens nothing.

Both halves are needed, and each was wrong on its own in an earlier attempt:
- Re-emitting without closing leaves the validator with an open partition. `mutation_fragment_stream_validator::on_end_of_stream()` requires the last fragment to be a partition end, so the read aborts with "invalid end-of-stream, last partition was not closed".
- Re-emitting whenever the compactor sits in a partition returns a row which an earlier page already returned. Run 752 of seed 1 caught this: a single replica, one live static cell in partition 3, `LIMIT 2` and a page size of 1. Page 0 returns the static-only row; page 1 asks for the range after partition 3's key, so its reader is at the end of its stream while the compactor still sits in partition 3. The specific-range test distinguishes the two cases.

After the fix, seed 1 reaches run 1999. The 39 tests of `read_page_resolution_test` and `paged_read_test`, the 95 of `database_test`, `querier_cache_test`, `mutation_query_test` and `multishard_query_test`, and the 78 of `mutation_test` pass.

### Fixed: a stale cursor of a cached querier repeats the paging state

Fixed by the port of `3a76dd076f`. The loop then reached run 900, which was a harness defect; see above.

Shrunk witness of run 310:

```
placed_history{
    {row_deletion{2, 3, 4}, 0b101},
    {row_marker_write{2, 5, 3, lifetime::expired}, 0b1},
    {regular_cell_write{4, 2, regular_column::v1, 4, 12, lifetime::permanent}, 0b1},
    {regular_cell_write{1, 5, regular_column::v1, 4, 11, lifetime::permanent}, 0b1},
}
select_query{}
read_options{.replica_count = 3, .extra_replicas = 2, .page_size = 2, .tombstone_limit = 1, .querier_cache = true, .schedule_seed = 2389981646}
```

The answer is row 5 of partition 1 and row 2 of partition 4. The client receives only row 5, and the query never ends:
1. Page 0 ends its middle range short, with the cursor at partition 2, row 3.
2. Page 1 continues that range. Replica 0 reuses its cached querier, finds nothing new, and reports a later cursor, partition 2 row 5. Replica 2 reuses its cached querier and reports the previous page's cursor, partition 2 row 3. Its page is not short.
3. The digests match. The decision takes the earliest reported cursor, which is replica 2's stale one, so page 1 returns nothing with the cursor of page 0, and the paging state repeats.

The historical series fixes this in three commits. `cbd6afe6cb` is ported as `ff7e4b5f75`; it does not fix this witness, because replica 2's cursor is earlier than the data cursor, so the earliest-cursor rule still takes it. `3a76dd076f` adds the short-read flag to the digest reply and ignores the cursor of a replica whose page is not short, which is what this witness needs. `1d62bea32f` then computes the cursor from the replies which reached the consistency level.

Next: port `3a76dd076f`, then `1d62bea32f`, and rerun the loop. `3a76dd076f` extends the `read_digest` verb with a `query::short_read` field, changes `query_result_local_digest()` to return it, and stores it in `foreground_reply_collector`. The harness must pass each digest reply's short-read flag to `add_digest()`; it currently drops it and shows it only in the trace.

### Fixed without a historical fix: a reconciled cursor at the end of a partition repeats

Shrunk witness of run 274:

```
placed_history{
    {static_cell_write{1, 1, 9, lifetime::expiring}, 0b1},
    {range_deletion{4, bound{5, true}, std::nullopt, 8}, 0b10},
}
select_query{.select_s = false}
read_options{.replica_count = 2, .page_size = 2, .page_size_in_bytes = 2}
```

The answer is the static-only row of partition 1. The client receives it on page 0, but the query never ends:
1. The digests differ, so the coordinator reconciles. Replica 0 returns partition 1 with only its static row. Replica 1 returns partition 4 with only its range tombstone. Both mutation pages are short, because of the byte limit.
2. `get_replica_last_position()` takes the after-all-rows sentinel as the stop of a partition without rows and range tombstones. So replica 0's stop is partition 1, after all rows. It is the earliest stop.
3. The converted cursor lies in partition 4. The port of `ac6608e490` lowers the cursor to the stop, and marks the page short.
4. The cursor has a clustered position, so the pager continues partition 1 with empty clustering ranges. Replica 0 again returns partition 1 with its static row, and its page is again short. The page returns nothing, with the same cursor, and the paging state repeats.

Replica 0 completed partition 1 on each page. The cursor at its end should move the next page to the next partition, but the pager continues the partition.

The historical series does not fix this. At its last commit `f72c59d9f1`, the executor's cursor code and the pager are the same as now. Its resolver differs only by `776bbaf665` and `70c0b43291`. Neither changes the stop of such a partition, and `70c0b43291` would also trim the result at the same stop. The witness needs a mutation page which a single static row fills. A real replica's byte limit makes that rare.

Comparison with master. The code before the ports should read like `origin/master` (`5f352afcf4`, the base of this branch at the time). On it, the same witness fails earlier. The resolver takes the stop of a partition without rows as a position before all rows, and trims the static row away. Page 0 is then short, without a partition and without a cursor (defect 2). A real coordinator would fail `SCYLLA_ASSERT` in `calculate_last_position()`. The original case of run 274 fails the same way on page 2. So the repetition comes from the ports of `f7e42a7d68` and `ac6608e490` together. I checked this with the eight ports undone in the working tree and a temporary fixed test. The experiment is not committed.

Fix, at the user's request (commit `7a5e055ff4`):
- The pager moves to the next partition from a cursor after all clustering rows.
- It still continues the partition if its static-only row is undecided. A data page can stop on the tombstone limit at the end of a range tombstone which covers the rest of the partition, before it decides that row.
- A page which continued a partition from the end of its rows decides the partition. So the pager continues from such a cursor at most once.
- A DISTINCT query always moves on from such a cursor.

After the fix, run 274 passes, and `read_page_resolution_test`, `paged_read_test` and `cql_query_test` pass. Side effect: the querier cache expects an inclusive start after a page which stopped at a clustering position. When the pager moves on from a cursor after all rows, the cache now drops the cached reader. The result stays correct.

### Resolved: the fix of `87a340bd41` depended on other fixes

The user chose to port the whole chain; see the table. Shrunk witness of run 96:

```
placed_history{
    {row_deletion{2, 1, 11}, 0b1},
    {range_deletion{1, std::nullopt, bound{3, true}, 1}, 0b1},
    {regular_cell_write{2, 2, regular_column::v1, 8, 3, lifetime::permanent}, 0b10},
    {regular_cell_write{1, 2, regular_column::v2, 5, 10, lifetime::expiring}, 0b1},
    {static_cell_write{4, 3, 4, lifetime::permanent}, 0b1},
    {regular_cell_write{1, 1, regular_column::v2, std::nullopt, 17, lifetime::permanent}, 0b1},
}
select_query{.distinct = true, .select_v1 = false, .select_v2 = false}
read_options{.replica_count = 2, .page_size = 2, .page_size_in_bytes = 777}
```

Reconciliation trims page 0 at replica 0's stop, partition 2, row 1. The DISTINCT pager starts page 1 after partition 2, so the live row 2 of replica 1 is lost.

The code of `87a340bd41` extends state which earlier fixes of the series added:
- `ff088ca0b3`: the paging-state field `partition_undecided`, the slice option `defer_undecided_static_only_row` and the cluster feature `DEFERRED_STATIC_ONLY_ROWS`.
- `3504bb07fd`: `continues_partition` and `returned_from_cursor_partition` in the pager.
- `64e4ca281b`: the compactor uses the page's slice. `ff088ca0b3` relies on it when it re-emits the static row of a cached querier.

No failure of the test had asked for these fixes. After their ports, run 96 passes.

## Done

### Mutation-page resolution (commit `307b34bd6c`)

The coordinator's mutation-page resolution now lives in its own component.

- [service/read_page_resolution.hh](service/read_page_resolution.hh) declares `resolve_mutation_page(schema, original_cmd, cmd, replies)`. It returns one of two results:
  - `accepted_mutation_page`: the client-visible `query::result` and the repair differences. The differences use the table schema, also for reversed queries.
  - `mutation_page_retry`: the command for the next round, with enlarged limits.
- [service/read_page_resolution.cc](service/read_page_resolution.cc) contains the logic from two former places:
  - the algorithm of `data_read_resolver`: merging, repair differences, the incomplete-information checks, and trimming;
  - from `abstract_read_executor::reconcile()`: the acceptance check, the conversion with the original command's limits, the un-reversal of repair differences, and the retry-limit calculation.
- In [service/storage_proxy.cc](service/storage_proxy.cc), `data_read_resolver` now only collects replies, errors and timeouts. The executor still dispatches reads, traces, updates stats, sets the `empty_replica_mutation_pages` option on the command, and schedules repairs and waits for them.
- [test/boost/read_page_resolution_test.cc](test/boost/read_page_resolution_test.cc) has 10 characterization tests in `combined_tests`. Their replies come from the real `replica::querier` and `reconcilable_result_builder`. The tests record the current behavior; they do not claim that it is correct. They cover:
  - a single reply, and identical replies;
  - merging and repair differences;
  - a retry after live rows are lost in the merge, and a short page instead when short reads are allowed;
  - a retry which disallows short reads when no live row survives;
  - trimming at a replica that stopped on its size limit;
  - a retry which raises the per-partition limit;
  - original limits kept in a retry round;
  - a reversed query, whose repair differences use the table schema.

Verification:
- A mechanical diff of the moved resolver against its original shows only the intended edits.
- I derived each test's expected outcome by hand from the original code before running the tests. All 10 pass.
- `combined_tests` and `scylla` build.
- Cluster tests were not run. Reconciliation runs only with several replicas, and no boost test reaches it through `storage_proxy`.

Known differences and open points:
- Two log messages now use the new `read_page_resolution` logger instead of `mutation_data` and `storage_proxy`: the "reconciled:" trace and the key-conflict internal error.
- The "Read stage is done" trace event now comes after the conversion instead of before it.
- `mutations_per_partition_key_map` moved to the new header. `storage_proxy.hh` includes that header, so it now also includes `mutation_query.hh`.
- The new source is registered only in `configure.py`. The CMake build lacks `read_page_resolution.cc`.
- The collector is still named `data_read_resolver`, although it no longer resolves anything.
- The executor, not the component, sets `allow_mutation_read_page_without_live_row` on the command. Commit `3c3de0e706` moved that step into the component.

### Digest-page decision and foreground response collector (commit `d5e4347bad`)

The first round of a read, with data and digest requests, now uses the same component.

- [service/read_page_resolution.hh](service/read_page_resolution.hh) adds two parts:
  - `foreground_reply_collector` holds the successful data and digest replies, counts replies toward CL, and owns the CL promise (`has_cl()`). It contains the former `digests_match()`, `min_position()`, the successful-response part of `got_response()`, and the target count. The caller passes, for each reply, whether it counts toward CL. `fail()` replaces the failure part of `on_failure()`.
  - `decide_digest_page(schema, cl_result, replies, empty_replica_pages)` contains the decision from the `has_cl()` continuation. It returns `accepted_digest_page` or `digest_page_mismatch`. It adjusts the cursor with `min_position()` of the collector's current replies, so it sees replies that arrived after CL.
- In [service/storage_proxy.cc](service/storage_proxy.cc), `digest_read_resolver` keeps topology (`waiting_for()`), errors, the timeout, the disconnect callback and the completion promise. It exposes the collector through `replies()`. The executor still runs the continuation. It also keeps the background-check threshold (`_block_for < _targets.size()`) and the cross-DC target filter, because both depend on the target list.
- [test/boost/read_page_resolution_test.cc](test/boost/read_page_resolution_test.cc) has 9 new tests, 19 in total. Data replies come from the real querier and `query_result_builder`, finalized with the querier's position like `table::query()`. Digest replies use the projection of `query_result_local_digest()`, without the short-read flag. The tests cover:
  - a single data reply without a digest;
  - CL waiting for a data reply, and replies that do not count toward CL;
  - a digest mismatch;
  - cursor selection with and without `empty_replica_pages`;
  - the scheduling gap: the tests attach the decision in a continuation of `has_cl()`, and deliver a reply before it runs.

Recorded behavior which the later properties are expected to reject:
- A matching reply that arrives after CL, but before the continuation, moves the page's cursor. A replay without that reply keeps the data reply's cursor.
- A conflicting digest reply that stops short after CL gives a page with rows 1 and 2 and a cursor at row 1. The page is accepted; only a background check follows.
- A failure after CL, but before the continuation, drops the replies. The cursor is then not adjusted.

Verification:
- I derived each new test's expected outcome by hand from the original code before running the tests. All 19 pass.
- `combined_tests` and `scylla` build.

Known differences and open points:
- `waiting_for()` now runs for every reply, also after CL was reached. It has no side effects.
- The executor now reads the `empty_replica_pages` feature also when the digests do not match.
- `abstract_read_resolver::_targets_count` is unused by the digest resolver; the collector counts targets.
- The scheduling tests reproduce the executor's continuation in the test (`decide_at_cl()`). They do not prove that the executor uses the same boundary. A shared helper for the continuation would add a task hop before `got_cl()`, so I did not add one.

### Shared replica page driver (commit `2ae3148316`)

The table and the test replica now read pages through the same code.

- [replica/querier.hh](replica/querier.hh) declares two functions:
  - `read_data_page(source, schema, permit, cmd, opts, ranges, trace_state, accounter, gc_state, config, saved_querier)` contains the range loop, the cursor finalization with `querier::current_position()`, and the decision to keep or close the querier, from `table::query()`.
  - `read_mutation_page(...)` contains the same parts of `table::mutation_query()`, for a single range.
- In [replica/table.cc](replica/table.cc), both table functions keep the zero-limit check, the gate, the latency metrics, the accounter creation and the `replica_query_wait` injection. They forward a failure of the helper with `coroutine::exception()`, without rethrowing it. `database` still does admission and the querier cache lookup and insertion.
- The helpers require positive limits. The table returns an empty result before calling them otherwise.
- The digest reply projection of `query_result_local_digest()` is unchanged. The test still copies it.
- [test/boost/read_page_resolution_test.cc](test/boost/read_page_resolution_test.cc) now reads replies through the helpers. It has 4 new tests, 23 in total:
  - a data page over two ranges, which takes its cursor from the second range;
  - a data page whose last range has no data. It has rows, but no cursor;
  - a data page and a mutation page which keep their querier when they reach a limit or stop short, and close it when they exhaust the source.

Verification:
- A mechanical diff of the helpers against the original table functions shows only the intended edits.
- I derived each new test's expected outcome by hand from the original code before running the tests. All 23 pass.
- `querier_cache_test`, `database_test` and `multishard_query_test` pass. My first version used `co_return co_await` in the table functions. It rethrew failures and broke `replica_read_timeout_no_exception`, which counts exceptions. The final version forwards them instead.
- `combined_tests` and `scylla` build.

Known differences and open points:
- The table now reads the tombstone warning threshold and the tombstone GC state once per page, not once per new querier. The mutation path now computes the GC state also when it reuses a saved querier. Neither has an effect.
- The saved querier is now taken over after the `replica_query_wait` injection, not before it. This has no effect.
- The multishard query path in [replica/multishard_query.cc](replica/multishard_query.cc) and the page loop in [replica/mutation_dump.cc](replica/mutation_dump.cc) do not use the helpers. The multishard path finalizes its position separately.
- The tests call the helpers with a saved querier directly. They do not use the querier cache and its validation.

### Mutation path characterization (commit `3c3de0e706`)

The mutation path now has characterization tests for its cursor and for reversed queries.

- [service/read_page_resolution.hh](service/read_page_resolution.hh) adds `prepare_mutation_read(cmd, empty_replica_mutation_pages)`. It sets `allow_mutation_read_page_without_live_row` when the feature is enabled. `abstract_read_executor::reconcile()` calls it for each round. The option changes where replicas may stop a mutation page, so a harness must take it from production code.
- As before, the first round sets the option on the client's command `_cmd`, which is also the `original_cmd` of the resolution. The option affects only mutation reads, so the conversion does not depend on it.
- [test/boost/read_page_resolution_test.cc](test/boost/read_page_resolution_test.cc) has 6 new tests, 29 in total. Three existing tests now also check the cursor. The tests cover:
  - the cursor of an accepted page. It is the position of the last fragment which the conversion consumed: the page's last row when the conversion stops at the original row limit, a trailing dead row, or the dead row of a page without rows;
  - the trimming of a reversed page at a replica which stopped short;
  - a reply in the legacy reversed format. The test converts the command and the reply with the real `reversed()` functions, as `make_mutation_data_request()` and the replica's `handle_read()` do. The converted reply equals the native reply and resolves to the same page;
  - with the option, a replica page which stops after a dead row, and the empty short page which such replies give.

Recorded behavior which the later properties are expected to reject:
- In a reversed query, trimming at a short replica drops a row which that replica has passed, and keeps a row which it has not reached. The replies use the reversed schema, whose clustering order is already the query order. `get_last_row()`, `get_last_reconciled_row()`, `less_compare_clustering` and the trimming range reverse it once more. This is the extra reversal which the design mentions.
- An accepted page's cursor can be at a dead row after the page's last row. A page without rows which is not short still has a cursor.

Verification:
- I derived each new test's expected outcome by hand from the original code before running the tests. All 29 pass.
- `combined_tests` and `scylla` build.

Known differences and open points:
- The legacy-format test reproduces the conversion calls of `make_mutation_data_request()` and `handle_read()`. It does not prove that those functions make the same calls.
- `reversed()` looks schemas up in the schema registry. The test initializes the registry with a dummy context, like `schema_registry_test`.

### Multishard comparison corpus (commit `0cf8efdcba`)

The page driver and the multishard producer now have a comparison corpus.

- `test_page_driver_and_multishard_producer` in [test/boost/read_page_resolution_test.cc](test/boost/read_page_resolution_test.cc) writes two partitions to a table with vnodes and tombstone GC disabled. It reads the same commands through `read_data_page()` and `read_mutation_page()`, and through `query_data_on_all_shards()` and `query_mutations_on_all_shards()`.
- storage_proxy reads a local range scan with `query_data_on_all_shards()` when the command has the `range_scan_data_variant` option. It sets that option when the feature of the same name is enabled. Otherwise it reads mutations on all shards and converts them with `to_data_query_result()`, whose cursor is the last fragment which the conversion consumed.
- With tablets, both variants read each tablet through `replica::database`, which uses the page driver. Only the vnodes path finalizes its cursor separately.
- The corpus has six cases: an exhausted page, the row limit, the partition limit, a short read, the per-partition limit, and a page whose last range has no data.

Recorded behavior:
- Both producers return the same rows, short-read flags and mutation pages in all six cases.
- On an exhausted page, the multishard producer has no cursor. The page driver has the position of the last fragment which its querier consumed, unless its last range had no data.
- On a page which stops at a limit or stops short, both have the same cursor. With the partition limit, it is at a dead row after the page's last row.

Verification:
- I derived each case's expected outcome by hand from the original code before running the test. All 30 tests pass.

Known differences and open points:
- The corpus compares mutation pages only for single ranges, because the page driver reads mutation pages of a single range.
- The multishard producer uses a mutation-read memory accounter also for data pages. That accounter stops on shard memory pressure; the page driver's data accounter does not. The corpus does not create memory pressure.
- The corpus uses fresh readers. It does not cover the multishard producer's saved readers.

### Small-history model (commit `64d4c1be19`)

Stage 2 starts with the reference for the complete answer of a query.

- [test/lib/read_model.hh](test/lib/read_model.hh) declares the model in `tests::read_model`. Its table is `(pk int, ck int, s int static, v1 int, v2 int, PRIMARY KEY (pk, ck))`, without tombstone GC.
  - A `history` is a list of writes: static and regular cells, row markers, and row, range and partition deletions. Timestamps are distinct. A value has a `lifetime`: permanent, expired, or expiring after the query time.
  - A `select_query` lists partitions or scans the ring. It can restrict the clustering key, reverse the clustering order, select columns, use DISTINCT, filter with `predicate`s, and have a limit, a per-partition limit and a partition limit.
  - `evaluate()` returns the complete answer as `answer_row`s. Each row keeps its identity and the values of the selected columns. The identity is the partition key, plus the clustering key unless the row is static-only or DISTINCT.
  - `evaluate()` does not use mutations, the compactor, the result builders or the coordinator. It uses the schema only for ring order.
  - `to_mutations()` builds the mutations of a history, for test replicas and tables. `to_cql()` gives the CQL statement of a query. `describe()` and the formatter of `select_query` print them as C++ code, for replay.
- The header lists the model's rules. A live static row gives a static-only row only when the query has no clustering restriction. It does so also when the query does not select the static column: readers return the static row for any selection, and the compactor decides its liveness from all its cells.
- [test/boost/read_model_test.cc](test/boost/read_model_test.cc) has 12 tests in `combined_tests`:
  - 10 tests without a database. They check hand-derived answers for newest-write resolution, marker-less rows with complementary cell tombstones, row, range and partition deletions, expiry, static-only rows, DISTINCT, order and limits, and the key order of listed partitions. They also check invalid inputs and the CQL text.
  - 2 comparisons with unpaged CQL reads on one node: 7 fixed histories with 20 to 26 queries each, and 30 random histories with 20 random queries each. A comparison reports every difference, with the history and the query as C++ code and the CQL statement.

Recorded behavior:
- CQL sorts the listed partition keys with the key type's comparator and removes duplicates (`to_sorted_vector()` in [cql3/restrictions/statement_restrictions.cc](cql3/restrictions/statement_restrictions.cc)). A query of listed partitions returns them in that order, not in ring order. The first random comparison found this, and the model now follows CQL.
- The pager trims its list of ranges by position: it removes the ranges before the one which contains the last partition key. It does not compare ranges in ring order, so key-ordered ranges do not break it.
- CQL counts the per-partition limit after the filter. A corpus query whose filter rejects the first row of a partition shows this.

Verification:
- All 12 tests pass. Before I added the last corpus query, `--repeat 5` with a new random seed for each run passed 60 of 60.
- `combined_tests` builds.

Known differences and open points:
- The model has no collections, counters, shadowable tombstones or equal timestamps.
- CQL cannot express a reversed query of several partitions, because it orders their rows by clustering key. The model supports it for read commands. The CQL comparison covers neither that nor the partition limit.
- The model accepts DISTINCT only with the static column and a limit.
- The random generator is uniform. The correlated families of the design come with step 3 below.
- The new sources are registered only in `configure.py`.

### Simulated replicas and the coordinator loop (commit `916e357dc9`)

CQL queries of the model now run through the production pager, the extracted coordinator decisions and the replica page driver.

- [cql3/statements/select_statement.hh](cql3/statements/select_statement.hh) adds `execute_with_query_function()`. It runs the body of `select_statement::do_execute()`, but reads through a `service::pager::query_function` instead of `storage_proxy::query_result()`. The paged path passes the function to the pager as its `query_function_override`. The unpaged path calls it through a static `query_through()` helper. `do_execute()` passes an empty function, so `execute()` does not change. The method does not run the `do_execute()` of a derived class.
- [test/lib/paged_read.hh](test/lib/paged_read.hh) declares `tests::paged_read`:
  - A `placed_history` is a history whose writes carry a bit mask of the replicas which hold them. A `read_case` is a history, a `select_query` and `read_options`: the replica count, the CQL page size (0 is unpaged), a page size in bytes and a tombstone limit which replace the configured ones on each command, and the two cluster features.
  - `harness::run()` prepares the statement of `to_cql()` and executes it page by page. It passes the paging state serialized, like a client. It stops at a repeated paging state or after a bound on the number of pages.
  - The query function plays the coordinator. For each range it adds the data reply of replica 0 and the digest replies of the others, and decides with `foreground_reply_collector` and `decide_digest_page()`. On a mismatch it runs rounds of `prepare_mutation_read()` and `resolve_mutation_page()`, at most 16. It assembles singular ranges like `query_singular()`, and other ranges like `query_partition_key_range_concurrent()` without vnode splitting, with `query::result_merger`.
  - Each replica reads a mutation source over `to_mutations()` of its writes with `read_data_page()` and `read_mutation_page()`, fresh readers and no tombstone GC.
  - `check()` returns the violated properties: the prefix property after every page, reaching the end of the query, and equality with `evaluate()`. The harness reports a short result without a partition and without a cursor as a failed read, because the pager would fail an assertion on it (see defect 2 below).
  - `harness::shrink()` greedily removes writes, replica placements, options and query clauses while `violation_kind()` (the violations without their digits) stays the same.
  - Each page keeps a trace: the command, each reply's row count, short-read flag and cursor, the digest decision, each reconciliation round, and the paging state. A digest reply's short-read flag appears only in the trace.
- [test/boost/paged_read_test.cc](test/boost/paged_read_test.cc) has 4 tests in `combined_tests`:
  - one replica, two identical replicas, and one replica with a page size of 1 byte, on a fixed history with 9 queries and several page sizes. They pass.
  - `test_random_campaign`, with random placements on two replicas, random queries and random options. It runs only when `SCYLLA_PAGED_READ_CAMPAIGN` sets the number of histories, because the baseline fails many cases. It reports each failure with its shrunk case as C++ code.
- `random_history()` and `random_query()` moved from `read_model_test` to [test/lib/read_model.hh](test/lib/read_model.hh).

To run the campaign, which is now `test_general`:

```
SCYLLA_PAGED_READ_CAMPAIGN=30 build/dev/test/boost/combined_tests --run_test=paged_read_test/test_general -- -c1 -m1G --overprovisioned --unsafe-bypass-fsync 1 --kernel-page-cache 1 --collectd 0 --random-seed 1
```

Campaign results with seed 1: 49 of 300 runs failed. 37 were short results without a partition or a cursor, 9 were wrong answers, 2 violated the prefix property and gave a wrong answer, and 1 violated the prefix property and then failed a read. Before the harness checked for the short empty result, the campaign aborted `combined_tests` in `query::result_view::calculate_last_position()`.

Baseline defects found. Each witness is the shrunk case as printed by the campaign. The descriptions come from the traces; I did not verify the causes inside the resolver.

1. **A dead static row stops a data page, and the pager drops the rest of the partition** (historical `7cf6143064`). One replica, tombstone limit 1. The data page is short with its cursor at the static row. The pager treats a cursor outside the clustering region as the end of the partition and removes the partition's range. Row 4 is lost.
   ```
   {static_cell_write{1, 4, 4, lifetime::expired}, 0b1}, {regular_cell_write{1, 4, regular_column::v2, 2, 3, lifetime::permanent}, 0b1}
   select_query{.partitions = std::vector<int32_t>{1}, .select_v2 = false}, read_options{.replica_count = 1, .page_size = 3, .tombstone_limit = 1}
   ```
2. **A reconciled page is short, without a partition and without a cursor** (historical `f7e42a7d68`). Default budgets. Replica 0 returns a static-only row and reaches the row limit of 1. Replica 1 returns a dead row 1. The pager calls `get_or_calculate_last_position()` on the page, and `calculate_last_position()` fails `SCYLLA_ASSERT(!ps.empty())`. `SCYLLA_ASSERT` calls `__assert_fail()` unconditionally, so a coordinator in a release build would abort too. The answer is the static-only row of partition 1.
   ```
   {static_cell_write{1, 8, 5, lifetime::permanent}, 0b1}, {regular_cell_write{1, 1, regular_column::v1, std::nullopt, 3, lifetime::permanent}, 0b10}
   select_query{}, read_options{.replica_count = 2, .page_size = 1}
   ```
3. **A spurious static-only row** (historical `aed19a5f1c`, `c05317d472`). Default budgets. Replica 0 has a live static cell and a dead row 1; replica 1 has a live row 2. Both reach the row limit of 1. The first page returns a static-only row of partition 2, and the second page returns row 2. The answer is row 2 only.
   ```
   {static_cell_write{2, 9, 6, lifetime::expiring}, 0b1}, {regular_cell_write{2, 1, regular_column::v2, std::nullopt, 4, lifetime::permanent}, 0b1}, {regular_cell_write{2, 2, regular_column::v2, 3, 2, lifetime::permanent}, 0b10}
   select_query{.select_s = false, .select_v1 = false, .select_v2 = false}, read_options{.replica_count = 2, .page_size = 1}
   ```
4. **A reversed page returns a static-only row instead of a live row.** The history of defect 3, with `select_query{.partitions = std::vector<int32_t>{2}, .reversed = true}` and `read_options{.replica_count = 2, .page_size = 3, .page_size_in_bytes = 1}`. Replica 0 stops short at row 1, which it reads after row 2 in reversed order. The reconciled page drops row 2 and returns a static-only row. This matches the extra reversal of the mutation path characterization above.
5. **A range tombstone beyond a replica's stop moves the cursor past a live row** (historical `ac6608e490`). Default budgets. Replica 0 has only the range deletion `(3, 6]`. Replica 1 has rows 2 and 4 and stops at its row limit on row 2. The reconciled page returns row 2 with the cursor after 6. Row 4 is lost.
   ```
   {regular_cell_write{3, 2, regular_column::v1, 0, 4, lifetime::permanent}, 0b10}, {row_marker_write{3, 4, 9, lifetime::expiring}, 0b10}, {range_deletion{3, bound{3, false}, bound{6, true}, 5}, 0b1}
   select_query{.partitions = std::vector<int32_t>{3}, .select_s = false, .select_v1 = false}, read_options{.replica_count = 2, .page_size = 1}
   ```
6. **A DISTINCT partition whose page stops at a dead row is dropped** (historical `87a340bd41`). Replica 1's mutation page stops short at the dead row 3, before the live row 4. The paging state is at partition 4, row 3. A DISTINCT pager has no clustering keys, so it removes partition 4's range. The row of partition 4 is lost.
   ```
   {row_deletion{4, 3, 6}, 0b10}, {regular_cell_write{4, 4, regular_column::v2, 6, 1, lifetime::permanent}, 0b10}
   select_query{.partitions = std::vector<int32_t>{4, 2}, .distinct = true, .select_v1 = false, .select_v2 = false}, read_options{.replica_count = 2, .page_size = 2, .page_size_in_bytes = 1}
   ```
7. **Digests miss an unselected static-only row** (historical `f72c59d9f1`). Default budgets. Only replica 1 has the live static cell; replica 0 has a dead row 2. No static column is selected. The digests match, and the page has no row. The answer is the static-only row of partition 1.
   ```
   {static_cell_write{1, 9, 1, lifetime::permanent}, 0b10}, {row_deletion{1, 2, 4}, 0b1}
   select_query{.partitions = std::vector<int32_t>{1}, .select_s = false, .select_v1 = false, .select_v2 = false}, read_options{.replica_count = 2, .page_size = 1}
   ```

Verification:
- `combined_tests` builds. The 3 fixed tests of `paged_read_test` pass, and the campaign skips itself without the variable. The 12 tests of `read_model_test` pass after the move of the generators.
- The `scylla` binary was not built, and no other tests of `select_statement` were run.

Known differences and open points:
- The trace prints row counts of digest replies. Digest results do not count rows, so these are 0. The trace should omit them.
- The campaign always enables both features. I did not check which defects depend on `empty_replica_pages` or `empty_replica_mutation_pages`.
- The campaign does not log its random seed. Its reports print the shrunk cases as C++ code.
- The harness has CL=ALL only, a fixed delivery order, no querier cache and no legacy reversed format. Its replicas use the page driver also for range scans, as with tablets; it ignores the `range_scan_data_variant` option.
- The new sources are registered only in `configure.py`.

### Part 1 of the new plan: steps 1 to 6

Commits `31d346cc1e`, `0632e42cab`, `7d9e811fb5`, `32fb204049` and `6718d5672f` add these features to the harness. A read case describes each of them, the printed case replays them, and the shrinker tries to remove each one. The campaign draws all of them at random.

- **Features and budgets.**
  - The campaign draws `empty_replica_pages` and `empty_replica_mutation_pages`. A case which enables the second feature also enables the first one, as in a real cluster.
  - Like `storage_proxy::get_tombstone_limit()`, the harness limits tombstones only when `empty_replica_pages` is enabled.
  - Page sizes include 0, which makes a query unpaged. Byte budgets range from 1 byte to 8 KB, and tombstone limits from 1 to 8.
- **Replicas and the reply schedule.**
  - A case has 1 to 4 replicas. The last `extra_replicas` of them do not count toward CL, like remote replicas with a LOCAL consistency level. CL requires the replies of all other replicas.
  - A write on an extra replica must also be on another replica. So the complete answer does not depend on which replies the coordinator uses, and one oracle covers every schedule.
  - A `schedule_seed` chooses, in each read of a range: the replicas with a data request, the order of the replies, the replies which arrive between CL and the decision, the reconciliation targets, and the order of the mutation replies. Without a seed, the harness behaves as before.
- **Querier cache.** Each replica can keep a real `replica::querier_cache`. Reads use it like `replica::database::query()` and `query_mutations()`. Before each page, the schedule may evict one or all of a replica's cached queriers.
- **Repairs and result limits.**
  - The coordinator checks that no repair mutation adds data to the merged contents of the replicas. It compares after a compaction at the query time without tombstone GC, so an expired value and its tombstone compare equal.
  - It checks that the result of each range stays within the row and partition limits of its command.
  - A case can also apply the repairs to the replicas. It then cannot keep queriers, because a cached querier would keep reading the old contents.

Verification:
- The 6 fixed tests of `paged_read_test` pass. Two of them are new and use the querier cache.
- After each step, I read every shrunk failure which still needed the new feature. None came from a harness error.
- The new checks never failed in the 300 runs of seed 1. A temporary change which added a partition tombstone to every repair mutation made 10 of 30 runs fail the repair check.
- With all features, seed 1 fails 16 of 300 runs.

Baseline behavior which the new features reach. The descriptions come from the traces; I did not verify the causes inside production code.
- A late digest reply, after CL but before the decision, moves the page's cursor back. The next page repeats rows or the paging state (historical `1d62bea32f`). With the querier cache, a replica whose cached querier found nothing new still reports the previous page's cursor, and the query never ends.
- When the replica with a live but unselected static-only row gets the data request, the digests of the other replicas miss that the row is dead after merging. The page returns a spurious static-only row. This is the other direction of defect 7 (historical `f72c59d9f1`).
- A cached querier which continues past a live row into a dead row, in a partition with static content, returns an empty page which is not short. The pager ends the query, and later partitions are lost (historical `64e4ca281b`).
- With a tombstone limit of 1 or 2, a data page stops at a range tombstone at its own start, and the query repeats its paging state (historical `5cf935471f`).

Correction: the seastar test runner prints `random-seed=<seed>` at startup, and `tests::random` uses that seed. The earlier note that the campaign does not log its seed was wrong.

### Part 1 of the new plan: steps 7 and 8

Commits `c36572b0bc` and `68ff4cee6b` complete part 1.

- **Range splitting.**
  - For the whole query, the schedule chooses up to 4 vnode boundaries at which a scan splits: right after the keys of a partition, right before them, or at a random token.
  - The harness splits scans with the production `query_ranges_to_vnodes_generator` and a splitter which returns these tokens.
  - Like `query_partition_key_range_concurrent()`, it reads the ranges in rounds of growing concurrency, with the remaining limits. The schedule chooses whether a round merges its contiguous ranges, as with vnodes, or reads them apart, as with tablets.
  - The rounds and the merging are a few dozen lines copied from `storage_proxy`. I did not extract them, because the production code interleaves them with topology and replica selection.
- **Legacy reversed format.**
  - A case can disable `native_reverse_queries`. Then the coordinator sends reversed reads to the replicas other than its own in the legacy reversed format, and the replicas convert them back. A mutation reply makes the round trip too. The schedule chooses the replica which the coordinator runs on, if any.
  - The harness calls the production `reversed()` conversions at the points where `make_data_request()`, `make_digest_request()`, `make_mutation_data_request()` and `handle_read()` call them.
  - The plan asked to extract the conversions from `storage_proxy`. I did not: the logic around the calls is a few lines, and an extraction would change the RPC code of `storage_proxy` for little gain.
  - A cluster which enables `native_reverse_queries` also enables `empty_replica_mutation_pages`, which is older. The harness rejects the other combination.

Verification:
- 8 fixed tests pass. The two new ones read the fixed history from identical replicas. One uses 10 schedules and checks that some scans split, with and without merging. The other disables `native_reverse_queries` and checks that some reads used the legacy format.
- With seed 1, the campaign fails 13 of 300 runs. One shrunk failure needed the schedule. It is the digest blind spot of defect 7 again: two replicas get data requests, and the first data reply comes from the replica without the static-only row.

Open point: a shrunk case keeps its schedule seed. The choices of the seed depend on the number of replicas and on the reads, so removing a replica changes the whole schedule. The witness above therefore kept 4 replicas. An explicit schedule, or trying other seeds while shrinking, could make such cases smaller.

### Part 2, step 1: the general test

Commits `045329d4de`, `1f2b80aa7b` and `bda71f868d`.

- `test_random_campaign` is now `test_general`. It draws histories of up to 6, 12 or 24 writes, instead of always up to 12. It logs each case at debug level before running it.
- `SCYLLA_PAGED_READ_STOP_AT_FAILURE` stops the test after its first failure.
- The general test with seed 12 ran out of memory. `select_statement` pages an unpaged query with a filter internally, until the pager is exhausted. When the pager repeated its position, this loop never ended, and the coordinator's trace and the result grew without bound. The harness bounded only the pages of the client. It now applies the same bound to the reads of each page of the client. A page which exceeds it fails with "The statement read more than N pages internally".
- The shrunk case of that failure is the known `5cf935471f` behavior, seen through the internal loop.
- Before the ports, with 40 histories, seed 12 failed 24 of 400 runs. Seeds 11, 13 and 14 failed 13, 26 and 17 of 400 runs, measured before the memory fix.

## Gaps left by stage 1

- The tests pass saved queriers to the page driver directly. They do not use the querier cache and its validation.
- No test shows that the executor uses the extracted decisions as the tests do. The scheduling tests reproduce the executor's continuation (`decide_at_cl()`), and the legacy-format test reproduces the conversion calls.
- The page loop in [replica/mutation_dump.cc](replica/mutation_dump.cc) does not use the page driver.

## Change of plan

Decided on 2026-09-14, after the first campaign.

There will be no generator families, and no tests for specific defects. Building generators from the known defects would bias the tests toward those defects. The known defects are more useful at the end of the process, as a check of the harness design. The tests should be designed mostly without looking at the defects. If the finished tests miss a known defect, our methods are not good enough, and we should improve the methods rather than add a targeted generator.

The rule is not strict. Where it saves effort, the plan may still use what we know about the defects. For example, the design's list of harness features came from the historical series.

The work has two parts:
1. Complete the harness features which a general test needs.
2. Build a single general test which draws all of them at random, and which is hopefully expressive enough to find all known defects. The test should avoid splitting into families or other special cases, because such specialization partly defeats its purpose.

This overrides these parts of [paging-test-redesign.md](paging-test-redesign.md):
- the mandatory generator families;
- the coverage obligations of each family;
- the per-commit table, used as a list of generator targets;
- the tracking of each known failure as its own test.

## Plan

### Part 1: harness features

Each step also extends the printed case, its replay and the shrinker, so a failing case stays reproducible and small.

1. **Hygiene.** Done.
   - Log the random seed of a run. The test runner already does this.
   - Omit the row counts of digest replies from the trace.
   - Apply the tombstone limit only when `empty_replica_pages` is enabled, as `storage_proxy::get_tombstone_limit()` does. Choose both features at random.
2. **Replicas and reply delivery.** Done.
   - Use 1 to 4 replicas.
   - The replicas which count toward CL are a fixed set, and the decision waits for all of them, as with CL=ALL, or with a LOCAL consistency level over the local datacenter.
   - Extra replicas do not count toward CL, like remote replicas with a LOCAL consistency level. They hold only writes which a CL replica also holds. The merged contents of all replicas therefore do not depend on which replies the coordinator uses, and the complete answer stays valid for every delivery order.
   - Choose the data replica, and one or two data requests, at random.
   - Deliver the first-round replies in random order. An extra reply can arrive before CL, between CL and the continuation, or after the continuation.
   - Reconcile with all targets, or only with the targets which replied, as the speculating executor does. Pass the mutation replies in random order.
3. **Querier cache.** Done.
   - Give each replica a real `querier_cache`.
   - Look up and insert queriers as `database::query()` and `database::query_mutations()` do, with the command's `query_uuid` and `is_first_page`.
   - Evict queriers at random between pages.
4. **Budgets and page sizes.** Done. Draw byte budgets from a wide range, not only 1 byte. Add unpaged queries to the random mix.
5. **Repairs.** Done.
   - Check that no repair mutation adds anything to the merged contents of the replicas.
   - Optionally apply the repair mutations to the replicas between pages, with the querier cache disabled.
6. **Result limits.** Done. Check that every result the coordinator returns stays within the client command's row and partition limits.
7. **Range splitting.** Done, without the extraction; see above. Split a scan into sub-ranges at random token boundaries, as `query_ranges_to_vnodes_generator` does. Read the sub-ranges in rounds, as `query_partition_key_range_concurrent()` does. If the harness would otherwise copy much of that code, extract the shared part from `storage_proxy`, without changing its behavior.
8. **Legacy reversed format.** Done, without the extraction; see above. With `native_reverse_queries` disabled, remote replicas receive commands in the legacy reversed format. Extract the conversions from `storage_proxy` and from the replica's request handler, so the harness calls production code instead of copying it.

Deferred:
- A pager kept for the whole query. Only internal paging keeps one; CQL clients get a new pager for every page, which the harness already does.
- Compound clustering keys and equal timestamps in the model.
- Consistency levels whose participants change from page to page, such as QUORUM without all replicas. The complete answer is not defined for them.

### Part 2: the general test

1. Replace `test_random_campaign` with one random test which draws the history, the placement, the query and all the options of part 1. Done; see [Handoff](#handoff-2026-09-14).
2. Run a large campaign. Match its failures to the known defects: the seven above and the historical series of the design. Record the defects which the test missed, and revise the methods for each miss.

The baseline fails many cases, so the general test runs only when an environment variable enables it, until the defects are fixed.
