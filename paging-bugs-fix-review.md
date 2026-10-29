# Review of the paging-bugs-fix series

Series: `5aea8fb138..f8c7142dac` (21 commits). Each commit was reviewed
separately for reachable correctness bugs; every finding below was then
re-checked against the code. Nothing was built or run, so the scenarios are
traced, not reproduced.

Line numbers refer to the series tip (f8c7142dac) unless stated otherwise.

## Summary

| # | Commit | Problem | Status at tip |
|---|--------|---------|---------------|
| 1 | 5b38f9f835 | Old coordinators get duplicate DISTINCT rows from new replicas | present |
| 2 | 353aa5fb21 | Paged SERIAL reads lose the withheld static-only / DISTINCT row | present |
| 3 | 2b6ceb2883 | Timeout racing the CL continuation reads cleared replies (UB) | present |
| 4 | 0bf07a2581 | Synchronous `bad_alloc` in `consume_page` aborts the node | present |
| 5 | dcd827d345 | Matching empty digests lose rows at QUORUM | fixed by 2b6ceb2883 (bisectability only) |
| 6 | d9e539e1f7 | Compactor records skips for every query, unaccounted | present (memory, not results) |

Commits with no findings: bc4bf8457a, ac63aa6331, 1e59e81319, aff06e77be,
e20b80898d, 277c6b747b, ad11967f77, 8f3b5d4863, 901758fc5c, 74086dd159,
44cedd78d9, b74f029fdc, 4e328d343d, e92c6774e2, f8c7142dac.

## 1. Duplicate DISTINCT rows in a mixed-version cluster (5b38f9f835)

The commit introduces this; it is reachable only while old and new versions
run together (rolling upgrade).

- Before the series, `filtering_query_pager::maybe_adjust_per_partition_limit`
  set `partition_row_limit = page_size` for every query, DISTINCT included.
  That was harmless: the compactor hard-coded a limit of 1 for DISTINCT.
- The commit makes the replica honour a DISTINCT slice's per-partition limit
  (`query::effective_partition_row_limit`, `query/query-request.hh:310`, used
  by `mutation/mutation_compactor.hh:413`). It stops sending the limit only on
  new coordinators. Nothing gates the replica-side change.

| Coordinator | Replica | Result |
|---|---|---|
| old | old | limit sent, ignored: correct |
| new | new | limit not sent: correct |
| new | old | limit not sent: correct |
| old | new | limit sent and honoured: **duplicate rows** |

Scenario: `PRIMARY KEY ((p1, p2), c)`, several live rows per partition, an old
coordinator runs `SELECT DISTINCT p1, p2 FROM t WHERE p2 = 1 ALLOW FILTERING`.
The client gets each partition up to min(live rows, page size) times. When the
read contacts both old and new replicas, the digests never match and every
read reconciles.

### Fix

Honour a DISTINCT slice's limit only when the slice also has
`send_read_frontier`; otherwise return 1.

- Old coordinators cannot set that option. New coordinators set it only under
  READ_FRONTIERS, and it travels with each command, so there is no window in
  which nodes turn on the feature at different times.
- `frontier_reconciliation` is the only code that raises a DISTINCT limit
  (`read_page_resolution.cc:790`), and it already refuses a command without the
  option (line 778). The legacy retry path (line 676) never fires for DISTINCT.
- `may_need_paging` (`query_pagers.cc:528`) sees the command before `do_query`
  adds the option, so it gets 1, as before the series.
- A replica-side feature check would be worse. A new coordinator could raise the
  limit and send it to a replica that has not turned on the feature yet, and
  that replica would cap the read at 1 row per partition.
- The DISTINCT early return in the filtering pager can stay; correctness no
  longer depends on it.
- The commit message's "no current read changes" holds only with this check.
- Test: run a DISTINCT slice with `partition_row_limit = page_size` through the
  compactor, with and without the option.

## 2. Paged SERIAL reads lose the withheld row (353aa5fb21)

### Background: the SERIAL read path

- A SERIAL or LOCAL_SERIAL read is linearizable (LWT). `do_query` routes it to
  `do_query_with_paxos` (`storage_proxy.cc:6505`) before it reaches the code
  that sets `send_read_frontier`.
- `do_query_with_paxos` wraps the read in a `read_cas_request` whose `apply()`
  keeps the `query::result` and returns no mutation, then calls `cas()`.
- `cas()` loops:
  1. **PREPARE.** The replicas promise the ballot and also run the read locally
     (`paxos_state.cc:157`). The first live replica returns data, the others
     return digests. If a quorum promises and the digests match, that data is
     used ("prefetched values").
  2. **Fallback read.** Otherwise `cas()` reads at QUORUM or LOCAL_QUORUM through
     `query()` (`storage_proxy.cc:6758`). That goes through `do_query` with a
     non-serial CL, so it sets `send_read_frontier` and uses the frontier rules
     in `query_singular`.
  3. **Apply and propose.** It applies the result and commits an empty mutation
     (#6299). On contention, the loop starts over.
- `do_query_with_paxos` wraps the kept result in a new
  `coordinator_query_result`.

### Defect

- Take partition P with a live static cell and no live clustering rows. If a
  page stops inside P's clustering rows, the coordinator can't know yet whether
  P's static-only row should be returned.
- 353aa5fb21 makes the coordinator withhold that row
  (`drop_last_partition` / `convert_last == false`). It sets
  `coordinator_query_result::rows_decided_before_cursor = true`. The pager
  (`query_pagers.cc:471`) turns that into `_partition_row_pending`, so the next
  page sets `always_return_static_content` and returns the row if P turns out
  to have none.
- `cas()` keeps only `cqr.query_result` from the fallback read, so
  `do_query_with_paxos` returns `rows_decided_before_cursor = false`. The
  pager never records the row as pending:
  - Without DISTINCT, the next page resumes inside P without
    `always_return_static_content`, and the row is never emitted.
  - With DISTINCT, the pager moves past P, and P's row is lost even if P has
    live rows after the stop.
- Trigger: a paged SERIAL read where PREPARE yields no usable data (replicas
  disagree), and the page stops inside such a partition (tombstone limit,
  memory limit, or a reconciliation stop).
- Before 353aa5fb21, page 1 returned the row, which was correct in this case.

### Fix

- `cas()` reports the flag of the read whose `qr` it applied: `false` for
  prefetched PREPARE data, `cqr.rows_decided_before_cursor` for the fallback
  read. Take it from the same loop iteration as the `qr` that is applied.
- Pass it out in `cas_result` (in memory only); `do_query_with_paxos` sets it on
  the `coordinator_query_result`.
- Setting it whenever `send_read_frontier` is on would be wrong for PREPARE
  data.
- Test: the randomized harness does not go through paxos. Add a targeted case or
  a cluster test.

### Related, not verified end to end: `send_read_frontier` leaks into PREPARE

- `do_query` sets `send_read_frontier` on the command object it is given
  (`storage_proxy.cc:6522`). `cas()` passes its shared `cmd`, which the paxos
  handler also holds.
- So after one fallback read, a contention retry's PREPARE asks the replicas for
  frontiers, and this path does not expect frontier results:
  - A local replica's `query::result` holds a frontier, and the pager's
    `last_position()` raises "the result holds a frontier".
  - A remote replica's `wire_position` carries a frontier stop that is read as a
    legacy last position.
- Suggested fix: do the fallback read on a copy of `cmd`, so PREPARE stays
  legacy and `false` is right for its data.

## 3. Timeout racing the CL continuation (2b6ceb2883)

- When CL is reached, `got_response()` calls `_cl_promise.set_value()`. The
  `has_cl().then_wrapped(...)` continuation (`storage_proxy.cc:5605`) is only
  queued, and the resolver's timeout timer is still armed.
- If `on_timeout()` runs first, `fail_request()` → `_replies.fail()` clears
  `_digest_results` (`read_page_resolution.cc:707`).
- The continuation then calls `decide_digest_page()` → `frontiers_at_cl()`
  (`read_page_resolution.hh:221`). That indexes `_digest_results[i]` for every
  `i < _replies_at_cl` without a size check: use-after-free.
- If the destroyed optionals happen to read as disengaged, the fallback path
  returns a result that still holds a frontier, and the pager raises an internal
  error.
- Before the commit, every read of per-reply data was behind
  `response_count() > 1`, which is 0 after the clear.
- Status: plausible, a narrow race.

### Fix

Work out the CL frontiers when CL is reached, the way `digests_match` already
is.

- Add `bool all_have_frontiers` and `std::optional<full_position> min_stop` (a
  copy) to `digest_read_result` (`read_page_resolution.hh:50`). It is a
  coordinator-local struct, the value of `_cl_promise`, not an RPC type.
- Fill them in `got_response()` just before `set_value()`.
  `decide_digest_page()` reads them from `cl_result`.
- Remove `_replies_at_cl` and `frontiers_at_cl()`.
- Replies that arrive after CL can then no longer change the decision.
- The legacy `lower_to_min_position` path can stay; skipping it after the clear
  is the pre-series behaviour.
- Alternatives:
  - A size check would make a page that reached CL fail because of a timer that
    fired later.
  - Keeping the replies alive in `fail()` leaves a rule the code doesn't
    enforce.
- Test: reach CL, call `fail()`, run `decide_digest_page()` under ASan.

## 4. Synchronous `bad_alloc` in `consume_page` aborts the node (0bf07a2581)

- Before the commit, `replica::consume_page` did all its work inside
  `reader.peek().then(...)`, so exceptions became failed futures.
- Now `start_new_page()` (which copies a key for the open range tombstone) and
  the pushes of the partition start and static row back into the reader run
  synchronously, before any future exists (`replica/querier.hh:~48`). All of
  them allocate.
- In `read_data_page` (`replica/read_page.hh:107`) such a throw escapes
  `coroutine::as_future(...)`. The cached querier, which has been read, is then
  destroyed without `close()`, and `~mutation_reader` aborts.
- `read_mutation_page` is safe (its try/catch closes the querier). The
  multishard path uses a fresh compaction state.
- Trigger: allocation failure while resuming a cached querier that stopped
  inside a partition.

### Fix

- Wrap the body of `replica::consume_page` in
  `seastar::futurize_invoke([&] { ... })`. The function returns a future, so it
  should not throw.
- `querier::consume_page`'s `then_wrapped` and `read_data_page`'s
  `fut.failed()` branch then close the querier and do not save it.
- Comment that a failed page may leave the reader and compaction state partly
  applied. That is fine only because every caller closes and discards the
  querier on failure.
- Alternatives:
  - Fixing only the caller leaves a future-returning function that can throw.
  - Turning `consume_page` into a coroutine would risk the lifetime of the
    `Consumer&&` parameter.
- Test (optional): `memory::with_allocation_failures` over a cached querier
  resuming inside a partition with an open range tombstone.

## 5. Matching empty digests lose rows (dcd827d345, d9e539e1f7)

- With `xxHash_without_empty_partitions`, a replica that stops at the tombstone
  limit after a tombstone-only partition sends the same digest as a replica
  that has nothing there.
- At these commits, `decide_digest_page` accepts the empty data reply as
  complete and not short. The pager ends the query and misses live data after
  the tombstones, which breaks QUORUM.
- The new digest is already switched on by READ_FRONTIERS at dcd827d345.
- 2b6ceb2883 fixes it (`decide_digest_page_at_stop`). If bisectability matters,
  move the digest switch in `digest_algorithm()` into 2b6ceb2883.

## 6. Skips recorded for every query (d9e539e1f7)

- `_skips.emplace_back(...)` (`mutation/mutation_compactor.hh:638`) runs
  whether or not the slice has `send_read_frontier`, and the result memory
  accounter does not track it.
- An unpaged `SELECT DISTINCT` or PER PARTITION LIMIT scan over many partitions
  keeps a key and position per partition in one contiguous vector, on top of
  the result. This causes large allocations and memory pressure, not wrong
  results.
- Fix: record skips only when the option is set.

## Other notes

- `query_result_visitor::accept_partition_end` (`query_pagers.cc:405-409`) can
  wrap `last_partition_row_count` to UINT64_MAX when the filter drops a
  static-only row. This predates the series and was not shown to be reachable.
- The randomized harness serves scans through the single-shard page driver
  (`read_data_page` / `read_mutation_page`), not `replica/multishard_query.cc`.
  This is a coverage gap, not a bug.
