# Bugfixes 2026-10-02: upload concurrency

- Branch: `bugfix-upload-concurrency-dbfef239a15f`
- Base: `origin/develop` at `ce92caecdc7a` (#194 merged; rebased
  2026-10-07)
- Sequenced after PR #195 (set-counts) for review and merge; no code
  dependency.
- Queue owner: `bugfix-set-counts-abca77d4de3c`,
  `.docs/bugfixes/261002-set-counts-abca77d4de3c.md`
- Origin: `deps`, `.docs/bugfixes/260930-deps-update-702d9698d638.md`

Each item is independent of the `deps` work: no `make test`, `make race` or
`make lint` run fails because of it. Found while fixing `deps` items.

- [x] Possible: requests from different sets for the same local file map to
  the same remote data object and can be handed to different put clients at
  once, so two clients could upload the same iRODS object concurrently (the
  race the `deps` hardlink fix closed for shared inode files). Unconfirmed:
  no failure has been traced to it.
  - Origin item: "Requests from different sets for the same local file map
    to the same remote data object".
  - Evidence: applying the hardlink reservation claim to every request's
    RemoteDataPath broke existing server tests that reserve such requests for
    separate clients concurrently. The one gate failure first attributed to
    it was the shared-collection wipe (fixed in e4c7b83).
  - Decision (user, 2026-10-05): try to reproduce it; fix only if it
    reproduces, otherwise record what was tried and leave it unfixed.
  - Reproduced (2026-10-08): a probe with two baton Putters putting the same
    file for setA and setB at once on real iRODS failed 10 of 10 times: one
    put got `-809000 CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME` and
    `ibackup:sets` held only the winner, so removing that set could delete an
    object the other set still backs up.
  - Red: `CGO_ENABLED=1 go test -tags netgo --count 1 -timeout 20m ./server
    -run '^TestServer$'`, new Convey "separate clients are not given both
    sets' requests for its remote object at the same time": `Expected: 1
    Actual: 2`.
  - Fixed in `server/claims.go`: `remoteClaims` claims every request's
    `RemoteDataPath()`, not only hardlinks; requests in one client batch
    still share a claim (sibling Convey). Existing tests whose fake client
    left other sets' requests for the same object reserved now finish or
    give them back (that was the race itself). Mutants (hardlink-only
    claims, claim on `Remote`, no in-batch sharing) fail. Gates pass;
    `make speed` Upload +1.3%, Remove +2.0% vs ce92cae.
  - Behaviour change: a shared object's requests wait for the client holding
    it (as hardlinks already did); a dead client delays them until TTR.
- [x] Possible: `baton/baton.go` `EnsureCollection` sends every concurrent
  caller's result back on one shared channel, so a caller may receive
  another collection's error. Spotted by reading the code; no failure seen.
  Not yet confirmed.
  - Origin item: "Possible: `baton/baton.go` `EnsureCollection`".
  - Decision (user, 2026-10-05): try to reproduce it; fix only if it
    reproduces, otherwise record what was tried and leave it unfixed.
  - Reproduced (2026-10-07): a per-caller version of the Cleanup-during-
    EnsureCollection test failed once (`Expected: 0 Actual: 1`): a caller got
    a nil result although its collection did not exist.
  - Red: `CGO_ENABLED=1 go test -tags netgo --count 1 -timeout 10m ./baton
    -run TestBatonConcurrentClientInit`, new Convey "Concurrent
    EnsureCollection callers each get their own collection's result": a
    failing caller A returned first with B's nil (`Expected: false Actual:
    true`); the Cleanup Convey tightened to per-caller checks also failed
    (`Expected: 0 Actual: 1`).
  - Fixed in `baton/baton.go`: each call sends a `collRequest` with its own
    buffer-1 reply channel; `collErrCh` is removed; cancel semantics are
    unchanged. Mutants (b9317e5 code, one shared reply channel, result wait
    ignoring done) fail. Gates pass; `make speed` Upload -2.5%, Remove +0.3%
    vs ce92cae.
- [x] `baton/baton.go` `GetMeta` is the only remote operation without
  `timeoutOp`, so any hang in it blocks its caller forever. One such hang:
  extendo (github.com/mjkw31/extendo/v2 v2.7.1-beta2, client.go
  execute/send) accepts a request after `Stop()` has cancelled its writer
  goroutine (`isRunning` stays true until the process exits) and blocks on
  an unbuffered send (client.go:848). A scratch probe calling `GetMeta`
  0-2ms after a concurrent `Cleanup()` on real iRODS hung 29 of 30 times.
  The extendo part is upstream.
  - Note (2026-10-07): go.mod now uses `github.com/wtsi-npg/extendo/v3
    v3.2.0`; re-check the hang against v3 before fixing.
  - Origin item: "TestTrashRemove intermittently fails in clean `make test`"
    on `deps`, whose fix stops the server triggering this hang by not
    overlapping removals with handler `Cleanup()`.
  - Re-checked (2026-10-08) against extendo v3.2.0: same hazard (`send`
    blocks on `client.in` after `Stop()`, client.go:848); a probe hung 25 of
    30 times. Upstream bug in wtsi-npg/extendo.
  - Red: `CGO_ENABLED=1 go test -tags netgo --count 1 -timeout 10m ./baton
    -run TestBatonConcurrentClientInit`, new Convey "GetMeta racing a
    concurrent Cleanup returns instead of blocking forever": `So(stuck,
    ShouldBeFalse)` got `true`.
  - Fixed in `baton/baton.go`: `GetMeta` runs `ListItem` in `timeoutOp` and
    returns nil on error (the abandoned op may still write its result). For
    test speed, `timeoutOp` is a method using a `Baton.opTimeout` field
    (set to `operationTimeout` in `GetBatonHandler`); the test uses 2s, so
    it adds ~8s, not ~65s. The goroutine blocked in extendo still leaks
    until the upstream fix. Gates pass; `make speed` Upload +1.2%, Remove
    -1.1% vs ce92cae.
- [x] `baton/baton.go` `Cleanup` (~line 725) reads `b.metaClient` and the
  other clients without holding `clientMu`, so it races with
  `setClientIfNotExists` (~line 368). The `deps` fix 1fa0608 stops the
  server reaching this, but the handler itself is still unsafe.
  - Origin item: "TestTrashRemove intermittently fails in clean `make test`"
    on `deps` (found reviewing its fix).
  - Red: `CGO_ENABLED=1 go test -tags netgo --count 1 -race -timeout 10m
    ./baton -run TestBatonConcurrentClientInit` (new Convey: concurrent `Stat`
    and `Cleanup` on a fresh handler): `WARNING: DATA RACE`, write in
    `setClientIfNotExists` (via `Stat`), read in `Cleanup`.
  - Fixed in `baton/baton.go`: the put, meta and remove clients are
    `atomic.Pointer[ex.Client]`; `clientMu` still serialises creation;
    `Cleanup` stops a snapshot (`collClients` cloned under `collMu`) outside
    any lock, never waiting on `clientMu`. A client created during Cleanup
    stays usable and the next Cleanup stops it. Taking `clientMu` in Cleanup
    was rejected: it stops a client just before its caller uses it (hang risk
    with GetMeta). Mutants (original code, meta client left out of the
    snapshot) fail. `make speed` passed (Upload +3.7%, Remove +3.4% vs
    ce92cae).
- [x] `server/server.go` (~lines 437-441): when
  `convertQueueItemToRemoveRequest` fails, the reserved item stays in
  `removeQueue`, so `Items` never reaches 0 and the storage handler's
  `Cleanup()` never runs again. Unchanged by the `deps` fix.
  - Origin item: as above.
  - Red: `CGO_ENABLED=1 go test -tags netgo --count 1 -timeout 20m ./server
    -run '^TestServer$'`: a new Convey queueing an unconvertible item ahead of
    a real removal in the set's reserve group failed with `not all removals
    finished: removed 0; 2 queued; 0 cleanups`.
  - Fixed in `server/server.go`: `reserveRemoveRequest` loops; an item that
    fails conversion goes to `dropUnremovableItem`, which records the error on
    the set (`recordSetError`) and removes it from the queue, then the next
    item is reserved. Mutants (drop but stop the loop, keep the item, no set
    error) fail. `make speed` passed (Upload +0.3%, Remove -1.3% vs ce92cae).
- [x] `transfer/put.go` `getSortedRequestCollections` (~272) only creates
  collections for `p.requests`, not `p.duplicateRequests`, so a request
  deduplicated by `RemoteDataPath()` (e.g. a second set's hardlink to the same
  remote inode file in the same batch) never gets its own `Remote` collection
  created, and its put fails with `open .../remote2/file.link2: no such file
  or directory`. Seen in a server test on the set-counts branch; same on
  develop.
  - Origin: found reviewing the set-counts self-hardlink fix (2026-10-07).
  - Red: `CGO_ENABLED=1 go test -tags netgo --count 1 -timeout 10m ./transfer
    -run TestPutMock` with the new "Hardlinks to the same inode with Remotes in
    different collections all upload" Convey failed: `Expected: 0 Actual: 1`,
    `.../dir2/file.link2 failed: open .../remote2/file.link2: no such file or
    directory`.
  - Fixed in `transfer/put.go`: `getSortedRequestCollections` walks
    `slices.Concat(p.requests, p.duplicateRequests)`, so `CreateCollections`
    (used by the server and `ibackup put`) also creates duplicates'
    collections. Mutants (only RemoteDataPath collections, last duplicate
    left out, no fix) fail. `make speed` passed (Upload -1.7%, Remove -0.8%
    vs ce92cae).
- [x] Possible (code reading only; reproduce before fixing): in an unfrozen
  set, an upload result that lands just before rediscovery processes its entry
  has that entry reset to Pending. If discovery's `enqueueEntries` then reaches
  the queue while the item is still there (between `SetEntryStatus` and
  `updateFileStatus` removing it), `AddMany` treats it as a duplicate, so the
  entry is never uploaded or counted until the next discovery.
  - Origin: found fixing the set-counts "results during discovery are counted
    twice" item (2026-10-07).
  - Reproduced. Red: `CGO_ENABLED=1 go test -tags netgo --count 1 -timeout
    20m ./server -run '^TestServer$'`, new Convey replaying
    `updateFileStatus`'s steps around a synchronous rediscovery:
    `got.Uploaded` `Expected: 1 Actual: 0` (nothing re-queued, set stuck
    PendingUpload).
  - Fixed in `server/setdb.go`: after a non-failed result's request leaves
    the queue, `requeueIfRediscovered` re-reads the set and, if a later
    discovery wants the entry uploaded (`ShouldUpload`), puts it back via
    `enqueueEntries`. A sibling frozen-set Convey asserts nothing is
    re-queued. Mutants (no fix, requeue before removal, only failed results,
    no `ShouldUpload` check) fail. Gates pass; `make speed` Upload -0.2%,
    Remove +1.7% vs ce92cae.
  - Done after rebasing onto #195 (96faa45): the frozen Convey now also
    asserts `Uploaded == 1` and `Complete`. They fail on the pre-#195 head
    (`Expected: 1 Actual: 0`) and pass on the rebase. Gates pass;
    `make speed` Upload -2.4%, Remove +1.7% vs 96faa45.
- [x] `server/server.go` `reserveRemoveRequest`: when `removeQueue.Reserve`
  fails with anything other than `ErrNothingReady` (e.g. `ErrQueueClosed`
  from wr v0.38.0 when the queue is closed), the error is only logged and
  `item.Data()` is then called on a nil item, which panics. Code reading.
  - Origin: found fixing the unconvertible remove-queue item (2026-10-07).
  - Red: `CGO_ENABLED=1 go test -tags netgo --count 1 -timeout 20m ./server
    -run '^TestServer$'`: a new Convey destroying `s.removeQueue` then running
    `handleRemoveRequests` failed with `runtime error: invalid memory address
    or nil pointer dereference`.
  - Fixed in `server/server.go`: any `Reserve` error is returned (logged
    unless `ErrNothingReady`), so the caller breaks to `finalizeRemoval` and
    `removalFinished` once. Not reachable in production today
    (`Server.stop` doesn't close `removeQueue`); defensive. Mutants (spin on
    closed queue, return without finalizing, original code) fail. `make
    speed` passed (Upload -0.5%, Remove +2.0% vs ce92cae).
- [x] `baton/baton.go` `timeoutOpAndMakeNewClientOnError` → `setClientByIndex`
  writes `b.collClients[i]` without `collMu`, racing with the collection
  worker goroutines and with `Cleanup`/`AllClientsStopped` reading
  `collClients`. Code reading.
  - Origin: found fixing the baton `Cleanup` client race (2026-10-07).
  - Red: `CGO_ENABLED=1 go test -tags netgo --count 1 -race -timeout 10m
    ./baton -run TestBatonConcurrentClientInit` (new Convey: EnsureCollection
    under a data object so MkDir fails and clients are replaced, while polling
    `AllClientsStopped`): `WARNING: DATA RACE`, write in `setClientByIndex`,
    read in `getCollClients`.
  - Fixed in `baton/baton.go`: `getCollClient(i)`/`swapCollClient(i, c)` take
    `collMu`; the swapped-out client is stopped. Removed the unreachable
    put/meta index cases. Mutant (unlocked swap) fails.
- [x] `baton/baton.go`: if `Cleanup` runs while `EnsureCollection` callers
  are still sending on `collCh`, it closes `collCh` and `collErrCh`, which
  can panic with "send on closed channel". Code reading.
  - Origin: as above.
  - Red: same command without `-race`, new Convey (32 concurrent
    `EnsureCollection` calls, `Cleanup` after the first returns): `panic:
    send on closed channel` in `EnsureCollection`.
  - Fixed in `baton/baton.go`: each start of collection creation gets a
    cancellable context; `Cleanup` and `CollectionsDone` cancel it instead of
    closing channels; callers and workers select on it; the context flows
    into retries, and `getCollClient`/`swapCollClient` refuse after cancel.
    Interrupted callers get `ErrCollectionsStopped`; calls after `Cleanup`
    restart creation. Mutants (close channels again, result wait without
    done, no context in workers) fail. Gates: lint, `-race ./baton`,
    `./baton ./server`, `make speed` (Upload -0.1%, Remove -2.3% vs
    ce92cae) pass.
- [x] `baton/baton.go`: `Cleanup` stops the collection clients but leaves them
  in `collClients`; `makeCollConnections` then reuses the stopped clients, so
  every `EnsureCollection` after a `Cleanup` fails once and waits at least 5s
  of backoff before a fresh client works.
  - Origin: found fixing the Cleanup/`collCh` panic (2026-10-07).
  - Red: `CGO_ENABLED=1 go test -tags netgo --count 1 -timeout 10m ./baton
    -run TestBatonConcurrentClientInit`, new Convey "EnsureCollection after
    Cleanup succeeds without retrying": `Expected '5.172220699s' to be less
    than '5s'`.
  - Fixed in `baton/baton.go`: `Cleanup` stops `collClients` under `collMu`
    (catching clients swapped in after its snapshot) and sets it to nil, as
    `CollectionsDone` does; the next call takes ~150ms. Mutant (stop but keep
    the clients) fails.
- [x] `baton/baton.go`: `CollectionsDone()` without an earlier
  `EnsureCollection` panics on a nil `collPool`. Probably unreachable; low.
  - Origin: as above.
  - Reachable after all: `Putter.CreateCollections` calls `CollectionsDone`
    even with no collections to create, and after `makeCollConnections`
    fails before setting `collPool`.
  - Red: same command, new Convey "CollectionsDone without an earlier
    EnsureCollection works": `Expected func() NOT to panic (error: 'runtime
    error: invalid memory address or nil pointer dereference')`.
  - Fixed in `baton/baton.go`: the collection teardown in `CollectionsDone`
    is guarded by `collRunning`, as in `Cleanup`. Mutant (only the pool close
    guarded) fails. Gates: lint, `-race ./baton`, `./baton ./transfer
    ./server`, `make speed` (Upload +0.0%, Remove -1.9% vs ce92cae) pass.
- [x] `baton/baton.go` `makeCollConnections`: when `connect` fails partway,
  `GetClientsFromPoolConcurrently` still returns the clients that started,
  and they are dropped with the pool without being stopped (leaked baton-do
  processes). Code reading.
  - Origin: found reviewing the `CollectionsDone` nil-pool fix (2026-10-08).
  - Red: `CGO_ENABLED=1 go test -tags netgo --count 1 -timeout 10m ./baton
    -run TestBatonConcurrentClientInit`, new Convey
    "GetClientsFromPoolConcurrently failing partway leaves no started client
    running" (pool of 1, 2 requested): `Expected [86081] to be empty` (a
    running baton-do child left after the error).
  - Fixed in `baton/baton.go`: on error `GetClientsFromPoolConcurrently`
    drains the channel and stops the started clients with
    `closeConnections`, returning a closed, empty channel. Mutant (first
    client not stopped) fails. Gates pass; `make speed` Upload -0.5%,
    Remove +2.6% vs ce92cae.
- [x] Possible: `baton/baton.go` `connect` (used by `makeCollConnections` and
  `getNewClient`) never closes its new pool on error, and nor does
  `timeoutOpAndMakeNewClientOnError` when `pool.Get()` fails; each such
  failure leaks extendo's `checkClients` goroutine and its 30s ticker (no
  processes). Code reading.
  - Origin: found fixing the partial-connect client leak (2026-10-08).
  - Reproduced. Red: `CGO_ENABLED=1 go test -tags netgo --count 1 -timeout
    10m ./baton -run TestBatonConcurrentClientInit`, new Conveys (baton-do
    unstartable via an empty PATH; pool check frequency 10ms) for put-client
    creation, collection-client creation and collection-client replacement:
    a `checkClients` goroutine left running (`Expected [442] to be empty`).
  - Fixed in `baton/baton.go`: `connect` closes its pool on error;
    `timeoutOpAndMakeNewClientOnError` uses `getNewClient` instead of its
    own pool. Mutants (no close in `connect`; old inline pool) fail.
- [x] Possible: `baton/baton.go` `Put`, `Get` and the `Put` in
  `removeAndRetry` call the put client without `timeoutOp` (probably
  deliberate, as large transfers outlast 60s), so they share extendo
  v3.2.0's hang if the put client's `Stop()` overlaps a request (`send`
  blocks on an unbuffered channel after the writer goroutine is cancelled;
  upstream wtsi-npg/extendo client.go:848). Code reading.
  - Origin: found fixing the `GetMeta` timeout (2026-10-08).
  - Reproduced on real iRODS: Put and Get just after a concurrent Cleanup
    hung in extendo `send` (client.go:848). Red: the same command's new Convey
    "Transfers racing a concurrent Cleanup return instead of blocking
    forever", and deterministic fake-baton Conveys in `TestUploadRetry`
    (Put, Get, and the Put retried after SYS_COPY_LEN_ERR): `transfer did not
    return`.
  - Fixed in `baton/baton.go`: `untilClientStops` runs the transfer and checks
    `IsRunning` every 100ms; once the client has stopped it waits a 5s grace
    (extendo's response timeout) and then returns `ErrClientStopped`. No time
    limit on real transfers. Rejected: making Cleanup wait for transfers
    (could wait hours) and a long `timeoutOp` (caps transfers). The blocked
    extendo goroutine leaks until the upstream fix. Mutants (no fix; Get or
    the retry Put unwrapped) fail. Gates: lint, `-race ./baton`, `./baton
    ./transfer ./server`, `make speed` pass.
- [x] "The doc comment states "Errors are logged, since the result itself was
  recorded", but the error from `GetByNameAndRequester` here is silently
  discarded (it shares the `return` with the `LastDiscovery` check). A
  transient DB read error on this path would skip the re-queue without any
  log line, making a stuck set harder to diagnose. Consider logging this
  error, as is done for `requeueEntry` below."
  - Source: PR #197 Copilot thread PRRT_kwDOIEe6nc6qb2XH / comment
    4221005222 (`server/setdb.go:1995`, `requeueIfRediscovered`).
  - Red: `CGO_ENABLED=1 go test -tags netgo --count 1 -timeout 5m ./server
    -run '^TestRequeueIfRediscoveredLogsErrors$'`, exit 1. New test: an
    Uploaded result through `trackUploadingAndStuckRequests` with a closed
    set DB (the only way `GetByNameAndRequester` errors is its bolt `View`
    failing):

    ```text
    .......x
    Failures:
      * .../server/server_test.go
      Line 282:
      Expected 'request done rid=7c8e8e737f79a9b00bb3fe29a22b03aa status=uploaded
      ' to contain substring 'request requeue after rediscovery failed
      rid=7c8e8e737f79a9b00bb3fe29a22b03aa err=database not open' (but it
      didn't)!
    8 total assertions
    --- FAIL: TestRequeueIfRediscoveredLogsErrors (0.02s)
    ```
  - Fixed in `server/setdb.go`: `requeueIfRediscovered` only calls
    `requeueEntry` when the read succeeded and a later discovery happened,
    and logs either error with the existing "request requeue after
    rediscovery failed rid=%s err=%s" line; the doc comment is now accurate.
    Test `TestRequeueIfRediscoveredLogsErrors` in `server/server_test.go`.
    Mutant (fix reverted, in a scratch copy) fails as above. A removed set
    is not this path: `GetByNameAndRequester` returns nil, nil for it.
    Gates: `make lint`, `make test`, `make race` pass. `make speed` skipped:
    the change only adds an error-path log.
- [x] Possible (code reading only; reproduce before fixing): if a set is
  deleted between `SetEntryStatus` and the reads that follow it in
  `server/setdb.go`, `GetByNameAndRequester` returns nil, nil, so
  `requeueIfRediscovered` (`given.LastDiscovery`, added on this branch) and
  `handleNewlyCompletedSets` (`completed.Status`, pre-existing) would
  dereference nil and panic.
  - Origin: found fixing the unlogged requeue read error (PR #197 Copilot
    comment 4221005222), 2026-10-08.
  - Reachable: `deleteSet` (`server/setdb.go`) has no status check, so a set
    with uploads in flight can be deleted. It never touches the put queue;
    `registerDeletionCallback` runs `db.Delete` in its own goroutine once
    the set's removals finish (`discoveryCoordinator.RemovalDone` in
    `server/discover.go`). Nothing orders that against
    `handleFileStatusUpdates`, so the delete can land after
    `SetEntryStatus` succeeds and before the reads that follow it.
  - Red: `CGO_ENABLED=1 go test -timeout 300s -count=1 -run
    'TestUpdateFileStatusForSetDeletedMidUpdate' ./server/`, exit 1. The
    new test replays `updateFileStatus`'s steps with `db.Delete` between
    `SetEntryStatus` and the rest; both functions panic:

    ```text
    ........E.........E.
      Line 327: - runtime error: invalid memory address or nil pointer dereference
      github.com/wtsi-hgi/ibackup/server.(*Server).handleNewlyCompletedSets(...)
        .../server/setdb.go:1955 +0x39
      Line 331: - runtime error: invalid memory address or nil pointer dereference
        .../server/setdb.go:1993 +0x67   (requeueIfRediscovered)
        .../server/setdb.go:1980 +0x1e8  (trackUploadingAndStuckRequests)
    --- FAIL: TestUpdateFileStatusForSetDeletedMidUpdate (0.05s)
    ```
  - Fixed in `server/setdb.go`: `handleNewlyCompletedSets` and
    `requeueIfRediscovered` treat a nil set as nothing to complete or
    requeue, without logging, since deleting a set is a normal user
    action. Test `TestUpdateFileStatusForSetDeletedMidUpdate` in
    `server/server_test.go` also checks the request still leaves the queue.
    Mutants (each nil check reverted separately, in a scratch copy) fail
    as above.
    Gates: `make lint`, `make test`, `make race` pass. `make speed`
    skipped: the change only adds nil checks after existing reads.
- [x] PR #197 review (mjkw31): "What's the reason for wrapping the cleanup
  function in another function? Can you not just do: `Reset(h.Cleanup)`"
  (`baton/baton_test.go:380`), "Again, what is the wrapping function
  achieving?" (`:420`), and "What is the start channel achieving here? It
  looks like completely removing it would have no effect." (`:399`)
  - Source: threads PRRT_kwDOIEe6nc6qthg5 (comment 4228357560),
    PRRT_kwDOIEe6nc6qtlIz (4228380462), PRRT_kwDOIEe6nc6qtj8V (4228373069).
  - Files: `baton/baton_test.go`.
  - Approach: the wrappers did nothing; this PR's 8 `Reset(func() {
    h.Cleanup() })` are now `Reset(h.Cleanup)` (`h` is never reassigned).
    The start channel is removed from "Cleanup is safe during lazy client
    init": `Stat` spends ~100ms starting baton-do, so the two goroutines
    overlap anyway; the `Cleanup` goroutine is `wg.Go(h.Cleanup)`.
  - Red: with 3ba9e3c's fix undone in a scratch copy (client pointers made
    plain, unsynchronised fields) and only this Convey run, `-race` gives
    `WARNING: DATA RACE` 10 of 10 runs without the channel and 10 of 10
    with it; the fixed code passes 3 of 3.
  - Gates (all three items together): `make lint`, `make test`, `make race`
    pass; `make speed` passed (Upload +4.9%, Remove +8.9% vs 96faa45, host
    load 22 on 8 CPUs, spreads 41-59%).
- [x] PR #197 review (mjkw31): "What is this 10 second wait at the end of a
  function doing?" (`baton/baton_test.go:579`, a
  `time.Sleep(2 * operationMinBackoff)` with no assertion after it)
  - Source: thread PRRT_kwDOIEe6nc6qtqW2 (comment 4228413891).
  - Files: `baton/baton.go`, `baton/baton_test.go`.
  - Purpose (2ca66d5): collection workers that ignored the cancel kept
    retrying after `Cleanup`, then indexed the `collClients` that
    `CollectionsDone()` had set to nil and panicked in a background
    goroutine. The sleep only gave that panic time to crash the binary
    during this Convey; it asserted nothing.
  - Approach: the workers started by `startCreatingCollections` now run in
    `b.collWorkers` (a `sync.WaitGroup`, one `Go` per collection client
    start). The sleep is replaced by `So(finishesWithin(&h.collWorkers,
    h.opTimeout+operationMinBackoff), ShouldBeTrue)`: every worker must
    exit once its stop is seen. The bound covers one in-flight operation;
    it normally returns at once, so the Convey is ~10s faster.
  - Red (scratch copy, only this Convey): workers given
    `context.Background()` (2ca66d5's "no context in workers") panic
    `index out of range [0] with length 0` 3 of 3; `retry.Do` given
    `context.Background()` fails `finishesWithin` (`Expected: true`) 3 of
    3, which the old sleep missed. Fixed code passes 3 of 3.
  - Gates (all three items together): `make lint`, `make test`, `make race`
    pass; `make speed` passed (Upload +4.9%, Remove +8.9% vs 96faa45, host
    load 22 on 8 CPUs, spreads 41-59%).
- [ ] PR #197 review (mjkw31): "This cannot be the best way of doing this;
  surely." (`baton/baton_test.go:689`, `waitForGoroutineIn` on the backoff
  sleeper) and "Again, there must be a better of doing this other than stack
  inspection." (`:1183`, `poolCheckerGoroutines`)
  - Source: threads PRRT_kwDOIEe6nc6qttda (comment 4228433612),
    PRRT_kwDOIEe6nc6qt06U (4228480735).
  - Files: `baton/baton.go`, `baton/baton_test.go`.
  - Approach: two seams on `Baton`, no stack inspection. `backoffSleeper`
    (set to `&btime.Sleeper{}` by `GetBatonHandler`, like `opTimeout`) does
    collection creation's retry sleeps; the test's `signallingSleeper`
    closes a channel when the first backoff starts, then sleeps like the
    real one. `poolOpened` (nil in production) is called by the new
    `newClientPool`, which `connect` uses and is the only place we open a
    pool; the test's `poolRecorder` keeps them and counts those still
    `IsOpen()` (extendo's own state: an open pool's `checkClients`
    goroutine keeps running). `withoutBatonDo` no longer shortens extendo's
    global `CheckClientFreq`, and the settle loop and `goroutineStacks`
    helpers are gone. The Conveys are renamed "... leaves no client pool
    open" and assert pools were opened (`opened > 0`, or more than before
    the replacement) and `open == 0`.
  - Red (scratch copy, only these Conveys, 3 runs each): `connect` not
    closing its pool on error (1f63e61) fails all three Conveys
    (`Expected: 0`); the pre-1f63e61 inline pool in
    `timeoutOpAndMakeNewClientOnError` (via `newClientPool`, closed only
    when `Get` succeeds) fails the replacement Convey 3 of 3. 2cd7ea1's
    test ("GetClientsFromPoolConcurrently failing partway") never used
    stack inspection and is unchanged (it checks for running baton-do
    child processes); undoing 2cd7ea1 still fails it 3 of 3. Fixed code
    passes 3 of 3. Limit: a pool opened with `ex.NewClientPool` directly,
    bypassing `newClientPool`, would not be seen.
  - Gates (all three items together): `make lint`, `make test`, `make race`
    pass; `make speed` passed (Upload +4.9%, Remove +8.9% vs 96faa45, host
    load 22 on 8 CPUs, spreads 41-59%).
