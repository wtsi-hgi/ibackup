# Bugfixes 2026-10-02: upload concurrency

- Branch: `bugfix-upload-concurrency-dbfef239a15f`
- Base: dependency `bugfix-tests-speedup-6d2903c9` (PR #194) at `13915cf`;
  integration base `origin/develop` at `43f73234a4d0` (resolved 2026-10-06)
- Dependency: PR #194 (`bugfix-tests-speedup-6d2903c9`), for its faster
  tests and test fixes. Held for dependency merge: keep this branch local;
  after #194 merges, rebase with `git rebase --onto origin/develop 13915cf`
  before pushing.
- Queue owner: `bugfix-tests-speedup-6d2903c9`,
  `.docs/bugfixes/261005-tests-speedup-31c758180c99.md`
- Origin: `deps`, `.docs/bugfixes/260930-deps-update-702d9698d638.md`

Each item is independent of the `deps` work: no `make test`, `make race` or
`make lint` run fails because of it. Found while fixing `deps` items.

- [ ] Possible: requests from different sets for the same local file map to
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
- [ ] Possible: `baton/baton.go` `EnsureCollection` sends every concurrent
  caller's result back on one shared channel, so a caller may receive
  another collection's error. Spotted by reading the code; no failure seen.
  Not yet confirmed.
  - Origin item: "Possible: `baton/baton.go` `EnsureCollection`".
  - Decision (user, 2026-10-05): try to reproduce it; fix only if it
    reproduces, otherwise record what was tried and leave it unfixed.
- [ ] `baton/baton.go` `GetMeta` is the only remote operation without
  `timeoutOp`, so any hang in it blocks its caller forever. One such hang:
  extendo (github.com/mjkw31/extendo/v2 v2.7.1-beta2, client.go
  execute/send) accepts a request after `Stop()` has cancelled its writer
  goroutine (`isRunning` stays true until the process exits) and blocks on
  an unbuffered send (client.go:848). A scratch probe calling `GetMeta`
  0-2ms after a concurrent `Cleanup()` on real iRODS hung 29 of 30 times.
  The extendo part is upstream.
  - Origin item: "TestTrashRemove intermittently fails in clean `make test`"
    on `deps`, whose fix stops the server triggering this hang by not
    overlapping removals with handler `Cleanup()`.
- [ ] `baton/baton.go` `Cleanup` (~line 725) reads `b.metaClient` and the
  other clients without holding `clientMu`, so it races with
  `setClientIfNotExists` (~line 368). The `deps` fix 1fa0608 stops the
  server reaching this, but the handler itself is still unsafe.
  - Origin item: "TestTrashRemove intermittently fails in clean `make test`"
    on `deps` (found reviewing its fix).
- [ ] `server/server.go` (~lines 437-441): when
  `convertQueueItemToRemoveRequest` fails, the reserved item stays in
  `removeQueue`, so `Items` never reaches 0 and the storage handler's
  `Cleanup()` never runs again. Unchanged by the `deps` fix.
  - Origin item: as above.
- [ ] `transfer/put.go` `getSortedRequestCollections` (~272) only creates
  collections for `p.requests`, not `p.duplicateRequests`, so a request
  deduplicated by `RemoteDataPath()` (e.g. a second set's hardlink to the same
  remote inode file in the same batch) never gets its own `Remote` collection
  created, and its put fails with `open .../remote2/file.link2: no such file
  or directory`. Seen in a server test on the set-counts branch; same on
  develop.
  - Origin: found reviewing the set-counts self-hardlink fix (2026-10-07).
- [ ] Possible (code reading only; reproduce before fixing): in an unfrozen
  set, an upload result that lands just before rediscovery processes its entry
  has that entry reset to Pending. If discovery's `enqueueEntries` then reaches
  the queue while the item is still there (between `SetEntryStatus` and
  `updateFileStatus` removing it), `AddMany` treats it as a duplicate, so the
  entry is never uploaded or counted until the next discovery.
  - Origin: found fixing the set-counts "results during discovery are counted
    twice" item (2026-10-07).
