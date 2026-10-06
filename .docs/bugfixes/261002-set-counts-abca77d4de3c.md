# Bugfixes 2026-10-02: set counts

- Branch: `bugfix-set-counts-abca77d4de3c`
- Base: dependency `bugfix-tests-speedup-6d2903c9` (PR #194) at `13915cf`;
  integration base `origin/develop` at `43f73234a4d0` (resolved 2026-10-06)
- Dependency: PR #194 (`bugfix-tests-speedup-6d2903c9`), which changes the
  same set-counting code and fixes the "Missing counted twice" item. Held for
  dependency merge: keep this branch local; after #194 merges, rebase with
  `git rebase --onto origin/develop 13915cf` before pushing.
- Queue owner: `bugfix-tests-speedup-6d2903c9`,
  `.docs/bugfixes/261005-tests-speedup-31c758180c99.md`
- Origin: `deps`, `.docs/bugfixes/260930-deps-update-702d9698d638.md`

Each item is independent of the `deps` work: no `make test`, `make race` or
`make lint` run fails because of it. Found while fixing `deps` items.

- [ ] Frozen sets: when a frozen set's uploaded file is deleted locally,
  discovery counts it Orphaned but keeps the old entry (status Uploaded), so
  removing it leaves `Orphaned: 1` with `Num files: 0`. At `origin/develop` it
  is worse (removal wraps Uploaded). Needs a decision on what frozen-set
  counts should be.
  - Decision (user, 2026-10-05): count it as Orphaned, not Uploaded.
  - Origin item: "Frozen sets: when a frozen set's uploaded file is deleted
    locally" (found reviewing the TestSync fix, 5855c25).
  - Evidence: reviewer probe in a set-package test: frozen set, uploaded file
    deleted locally, rediscovered, then `RemoveFileEntry` +
    `UpdateBasedOnRemovedEntry` -> final `Orphaned` expected 0, actual 1.
- [ ] Missing entries are counted at discovery and also queued for upload
  (`ShouldUpload` is true), so when their "missing" result comes back they are
  counted again: a set with 1 missing file and 1 pending file goes "complete"
  with `Missing: 2` before the other file uploads. Same at `origin/develop`.
  - Origin item: "Missing entries are counted at discovery and also queued for
    upload" (found reviewing the TestSync fix).
  - Update (2026-10-06): fixed on branch `bugfix-tests-speedup-6d2903c9`
    (an upload result now replaces the current discovery's count for that
    entry; covers Missing and Orphaned). Close this once that branch merges
    and this branch is rebased onto it.
- [ ] If a removal's inode cleanup fails, the entry is already deleted but
  not counted as removed; retries then fail with "set ... has no path", so
  the set error shows that instead of the inode error, and Num files keeps
  counting the file. Same at `origin/develop`.
  - Origin item: "If a removal's inode cleanup fails" (found reviewing the
    TestSync fix; see `server/setdb.go` `processDBFileRemoval`).
  - Update (2026-10-06): largely resolved on `deps` by the one-transaction
    removal fix (deps commit after 9774642, "Do a removal's database side in
    one transaction"): a failed inode cleanup now rolls the whole removal
    back, so the entry and Num files stay consistent and retries report the
    real inode error. Re-check after rebasing onto deps; close if nothing
    remains.
- [ ] Trashing a subfolder that isn't itself in the set, for a legacy set
  without a discovered-folders bucket, fails on the folder's own entry in
  `trashDirFromDB`: the set ends with `Error when removing: invalid set
  entry [set ... has no path .../dir1/dir2]` and `removed 1 of 2`. Present at
  `origin/develop`; previously hidden by a test helper that ignored its
  timeout.
  - Origin item: "TestServer (server package) is flaky" on `deps` (server
    test "Trash on a folder not specified should still work").
- [ ] A removal retried after its remote delete succeeded (released after a
  failed `UpdateRemoveRequest`, `PutEntryInTrash` or `RemoveFileEntry`, or
  re-run by `recoverRemoveQueue` after a crash) re-reads the entry in
  `removeFileFromIRODSandDB`; if a `missing` upload result zeroed its size
  meanwhile, SizeRemoved gains 0 B. Needs `deps` at `db513d8` or later
  (`UpdateBasedOnRemovedEntry` takes the removed size).
  - Origin item: "TestSync intermittently fails in clean `make race` (after
    8a854d1)" (found reviewing its fix, db513d8).
  - Evidence: code reading only; reachable only after a database failure or
    crash. Suggested fix: record the object's size on `RemoveReq` when it
    moves to AboutToBeRemoved (persisted by `UpdateRemoveRequest`) and pass
    that as the removed size.
- [ ] Possible: `set` `RemoveFileFromInode` errors with "invalid transformer
  path concatenation" when the original file of an inode with 3 or more
  paths is removed before its hardlinks: removing the original blanks
  `files[0]`, and the next call fails splitting `""` in
  `removePathFromInodeFiles`. Code is unchanged from develop; not confirmed by
  a run at base.
  - Origin: found while fixing the deps removal-retry race (2026-10-06).
- [ ] Possible: a ToRemove of an entry with no inode record fails with "key
  not found in inode bucket [0/]". Seen in set tests whose entries had no
  inode records; not checked whether production can reach it.
  - Origin: found while fixing the deps removal-retry race (2026-10-06).
- [ ] Removals interrupted under a build before the deps removal-retry fix
  still fail after upgrading: the entry is gone and the stored request has no
  saved `RemovedEntry`, so the retry fails with "has no path".
  - Origin: limitation of the deps removal-retry fix (2026-10-06).
- [ ] An uploaded (or orphaned) entry whose remote object was deleted outside
  ibackup can never be removed: `processRemoteFileRemoval` tolerates GetMeta
  "does not exist" only when the request was already AboutToBeRemoved
  (`mayMissInRemote`, server/setdb.go ~861), so every fresh attempt fails.
  Same logic on develop. Needs a decision: should a vanished remote object
  count as successfully removed?
  - Origin: found while fixing the deps removal-retry race (2026-10-06).
- [ ] Upload results that arrive while a discovery is running are counted
  twice: with NumFiles reset to 0, a result triggers a full `fixCounts`
  recount, and the running discovery then counts the entry again. Same on
  develop. Found reviewing the tests speed-up branch's double-count fix
  (2026-10-06); no gate fails.
