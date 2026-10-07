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

- [x] Frozen sets: when a frozen set's uploaded file is deleted locally,
  discovery counts it Orphaned but keeps the old entry (status Uploaded), so
  removing it leaves `Orphaned: 1` with `Num files: 0`. At `origin/develop` it
  is worse (removal wraps Uploaded). Needs a decision on what frozen-set
  counts should be.
  - Decision (user, 2026-10-05): count it as Orphaned, not Uploaded.
  - Cause: `existingOrNewEncodedEntry`'s frozen guard returned the old stored
    entry (status Uploaded, no discovery stamp) after discovery had counted
    it Orphaned, so removal didn't decrement Orphaned; and
    `updateTypeDestAndInode` mapped only Uploaded to Orphaned, so a deleted
    Skipped/Replaced/Orphaned file became Missing (same mismatch for a frozen
    Skipped file).
  - Red: a set test (frozen set, upload, local delete, rediscover, remove)
    failed with entry Uploaded vs Orphaned, then `Orphaned: 1` after removal.
  - Fixed in `set/entries.go`: the frozen guard stores a newly Orphaned entry
    with its counted status and stamp (other changes to frozen uploaded
    entries are still ignored, so no re-upload); any uploaded-type entry
    (Uploaded, Replaced, Skipped, Orphaned) that goes missing becomes
    Orphaned. In unfrozen sets a deleted Skipped/Replaced file is now counted
    Orphaned at discovery rather than Missing; its put result still replaces
    that count, so final counts are unchanged.
  - Tests in `set/set_test.go`: the frozen orphan scenario (removal, still
    deleted, restored); deleted Skipped and Replaced files in frozen and
    unfrozen sets. Mutants narrowing the mapping or dropping either change
    fail.
  - Decision (user, 2026-10-07): a frozen file deleted then restored locally
    stays Orphaned and is not re-uploaded (accepted as-is); its count is
    part of the "existing frozen files never counted" item below.
  - Origin item: "Frozen sets: when a frozen set's uploaded file is deleted
    locally" (found reviewing the TestSync fix, 5855c25).
  - Evidence: reviewer probe in a set-package test: frozen set, uploaded file
    deleted locally, rediscovered, then `RemoveFileEntry` +
    `UpdateBasedOnRemovedEntry` -> final `Orphaned` expected 0, actual 1.
- [x] Missing entries are counted at discovery and also queued for upload
  (`ShouldUpload` is true), so when their "missing" result comes back they are
  counted again: a set with 1 missing file and 1 pending file goes "complete"
  with `Missing: 2` before the other file uploads. Same at `origin/develop`.
  - Origin item: "Missing entries are counted at discovery and also queued for
    upload" (found reviewing the TestSync fix).
  - Update (2026-10-06): fixed on branch `bugfix-tests-speedup-6d2903c9`
    (an upload result now replaces the current discovery's count for that
    entry; covers Missing and Orphaned). Close this once that branch merges
    and this branch is rebased onto it.
  - Closed: fixed on the base branch by 374a095 (PR #194): an upload result
    replaces the current discovery's count; test "an upload result for an
    entry discovery counted replaces that count" in `set/set_test.go`.
- [x] If a removal's inode cleanup fails, the entry is already deleted but
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
  - Closed: fixed by d200a1d (PR #193): a removal's database side is one
    transaction, so a failed inode cleanup rolls it back; the entry and Num
    files stay consistent and retries report the inode error. Test "so a
    failure to clean up its inode record leaves its database removal undone"
    in `server/server_test.go`.
- [x] Trashing a subfolder that isn't itself in the set, for a legacy set
  without a discovered-folders bucket, fails on the folder's own entry in
  `trashDirFromDB`: the set ends with `Error when removing: invalid set
  entry [set ... has no path .../dir1/dir2]` and `removed 1 of 2`. Present at
  `origin/develop`; previously hidden by a test helper that ignored its
  timeout.
  - Origin item: "TestServer (server package) is flaky" on `deps` (server
    test "Trash on a folder not specified should still work").
  - Still present after d200a1d: `RemoveDirEntry` → `trashDirEntry` looked up
    the subfolder's own entry to copy it into the trash set; a legacy set
    (no discovered-folders bucket) has none for an unspecified subfolder, so
    the lookup failed "has no path" and rolled the removal back.
  - Red: a set test trashing dir1/dir2 of a legacy set failed with `has no
    path .../dir2`; the focused server test failed with the reported
    `removed 1 of 2` symptom.
  - Fixed in `set/db.go`: when the folder has no entry (only possible for
    legacy sets; `getEntry`'s only error is not-found), a plain dir entry for
    the path is put in the trash set, so the folder stays targetable there
    (e.g. `trash --remove --path`) and expires as usual; counts unchanged.
  - Tests: `set/set_test.go` trashes a nested folder in normal and legacy sets
    and checks the trash set's dirs and that the folder validates in the
    trash set; the server test now fails on timeout and asserts no error,
    2/2 removed, and the file in trash. A "skip the trash entry" mutant fails.
- [x] A removal retried after its remote delete succeeded (released after a
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
  - Still applied after d200a1d: each retry re-read the entry, and a result
    in the window between the remote step and the database transaction
    (Missing for remove, Failed for trash) changed its size, so SizeRemoved
    gained the wrong amount after a stop and restart.
  - Red: server tests stopping after the remote step, changing the entry's
    size, then restarting: SizeRemoved expected 1, actual 0 (remove) / 3
    (trash).
  - Fixed in `set/db.go` and `server/setdb.go`: `RemoveReq.ObjectSize` is
    recorded once from the entry's size when the request moves to
    AboutToBeRemoved (saved before any remote call) and used as the removed
    size; requests stored before this field fall back to the entry's size.
    Status counts and SizeTotal still use the stored entry.
  - Tests in `server/server_test.go`: stopped after Removed was saved, and
    stopped before it (the stored request is captured at the first remote
    call), for remove and trash. Mutants recording the size after the remote
    call, ignoring it, or overwriting it on each attempt fail.
- [x] Possible: `set` `RemoveFileFromInode` errors with "invalid transformer
  path concatenation" when the original file of an inode with 3 or more
  paths is removed before its hardlinks: removing the original blanks
  `files[0]`, and the next call fails splitting `""` in
  `removePathFromInodeFiles`. Code is unchanged from develop; not confirmed by
  a run at base.
  - Origin: found while fixing the deps removal-retry race (2026-10-06).
  - Reproduced: removing 3 hardlinked paths in either order that takes the
    original first failed on the next removal; also, removing a hardlink
    added by a set with another transformer failed `element not in slice`.
  - Fixed in `set/inode.go`: `removePathFromInodeFiles` finds the entry by its
    path part, ignoring the transformer, blanking index 0 (the original) and
    deleting any other. Tests: all 6 removal orders and the cross-transformer
    case (`set/set_test.go`), and a server-level original-then-hardlinks
    removal that keeps the inode file until the last link.
- [x] Possible: a ToRemove of an entry with no inode record fails with "key
  not found in inode bucket [0/]". Seen in set tests whose entries had no
  inode records; not checked whether production can reach it.
  - Origin: found while fixing the deps removal-retry race (2026-10-06).
  - Reproduced via inode reuse (a deleted file's inode given to a new file in
    another set, immediate on ext4) and a mount point change across a
    restart; either made the removal fail on every retry.
  - Fixed in `set/inode.go`: a missing record means nothing is recorded:
    `removeFileFromInode` returns nil and `GetFilesFromInode` returns no
    files, so the server falls back to the remote hardlink query (the inode
    file is only removed if no remote object still points at it). A corrupt
    record still errors and rolls the removal back; d200a1d's server leaf
    that used a deleted record now expects the removal to complete, and the
    rollback check moved to a corrupt-record set test.
- [x] Removals interrupted under a build before #193, after the entry was
  deleted but before the request completed, fail with "has no path" on retry
  after upgrading: after 3 retries (~15s) the request is marked complete with
  set error `Error when removing: invalid set entry [... has no path ...]`.
  If the old build had not counted it yet, removal status stays one short
  permanently (NumObjectsRemoved is never repaired) and NumFiles is wrong
  until the next discovery; ToRemove also leaks the inode record. Needs a
  decision: fix (treat "has no path" on a recovered request as already
  removed) or accept as a one-off upgrade window.
  - Origin: limitation of the deps removal-retry fix; behaviour confirmed by
    probe on 03efdf6 (2026-10-07).
  - Decision (user, 2026-10-07): fix it.
  - Red: server restart tests recreating the old build's state (remote
    removed, entry deleted, request incomplete; for remove and trash, stopped
    before and after counting) failed with `removed 0 of 1` and the "has no
    path" set error.
  - Fixed in `server/setdb.go` and `set/db.go`: a request that is incomplete,
    whose remote removal had started (AboutToBeRemoved or Removed) and whose
    entry is gone is treated as already removed: `RemoveDeletedFileEntry`
    removes the failed lookup, cleans the inode record for ToRemove (inode
    from stat'ing the local path), counts the removal unless the old build
    already did, and completes the request, in one transaction. A path never
    in the set (NotRemoved) or a completed request still fails as before.
  - Already-counted check: skip if NumObjectsRemoved plus the set's
    incomplete requests exceeds NumObjectsToBeRemoved. Trade-off: a set with
    any failed removal since its counts were last equal over-counts once if
    the old build had counted (never worse than always counting); a
    removal submitted in the narrow window before NumObjectsToBeRemoved is
    raised may make a recovered request skip its count. For remove, NumFiles
    and status counts update at the next discovery (the entry is gone); trash
    uses the trashed copy.
  - Tests: the server restart cases and a never-in-the-set case; set tests
    for two-file removals (with an earlier completed request and a failed
    entry). Mutants (always/never count, no inode cleanup, no trashed-copy
    lookup, accepting NotRemoved, counting completed requests, keeping the
    failed lookup) fail.
- [x] An uploaded (or orphaned) entry whose remote object was deleted outside
  ibackup can never be removed: `processRemoteFileRemoval` tolerates GetMeta
  "does not exist" only when the request was already AboutToBeRemoved
  (`mayMissInRemote`, server/setdb.go ~861), so every fresh attempt fails.
  Same logic on develop. Needs a decision: should a vanished remote object
  count as successfully removed?
  - Origin: found while fixing the deps removal-retry race (2026-10-06).
  - Decision (user, 2026-10-07): treat the missing remote object as removed
    (removal succeeds) and log a warning.
  - Red: with a baton-like GetMeta (`file does not exist` for a vanished
    object), ToTrash failed every attempt (`Error when removing: file does
    not exist`, `removed 0 of 1`), ToRemove only succeeded silently on its
    retry, and removing a vanished hardlink failed.
  - Fixed in `server/setdb.go`: a GetMeta `errs.PathError` with
    `ErrFileDoesNotExist`, for an entry not marked as never uploaded, logs a
    warning ("does not exist or is not readable, so treating it as removed";
    baton reports both the same way) and treats the remote step as done; any
    other error still fails. Trash still records the DB trash entry, and
    later removal from the trash set is tolerated the same way. A vanished
    hardlink's inode file path is rebuilt as discovery built it, and the
    inode file is removed only if unused (an already-missing inode file is
    tolerated).
  - Tests: server leaves for remove, trash, a missing file (no warning), a
    timeout-like PathError that must still fail, vanished hardlinks (inode
    file kept while needed, removed with the last, already gone, kept on
    trash). Main tests that injected failures by deleting the remote object
    now deny permission instead (`denyRemoteChanges`), plus new leaves for
    vanished objects. Mutants (no guard, no not-exist check, any PathError,
    no hardlink handling, no unused check, no missing-inode tolerance, no
    warning) fail.
- [x] Upload results that arrive while a discovery is running are counted
  twice: with NumFiles reset to 0, a result triggers a full `fixCounts`
  recount, and the running discovery then counts the entry again. Same on
  develop. Found reviewing the tests speed-up branch's double-count fix
  (2026-10-06); no gate fails.
  - Note (2026-10-07): for frozen sets this now persists (nothing is queued
    to trigger a recount, e.g. `up=4 num=2`), and sizes are double-counted
    too since discovery counts sizes.
  - Red: new set test block "upload results that arrive during rediscovery
    are counted once" (frozen and unfrozen; results before/after discovery
    reaches the entry, repeated failures, removal during discovery): 7
    assertions failed, e.g. Uploaded 2 vs 0, frozen Uploaded 4 vs 2, size 8
    vs 3, Failed 1 vs 0, Uploaded 3 vs 1 after removal.
  - Fixed in `set/set.go`, `set/db.go` and `set/entries.go` by extending the
    `CountedInDiscovery` stamp: a result during discovery takes back any count
    stamped this discovery, counts the entry as discovery would (status, size
    if uploaded, Failed once), stamps it, and skips recount, completion and
    the Uploading flip; discovery reaching a result-stamped entry takes that
    count back first; removal during discovery decrements a stamped entry.
    The set now stays "pending discovery" until discovery completes.
  - Mutants (no discovery take-back, no stamp, no removal decrement, Failed
    not forced, no size, no result take-back) fail. `make speed` passed
    (Upload -1.2%, Remove +3.4% pooled vs 43f7323).
  - Not fixed (predates this): an unfrozen set whose results all arrive
    during discovery is marked Complete at `DiscoveryCompleted` although the
    files are re-queued, so a second completion Slack message follows.
  - Incidental: possible rediscovery enqueue duplicate race routed to
    upload-concurrency (`.docs/bugfixes/261002-upload-concurrency-dbfef239a15f.md`).
- [x] A frozen set rediscovered while its uploaded files still exist never
  counts them: the frozen guard keeps the stored entry uncounted and it is
  never queued (`ShouldUpload` false), so the set shows `Uploaded 0` and stays
  "pending upload" forever. Same on develop (43f7323). Needs a decision:
  count them as Uploaded or as Skipped. Also covers restored frozen files.
  - Origin: found fixing the frozen-sets item; confirmed by its reviewer.
  - Decision (user, 2026-10-07): count them as Uploaded.
  - Red: set tests (frozen set uploaded, rediscovered with files present; a
    restored frozen orphan) got Uploaded 0 / Orphaned 0, stuck pending upload.
  - Fixed in `set/entries.go`, `set/set.go` and `set/db.go`: an entry kept by
    the frozen guard is counted by its stored status (Uploaded, Replaced,
    Skipped, Orphaned) and stamped, unless discovery already counted another
    status; `DiscoveryCompleted` checks completion, so a frozen set with
    nothing queued completes. Sizes: discovery now adds an uploaded-type
    entry's size to SizeTotal (and SizeUploaded unless Skipped/Orphaned) for
    all sets (a frozen-only rule would wrap after `edit --unfreeze`); an
    upload result replacing that count takes the discovered size back, and
    removal subtracts it. Unfrozen sets' final sizes are unchanged; SizeTotal
    includes orphan sizes from discovery end. Monitored frozen sets now
    re-arm (they never completed before).
  - After rediscovering a frozen set, SizeUploaded and the Slack completion
    message report the kept files' earlier upload sizes although nothing was
    uploaded this run (follows from counting them as Uploaded).
  - Tests: frozen existing files (Uploaded/Replaced/Skipped, second
    rediscovery, removal to zero, FIFO, new file), restored frozen orphan,
    frozen-then-unfrozen re-upload and skip, uploaded file replaced by a FIFO
    then removed, all with size assertions. Mutants (no stamp, no
    completion check, all as Uploaded, no discovery size, no removal
    subtraction, no take-back, take-back with the new size, SizeUploaded for
    every status, no IsUploaded guard on removal) fail.
- [ ] A frozen set's uploaded file replaced by an abnormal file (e.g. a FIFO)
  is counted Abnormal but the frozen guard keeps the stored Uploaded entry,
  so removing it would leave Abnormal 1. Same on develop. Needs a design
  that keeps frozen from re-uploading a later regular file at that path.
  - Origin: found fixing the frozen-sets item; confirmed by its reviewer.
- [ ] A trash removal retried in the same window, after a re-upload attempt
  failed (entry now WasNotUploaded), skips `putEntryInTrash` in
  `cleanUpRemovedFile`, so the remote object is tagged with the trash set but
  the trash set has no entry for it (it can't be listed or expired). A fix
  would record the decision to trash on the request at AboutToBeRemoved.
  Code reading only; low priority.
  - Origin: found fixing the removed-size retry item; confirmed (narrowed)
    by its reviewer.
- [x] `handleInode` treats the same path added by sets with different
  transformers as a hardlink of itself (it compares full `transformerID:path`
  strings): the second set's entry becomes Type Hardlink with Dest the same
  path, and after both sets remove it a stale record `["", path]` remains, so
  a later set also gets it as a hardlink to itself. Same on develop.
  Possible fix: compare by path part in `alreadyInFiles`. Also
  `RemoveElementFromSlice` (set/inode.go) is now unused.
  - Origin: found reviewing the inode removal fixes (2026-10-07); confirmed
    by probe.
  - Red: a set test (sets with transformers `/remote` and `/remote2` both
    add one plain file) failed: the second set counted 2 hardlinks, not 1;
    with that softened, a record `["", path]` remained after both removed
    it, and a third set got it as a hardlink. A server test with a remote
    hardlink location: the second set's request had `Hardlink` set to
    `<hardlinks>/<path>/<inode>`, and its upload failed (`open
    .../remote2/file.link1: no such file or directory`).
  - Fixed in `set/inode.go`. The original (`files[0]`) matches by path
    part only, whatever transformer added it, so another set's entry for it
    is Regular and adds nothing to the record; the original is uploaded as
    a regular file and doesn't use the remote inode file. A hardlink is
    recorded once per `transformerID:path`, since each is a separate remote
    object pointing to the inode file, and the server's
    `getFilesWithSameInode` threshold counts them.
  - Removal: once no set has the path, every entry for it goes (the
    original is blanked), so no stale `["", path]` remains. Until then, a
    hardlink entry goes once no set with its transformer has the path, as
    the remote side removes that set's object before the database side
    runs; the original stays. `RemoveElementFromSlice`, `alreadyInFiles`
    and `ErrElementNotInSlice` are removed.
  - Review cycle 1 FAIL: cycle 1 recorded one entry per path, so the
    threshold undercounted. With `/remote` holding link1-3 and `/remote2`
    holding link2, removing `/remote`'s link3 and link2 deleted the inode
    file that `/remote2`'s link2 still pointed to. Removing every entry only
    once no set had the path (the reviewer's suggestion) would instead
    leave the inode file behind after `/remote2`'s link2 went, since its
    removal still counted `/remote`'s entry.
  - Tests: `set/set_test.go` (the scenario, plus a real hardlink in the
    second set staying a hardlink to the first set's original) and
    `server/server_test.go` (the second set uploads a full regular file
    with no hardlink metadata; and the data-loss scenario above: the inode
    file stays while `/remote2` has link2, then goes once no set has a
    hardlink). The cycle 1 mutant fails the "stays" assertion; the
    path-only-removal mutant fails the "goes" assertion.
  - The data-loss test pre-creates `/remote2`'s collection:
    `transfer/put.go` `getSortedRequestCollections` ignores
    `duplicateRequests`, so the put fails with "no such file or directory"
    otherwise. Separate pre-existing bug, not fixed here.
  - Same-transformer guard: tests where two sets with the same transformer
    share a hardlink path (set level: the record keeps the path until both
    remove it; server level: the inode file survives one set removing its
    links while the other still holds one). A mutant ignoring same-
    transformer sets fails both.
- [x] When an original is removed while its hardlinks remain (record
  `["", h1, h2]`), the next discovery makes h1 the Dest: h1 becomes a
  hardlink of itself, both links' storage path moves from `<orig>/<ino>` to
  `<h1>/<ino>` and they are re-uploaded, and (from code reading) the old inode
  file is left orphaned in iRODS. Same on develop; records left as
  `["", path]` by older builds have the same shape.
  - Decision (user, 2026-10-07): option (a), keep the hardlinks pointing at
    the original's existing inode file (use the hardlink entries' stored Dest
    when the record's first slot is blank); no re-upload.
  - Origin: found fixing the self-hardlink item; confirmed by its reviewer.
  - Red: a set test (original + 2 hardlinks, original removed, rediscovered)
    gave h1 Dest h1 instead of the original; a server test's storage path
    moved from file.link1 to file.link2; a model probe (HEAD) re-uploaded.
  - Fixed in `set/inode.go` and `set/entries.go`: when the record's first
    slot is blank, a hardlink's Dest comes from the discovering set's own
    stored entry (trusted only if the record already holds this file, so a
    reused inode number can't mislead it) or else from another set's stored
    hardlink entry for one of the files (Hardlink with the same inode); if
    none knows it, the file becomes the original (and every stale entry for
    its path, whatever transformer, is dropped). Re-adding a removed original
    makes it a hardlink of itself reusing its existing inode file (accepted
    under option (a): no re-upload). The own-entry check keeps rediscovery
    cost flat (~0.2s for 500 such links from 10 to 2000 sets).
  - Tests: links keep the original's Dest and storage after its removal
    (server: no re-upload, inode file removed with the last link); first
    link deleted locally / re-linked to another inode; a stale own entry
    (inode 0, other inode, reused inode number); two sets with one stale
    entry; an older-build ghost record; re-adding the original. Mutants
    (old first-non-blank Dest, only the first link consulted, no inode
    check, only the first set, no stale-entry guard, shortcut without inode
    check or fallthrough, ghost kept) all fail; model probe 0/600.
- [x] Trash sets' file counts are never added to when files are trashed
  (`putEntryInTrash` doesn't count), so removing a file from a trash set
  decrements them below zero: a probe trashing file1 then removing it from
  the trash set gave `NumFiles=18446744073709551615 Uploaded=18446744073709551615`
  (SizeTotal wraps too with non-empty files). Same on develop (43f7323).
  - Origin: found fixing the old-build removals item; confirmed by probe by
    its reviewer (2026-10-07).
  - Red: `TestTrashSetCounts` (trash a 3-byte file and a symlink, remove them
    from the trash set, re-trash, remove again) wrapped NumFiles, SizeTotal,
    Uploaded and Symlinks; a server leaf running the probe via the admin
    client got NumFiles 18446744073709551615.
  - Fixed in `set/set.go` `countRemovedEntry`: a trash set keeps no file
    counts (it is never discovered, so `status` shows its Num files and Size
    as pending); removing from it only counts the removal (NumObjectsRemoved,
    SizeRemoved). `ibackup summary` thus adds 0 for trash sets (trashed files
    still in iRODS aren't in usage figures, as before the bug).
  - Tests: `TestTrashSetCounts`, including an old build's trashed copy (with
    TrashDate) counted against the normal set; the server old-build fixture
    now gives its trashed copy a TrashDate. Mutants (no removal count,
    skipping only status/type counts, deciding by the entry's TrashDate)
    fail.
  - Decision (user, 2026-10-07): leave trash sets whose counts already
    wrapped as they are (no repair); the fix only stops new wrapping.
- [ ] Directories removed under a build before #193 could be counted twice:
  the old `removeDirFromDB` ran `RemoveDirEntry`, `IncrementSetTotalRemoved`,
  then `finalizeRemoveReq` separately, so a stop after the count makes the
  retry count again; for trash, a stop after the delete makes the retry
  overwrite the trashed dir entry with a plain one. Not cheap to detect:
  new batches' dir requests run before `UpdateSetTotalToRemove`, and legacy
  sets lack entries for discovered subfolders. Low priority (tiny window).
  - Origin: found fixing the old-build removals item.
- [ ] Every trash request resets the trash set's removal counts:
  `removeToTrashSet` (server/setdb.go ~566-571) calls `AddOrUpdate` with a
  fresh `BuildTrashSetFromSet`, and `copyUserProperties` copies its zero
  NumObjectsRemoved, NumObjectsToBeRemoved and SizeRemoved over the stored
  ones, so an expiry or admin removal running on that trash set loses its
  "Removal status" (and the old-build already-counted check can misjudge for
  it). Same on develop. Code reading only; low priority.
  - Origin: found reviewing the trash-set counts fix (2026-10-07).
- [x] An unfrozen set whose upload results all arrive during a rediscovery,
  after discovery processed each file, is marked Complete by
  `DiscoveryCompleted` → `checkIfComplete` although those files are re-queued;
  when they finish it completes again and sends a second "completed backup"
  Slack message. Predates the results-during-discovery fix (the old recount
  path did the same).
  - Origin: found reviewing the results-during-discovery fix (2026-10-07).
  - Red: new leaf in the rediscovery block (all results arrive after
    discovery processed each file) got `Expected: set.Status(1)`,
    `Actual: set.Status(4)`; a completion-check-only fix still sent two
    "completed backup" Slack messages.
  - Fixed in `set/set.go` and `set/db.go`: `DiscoveryCompleted` takes an
    `uncountQueuedResults` callback, run after `LastDiscovery`/`NumFiles` are
    set, that takes back (status and size) and unstamps every entry stamped
    this discovery that `ShouldUpload` will re-queue, except Missing,
    Orphaned and Abnormal (whose results replace the count, per 374a095).
    Skipped when no uploaded/failed counts exist. Frozen sets with nothing
    queued still complete at discovery end.
  - Behaviour change: after a rediscovery, such files show pending until their
    re-upload results arrive (two existing leaves now expect Uploaded/size 0
    and Failed 0 at discovery end; final counts unchanged).
  - Tests: unfrozen and directory-set leaves with one completion message, and
    a deleted file staying Missing. Mutants (no ShouldUpload check, no status
    exclusion, stamp not stored, file bucket only, callback before
    LastDiscovery, no size, shortcut ignoring Failed) fail. `make speed`
    passed (Upload +1.4%, Remove +0.3% vs ce92cae).
- [ ] A set already Complete completes again, sending another "completed
  backup" Slack message, when a queued result arrives while every file is
  counted: `checkIfComplete` has no already-Complete guard, and results for
  discovery-counted Missing or Orphaned entries replace their count. E.g. a
  set whose files are all missing completes at discovery ("due to no files"),
  then again for each missing result; a frozen set with kept files plus a
  missing file; an unfrozen set whose missing file's result comes last.
  Predates this branch. Code reading only.
  - Origin: found fixing the early-completion item (2026-10-07).
