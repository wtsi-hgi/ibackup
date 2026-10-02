# Bugfixes 2026-10-02: set counts

- Branch: `bugfix-set-counts-abca77d4de3c`
- Base: dependency `deps` at `24c0c9ab0123`; integration base `origin/develop`
  at `1727e87b34e8` (resolved 2026-10-02)
- Dependency: `deps` (PR not yet opened). Held for dependency merge: keep this
  branch local; don't push or open a PR until `deps` is merged and this
  branch is moved onto the updated `origin/develop`.
- Queue owner: `deps`, `.docs/bugfixes/260930-deps-update-702d9698d638.md`
- Origin: `deps`, `.docs/bugfixes/260930-deps-update-702d9698d638.md`

Each item is independent of the `deps` work: no `make test`, `make race` or
`make lint` run fails because of it. Found while fixing `deps` items.

- [ ] Frozen sets: when a frozen set's uploaded file is deleted locally,
  discovery counts it Orphaned but keeps the old entry (status Uploaded), so
  removing it leaves `Orphaned: 1` with `Num files: 0`. At `origin/develop` it
  is worse (removal wraps Uploaded). Needs a decision on what frozen-set
  counts should be.
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
- [ ] If a removal's inode cleanup fails, the entry is already deleted but
  not counted as removed; retries then fail with "set ... has no path", so
  the set error shows that instead of the inode error, and Num files keeps
  counting the file. Same at `origin/develop`.
  - Origin item: "If a removal's inode cleanup fails" (found reviewing the
    TestSync fix; see `server/setdb.go` `processDBFileRemoval`).
