# Bugfixes 2026-10-02: upload concurrency

- Branch: `bugfix-upload-concurrency-dbfef239a15f`
- Base: dependency `deps` at `24c0c9ab0123`; integration base `origin/develop`
  at `1727e87b34e8` (resolved 2026-10-02)
- Dependency: `deps` (PR not yet opened). Held for dependency merge: keep this
  branch local; don't push or open a PR until `deps` is merged and this
  branch is moved onto the updated `origin/develop`.
- Queue owner: `deps`, `.docs/bugfixes/260930-deps-update-702d9698d638.md`
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
- [ ] Possible: `baton/baton.go` `EnsureCollection` sends every concurrent
  caller's result back on one shared channel, so a caller may receive
  another collection's error. Spotted by reading the code; no failure seen.
  Not yet confirmed.
  - Origin item: "Possible: `baton/baton.go` `EnsureCollection`".
