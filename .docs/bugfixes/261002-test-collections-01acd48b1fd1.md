# Bugfixes 2026-10-02: test collections

- Branch: `bugfix-test-collections-01acd48b1fd1`
- Base: `origin/develop` at `ce92caecdc7a` (#194 merged; rebased
  2026-10-07)
- Sequenced after upload-concurrency for review and merge; no code
  dependency.
- Queue owner: `bugfix-set-counts-abca77d4de3c`,
  `.docs/bugfixes/261002-set-counts-abca77d4de3c.md`
- Origin: `deps`, `.docs/bugfixes/260930-deps-update-702d9698d638.md`

Each item is independent of the `deps` work: no `make test`, `make race` or
`make lint` run fails because of it. Found while fixing `deps` items.

- [ ] Test collections leak when a test process is killed or times out
  (t.Cleanup doesn't run), and each Convey leaf creates its own collection,
  so one killed run can leave dozens of `ibackup_test_*` collections in the
  shared base. The removed TestMain wipe (e4c7b83) used to hide this.
  Possible safe fix: in `RequireIRODSTestCollection`, remove only
  `ibackup_test_*` collections older than a cutoff far beyond the longest
  test run (e.g. 24h).
  - Origin item: "Test collections leak when a test process is killed".
  - Evidence: 32 leftover `ibackup_test_*` collections were found in the base
    on 2026-10-01 after killed runs.
