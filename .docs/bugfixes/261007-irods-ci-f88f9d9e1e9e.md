# Bugfixes 2026-10-07: iRODS in CI

- Branch: `bugfix-irods-ci-1dee5dab`
- Base: `origin/develop` at `5bb2013bce57` (#195, #199, #197 and #200
  merged; rebased 2026-10-13)
- Sequenced after test-collections for review and merge; no code
  dependency.
- Queue owner: `bugfix-set-counts-2-586f032c7c79`,
  `.docs/bugfixes/261008-set-counts-2-586f032c7c79.md`
- Origin: PR #194 review thread on `internal/testutil/irods_prefetch_test.go:88`
  (comment 4204477916)

- [ ] Tests run against the live production iRODS zone, and CI has no iRODS,
  so the 17 iRODS-dependent main tests (and iRODS sub-package tests) are
  skipped in CI. Spin up iRODS in a container in CI (and locally where
  possible) so the full suite runs everywhere and with less latency.
  - Source: PR #194, mjkw31, comment 4204477916: "we really should not be
    using a live, production system for testing; it would be much better if
    we spun up iRODS in a container so we can test both locally and in the
    CI. It might also speed up the testing as it would take a lot of the
    latency out. The extendo CI has an example of how to do this IIRC."
    The user asked for it to be queued as its own branch (2026-10-07).
  - Notes from analysis: extendo's `.github/workflows/run-tests.yml` runs
    `ghcr.io/wtsi-npg/ub-22.04-irods-4.3.5` as a GitHub Actions service
    container (port 1247, health check), installs singularity-ce, puts the
    client image's icommands and `baton-do` on PATH via
    `singularity-wrapper -s install`, writes `irods_environment.json`
    (testZone, user irods, replResc) and runs `iinit`. ibackup also needs
    wr installed and a `development` manager, `IBACKUP_TEST_SCHEDULER`
    and `IBACKUP_TEST_COLLECTION`, and fixes to any Sanger-zone
    assumptions (groups for `ichmod`, other users, resources). The build
    host (farm22-wrstat01) has no docker/podman, only apptainer/singularity
    via modules, so local runs stay on the test zone for now. Estimate 1-3
    days.
  - Implemented (2026-10-08), not yet proven in CI: `tests.yml` (pinned
    ubuntu-24.04; server image 4.3.5 tag 9.8 by digest; singularity-ce 4.2.2
    with a real checksum check; client-image wrappers incl. baton 6.1.0 on
    PATH; admin `irods` creates rodsuser `ibackup`, tests run as it; wr
    v0.38.0 `development` manager; IBACKUP_TEST_SCHEDULER and
    IBACKUP_TEST_COLLECTION set; 120-minute job timeout; read-only
    permissions; bash with pipefail; `script -e` so a failed iinit fails).
    Tests: `testutil.UserInfo()` reads the iRODS user and groups, used by
    `TestTrashRemove`'s `ichmod` instead of the OS user name; test servers'
    stripped env passes `USER` (the wrappers need it). Live-zone runs pass
    unchanged. Local container validation was blocked (apptainer: the server
    hangs after one or two connections; no docker on the host).
  - Rebased onto develop `5bb2013bce57` (2026-10-13): develop's
    `denyRemoteChanges` helper replaced the inline `ichmod` blocks in
    `TestRemove` and `TestTrashRemove`, so the conflicts took develop's helper
    calls (its 30s removal wait already covered the widened wait) and kept
    `bufio`, which the helper uses. The container-zone fixes to those blocks
    move into the helper under the `denyRemoteChanges` item. `tests.yml` now
    installs wr v0.39.0 to match `go.mod` after #199. #200's collection
    sweep in `internal/testutil` merged without conflicts.
- [x] `main_test.go` `denyRemoteChanges` (added on `bugfix-set-counts-abca77d4de3c`,
  commit aab1a7d) runs `ichmod` with `user.Current().Username`, assuming the
  iRODS user name equals the OS user name; it will fail in CI's container zone
  (rodsuser `ibackup`, OS user `runner`). Switch it to `UserInfo()` once #195
  is merged under this branch.
  - Origin: found while making the tests run against a container zone
    (2026-10-08).
  - Red (2026-10-13): the OS and iRODS users are the same on the build
    host, so a throwaway harness (not committed) in package `main` put fake
    iCommands on `PATH`, with `iuserinfo` reporting iRODS user
    `ci_irods_user` in groups `ci_irods_user` and `public`, and recorded the
    `ichmod` calls of `denyRemoteChanges(t, "/zone/obj")` plus its restore:
    `CGO_ENABLED=1 go test -tags netgo -count 1 -timeout 10m -run
    TestScratchDenyRemoteChangesUsesIRODSUser .` exited 1:

    ```text
    Expected: []string{"read public /zone/obj", "read ci_irods_user /zone/obj", "own ci_irods_user /zone/obj"}
    Actual:   []string{"read [REDACTED-os-user] /zone/obj", "read ci_irods_user /zone/obj", "read public /zone/obj", "own [REDACTED-os-user] /zone/obj"}
    ```

    It also shows develop's helper downgrading the user before its groups
    and restoring with a plain `ichmod own`, which the container-zone fixes
    in `TestTrashRemove` had changed (user last, restore via `GrantOwn`,
    since iRODS 4.3 users cannot take back own access). The CI container-zone
    run (rodsuser `ibackup`, OS user `runner`) is the end-to-end red/green
    evidence once pushed.
  - Fix: `main_test.go` `denyRemoteChanges` takes the name and groups from
    `testutil.ICommander.UserInfo()`, downgrades the groups other than the
    user's own-name group first and the user last, and restores (explicitly
    and on Convey reset) with `GrantOwn`. Its callers are in `TestRemove`,
    `TestTrashRemove` (twice) and `TestEdit`.
    The harness above then passed (exit 0) on the final rebase.
  - Other OS-user uses checked after the rebase: no other test passes
    `user.Current()` or `$USER` to an iCommand. The remaining uses compare
    ibackup's requester metadata or set owner (`main_test.go`
    `TestTrashRemove` "different user" block and `TestStatus` requester
    output, `server/server_test.go` admin), local file ownership
    (`TestManualMode`), or the speed benchmark's set owner
    (`developers/speed/upload_test.go`); those are ibackup's local user names,
    not iRODS ones, so they stay.
  - Gates (2026-10-13/14, on `origin/develop` `5bb2013bce57` plus this
    branch, live test zone): `make lint` 0 issues; `make test` exit 0 (27
    main tests passed, including `TestRemove`, `TestTrashRemove` and
    `TestEdit`; all sub-packages ok); `make race` exit 0 (27 main tests
    passed, no data races; all sub-packages ok); `make speed` passed
    (BenchmarkUpload +0.8%, BenchmarkRemove +8.1%, both under the 10%
    limit; the diff is test-only, so the Remove change is noise).
  - Review: PASS (clean-context reviewer checked the rebase with
    range-diff (nothing lost from either side; wr v0.39.0 matches go.mod),
    reproduced the fake-iCommand red and green including the Convey reset
    restore, accepted CI's container zone as the end-to-end evidence, and
    re-ran make lint and make test).
- [ ] `remove/remove.go` `UpdateSetsAndRequestersOnRemoteFile` builds
  `metaToRemove` from `meta[sets]` and `meta[requester]` without checking
  either is present, so an object lacking one of those AVUs gets a baton
  remove with an empty value: it removes the other AVU, then fails with
  "attr_value was empty" (-816000), and every retry fails the same way, so
  the object can never be removed or trashed. Verified with baton-do on the
  live zone; suspected cause of `TestRemove`'s stall in CI run 1. Deferred
  to the set-counts follow-up branch (removal code; needs #195).
  - Origin: found diagnosing PR #196 CI run 37754887894 (2026-10-08).
- [x] Data race in `gopkg.in/tylerb/graceful.v1` v1.2.15 (graceful.go:399
  sets `srv.Server.ConnState = nil` on its kill path while net/http's
  `(*conn).setState` reads it), reached through `go-authserver` v1.6.0
  `(*Server).Start` → `graceful.ListenAndServeTLS` whenever ibackup's server
  stops with connections still open after the stop timeout. Fails `TestEdit`
  and `TestRemove` under `make race` in CI (run 37759317040), where slower
  stops hit the timeout more often. The fix belongs in go-authserver
  (replace graceful with `net/http`'s `Server.Shutdown`); needs a user
  decision since it is another repository.
  - Origin: found diagnosing PR #196 CI run 37759317040 (2026-10-08).
  - Decision (user, 2026-10-08): fork graceful and fix it there.
  - Fixed upstream of ibackup: wtsi-hgi/graceful#1 (module
    `github.com/wtsi-hgi/graceful`, released v1.2.16; red test
    `TestStopTimeoutWithActiveConnectionHasNoRace` reported the race 5/5 on
    v1.2.15) and wtsi-hgi/go-authserver#8 (released v1.6.1). ibackup now
    requires go-authserver v1.6.1, which pulls in the fork. CI's `make race`
    is the evidence here.
