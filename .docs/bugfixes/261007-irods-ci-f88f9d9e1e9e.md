# Bugfixes 2026-10-07: iRODS in CI

- Branch: `bugfix-irods-ci-1dee5dab`
- Base: `origin/develop` at `ce92caecdc7a` (#194 merged; rebased
  2026-10-07)
- Sequenced after test-collections for review and merge; no code
  dependency.
- Queue owner: `bugfix-set-counts-abca77d4de3c`,
  `.docs/bugfixes/261002-set-counts-abca77d4de3c.md`
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
- [ ] `main_test.go` `denyRemoteChanges` (added on `bugfix-set-counts-abca77d4de3c`,
  commit aab1a7d) runs `ichmod` with `user.Current().Username`, assuming the
  iRODS user name equals the OS user name; it will fail in CI's container zone
  (rodsuser `ibackup`, OS user `runner`). Switch it to `UserInfo()` once #195
  is merged under this branch.
  - Origin: found while making the tests run against a container zone
    (2026-10-08).
- [ ] `remove/remove.go` `UpdateSetsAndRequestersOnRemoteFile` builds
  `metaToRemove` from `meta[sets]` and `meta[requester]` without checking
  either is present, so an object lacking one of those AVUs gets a baton
  remove with an empty value: it removes the other AVU, then fails with
  "attr_value was empty" (-816000), and every retry fails the same way, so
  the object can never be removed or trashed. Verified with baton-do on the
  live zone; suspected cause of `TestRemove`'s stall in CI run 1. Deferred
  to the set-counts follow-up branch (removal code; needs #195).
  - Origin: found diagnosing PR #196 CI run 37754887894 (2026-10-08).
- [ ] Data race in `gopkg.in/tylerb/graceful.v1` v1.2.15 (graceful.go:399
  sets `srv.Server.ConnState = nil` on its kill path while net/http's
  `(*conn).setState` reads it), reached through `go-authserver` v1.6.0
  `(*Server).Start` → `graceful.ListenAndServeTLS` whenever ibackup's server
  stops with connections still open after the stop timeout. Fails `TestEdit`
  and `TestRemove` under `make race` in CI (run 37759317040), where slower
  stops hit the timeout more often. The fix belongs in go-authserver
  (replace graceful with `net/http`'s `Server.Shutdown`); needs a user
  decision since it is another repository.
  - Origin: found diagnosing PR #196 CI run 37759317040 (2026-10-08).
