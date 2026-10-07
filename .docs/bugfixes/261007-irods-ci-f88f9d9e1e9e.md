# Bugfixes 2026-10-07: iRODS in CI

- Branch: `bugfix-irods-ci-1dee5dab`
- Base: dependency `bugfix-tests-speedup-6d2903c9` (PR #194) at `13915cf`;
  integration base `origin/develop` at `43f73234a4d0` (resolved 2026-10-07)
- Dependency: PR #194. Held for dependency merge: keep this branch local;
  after #194 merges, rebase with `git rebase --onto origin/develop 13915cf`
  before pushing.
- Queue owner: `bugfix-tests-speedup-6d2903c9`,
  `.docs/bugfixes/261005-tests-speedup-31c758180c99.md`
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
