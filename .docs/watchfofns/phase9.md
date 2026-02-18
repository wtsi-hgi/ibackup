# Phase 9: Orchestration (fofn/watcher.go)

Ref: [spec.md](spec.md) sections H1, H2, H3, H4, H5

## Instructions

Use the `orchestrator` skill to complete this phase, coordinating
subagents with the `go-implementor` and `go-reviewer` skills.

- Tests must use a mock JobSubmitter as described in spec.md (Architecture >
  "Interfaces for testability" and Appendix > "Testing strategy").

## Items

These items MUST be implemented in the listed order, except where noted as
parallel.

### Item 9.1: H1 - Process single subdirectory end-to-end

spec.md section: H1

Implement ProcessSubDir in fofn/watcher.go. It reads config.yml, looks up the
named transformer, passes the transform function to WriteShuffledChunks, then
submits jobs with metadata flags. Write GoConvey tests in fofn/watcher_test.go
covering all 7 acceptance tests from spec.md section H1.

- [x] implemented
- [x] reviewed

### Item 9.2: H2 - Generate status after run completion

spec.md section: H2

Depends on H1. Implement GenerateStatus in fofn/watcher.go. It writes the
status file and creates the status symlink. Write GoConvey tests in
fofn/watcher_test.go covering all 4 acceptance tests from spec.md section H2.

- [x] implemented
- [x] reviewed

### Item 9.3: H3 - Handle fofn update while jobs running or buried

spec.md section: H3

Depends on H2. Implement the Watcher type with its Poll method and the poll
cycle logic described in spec.md section H3. Write GoConvey tests in
fofn/watcher_test.go covering all 8 acceptance tests from spec.md section H3.

- [x] implemented
- [x] reviewed

### Batch 1 (parallel, after item 9.3 is reviewed)

#### Item 9.4: H4 - Restart resilience

spec.md section: H4

Depends on H3. Implement startup detection of existing wr jobs from a
previous instance. Write GoConvey tests in fofn/watcher_test.go covering all
2 acceptance tests from spec.md section H4.

- [x] implemented
- [x] reviewed

#### Item 9.5: H5 - Parallel processing [parallel with 9.4]

spec.md section: H5

Depends on H3. Implement parallel processing of multiple subdirectories in a
single poll cycle. Write GoConvey tests in fofn/watcher_test.go covering all
2 acceptance tests from spec.md section H5.

- [x] implemented
- [x] reviewed

