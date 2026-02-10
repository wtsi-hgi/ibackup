# Phase 9: Orchestration (fofn/watcher.go)

Ref: [spec.md](spec.md) sections H1, H2, H3, H4, H5

## General Requirements

- Follow the TDD cycle defined in spec.md (Appendix > "TDD cycle") exactly,
  for every acceptance test. Do not skip any step.
- Every acceptance test listed in spec.md for the referenced user stories MUST
  have a corresponding GoConvey test. Do not skip, stub out, or circumvent any
  test. Do not hardcode expected results in implementations to make tests pass.
- All new source files must include the copyright boilerplate defined in
  spec.md (Appendix > "Boilerplate").
- All tests must genuinely pass - no tricks, no test helpers that silently
  swallow failures, no build tags that exclude tests.
- Consult spec.md for the full acceptance test details, function signatures,
  types, and package structure.
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

- [ ] implemented
- [ ] reviewed

### Item 9.2: H2 - Generate status after run completion

spec.md section: H2

Depends on H1. Implement GenerateStatus in fofn/watcher.go. It writes the
status file and creates the status symlink. Write GoConvey tests in
fofn/watcher_test.go covering all 4 acceptance tests from spec.md section H2.

- [ ] implemented
- [ ] reviewed

### Item 9.3: H3 - Handle fofn update while jobs running or buried

spec.md section: H3

Depends on H2. Implement the Watcher type with its Poll method and the poll
cycle logic described in spec.md section H3. Write GoConvey tests in
fofn/watcher_test.go covering all 8 acceptance tests from spec.md section H3.

- [ ] implemented
- [ ] reviewed

### Batch 1 (parallel, after item 9.3 is reviewed)

#### Item 9.4: H4 - Restart resilience

spec.md section: H4

Depends on H3. Implement startup detection of existing wr jobs from a
previous instance. Write GoConvey tests in fofn/watcher_test.go covering all
2 acceptance tests from spec.md section H4.

- [ ] implemented
- [ ] reviewed

#### Item 9.5: H5 - Parallel processing [parallel with 9.4]

spec.md section: H5

Depends on H3. Implement parallel processing of multiple subdirectories in a
single poll cycle. Write GoConvey tests in fofn/watcher_test.go covering all
2 acceptance tests from spec.md section H5.

- [ ] implemented
- [ ] reviewed

## Workflow

1. The implementor implements one item (or a batch of parallel items) at a
   time. For each item, write all GoConvey tests corresponding to the
   acceptance tests in spec.md, then write the implementation code to make
   those tests pass - strictly following the TDD cycle in spec.md
   (Appendix > "TDD cycle").
2. The implementor checks the "implemented" checkbox for each completed item,
   then STOPS and waits for review.
3. A reviewer (who MUST be a separate entity from the implementor) reviews the
   work:
   - Confirms every acceptance test from spec.md has a corresponding GoConvey
     test.
   - Confirms all tests pass without any tricks that provide false positive
     passes.
   - Confirms the mock JobSubmitter is used correctly and tests exercise the
     full poll cycle logic.
   - Confirms the implementation follows the spec (streaming, group
     ownership, symlink management, poll cycle state transitions).
   - If satisfied, checks the "reviewed" checkbox.
   - If not satisfied, provides feedback. The implementor must address the
     feedback before the item can be marked reviewed.
4. Only after the current item (or batch) is marked "reviewed" may the
   implementor proceed to the next item(s).
5. Repeat until all items in this phase are implemented and reviewed.
