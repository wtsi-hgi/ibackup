# Phase 8: Directory scanning and group ownership

Ref: [spec.md](spec.md) sections G4, G1, G2, G3

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

## Items

Items marked "parallel" are independent of each other and MUST be implemented
concurrently using separate AI subagents — one subagent per item.

### Batch 1 (parallel)

#### Item 8.1: G4 - Group ownership (internal/ownership/)

spec.md section: G4

Implement GetDirGID, CreateDirWithGID, and WriteFileWithGID in
internal/ownership/ownership.go. Write GoConvey tests in
internal/ownership/ownership_test.go covering all 4 acceptance tests from
spec.md section G4.

- [ ] implemented
- [ ] reviewed

#### Item 8.2: G1 - Discover subdirectories with fofn files [parallel with 8.1, 8.3]

spec.md section: G1

Implement ScanForFOFNs in fofn/scan.go. Write GoConvey tests in
fofn/scan_test.go covering all 4 acceptance tests from spec.md section G1.

- [ ] implemented
- [ ] reviewed

#### Item 8.3: G2 - Parse and create config.yml [parallel with 8.1, 8.2]

spec.md section: G2

Implement ReadConfig, WriteConfig, and the SubDirConfig type (including
UserMetaString method) in fofn/config.go. Write GoConvey tests in
fofn/config_test.go covering all 13 acceptance tests from spec.md section G2.

- [ ] implemented
- [ ] reviewed

### Batch 2 (after batch 1 is reviewed)

#### Item 8.4: G3 - Detect fofn needing processing

spec.md section: G3

Depends on G1 (uses SubDir type). Implement NeedsProcessing in fofn/scan.go.
Write GoConvey tests in fofn/scan_test.go covering all 3 acceptance tests
from spec.md section G3.

- [ ] implemented
- [ ] reviewed

## Workflow

1. The implementor processes one batch at a time. **Parallel items within a
   batch MUST be implemented concurrently using separate subagents** — one
   subagent per item, each given the spec.md context and the item
   requirements. Each subagent writes all GoConvey tests corresponding to
   the acceptance tests in spec.md, then writes the implementation code to
   make those tests pass - strictly following the TDD cycle in spec.md
   (Appendix > "TDD cycle").
2. Once all subagents in the batch complete, the implementor checks the
   "implemented" checkbox for each item, then launches a **review
   subagent** — a separate AI subagent with clean context (no memory of
   implementation decisions) that reviews ALL items in the batch together.
3. The review subagent:
   - Reads spec.md for the referenced sections and all implemented source
     and test files in the batch.
   - Runs all tests (`CGO_ENABLED=1 go test -tags netgo --count 1 ...`).
   - Confirms every acceptance test from spec.md has a corresponding GoConvey
     test for each item.
   - Confirms all tests pass without any tricks that provide false positive
     passes.
   - Confirms the implementation follows the spec (correct packages, files,
     function signatures, YAML parsing, file permissions, GID handling).
   - Returns a verdict per item: PASS (checks the "reviewed" checkbox) or
     FAIL with specific feedback.
4. If the review subagent returns FAIL for any item, the implementor (or a
   fix subagent) addresses the feedback and re-launches a fresh review
   subagent. This cycle repeats until the review subagent returns PASS for
   all items in the batch.
5. Only after all items in the current batch are marked "reviewed" may the
   implementor proceed to the next batch.
6. Repeat until all items in this phase are implemented and reviewed.
