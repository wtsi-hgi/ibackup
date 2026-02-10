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

Items marked "parallel" are independent of each other and MAY be implemented
simultaneously (e.g. by an AI agent dispatching sub-agents).

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

1. The implementor implements one batch of items at a time. For parallel
   items within a batch, they may be implemented simultaneously. For each
   item, write all GoConvey tests corresponding to the acceptance tests in
   spec.md, then write the implementation code to make those tests pass -
   strictly following the TDD cycle in spec.md (Appendix > "TDD cycle").
2. The implementor checks the "implemented" checkbox for each completed item
   in the batch, then STOPS and waits for review.
3. A reviewer (who MUST be a separate entity from the implementor) reviews the
   work:
   - Confirms every acceptance test from spec.md has a corresponding GoConvey
     test.
   - Confirms all tests pass without any tricks that provide false positive
     passes.
   - Confirms the implementation follows the spec (correct packages, files,
     function signatures, YAML parsing, file permissions, GID handling).
   - If satisfied, checks the "reviewed" checkbox for each item in the batch.
   - If not satisfied, provides feedback. The implementor must address the
     feedback before the items can be marked reviewed.
4. Only after all items in the current batch are marked "reviewed" may the
   implementor proceed to the next batch.
5. Repeat until all items in this phase are implemented and reviewed.
