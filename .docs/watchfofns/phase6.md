# Phase 6: wr job management (fofn/jobs.go)

Ref: [spec.md](spec.md) sections F1, F2, F3, F4, F5, F6

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
- Tests must use a mock JobSubmitter (defined in fofn/ test files) that records
  submitted jobs and allows tests to control job state responses. See spec.md
  (Architecture > "Interfaces for testability") for the JobSubmitter interface.

## Items

Items marked "parallel" are independent of each other and MAY be implemented
simultaneously (e.g. by an AI agent dispatching sub-agents).

### Batch 1 (parallel)

#### Item 6.1: F1 - Create put job commands

spec.md section: F1

Implement BuildPutCommand in fofn/jobs.go. Write GoConvey tests in
fofn/jobs_test.go covering all 4 acceptance tests from spec.md section F1.

- [ ] implemented
- [ ] reviewed

#### Item 6.2: F3 - Submit jobs via interface [parallel with 6.1, 6.3, 6.4]

spec.md section: F3

Define the mock JobSubmitter in test files and test SubmitJobs via the
interface. Write GoConvey tests in fofn/jobs_test.go covering all 2
acceptance tests from spec.md section F3.

- [ ] implemented
- [ ] reviewed

#### Item 6.3: F4 - Check run completion [parallel with 6.1, 6.2, 6.4]

spec.md section: F4

Implement IsRunComplete in fofn/jobs.go. Write GoConvey tests in
fofn/jobs_test.go covering all 2 acceptance tests from spec.md section F4.

- [ ] implemented
- [ ] reviewed

#### Item 6.4: F5 - Identify buried chunks [parallel with 6.1, 6.2, 6.3]

spec.md section: F5

Implement FindBuriedChunks in fofn/jobs.go. Write GoConvey tests in
fofn/jobs_test.go covering all 2 acceptance tests from spec.md section F5.

- [ ] implemented
- [ ] reviewed

### Batch 2 (parallel, after batch 1 is reviewed)

#### Item 6.5: F2 - Create wr jobs for chunks

spec.md section: F2

Depends on F1 (uses BuildPutCommand). Implement CreateJobs in fofn/jobs.go.
Write GoConvey tests in fofn/jobs_test.go covering all 3 acceptance tests
from spec.md section F2.

- [ ] implemented
- [ ] reviewed

#### Item 6.6: F6 - Delete buried jobs [parallel with 6.5]

spec.md section: F6

Depends on F5. Implement DeleteBuriedJobs in fofn/jobs.go. Write GoConvey
tests in fofn/jobs_test.go covering all 2 acceptance tests from spec.md
section F6.

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
   - Confirms the mock JobSubmitter is correctly structured and tests exercise
     the interface properly.
   - Confirms the implementation follows the spec (correct RepGroup format,
     job fields, command construction).
   - If satisfied, checks the "reviewed" checkbox for each item in the batch.
   - If not satisfied, provides feedback. The implementor must address the
     feedback before the items can be marked reviewed.
4. Only after all items in the current batch are marked "reviewed" may the
   implementor proceed to the next batch.
5. Repeat until all items in this phase are implemented and reviewed.
