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

Items marked "parallel" are independent of each other and MUST be implemented
concurrently using separate AI subagents — one subagent per item.

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
   - Confirms the mock JobSubmitter is correctly structured and tests exercise
     the interface properly.
   - Confirms the implementation follows the spec (correct RepGroup format,
     job fields, command construction).
   - Runs `golangci-lint run` and confirms it reports no issues.
   - Returns a verdict per item: PASS (checks the "reviewed" checkbox) or
     FAIL with specific feedback.
4. If the review subagent returns FAIL for any item, the implementor (or a
   fix subagent) addresses the feedback — including running
   `golangci-lint run --fix` and fixing any remaining lint issues — and
   re-does the complete TDD cycle as defined in spec.md (Appendix > "TDD
   cycle"), then re-launches a fresh review subagent. This cycle repeats
   until the review subagent returns PASS for all items in the batch.
5. Only after all items in the current batch are marked "reviewed" may the
   implementor proceed to the next batch.
6. Repeat until all items in this phase are implemented and reviewed.
