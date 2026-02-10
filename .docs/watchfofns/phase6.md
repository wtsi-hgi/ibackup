# Phase 6: wr job management (fofn/jobs.go)

Ref: [spec.md](spec.md) sections F1, F2, F3, F4, F5, F6

## Instructions

Use the `orchestrator` skill to complete this phase, coordinating
subagents with the `go-implementor` and `go-reviewer` skills.

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

