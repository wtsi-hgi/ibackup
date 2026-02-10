# Phase 4: Put enhancements (transfer/ + cmd/)

Ref: [spec.md](spec.md) sections C1, C2, C3, C4, C5

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

#### Item 4.1: C1 - RequestStatusFrozen constant

spec.md section: C1

Add RequestStatusFrozen constant with string value "frozen" to the transfer
package. Write a GoConvey test in transfer/request_test.go covering the 1
acceptance test from spec.md section C1.

- [ ] implemented
- [ ] reviewed

#### Item 4.2: C4 - MetaKeyFofn constant [parallel with 4.1]

spec.md section: C4

Add MetaKeyFofn constant with string value "ibackup:fofn" to the transfer
package. Write a GoConvey test in transfer/meta_test.go covering the 1
acceptance test from spec.md section C4.

- [ ] implemented
- [ ] reviewed

### Batch 2 (parallel, after batch 1 is reviewed)

#### Item 4.3: C2 - Putter no-replace mode

spec.md section: C2

Depends on C1. Add SetNoReplace method to Putter. When enabled, files that
already exist in iRODS with a different mtime are skipped with "frozen" status
instead of re-uploaded. Write GoConvey tests in transfer/put_test.go covering
all 4 acceptance tests from spec.md section C2.

- [ ] implemented
- [ ] reviewed

#### Item 4.4: C5 - --fofn CLI flag [parallel with 4.3]

spec.md section: C5

Depends on C4. Add --fofn flag to ibackup put that applies ibackup:fofn
metadata to every uploaded file. Write GoConvey tests in main_test.go covering
all 2 acceptance tests from spec.md section C5.

- [ ] implemented
- [ ] reviewed

### Batch 3 (after batch 2 is reviewed)

#### Item 4.5: C3 - --no_replace CLI flag

spec.md section: C3

Depends on C2. Add --no_replace flag to ibackup put CLI. Write GoConvey tests
in main_test.go covering all 2 acceptance tests from spec.md section C3.

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
     function signatures, status values).
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
