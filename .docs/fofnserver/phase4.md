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

Items marked "parallel" are independent of each other and MAY be implemented
simultaneously (e.g. by an AI agent dispatching sub-agents).

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
     function signatures, status values).
   - If satisfied, checks the "reviewed" checkbox for each item in the batch.
   - If not satisfied, provides feedback. The implementor must address the
     feedback before the items can be marked reviewed.
4. Only after all items in the current batch are marked "reviewed" may the
   implementor proceed to the next batch.
5. Repeat until all items in this phase are implemented and reviewed.
