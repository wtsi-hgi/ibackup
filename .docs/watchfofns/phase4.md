# Phase 4: Put enhancements (transfer/ + cmd/)

Ref: [spec.md](spec.md) sections C1, C2, C3, C4, C5

## Instructions

Use the `orchestrator` skill to complete this phase, coordinating
subagents with the `go-implementor` and `go-reviewer` skills.

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

For parallel batch items, use separate subagents per item.
Launch review subagents using the `go-reviewer` skill (review all items
in the batch together in a single review pass).
