# Phase 8: Directory scanning and group ownership

Ref: [spec.md](spec.md) sections G4, G1, G2, G3

## Instructions

Use the `orchestrator` skill to complete this phase, coordinating
subagents with the `go-implementor` and `go-reviewer` skills.

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

