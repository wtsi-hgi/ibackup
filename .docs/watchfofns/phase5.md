# Phase 5: Put report output (cmd/)

Ref: [spec.md](spec.md) section D3

## Instructions

Use the `orchestrator` skill to complete this phase, coordinating
subagents with the `go-implementor` and `go-reviewer` skills.

## Items

### Item 5.1: D3 - --report CLI flag for ibackup put

spec.md section: D3

Add --report flag to ibackup put that writes a machine-parsable per-file
status report using fofn.WriteReportEntry (from phase 1). Write GoConvey tests
in main_test.go covering all 2 acceptance tests from spec.md section D3. The
report must be round-trippable through fofn.CollectReport().

- [x] implemented
- [x] reviewed

