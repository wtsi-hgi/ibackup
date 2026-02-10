# Phase 5: Put report output (cmd/)

Ref: [spec.md](spec.md) section D3

## General Requirements

Use the `go-implementor` skill for TDD cycle, boilerplate, code quality
standards, and implementation workflow.

Use the `go-reviewer` skill when launching review subagents.

## Items

### Item 5.1: D3 - --report CLI flag for ibackup put

spec.md section: D3

Add --report flag to ibackup put that writes a machine-parsable per-file
status report using fofn.WriteReportEntry (from phase 1). Write GoConvey tests
in main_test.go covering all 2 acceptance tests from spec.md section D3. The
report must be round-trippable through fofn.CollectReport().

- [ ] implemented
- [ ] reviewed

## Workflow

Follow the implementation workflow in the `go-implementor` skill.
Launch review subagents using the `go-reviewer` skill.
