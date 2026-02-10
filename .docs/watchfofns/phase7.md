# Phase 7: Status generation (fofn/status.go)

Ref: [spec.md](spec.md) section E1

## General Requirements

Use the `go-implementor` skill for TDD cycle, boilerplate, code quality
standards, memory-bounded test pattern, and implementation workflow.

Use the `go-reviewer` skill when launching review subagents.

## Items

### Item 7.1: E1 - fofn.WriteStatusFromRun (streaming, memory test)

spec.md section: E1

Implement WriteStatusFromRun and ParseStatus in fofn/status.go. The
implementation should stream entries from report files via
ParseReportCallback (from phase 1) and write them to the status file as it
goes, tallying counts, then append a SUMMARY line. For buried chunks, read
the chunk file to identify files that were not reported and mark them as
"not_processed". Write GoConvey tests in fofn/status_test.go covering all 6
acceptance tests from spec.md section E1, including the memory-bounded test
with 1,000,000 entries.

- [ ] implemented
- [ ] reviewed

## Workflow

Follow the implementation workflow in the `go-implementor` skill.
Launch review subagents using the `go-reviewer` skill.
