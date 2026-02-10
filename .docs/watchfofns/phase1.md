# Phase 1: Report and status format (fofn/report.go)

Ref: [spec.md](spec.md) sections D1, D2

## Instructions

Use the `orchestrator` skill to complete this phase, coordinating
`go-implementor` and `go-reviewer` subagents.

## Items

These items MUST be implemented in the listed order. D2 depends on the types
and functions defined in D1.

### Item 1.1: D1 - Report line formatting and parsing

spec.md section: D1

Write FormatReportLine and ParseReportLine functions in fofn/report.go, with
GoConvey tests in fofn/report_test.go covering all 5 acceptance tests from
spec.md section D1.

- [x] implemented
- [x] reviewed

### Item 1.2: D2 - Streaming report write and callback read

spec.md section: D2

Write WriteReportEntry (streaming writer) and ParseReportCallback (streaming
callback reader) in fofn/report.go, with GoConvey tests in
fofn/report_test.go covering all 4 acceptance tests from spec.md section D2.
A CollectReport convenience wrapper may also be added for test use.

- [x] implemented
- [x] reviewed

