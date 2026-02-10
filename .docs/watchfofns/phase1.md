# Phase 1: Report and status format (fofn/report.go)

Ref: [spec.md](spec.md) sections D1, D2

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

These items MUST be implemented in the listed order. D2 depends on the types
and functions defined in D1.

### Item 1.1: D1 - Report line formatting and parsing

spec.md section: D1

Write FormatReportLine and ParseReportLine functions in fofn/report.go, with
GoConvey tests in fofn/report_test.go covering all 5 acceptance tests from
spec.md section D1.

- [ ] implemented
- [ ] reviewed

### Item 1.2: D2 - Streaming report write and callback read

spec.md section: D2

Write WriteReportEntry (streaming writer) and ParseReportCallback (streaming
callback reader) in fofn/report.go, with GoConvey tests in
fofn/report_test.go covering all 4 acceptance tests from spec.md section D2.
A CollectReport convenience wrapper may also be added for test use.

- [ ] implemented
- [ ] reviewed

## Workflow

1. The implementor implements ONE item, writing all GoConvey tests
   corresponding to the acceptance tests in spec.md, then writing the
   implementation code to make those tests pass - strictly following the TDD
   cycle in spec.md (Appendix > "TDD cycle").
2. The implementor checks the "implemented" checkbox for the completed item,
   then launches a **review subagent** — a separate AI subagent with clean
   context (no memory of implementation decisions) that performs the review.
3. The review subagent:
   - Reads spec.md for the referenced sections and the implemented source
     and test files.
   - Runs the tests (`CGO_ENABLED=1 go test -tags netgo --count 1 ...`).
   - Confirms every acceptance test from spec.md has a corresponding GoConvey
     test.
   - Confirms all tests pass without any tricks that provide false positive
     passes.
   - Confirms the implementation follows the spec (correct packages, files,
     function signatures, escaping via strconv.Quote).
   - Returns a verdict: PASS (checks the "reviewed" checkbox) or FAIL with
     specific feedback.
4. If the review subagent returns FAIL, the implementor addresses the
   feedback and re-launches a fresh review subagent. This cycle repeats
   until the review subagent returns PASS.
5. Only after the current item is marked "reviewed" may the implementor
   proceed to the next item.
6. Repeat until all items in this phase are implemented and reviewed.
