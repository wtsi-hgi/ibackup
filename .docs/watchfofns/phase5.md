# Phase 5: Put report output (cmd/)

Ref: [spec.md](spec.md) section D3

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

### Item 5.1: D3 - --report CLI flag for ibackup put

spec.md section: D3

Add --report flag to ibackup put that writes a machine-parsable per-file
status report using fofn.WriteReportEntry (from phase 1). Write GoConvey tests
in main_test.go covering all 2 acceptance tests from spec.md section D3. The
report must be round-trippable through fofn.CollectReport().

- [ ] implemented
- [ ] reviewed

## Workflow

1. The implementor implements the single item, writing all GoConvey tests
   corresponding to the acceptance tests in spec.md, then writing the
   implementation code to make those tests pass - strictly following the TDD
   cycle in spec.md (Appendix > "TDD cycle").
2. The implementor checks the "implemented" checkbox, then launches a
   **review subagent** — a separate AI subagent with clean context (no
   memory of implementation decisions) that performs the review.
3. The review subagent:
   - Reads spec.md for the referenced sections and the implemented source
     and test files.
   - Runs the tests (`CGO_ENABLED=1 go test -tags netgo --count 1 ...`).
   - Confirms every acceptance test from spec.md (both for D3) has a
     corresponding GoConvey test.
   - Confirms all tests pass without any tricks that provide false positive
     passes.
   - Confirms the report output uses the format defined in D1/D2 and is
     parseable by fofn.CollectReport().
   - Runs `golangci-lint run` and confirms it reports no issues.
   - Returns a verdict: PASS (checks the "reviewed" checkbox) or FAIL with
     specific feedback.
4. If the review subagent returns FAIL, the implementor addresses the
   feedback — including running `golangci-lint run --fix` and fixing any
   remaining lint issues — and re-does the complete TDD cycle as defined in
   spec.md (Appendix > "TDD cycle"), then re-launches a fresh review
   subagent. This cycle repeats until the review subagent returns PASS.
5. Once the item is marked "reviewed", this phase is complete.
