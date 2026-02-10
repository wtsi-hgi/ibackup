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
2. The implementor checks the "implemented" checkbox, then STOPS and waits
   for review.
3. A reviewer (who MUST be a separate entity from the implementor) reviews the
   work:
   - Confirms every acceptance test from spec.md (both for D3) has a
     corresponding GoConvey test.
   - Confirms all tests pass without any tricks that provide false positive
     passes.
   - Confirms the report output uses the format defined in D1/D2 and is
     parseable by fofn.CollectReport().
   - If satisfied, checks the "reviewed" checkbox.
   - If not satisfied, provides feedback. The implementor must address the
     feedback before the item can be marked reviewed.
4. Once the item is marked "reviewed", this phase is complete.
