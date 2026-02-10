# Phase 2: Null-terminated scanning (internal/scanner/)

Ref: [spec.md](spec.md) section A1

## General Requirements

- Follow the TDD cycle defined in spec.md (Appendix > "TDD cycle") exactly,
  for every acceptance test. Do not skip any step.
- Every acceptance test listed in spec.md for the referenced user stories MUST
  have a corresponding GoConvey test. Do not skip, stub out, or circumvent any
  test. Do not hardcode expected results in implementations to make tests pass.
- Memory-bounded acceptance tests (A1 test 7) must use
  `runtime.ReadMemStats` with `runtime.GC()` before each measurement, as
  described in spec.md (Appendix > "Memory-bounded test pattern").
- All new source files must include the copyright boilerplate defined in
  spec.md (Appendix > "Boilerplate").
- All tests must genuinely pass - no tricks, no test helpers that silently
  swallow failures, no build tags that exclude tests.
- Consult spec.md for the full acceptance test details, function signatures,
  types, and package structure.

## Items

### Item 2.1: A1 - scanner.ScanNullTerminated (streaming, memory test)

spec.md section: A1

Move the existing scanNulls and fofnLineSplitter from cmd/put.go to
internal/scanner/scanner.go. Provide ScanNullTerminated (callback-based) and
optionally CollectNullTerminated (convenience wrapper). Update cmd/put.go to
import from internal/scanner/. Write GoConvey tests in
internal/scanner/scanner_test.go covering all 7 acceptance tests from spec.md
section A1, including the memory-bounded test with 1,000,000 entries.

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
   - Confirms every acceptance test from spec.md (all 7 for A1) has a
     corresponding GoConvey test.
   - Confirms all tests pass without any tricks that provide false positive
     passes.
   - Confirms the implementation streams entries via callback and does not
     accumulate them in a slice.
   - Confirms cmd/put.go now imports from internal/scanner/ instead of using
     local unexported functions.
   - If satisfied, checks the "reviewed" checkbox.
   - If not satisfied, provides feedback. The implementor must address the
     feedback before the item can be marked reviewed.
4. Once the item is marked "reviewed", this phase is complete.
