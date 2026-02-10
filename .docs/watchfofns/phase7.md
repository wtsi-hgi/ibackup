# Phase 7: Status generation (fofn/status.go)

Ref: [spec.md](spec.md) section E1

## General Requirements

- Follow the TDD cycle defined in spec.md (Appendix > "TDD cycle") exactly,
  for every acceptance test. Do not skip any step.
- Every acceptance test listed in spec.md for the referenced user stories MUST
  have a corresponding GoConvey test. Do not skip, stub out, or circumvent any
  test. Do not hardcode expected results in implementations to make tests pass.
- Memory-bounded acceptance tests (E1 test 6) must use
  `runtime.ReadMemStats` with `runtime.GC()` before each measurement, as
  described in spec.md (Appendix > "Memory-bounded test pattern").
- All new source files must include the copyright boilerplate defined in
  spec.md (Appendix > "Boilerplate").
- All tests must genuinely pass - no tricks, no test helpers that silently
  swallow failures, no build tags that exclude tests.
- Consult spec.md for the full acceptance test details, function signatures,
  types, and package structure.

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
   - Confirms every acceptance test from spec.md (all 6 for E1) has a
     corresponding GoConvey test.
   - Confirms all tests pass without any tricks that provide false positive
     passes.
   - Confirms the implementation streams entries and does not accumulate them
     in memory.
   - Confirms the SUMMARY line format matches the spec (tab-separated
     key=value pairs for each status).
   - Confirms ParseStatus correctly returns both entries and counts.
   - Returns a verdict: PASS (checks the "reviewed" checkbox) or FAIL with
     specific feedback.
4. If the review subagent returns FAIL, the implementor addresses the
   feedback and re-launches a fresh review subagent. This cycle repeats
   until the review subagent returns PASS.
5. Once the item is marked "reviewed", this phase is complete.
