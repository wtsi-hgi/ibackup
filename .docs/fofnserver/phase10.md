# Phase 10: CLI and integration (cmd/ + main_test.go)

Ref: [spec.md](spec.md) sections I1, J1

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
- Integration tests must use client.PretendSubmissions to avoid needing a real
  wr server, and internal.LocalHandler to avoid needing real iRODS, as
  described in spec.md (Appendix > "Testing strategy").

## Items

These items MUST be implemented in the listed order. J1 depends on I1.

### Item 10.1: I1 - ibackup fofnserver subcommand

spec.md section: I1

Implement the cobra subcommand in cmd/fofnserver.go with all flags listed in
spec.md section I1. Write GoConvey tests in main_test.go covering all 3
acceptance tests from spec.md section I1.

- [ ] implemented
- [ ] reviewed

### Item 10.2: J1 - End-to-end integration test

spec.md section: J1

Depends on I1. Write the full end-to-end integration test in main_test.go
using PretendSubmissions and LocalHandler. Cover all 5 acceptance tests from
spec.md section J1 (new fofn processing, freeze mode, restart resilience,
config.yml creation helper, buried jobs with fofn update).

- [ ] implemented
- [ ] reviewed

## Workflow

1. The implementor implements ONE item, writing all GoConvey tests
   corresponding to the acceptance tests in spec.md, then writing the
   implementation code to make those tests pass - strictly following the TDD
   cycle in spec.md (Appendix > "TDD cycle").
2. The implementor checks the "implemented" checkbox for the completed item,
   then STOPS and waits for review.
3. A reviewer (who MUST be a separate entity from the implementor) reviews the
   work:
   - Confirms every acceptance test from spec.md has a corresponding GoConvey
     test.
   - Confirms all tests pass without any tricks that provide false positive
     passes.
   - Confirms the CLI flags, validation, and signal handling match the spec.
   - For J1, confirms the integration test exercises the full pipeline and
     verifies all specified assertions (status file contents, group
     ownership, metadata flags, symlinks, etc.).
   - If satisfied, checks the "reviewed" checkbox.
   - If not satisfied, provides feedback. The implementor must address the
     feedback before the item can be marked reviewed.
4. Only after the current item is marked "reviewed" may the implementor
   proceed to the next item.
5. Repeat until all items in this phase are implemented and reviewed.
