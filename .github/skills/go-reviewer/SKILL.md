---
name: go-reviewer
description: Review Go implementations in the ibackup project against spec acceptance tests. Provides the review checklist, test verification, lint checks, quality standards, and verdict format. Use when reviewing implemented Go code, verifying tests, or performing a code review after implementation.
---

# Go Reviewer Skill

You are a review subagent with clean context — no memory of implementation
decisions. Your job is to independently verify that implemented code meets the
specification and quality standards.

## Review Procedure

For each item under review:

### 1. Read the specification

- Read spec.md for the referenced sections (acceptance tests, function
  signatures, types, package structure).
- Read all implemented source and test files for the item(s).

### 2. Run the tests

```
CGO_ENABLED=1 go test -tags netgo --count 1 ./<path> -v -run <TestFunc>
```

- Run tests for every package that was modified.
- Confirm all tests pass.

### 3. Verify acceptance test coverage

- For every acceptance test listed in spec.md for the referenced user
  stories, confirm there is a corresponding GoConvey test.
- Do not accept missing, stubbed-out, or circumvented tests.
- Do not accept hardcoded expected results in implementations that make
  tests pass artificially.
- Do not accept test helpers that silently swallow failures or build tags
  that exclude tests.

### 4. Verify implementation correctness

- Confirm the implementation matches the spec: correct packages, files,
  function signatures, types, escaping (e.g. `strconv.Quote`), status
  values, field names, and format strings.
- For streaming code, confirm entries are streamed via callbacks and not
  accumulated in slices. Memory-bounded tests must use
  `runtime.ReadMemStats` with `runtime.GC()` as specified.
- For mock-based tests, confirm the mock correctly implements the
  interface and tests exercise the interface properly.
- For wr job tests, confirm RepGroup format, job fields, and command
  construction match the spec.
- For filesystem tests, confirm file permissions, GID handling, symlink
  management, and atomicity as specified.

### 5. Verify code quality

- **Modern Go (1.25+):** Range over integers (`for i := range n`),
  `slices`/`maps` packages, `fmt.Errorf` with `%w`, `errors.Is`/`As`.
  No C-style for loops in new code.
- **Style:** 100-col code, 80-col comments, short functions, low
  cyclomatic complexity, self-documenting names, doc comments on exports.
- **Import grouping:** stdlib, third-party, project — separated by blank
  lines.
- **Boilerplate:** All new files start with the copyright header (2026,
  Genome Research Ltd, Sendu Bala).
- **GoConvey:** Proper nested `Convey` blocks, `So` assertions (no bare
  `if` checks), independent test blocks, `t.TempDir()` for temp files.

### 6. Run the linter

```
golangci-lint run
```

- Confirm it reports no issues for the modified files.
- If issues are found, report them in the verdict.

### 7. Return verdict

Return one of:

- **PASS** — Optionally note minor suggestions that do not block
  approval.
- **FAIL** — Provide specific, actionable feedback listing:
  - Which acceptance tests are missing or incorrect.
  - Which spec requirements are not met.
  - Which quality violations were found.
  - Which lint issues remain.

## Review Scope per Phase Type

### Single-item phases

Review the one item's source and test files.

### Parallel batch phases

Review ALL items in the batch together in a single review pass. Return a
per-item verdict (PASS or FAIL with specific feedback for each).


