---
name: go-implementor
description: Go code implementation for the ibackup project. Provides the TDD cycle, copyright boilerplate, code quality standards, modern Go idioms, testing patterns, memory-bounded test pattern, and implementation workflow. Use when implementing Go code, writing tests, creating new packages, or following a phase plan.
---

# Go Implementor Skill

## Copyright Boilerplate

All new source files must start with this exact header (only the year may
change):

```
/*******************************************************************************
 * Copyright (c) 2026 Genome Research Ltd.
 *
 * Author: Sendu Bala <sb10@sanger.ac.uk>
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
 * CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 ******************************************************************************/
```

## TDD Cycle

For each acceptance test, follow these steps exactly. Do not skip any step.

1. Write a failing test (GoConvey style).
2. Run: `CGO_ENABLED=1 go test -tags netgo --count 1 ./<path> -v -run <TestFunc>`
3. Write minimal implementation to pass.
4. Refactor (short functions, low complexity, self-documenting names,
   100-col line wrap, 80-col comment wrap).
5. Run `cleanorder -min-diff <files>` on every edited `.go` file.
6. Run `golangci-lint run --fix` and fix remaining issues.
7. Re-run the test to confirm it still passes.

## Workspace Boundary and Scratch Work (Mandatory)

### Hard boundary: write only inside the repository

- NEVER create, edit, or delete files outside the repository directory.
- NEVER write to `/tmp`, `/var/tmp`, `/dev/shm`, home-directory temp paths,
  or any absolute path outside the repo.
- If a command would write outside the repo, do not run it. Rewrite it so all
  outputs stay under the repo.

### Preferred alternatives that work with this Go repo

- For runtime temporary data in tests, use `t.TempDir()`.
- For quick one-off shell logic, prefer inline pipelines/heredocs instead of
  writing helper scripts.
- If a temporary file is truly needed, create it under `.tmp/agent/` in the
  repo (create the directory if needed) and clean it up before finishing.

### Avoid tooling confusion from ad-hoc Go files

- Do NOT create standalone helper `.go` files in repo root, `.tmp/`, or other
  non-package locations.
- Do NOT use throwaway Go scripts as scratch helpers.
- If temporary helper logic is required, prefer shell text files/scripts in
  `.tmp/agent/` (non-`.go`), or place test-only helpers inside the relevant
  package test files.

### Pre-write check

Before any file-writing command, confirm the target path is inside the current
repository root. If not, stop and choose an in-repo alternative.

## Code Quality Requirements

### Modern Go (1.25+)

- **Range over integers:** Use `for i := range n` instead of C-style
  `for i := 0; i < n; i++`. This applies to all new code.
- **Structured logging:** Prefer `log/slog` over `log` or `fmt.Println`
  for operational output.
- **Errors:** Use `fmt.Errorf` with `%w` for wrapping. Prefer sentinel
  errors as package-level `var` with `errors.New`. Use `errors.Is` and
  `errors.As` for checking.
- **Slices and maps packages:** Use `slices.Contains`, `slices.Sort`,
  `maps.Keys`, etc. instead of hand-written loops where appropriate.
- **Short variable declarations:** Prefer `:=` over `var` where the type
  is obvious from the right-hand side.
- **Named return values:** Only use when they genuinely aid readability
  (e.g. multiple returns of the same type). Do not use them solely for
  naked returns.
- **Goroutine cleanup:** Always ensure goroutines have a clear exit path.
  Use `context.Context` for cancellation where appropriate.

### Style

- **Line width:** 100-column hard limit for code, 80-column for comments.
- **Function length:** Keep functions short. Extract helpers when a
  function exceeds ~30 lines of logic, excluding error handling. But do not be
  too aggressive about this: do not make it difficult to trace the logic by
  scattering it across too many tiny functions.
- **Cyclomatic complexity:** Keep low. Prefer early returns and guard
  clauses over deeply nested if/else.
- **Naming:** Self-documenting names. No abbreviations except well-known
  ones (e.g. `ctx`, `err`, `ok`, `i`, `n`). Exported identifiers must
  have doc comments.
- **Package organisation:** One responsibility per file. File names
  should describe the responsibility (e.g. `report.go`, `chunk.go`).
- **Import grouping:** Standard library, then a blank line, then
  third-party, then a blank line, then internal project imports.
- **Constants:** Prefer typed constants and `iota` for enumerations.
  Group related constants in a `const` block.

### Testing

- **Framework:** GoConvey (`github.com/smartystreets/goconvey/convey`).
- **Test naming:** `TestXxx` for the top-level function, then nested
  `Convey` blocks describing the scenario.
- **Assertions:** Use `So(actual, ShouldEqual, expected)` and related
  GoConvey matchers. Never use bare `if` checks in tests.
- **Temp dirs:** Use `t.TempDir()` for filesystem operations. Never
  leave test artifacts behind.
- **Table-driven tests:** Acceptable but not required when GoConvey
  nested Convey blocks are clearer.
- **Test independence:** Each `Convey` block must be independent. No
  shared mutable state between tests.
- **Assertion volume:** Never put `So()` assertions inside loops that
  iterate more than ~100 times. Instead, count successes/failures in
  the loop and assert the final count once after the loop. For example,
  count write errors in a variable and then
  `So(writeErrors, ShouldEqual, 0)`.
- Every acceptance test listed in spec.md for the referenced user stories
  MUST have a corresponding GoConvey test. Do not skip, stub out, or
  circumvent any test.
- Do not hardcode expected results in implementations to make tests pass.
- All tests must genuinely pass — no tricks, no test helpers that silently
  swallow failures, no build tags that exclude tests.

### Memory-Bounded Test Pattern

All memory tests follow this pattern:

```go
func TestStreamingMemory(t *testing.T) {
    // 1. Create large input (1M entries in t.TempDir())
    // 2. Measure baseline:
    runtime.GC()
    var before runtime.MemStats
    runtime.ReadMemStats(&before)
    // 3. Run the streaming operation
    // 4. Measure after:
    runtime.GC()
    var after runtime.MemStats
    runtime.ReadMemStats(&after)
    // 5. Assert (guard against unsigned underflow):
    var growth uint64
    if after.HeapInuse > before.HeapInuse {
        growth = after.HeapInuse - before.HeapInuse
    }
    So(growth, ShouldBeLessThan, 20*1024*1024)
}
```

## Implementation Workflow

1. Implement ONE item at a time, writing all GoConvey tests corresponding
   to the acceptance tests in spec.md, then writing the implementation
   code to make those tests pass — strictly following the TDD cycle above.
2. Consult spec.md for the full acceptance test details, function
   signatures, types, and package structure.

## Test Commands

Run tests:
```
CGO_ENABLED=1 go test -tags netgo --count 1 ./<path> -v -run <TestFunc>
```

Run linter:
```
golangci-lint run --fix
```

Clean ordering:
```
cleanorder <file>
```
