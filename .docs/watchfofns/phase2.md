# Phase 2: Null-terminated scanning (internal/scanner/)

Ref: [spec.md](spec.md) section A1

## Instructions

Use the `orchestrator` skill to complete this phase, coordinating
subagents with the `go-implementor` and `go-reviewer` skills.

The memory-bounded test pattern is defined in the `go-implementor` skill.

## Items

### Item 2.1: A1 - scanner.ScanNullTerminated (streaming, memory test)

spec.md section: A1

Move the existing scanNulls and fofnLineSplitter from cmd/put.go to
internal/scanner/scanner.go. Provide ScanNullTerminated (callback-based) and
optionally CollectNullTerminated (convenience wrapper). Update cmd/put.go to
import from internal/scanner/. Write GoConvey tests in
internal/scanner/scanner_test.go covering all 7 acceptance tests from spec.md
section A1, including the memory-bounded test with 1,000,000 entries.

- [x] implemented
- [x] reviewed

