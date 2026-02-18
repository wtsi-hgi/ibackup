# Phase 3: Streaming shuffle and chunking (fofn/chunk.go)

Ref: [spec.md](spec.md) section B1

## Instructions

Use the `orchestrator` skill to complete this phase, coordinating
subagents with the `go-implementor` and `go-reviewer` skills.

The memory-bounded test pattern is defined in the `go-implementor` skill.

## Items

### Item 3.1: B1 - fofn.WriteShuffledChunks (streaming, memory test)

spec.md section: B1

Implement WriteShuffledChunks in fofn/chunk.go. It uses
scanner.ScanNullTerminated internally to stream entries, applies a transform
function, and writes base64-encoded local/remote pairs to chunk files. Write
GoConvey tests in fofn/chunk_test.go covering all 7 acceptance tests from
spec.md section B1, including the deterministic seed test and the
memory-bounded test with 1,000,000 entries.

- [x] implemented
- [x] reviewed

