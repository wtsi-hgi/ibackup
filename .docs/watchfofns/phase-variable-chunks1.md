# Phase VC1: Variable chunk sizing (fofn/chunk.go + callers)

Ref: [spec-variable-chunks.md](spec-variable-chunks.md) sections VC1, VC2, VC3

## Instructions

Use the `orchestrator` skill to complete this phase, coordinating
subagents with the `go-implementor` and `go-reviewer` skills.

The memory-bounded test pattern is defined in the `go-implementor` skill.

## Items

These items MUST be implemented in the listed order, except where noted as
parallel.

### Item 1.1: VC1 - CalculateChunks pure function

spec-variable-chunks.md section: VC1

Add the `TargetChunks` constant (100) and implement the `CalculateChunks`
function in `fofn/chunk.go`. This is a pure function with no side effects. Write
GoConvey tests in `fofn/chunk_test.go` covering all 21 acceptance tests from
spec-variable-chunks.md section VC1, including the comprehensive table-driven
test with the full worked-examples table.

- [ ] implemented
- [ ] reviewed

### Item 1.2: VC2 - WriteShuffledChunks with variable sizing

spec-variable-chunks.md section: VC2

Depends on item 1.1. Change the `WriteShuffledChunks` signature to accept
`minChunk, maxChunk int` instead of `chunkSize int`. Internally, after counting
entries, call `CalculateChunks(count, minChunk, maxChunk)` to determine
`numChunks`. Update all existing B1 tests in `fofn/chunk_test.go` to use the
new signature (use `minChunk = maxChunk = <old chunkSize>` where deterministic
behaviour is needed). Add the 6 new acceptance tests from spec-variable-chunks.md
section VC2, including the memory-bounded test. Ensure all existing tests still
pass.

- [ ] implemented
- [ ] reviewed

### Item 1.3: VC3 - Update callers (watcher, CLI, integration tests)

spec-variable-chunks.md section: VC3

Depends on item 1.2. Update all callers of `WriteShuffledChunks` and
`ProcessSubDirConfig`:

1. **`fofn/watcher.go`**: Replace `ChunkSize int` with `MinChunk int` and
   `MaxChunk int` in `ProcessSubDirConfig`. Update `writeChunksWithGID` and
   `prepareChunks` to pass `minChunk, maxChunk` to `WriteShuffledChunks`.

2. **`fofn/watcher_test.go`**: Update all tests that set `ChunkSize` in
   `ProcessSubDirConfig` to set `MinChunk` and `MaxChunk` instead.

3. **`cmd/watchfofns.go`**: Remove `--chunk-size` flag, `defaultWatchChunkSize`
   constant, and `watchChunkSize` variable. Add `--min-chunk` and `--max-chunk`
   flags with defaults 250 and 10000. Update `createWatcher` to pass
   `MinChunk` and `MaxChunk`.

4. **`main_test.go`**: Update all tests that reference `ChunkSize` or
   `--chunk-size` to use the new fields/flags.

Write GoConvey tests covering the 3 acceptance tests from spec-variable-chunks.md
section VC3. Ensure all existing tests still pass.

- [ ] implemented
- [ ] reviewed
