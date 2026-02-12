# Phase VC1: Variable chunk sizing

Ref: [spec-variable-chunks.md](spec-variable-chunks.md) sections VC0, VC1, VC2, VC3

## Instructions

Use the `orchestrator` skill to complete this phase, coordinating
subagents with the `go-implementor` and `go-reviewer` skills.

The memory-bounded test pattern is defined in the `go-implementor` skill.

## Items

Items marked "parallel" are independent of each other and MUST be implemented
concurrently using separate AI subagents — one subagent per item.

### Batch 1 (parallel)

**Note:** Both items in this batch modify `fofn/chunk.go` — Item 1.0 changes the
private `countEntries` function, while Item 1.1 adds new exported symbols
(`TargetChunks`, `CalculateChunks`). These edits touch different parts of the
file and should not conflict, but the orchestrator must apply them sequentially.

#### Item 1.0: VC0 - Fast entry counting [parallel with 1.1]

spec-variable-chunks.md section: VC0

Add `CountNullTerminated` to `internal/scanner/scanner.go`. The implementation
reads the file in large buffer chunks and uses `bytes.Count` to tally null bytes,
avoiding all per-entry string allocation. If the file is non-empty and does not
end with a null byte, the trailing content counts as one additional entry
(matching `ScanNullTerminated` semantics). Write GoConvey tests in
`internal/scanner/scanner_test.go` covering all 9 acceptance tests from
spec-variable-chunks.md section VC0, including the consecutive-null-bytes edge
case (test 9), the consistency check against `ScanNullTerminated` (test 7), and
the memory-bounded test with 1,000,000 entries (test 8).

After implementation, update `fofn/chunk.go`'s private `countEntries` function
to use `scanner.CountNullTerminated` instead of `scanner.ScanNullTerminated`
with a counting callback. All existing chunk tests must still pass.

- [x] implemented
- [x] reviewed

#### Item 1.1: VC1 - CalculateChunks pure function [parallel with 1.0]

spec-variable-chunks.md section: VC1

Add the `TargetChunks` constant (100) and implement the `CalculateChunks`
function in `fofn/chunk.go`. This is a pure function with no side effects and
no error return: it assumes valid inputs (minChunk >= 1, maxChunk >= 1,
minChunk <= maxChunk). Write GoConvey tests in `fofn/chunk_test.go` covering
all 26 acceptance tests from spec-variable-chunks.md section VC1, including:

- Individual named tests for each edge case and boundary value (tests 1-25).
- The comprehensive table-driven test (test 26) with the full worked-examples
  table (22 rows including n=501 and n=100000000).
- The degenerate min=max tests (tests 21-22) confirming equivalence with the
  old fixed chunkSize behaviour.

- [x] implemented
- [x] reviewed

### Batch 2 (after batch 1 is reviewed)

#### Item 1.2: VC2 - WriteShuffledChunks with variable sizing

spec-variable-chunks.md section: VC2

Depends on items 1.0 and 1.1. Change the `WriteShuffledChunks` signature to
accept `minChunk, maxChunk int` instead of `chunkSize int`. Add input
validation: return an error if `minChunk < 1`, `maxChunk < 1`, or
`minChunk > maxChunk`. Internally, use `scanner.CountNullTerminated` (from
item 1.0) for the count pass, then call `CalculateChunks(count, minChunk,
maxChunk)` (from item 1.1) to determine `numChunks`.

Update all existing B1 tests in `fofn/chunk_test.go` to use the new signature
(use `minChunk = maxChunk = <old chunkSize>` where deterministic behaviour is
needed). Add the 10 new acceptance tests from spec-variable-chunks.md
section VC2, including:

- End-to-end adaptive tests (tests 1-6): verify correct chunk count with
  various fofn sizes and min/max combinations. Note that test 4 uses a small
  201-entry fofn with min=max=2 to produce 101 chunks efficiently.
- Validation error tests (tests 7-9): minChunk=0, maxChunk=0, minChunk>maxChunk.
- Memory-bounded test (test 10): 1,000,000 entries with default bounds.

Ensure all existing tests still pass after the signature change.

- [x] implemented
- [x] reviewed

### Batch 3 (after batch 2 is reviewed)

#### Item 1.3: VC3 - Update callers (watcher, CLI, integration tests)

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

- [x] implemented
- [x] reviewed
