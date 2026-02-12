# Variable Chunk Sizing Specification

## Overview

Replace the fixed `--chunk-size` flag in `ibackup watchfofns` with adaptive
chunk sizing. Instead of the user specifying a single files-per-chunk value, the
system determines the optimal chunk count automatically based on the number of
files in the fofn and configurable minimum/maximum files-per-chunk bounds.

The goal is to target a constant number of chunks (100 by default) for every
fofn, producing roughly even-sized chunks. The system only deviates from 100
chunks when forced to by the minimum or maximum files-per-chunk constraints.

This keeps the number of wr jobs manageable (100 is easy to monitor) while
ensuring each job processes a reasonable number of files (not so few that job
overhead dominates, not so many that a single failure loses too much progress).

## Algorithm

### Constants and defaults

```go
const TargetChunks = 100 // ideal number of chunks
```

| CLI Flag          | Type | Default | Description                    |
|-------------------|------|---------|--------------------------------|
| `--min-chunk`     | int  | 250     | Minimum files per chunk        |
| `--max-chunk`     | int  | 10000   | Maximum files per chunk        |

The old `--chunk-size` flag is removed.

### Chunk count calculation

Given `n` files in the fofn, `minChunk`, `maxChunk`, and `TargetChunks`:

```
func CalculateChunks(n, minChunk, maxChunk int) (numChunks int)
```

The algorithm:

1. If `n == 0`, return 0.
2. Compute the ideal files per chunk: `ideal = ceil(n / TargetChunks)`.
3. Clamp `ideal` to `[minChunk, maxChunk]`, giving `perChunk`.
4. Compute `numChunks = ceil(n / perChunk)`.
5. Return `numChunks`.

In step 2, `ceil(n / TargetChunks)` is computed as `(n + TargetChunks - 1) /
TargetChunks` using integer arithmetic.

In step 4, `ceil(n / perChunk)` is computed as `(n + perChunk - 1) / perChunk`.

### Worked examples

| n (files) | minChunk | maxChunk | ideal (ceil(n/100)) | perChunk (clamped) | numChunks (ceil(n/perChunk)) |
|-----------|----------|----------|---------------------|--------------------|------------------------------|
| 0         | 250      | 10000    | -                   | -                  | 0                            |
| 1         | 250      | 10000    | 1                   | 250                | 1                            |
| 100       | 250      | 10000    | 1                   | 250                | 1                            |
| 249       | 250      | 10000    | 3                   | 250                | 1                            |
| 250       | 250      | 10000    | 3                   | 250                | 1                            |
| 251       | 250      | 10000    | 3                   | 250                | 2                            |
| 500       | 250      | 10000    | 5                   | 250                | 2                            |
| 1000      | 250      | 10000    | 10                  | 250                | 4                            |
| 10000     | 250      | 10000    | 100                 | 250                | 40                           |
| 24999     | 250      | 10000    | 250                 | 250                | 100                          |
| 25000     | 250      | 10000    | 250                 | 250                | 100                          |
| 25001     | 250      | 10000    | 251                 | 251                | 100                          |
| 50000     | 250      | 10000    | 500                 | 500                | 100                          |
| 100000    | 250      | 10000    | 1000                | 1000               | 100                          |
| 500000    | 250      | 10000    | 5000                | 5000               | 100                          |
| 999999    | 250      | 10000    | 10000               | 10000              | 100                          |
| 1000000   | 250      | 10000    | 10000               | 10000              | 100                          |
| 1000001   | 250      | 10000    | 10001               | 10000              | 101                          |
| 2000000   | 250      | 10000    | 20000               | 10000              | 200                          |
| 10000000  | 250      | 10000    | 100000              | 10000              | 1000                         |

Key observations from the table:
- For small fofns (n <= minChunk), we get 1 chunk.
- Between minChunk and minChunk * TargetChunks (25,000 at defaults), we get
  fewer than 100 chunks because the minimum constraint forces larger chunks
  than ideal.
- Between 25,000 and maxChunk * TargetChunks (1,000,000 at defaults), we get
  exactly 100 chunks.
- Above 1,000,000, we get more than 100 chunks because the maximum constraint
  prevents chunks from growing large enough.

## Changes

### `fofn/chunk.go`

#### New function: `CalculateChunks`

```go
const TargetChunks = 100

func CalculateChunks(n, minChunk, maxChunk int) int
```

Pure function. Returns the number of chunks as described in the algorithm above.

#### Modified function: `WriteShuffledChunks`

The signature changes from:

```go
func WriteShuffledChunks(
    fofnPath string,
    transform func(string) (string, error),
    dir string,
    chunkSize int,
    randSeed int64,
) ([]string, error)
```

to:

```go
func WriteShuffledChunks(
    fofnPath string,
    transform func(string) (string, error),
    dir string,
    minChunk, maxChunk int,
    randSeed int64,
) ([]string, error)
```

Internally, the first pass counts entries (as before), then calls
`CalculateChunks(count, minChunk, maxChunk)` to determine `numChunks`, then
streams entries to that many chunk files (as before).

### `fofn/watcher.go`

#### `ProcessSubDirConfig`

Replace `ChunkSize int` with `MinChunk int` and `MaxChunk int`.

#### `writeChunksWithGID`

Pass `minChunk, maxChunk` instead of `chunkSize` to `WriteShuffledChunks`.

### `cmd/watchfofns.go`

#### CLI flags

Remove the `--chunk-size` flag. Add:

| Flag          | Type | Default | Description             |
|---------------|------|---------|-------------------------|
| `--min-chunk` | int  | 250     | Minimum files per chunk |
| `--max-chunk` | int  | 10000   | Maximum files per chunk |

Remove:
- `defaultWatchChunkSize` constant.
- `watchChunkSize` variable.

Add:
- `defaultWatchMinChunk = 250` constant.
- `defaultWatchMaxChunk = 10000` constant.
- `watchMinChunk` variable.
- `watchMaxChunk` variable.

#### `createWatcher`

Pass `MinChunk: watchMinChunk` and `MaxChunk: watchMaxChunk` instead of
`ChunkSize: watchChunkSize`.

### `fofn/chunk_test.go`

All existing tests that call `WriteShuffledChunks` with a `chunkSize` argument
must be updated to pass `minChunk, maxChunk` instead. The existing acceptance
tests from spec.md section B1 remain valid; they just use `minChunk = maxChunk =
<old chunkSize>` to get the same deterministic behaviour.

New tests for `CalculateChunks` are added (see acceptance tests below).

### `fofn/watcher_test.go`

All existing tests that set `ChunkSize` in `ProcessSubDirConfig` must be updated
to set `MinChunk` and `MaxChunk` instead.

### `main_test.go`

All existing tests that set `ChunkSize` in config structs or pass `--chunk-size`
on the CLI must be updated to use `MinChunk`/`MaxChunk` and
`--min-chunk`/`--max-chunk` instead.

---

## Acceptance Tests

### VC1: CalculateChunks pure function

As a developer, I want a pure function that computes the optimal number of
chunks given a file count and min/max constraints, so that the chunk count
logic is independently testable.

**Package:** `fofn/`
**File:** `fofn/chunk.go`
**Test file:** `fofn/chunk_test.go`

**Acceptance tests:**

1. Given n=0, minChunk=250, maxChunk=10000, when I call
   `fofn.CalculateChunks(0, 250, 10000)`, then I get 0.

2. Given n=1, minChunk=250, maxChunk=10000, when I call
   `fofn.CalculateChunks(1, 250, 10000)`, then I get 1.

3. Given n=250, minChunk=250, maxChunk=10000, when I call
   `fofn.CalculateChunks(250, 250, 10000)`, then I get 1 (exactly minChunk
   files fits in 1 chunk).

4. Given n=251, minChunk=250, maxChunk=10000, when I call
   `fofn.CalculateChunks(251, 250, 10000)`, then I get 2 (spills into a
   second chunk).

5. Given n=500, minChunk=250, maxChunk=10000, when I call
   `fofn.CalculateChunks(500, 250, 10000)`, then I get 2 (exactly 2 chunks
   of 250).

6. Given n=10000, minChunk=250, maxChunk=10000, when I call
   `fofn.CalculateChunks(10000, 250, 10000)`, then I get 40
   (ideal=100, clamped to 250, ceil(10000/250)=40).

7. Given n=25000, minChunk=250, maxChunk=10000, when I call
   `fofn.CalculateChunks(25000, 250, 10000)`, then I get 100 (the sweet
   spot: ideal=250=minChunk, exactly 100 chunks).

8. Given n=50000, minChunk=250, maxChunk=10000, when I call
   `fofn.CalculateChunks(50000, 250, 10000)`, then I get 100 (ideal=500,
   in range, exactly 100 chunks).

9. Given n=100000, minChunk=250, maxChunk=10000, when I call
   `fofn.CalculateChunks(100000, 250, 10000)`, then I get 100 (ideal=1000,
   in range, exactly 100 chunks).

10. Given n=1000000, minChunk=250, maxChunk=10000, when I call
    `fofn.CalculateChunks(1000000, 250, 10000)`, then I get 100
    (ideal=10000=maxChunk, exactly 100 chunks).

11. Given n=1000001, minChunk=250, maxChunk=10000, when I call
    `fofn.CalculateChunks(1000001, 250, 10000)`, then I get 101 (ideal >
    maxChunk, forced to exceed 100 chunks).

12. Given n=2000000, minChunk=250, maxChunk=10000, when I call
    `fofn.CalculateChunks(2000000, 250, 10000)`, then I get 200.

13. Given n=10000000, minChunk=250, maxChunk=10000, when I call
    `fofn.CalculateChunks(10000000, 250, 10000)`, then I get 1000.

14. Given n=100, minChunk=250, maxChunk=10000, when I call
    `fofn.CalculateChunks(100, 250, 10000)`, then I get 1 (fewer than
    minChunk files, still 1 chunk).

15. Given n=249, minChunk=250, maxChunk=10000, when I call
    `fofn.CalculateChunks(249, 250, 10000)`, then I get 1 (just under
    minChunk, 1 chunk).

16. Given n=999999, minChunk=250, maxChunk=10000, when I call
    `fofn.CalculateChunks(999999, 250, 10000)`, then I get 100 (just under
    the threshold where maxChunk kicks in).

17. Given n=25001, minChunk=250, maxChunk=10000, when I call
    `fofn.CalculateChunks(25001, 250, 10000)`, then I get 100 (one above
    the minChunk boundary, ideal=251, perChunk=251, ceil(25001/251)=100).

18. Given custom bounds n=50, minChunk=10, maxChunk=20, when I call
    `fofn.CalculateChunks(50, 10, 20)`, then I get 5 (ideal=1, clamped to
    10, ceil(50/10)=5).

19. Given custom bounds n=2500, minChunk=10, maxChunk=20, when I call
    `fofn.CalculateChunks(2500, 10, 20)`, then I get 125 (ideal=25, clamped
    to 20, ceil(2500/20)=125).

20. Given custom bounds n=1500, minChunk=10, maxChunk=20, when I call
    `fofn.CalculateChunks(1500, 10, 20)`, then I get 100 (ideal=15, in
    range, exactly 100 chunks).

21. (**Table-driven comprehensive test**) Given the full worked-examples table
    above, when I call `fofn.CalculateChunks` for each row, then every result
    matches the expected `numChunks`.

### VC2: WriteShuffledChunks with variable sizing

As a developer, I want `WriteShuffledChunks` to accept min/max chunk parameters
instead of a fixed chunk size, so that the adaptive algorithm is used end-to-end.

**Package:** `fofn/`
**File:** `fofn/chunk.go`
**Test file:** `fofn/chunk_test.go`

**Acceptance tests:**

1. Given a fofn with 25 paths and minChunk=10, maxChunk=10, when I call
   `fofn.WriteShuffledChunks(fofnPath, transform, dir, 10, 10, 1)`, then 3
   chunk files are created (same as old behaviour with chunkSize=10).

2. Given a fofn with 50000 paths and minChunk=250, maxChunk=10000, when I call
   `fofn.WriteShuffledChunks(...)`, then exactly 100 chunk files are created
   (ideal=500, in range, exactly 100 chunks).

3. Given a fofn with 100 paths and minChunk=250, maxChunk=10000, when I call
   `fofn.WriteShuffledChunks(...)`, then 1 chunk file is created (all 100 files
   fit within minChunk).

4. Given a fofn with 2000000 paths and minChunk=250, maxChunk=10000, when I
   call `fofn.WriteShuffledChunks(...)`, then 200 chunk files are created
   (forced above 100 by maxChunk constraint).

5. Given an empty fofn, when I call `fofn.WriteShuffledChunks(...)` with any
   min/max values, then 0 chunk files are created and nil is returned.

6. (**Memory test**) Given a fofn with 1,000,000 null-terminated 100-byte paths,
   when I call `fofn.WriteShuffledChunks(...)` with minChunk=250,
   maxChunk=10000, then `runtime.MemStats.HeapInuse` does not increase by more
   than 20 MB above the baseline (proving entries are still streamed).

### VC3: Updated callers

As a developer, I want all callers of `WriteShuffledChunks` and
`ProcessSubDirConfig` to use the new min/max parameters, so that the old
`chunkSize` parameter is fully removed.

**Package:** `fofn/`, `cmd/`
**Files:** `fofn/watcher.go`, `cmd/watchfofns.go`
**Test files:** `fofn/watcher_test.go`, `main_test.go`

**Acceptance tests:**

1. Given a `ProcessSubDirConfig` with MinChunk=250, MaxChunk=10000, and a fofn
   with 50000 paths, when I call `fofn.ProcessSubDir(...)`, then 100 chunk
   files are created and 100 jobs are submitted.

2. Given `--min-chunk 500 --max-chunk 5000` on the CLI, when I run `ibackup
   watchfofns --help`, then the help output includes `--min-chunk` and
   `--max-chunk` flags and does not include `--chunk-size`.

3. Given the default flags (no `--min-chunk` or `--max-chunk` specified), when
   the watcher runs, then it uses minChunk=250 and maxChunk=10000.

---

## Implementation Order

This change modifies existing code and tests rather than creating new packages.
The implementation is a single phase.

Phase: See [phase-variable-chunks1.md](phase-variable-chunks1.md).
