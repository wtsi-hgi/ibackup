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

To handle very large fofns (tens of millions of paths) efficiently, the counting
pass that determines the total number of entries uses a max-speed byte-counting
approach — counting null terminators directly without allocating strings or
invoking per-entry callbacks. This avoids the overhead of
`scanner.ScanNullTerminated` during the count-only first pass while retaining
identical semantics for determining the entry count.

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

### Preconditions

`CalculateChunks` is a pure function that assumes valid inputs. Callers must
ensure:

- `minChunk >= 1`
- `maxChunk >= 1`
- `minChunk <= maxChunk`

`WriteShuffledChunks` validates these preconditions and returns an error if any
condition is violated. The CLI layer should also reject invalid flag
combinations, but the library function must be independently safe.

### Worked examples

| n (files)   | minChunk | maxChunk | ideal (ceil(n/100)) | perChunk (clamped) | numChunks (ceil(n/perChunk)) |
|-------------|----------|----------|---------------------|--------------------|------------------------------|
| 0           | 250      | 10000    | -                   | -                  | 0                            |
| 1           | 250      | 10000    | 1                   | 250                | 1                            |
| 100         | 250      | 10000    | 1                   | 250                | 1                            |
| 249         | 250      | 10000    | 3                   | 250                | 1                            |
| 250         | 250      | 10000    | 3                   | 250                | 1                            |
| 251         | 250      | 10000    | 3                   | 250                | 2                            |
| 500         | 250      | 10000    | 5                   | 250                | 2                            |
| 501         | 250      | 10000    | 6                   | 250                | 3                            |
| 1000        | 250      | 10000    | 10                  | 250                | 4                            |
| 10000       | 250      | 10000    | 100                 | 250                | 40                           |
| 24999       | 250      | 10000    | 250                 | 250                | 100                          |
| 25000       | 250      | 10000    | 250                 | 250                | 100                          |
| 25001       | 250      | 10000    | 251                 | 251                | 100                          |
| 50000       | 250      | 10000    | 500                 | 500                | 100                          |
| 100000      | 250      | 10000    | 1000                | 1000               | 100                          |
| 500000      | 250      | 10000    | 5000                | 5000               | 100                          |
| 999999      | 250      | 10000    | 10000               | 10000              | 100                          |
| 1000000     | 250      | 10000    | 10000               | 10000              | 100                          |
| 1000001     | 250      | 10000    | 10001               | 10000              | 101                          |
| 2000000     | 250      | 10000    | 20000               | 10000              | 200                          |
| 10000000    | 250      | 10000    | 100000              | 10000              | 1000                         |
| 100000000   | 250      | 10000    | 1000000             | 10000              | 10000                        |

Key observations from the table:
- For small fofns (n ≤ minChunk), we get 1 chunk.
- Between minChunk and minChunk × TargetChunks (25,000 at defaults), we get
  fewer than 100 chunks because the minimum constraint forces each chunk to
  contain at least minChunk files.
- Between 25,000 and maxChunk × TargetChunks (1,000,000 at defaults), we get
  exactly 100 chunks — the ideal operating range.
- Above 1,000,000, we get more than 100 chunks because the maximum constraint
  prevents chunks from growing large enough to keep the count at 100.
- When minChunk = maxChunk, the algorithm degenerates to the old fixed
  chunkSize behaviour: `numChunks = ceil(n / chunkSize)`.

## Changes

### `internal/scanner/scanner.go`

#### New function: `CountNullTerminated`

```go
func CountNullTerminated(path string) (int, error)
```

Counts the number of null-terminated entries in a file by scanning for null
bytes directly, without parsing entries into strings. The implementation reads
the file in large buffer chunks (e.g. 32 KB) and uses `bytes.Count` to tally
null bytes, avoiding all per-entry string allocation. This is significantly
faster than `ScanNullTerminated` with a counting callback for large files.

If the file is non-empty and does not end with a null byte, the trailing content
is counted as an additional entry (matching `ScanNullTerminated` semantics for
files without a trailing null). An empty file returns 0.

### `fofn/chunk.go`

#### New constant and function: `TargetChunks`, `CalculateChunks`

```go
const TargetChunks = 100

func CalculateChunks(n, minChunk, maxChunk int) int
```

Pure function. Returns the number of chunks as described in the algorithm above.
Has no side effects and assumes valid inputs (see Preconditions).

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

Before counting, `WriteShuffledChunks` validates that `minChunk >= 1`,
`maxChunk >= 1`, and `minChunk <= maxChunk`, returning an error if any
condition is violated.

Internally, the counting pass switches from `scanner.ScanNullTerminated` with
a counter callback to `scanner.CountNullTerminated` for maximum speed. Then it
calls `CalculateChunks(count, minChunk, maxChunk)` to determine `numChunks`,
and streams entries to that many chunk files (as before).

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

### Test file updates

#### `internal/scanner/scanner_test.go`

New tests for `CountNullTerminated` (acceptance tests VC0).

#### `fofn/chunk_test.go`

All existing tests that call `WriteShuffledChunks` with a `chunkSize` argument
must be updated to pass `minChunk, maxChunk` instead. The existing acceptance
tests from spec.md section B1 remain valid; they just use `minChunk = maxChunk =
<old chunkSize>` to get the same deterministic behaviour.

New tests for `CalculateChunks` are added (see acceptance tests VC1).

#### `fofn/watcher_test.go`

All existing tests that set `ChunkSize` in `ProcessSubDirConfig` must be updated
to set `MinChunk` and `MaxChunk` instead.

#### `main_test.go`

All existing tests that set `ChunkSize` in config structs or pass `--chunk-size`
on the CLI must be updated to use `MinChunk`/`MaxChunk` and
`--min-chunk`/`--max-chunk` instead.

---

## Acceptance Tests

### VC0: Fast entry counting

As a developer, I want a function that counts null-terminated entries in a file
as fast as possible by counting null bytes directly, so that the chunk sizing
algorithm can determine the number of files in a multi-million-entry fofn
without the overhead of per-entry string allocation and callbacks.

**Package:** `internal/scanner/`
**File:** `internal/scanner/scanner.go`
**Test file:** `internal/scanner/scanner_test.go`

**Acceptance tests:**

1. Given a file containing three null-terminated paths `/a/b\0/c/d\0/e/f\0`,
   when I call `scanner.CountNullTerminated(path)`, then I get 3 and no error.

2. Given an empty file, when I call `scanner.CountNullTerminated(path)`, then
   I get 0 and no error.

3. Given a file with paths `/a/b\0/c/d` (no trailing null after last entry),
   when I call `scanner.CountNullTerminated(path)`, then I get 2 (the trailing
   content without a null terminator counts as one entry, matching
   `ScanNullTerminated` semantics).

4. Given a non-existent file path, when I call
   `scanner.CountNullTerminated(path)`, then I get 0 and a non-nil error.

5. Given a file containing entries with embedded newlines `/a\nb\0/c/d\0`,
   when I call `scanner.CountNullTerminated(path)`, then I get 2 (newlines are
   not treated as terminators).

6. Given a file containing a single null byte `\0`, when I call
   `scanner.CountNullTerminated(path)`, then I get 1 (one entry whose content
   is the empty string).

7. Given a file containing 1,000,000 null-terminated paths, when I call
   `scanner.CountNullTerminated(path)`, then the result equals the count
   obtained by `scanner.ScanNullTerminated` with a counting callback
   (consistency check between the two implementations).

8. (**Memory test**) Given a file containing 1,000,000 null-terminated 100-byte
   paths (~100 MB), when I call `scanner.CountNullTerminated(path)`, then
   `runtime.MemStats.HeapInuse` does not increase by more than 1 MB above the
   baseline (proving no per-entry string allocation).

9. Given a file containing `a\0\0b\0` (consecutive null bytes producing an
   empty entry between "a" and "b"), when I call
   `scanner.CountNullTerminated(path)`, then I get 3 (entries are "a", "",
   "b" — matching `ScanNullTerminated` semantics where consecutive null bytes
   delimit an empty-string entry).

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

21. Given minChunk=maxChunk=10 and n=25, when I call
    `fofn.CalculateChunks(25, 10, 10)`, then I get 3 (degenerates to the
    old fixed chunkSize=10 behaviour: ceil(25/10)=3).

22. Given minChunk=maxChunk=10 and n=100, when I call
    `fofn.CalculateChunks(100, 10, 10)`, then I get 10 (degenerates to
    fixed chunkSize: ceil(100/10)=10).

23. Given minChunk=1, maxChunk=100, n=50, when I call
    `fofn.CalculateChunks(50, 1, 100)`, then I get 50 (ideal=1, clamped to
    1, ceil(50/1)=50 — minimum of 1 file per chunk yields maximum number
    of chunks).

24. Given n=100000000 (100 million), minChunk=250, maxChunk=10000, when I call
    `fofn.CalculateChunks(100000000, 250, 10000)`, then I get 10000 (massive
    fofn, ideal=1000000, clamped to maxChunk 10000, ceil(100000000/10000)=10000).

25. Given n=501, minChunk=250, maxChunk=10000, when I call
    `fofn.CalculateChunks(501, 250, 10000)`, then I get 3 (ideal=6, clamped
    to 250, ceil(501/250)=3 — just above the 2-chunk boundary).

26. (**Table-driven comprehensive test**) Given the full worked-examples table
    above (including the n=501 and n=100000000 rows), when I call
    `fofn.CalculateChunks` for each row, then every result matches the expected
    `numChunks`.

### VC2: WriteShuffledChunks with variable sizing

As a developer, I want `WriteShuffledChunks` to accept min/max chunk parameters
instead of a fixed chunk size, so that the adaptive algorithm is used end-to-end
and invalid parameter combinations are rejected.

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

4. Given a fofn with 201 paths and minChunk=2, maxChunk=2, when I call
   `fofn.WriteShuffledChunks(fofnPath, transform, dir, 2, 2, 1)`, then 101
   chunk files are created (degenerate min=max=2 forces ceil(201/2)=101 chunks,
   demonstrating the >100 chunks case without creating a multi-million-entry
   test fofn).

5. Given an empty fofn, when I call `fofn.WriteShuffledChunks(...)` with any
   min/max values, then 0 chunk files are created and nil is returned.

6. Given a fofn with 1 path and minChunk=250, maxChunk=10000, when I call
   `fofn.WriteShuffledChunks(...)`, then 1 chunk file is created containing
   1 line.

7. Given minChunk=0 and any fofn, when I call `fofn.WriteShuffledChunks(...)`,
   then a non-nil error is returned and no chunk files are created.

8. Given maxChunk=0 and any fofn, when I call `fofn.WriteShuffledChunks(...)`,
   then a non-nil error is returned and no chunk files are created.

9. Given minChunk=500, maxChunk=100 (min > max), when I call
   `fofn.WriteShuffledChunks(...)`, then a non-nil error is returned and no
   chunk files are created.

10. (**Memory test**) Given a fofn with 1,000,000 null-terminated 100-byte
    paths, when I call `fofn.WriteShuffledChunks(...)` with minChunk=250,
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
