# fofnserver Specification

## Overview

The `ibackup fofnserver` subcommand is a long-running process that monitors a
watch directory for subdirectories containing null-terminated fofn files. For
each fofn it discovers, it:

1. Reads the fofn and a `.transformer` file to determine local-to-remote path
   mappings.
2. Splits the mappings into shuffled chunks.
3. Submits `ibackup put` jobs to wr via the Go client API, including metadata
   flags that apply the fofn directory name (as `ibackup:fofn` in the standard
   namespace) and any user-defined key-value pairs from an optional `.metadata`
   file (in the `ibackup:user:` namespace) to every uploaded file.
4. Waits for all jobs to complete (or be buried after exhausting retries).
5. Parses per-chunk report files and writes an aggregated status file.

It handles fofn updates by waiting for running jobs to finish. If any jobs from
the previous run were buried, it deletes them and starts the new run fresh. Once
a new run completes successfully, it deletes any older run directories to save
space. It supports a `.freeze` mode where files already in iRODS are not
re-uploaded even if modified locally.

All files and directories created by the system must be readable by the same
unix group that owns the watched directory.

## Architecture

### New packages and files

#### `internal/scanner/`

Null-terminated file scanning. Used by both `cmd/put.go` and `fofn/`.

- `internal/scanner/scanner.go`
- `internal/scanner/scanner_test.go`

#### `internal/ownership/`

GID lookup and file/directory creation with group ownership.

- `internal/ownership/ownership.go`
- `internal/ownership/ownership_test.go`

#### `fofn/`

All fofnserver-specific logic, fully tested with GoConvey. Source files
are split by responsibility:

| File                   | Responsibility                          |
|------------------------|-----------------------------------------|
| `fofn/report.go`       | Report line format/parse, ReportEntry   |
| `fofn/report_test.go`  | D1, D2 tests                            |
| `fofn/status.go`       | Status file generation, ParseStatus     |
| `fofn/status_test.go`  | E1 tests                                |
| `fofn/chunk.go`        | Streaming shuffle and chunk writing     |
| `fofn/chunk_test.go`   | B1 tests                                |
| `fofn/jobs.go`         | wr job creation, submission, monitoring |
| `fofn/jobs_test.go`    | F1–F6 tests                             |
| `fofn/scan.go`         | Watch dir scanning, config file reading |
| `fofn/scan_test.go`    | G1–G5 tests                             |
| `fofn/server.go`       | Server orchestration, ProcessSubDir     |
| `fofn/server_test.go`  | H1–H5 tests                             |

### Changes to existing packages

- `internal/scanner/`: New sub-package for null-terminated file scanning
  (moved from `cmd/put.go`).
- `internal/ownership/`: New sub-package for GID lookup and group-owned
  file/directory creation.
- `transfer/meta.go`: Add `MetaKeyFofn` constant.
- `transfer/request.go`: Add `RequestStatusFrozen`.
- `transfer/put.go`: Add no-replace mode to `Putter`.
- `cmd/put.go`: Add `--no_replace`, `--report`, and `--fofn` flags; refactor to
  use `internal/scanner/` for null scanning.
- `cmd/fofnserver.go`: CLI-only cobra subcommand.
- `main_test.go`: Integration test for the fofnserver CLI.

### Interfaces for testability

The fofn package defines a `JobSubmitter` interface that abstracts wr client
operations:

```go
type JobSubmitter interface {
    SubmitJobs(jobs []*jobqueue.Job) error
    FindCompleteJobsByRepGroup(
        prefix string,
    ) ([]*jobqueue.Job, error)
    FindBuriedJobsByRepGroup(
        prefix string,
    ) ([]*jobqueue.Job, error)
    FindIncompleteJobsByRepGroup(
        prefix string,
    ) ([]*jobqueue.Job, error)
    DeleteJobs(jobs []*jobqueue.Job) error
    Disconnect() error
}
```

In tests, a mock implementation records submitted jobs and lets tests control
job states. In production, a thin wrapper around `*client.Scheduler` satisfies
this interface.

The fofn package also defines a `PutCommandBuilder` interface so tests can
verify command construction without needing a real ibackup binary:

```go
type PutCommandBuilder interface {
    BuildCommand(
        chunkPath string,
        reportPath string,
        logPath string,
        outPath string,
        noReplace bool,
        fofnName string,
        userMeta string,
    ) string
}
```

### Directory layout

Given a watch directory `/watch` with a subdirectory `project1/`, the layout is:

```
/watch/project1/
    fofn                     # null-terminated file of filenames
    .transformer             # contains transformer name
    .freeze                  # optional; enables no-replace mode
    .metadata                # optional; TSV of key\tvalue user metadata
    status                   # latest aggregated status file
    1738000000/              # run dir (fofn mtime as Unix secs)
        chunk.000000         # base64-encoded local\tremote
        chunk.000000.log     # ibackup put log output
        chunk.000000.out     # ibackup put stdout+stderr
        chunk.000000.report  # machine-parsable report
        chunk.000001
        ...
```

### Unix group ownership

On startup, the server determines the group owner of the watch directory. All
directories and files it creates (run dirs, chunk files, status files, etc.)
must have their group set to this GID and be group-readable (directories
group-executable). The implementation should call `os.Chown(-1, gid, path)`
after creating each file or directory, and ensure the umask permits group read
(e.g. 0027 or use explicit `os.Chmod`).

### Status file format

The status file is a TSV with 4 columns per data line. Paths are escaped using
Go's `strconv.Quote` (including surrounding double quotes). The file ends with a
SUMMARY line.

```
"/local/file1"\t"/remote/file1"\tuploaded\t""
"/local/file2"\t"/remote/file2"\tunmodified\t""
"/path with\ttab"\t"/remote/tab"\tfailed\t"conn reset"
SUMMARY\tuploaded=1\treplaced=0\tunmodified=1 ...
```

Statuses are: `uploaded`, `replaced`, `unmodified`, `missing`, `failed`,
`frozen`, `orphaned`, `warning`, `hardlink`, `not_processed`.

The SUMMARY line has tab-separated key=value pairs for each status, giving the
count of files with that status.

### Report file format

Per-chunk report files use the same format as the status file (without the
SUMMARY line). They are generated by `ibackup put --report`.

---

## Section A: FOFN Parsing and Path Transformation

### A1: Parse null-terminated file (streaming)

As a developer, I want to stream entries from a null-terminated file via a
callback, so that arbitrarily large fofn files (10s of millions of paths) can be
processed without loading them into memory.

`scanNulls` and `fofnLineSplitter` in `cmd/put.go` already implement
null-terminated scanning but are unexported and in the `cmd` package. Move them
to `internal/scanner/` so both `cmd/put.go` and the fofn package can import
them.

**Package:** `internal/scanner/`
**File:** `internal/scanner/scanner.go`
**Test file:** `internal/scanner/scanner_test.go`

The primary API is callback-based:

```go
func ScanNullTerminated(
    path string, cb func(entry string) error,
) error
```

A convenience wrapper `scanner.CollectNullTerminated(path) ([]string, error)`
may exist for small-file uses (tests, config), but production fofn parsing must
use the callback form.

**Acceptance tests:**

1. Given a file containing three null-terminated absolute paths
   `/a/b\0/c/d\0/e/f\0`, when I call `scanner.ScanNullTerminated(path, cb)`,
   then the callback is invoked 3 times with "/a/b", "/c/d", "/e/f" in order,
   and no error is returned.

2. Given an empty file, when I call `scanner.ScanNullTerminated(path, cb)`, then
   the callback is never invoked and no error is returned.

3. Given a file with a path containing a newline character `/a/b\n/c\0`, when I
   call `scanner.ScanNullTerminated(path, cb)`, then the callback receives
   `"/a/b\n/c"` (one path with embedded newline) and no error is returned.

4. Given a non-existent file path, when I call `scanner.ScanNullTerminated(path,
   cb)`, then a non-nil error is returned.

5. Given a file with paths `/a/b\0/c/d\0` (no trailing null on last), when I
   call `scanner.ScanNullTerminated(path, cb)`, then the callback receives
   "/a/b" and "/c/d" and no error is returned (handles both with and without
   trailing null).

6. Given a callback that returns an error on the second entry, when I call
   `scanner.ScanNullTerminated(path, cb)`, then the function stops early and
   returns that error.

7. (**Memory test**) Given a file containing 1,000,000 null-terminated 100-byte
   paths (~100 MB), when I call `scanner.ScanNullTerminated(path, cb)` where the
   callback discards each entry, then `runtime.MemStats.HeapInuse` does not
   increase by more than 10 MB above the baseline measured before the call
   (proving the implementation streams rather than accumulating).

### A2: Transform paths using existing transformer infrastructure

Path transformation is already implemented in the `transformer` and `transfer`
packages:

- `transformer.MakePathTransformer(name)` returns a `PathTransformer` func for
  a registered named transformer.
- `transfer.NewRequestWithTransformedLocal(local, pt)` creates a
  `*transfer.Request` with Local and Remote set.

The fofn package should use these directly. No new transformation code is
needed. The fofn package's `ProcessSubDir` (H1) calls
`transformer.MakePathTransformer` with the name read from `.transformer`, then
iterates parsed paths calling `transfer.NewRequestWithTransformedLocal` for
each.

---

## Section B: Chunk File Management

### B1: Streaming shuffle and chunk writing

As a developer using the fofn package, I want to stream path pairs from a
callback source directly into shuffled chunk files without holding all entries
in memory, so that fofns with 10s of millions of paths can be chunked.

The approach: `fofn.WriteShuffledChunks` accepts a callback source (the same
pattern as `scanner.ScanNullTerminated`), a chunk size, and an output directory.
It opens up to `numChunks` file handles simultaneously (where `numChunks` can be
estimated by a first pass counting entries, or by using a default upper bound).
Each incoming entry is assigned to a random chunk index (`rand.Intn(numChunks)`)
and written immediately. This achieves shuffle without storing all entries.

For small inputs where chunk count is unknown in advance, a two-pass approach
is acceptable:
1. First pass: count entries via `scanner.ScanNullTerminated`.
2. Compute `numChunks = ceil(count / chunkSize)`.
3. Second pass: stream entries, assigning each to `rand.Intn(numChunks)` and
   writing to the corresponding chunk file.

Chunk files may end up with slightly uneven sizes (binomial distribution around
`chunkSize`), which is acceptable.

**Package:** `fofn/`
**File:** `fofn/chunk.go`
**Test file:** `fofn/chunk_test.go`

**Acceptance tests:**

1. Given a fofn with 25 null-terminated paths and a chunk size of 10, when I
   call `fofn.WriteShuffledChunks(fofnPath, transformerName, dir, 10)`, then 3
   chunk files are created in `dir` named `chunk.000000`, `chunk.000001`,
   `chunk.000002`, the total line count across all chunks is 25, and each line
   has base64-encoded local and remote paths separated by a tab.

2. Given a fofn with 10 paths and a chunk size of 10, when I call
   `fofn.WriteShuffledChunks(...)`, then 1 chunk file is created containing 10
   lines.

3. Given an empty fofn, when I call `fofn.WriteShuffledChunks(...)`, then 0
   chunk files are created and an empty slice of chunk names is returned.

4. Given chunk files written by `fofn.WriteShuffledChunks`, when I read a chunk
   file and base64-decode each field, then the decoded local and remote paths
   are a subset of the original entries.

5. Given a fofn with 1000 sorted paths, when I call
   `fofn.WriteShuffledChunks(...)` twice, then the concatenated contents of all
   chunk files differ between the two runs (with very high probability),
   confirming entries are shuffled.

6. (**Memory test**) Given a fofn with 1,000,000 null-terminated 100-byte paths,
   when I call `fofn.WriteShuffledChunks(...)` with chunkSize=10000, then
   `runtime.MemStats.HeapInuse` does not increase by more than 20 MB above the
   baseline (proving entries are streamed, not accumulated).

---

## Section C: Put Enhancements

### C1: RequestStatusFrozen constant

As a developer, I want a `RequestStatusFrozen` status in the transfer package,
so that files skipped due to no-replace mode have a distinct status.

**Package:** `transfer/`
**Test file:** `transfer/request_test.go`

**Acceptance tests:**

1. `RequestStatusFrozen` has the string value `"frozen"`.

### C2: Putter no-replace mode

As a developer using the transfer package, I want to create a Putter in
no-replace mode, so that files already in iRODS with a different mtime are
skipped with "frozen" status instead of being re-uploaded.

**Package:** `transfer/`
**Test file:** `transfer/put_test.go`

**Acceptance tests:**

1. Given a local file and a remote file that already exists with an earlier
   mtime, when I Put() with a Putter that has SetNoReplace(true), then the
   request's status is `RequestStatusFrozen` and the file is NOT re-uploaded
   (the remote file remains unchanged).

2. Given a local file and a remote path that does not exist yet, when I Put()
   with a Putter that has SetNoReplace(true), then the request's status is
   `RequestStatusUploaded` (new files are still uploaded even in no-replace
   mode).

3. Given a local file and a remote file that already exists with the same mtime,
   when I Put() with a Putter that has SetNoReplace(true), then the request's
   status is `RequestStatusUnmodified` (same behaviour as without no-replace).

4. Given a local file that does not exist, when I Put() with a Putter that has
   SetNoReplace(true), then the request's status is `RequestStatusMissing` (same
   behaviour as without no-replace).

### C3: --no_replace CLI flag for ibackup put

As a user of ibackup put, I want a `--no_replace` flag, so that I can upload new
files without replacing existing ones that have changed.

**Package:** `cmd/`
**Test file:** `main_test.go`

**Acceptance tests:**

1. Given a 2-column put file with a file already in iRODS at a different mtime,
   when I run `ibackup put --no_replace -f <file>`, then the file is not
   re-uploaded and the verbose output shows "frozen" status.

2. Given a 2-column put file with a new file not yet in iRODS, when I run
   `ibackup put --no_replace -f <file>`, then the file is uploaded normally.

### C4: MetaKeyFofn constant

As a developer, I want a `MetaKeyFofn` constant in the transfer package, so that
the fofn directory name can be stored as standard ibackup metadata.

**Package:** `transfer/`
**Test file:** `transfer/meta_test.go`

**Acceptance tests:**

1. `MetaKeyFofn` has the string value `"ibackup:fofn"`.

### C5: --fofn CLI flag for ibackup put

As a user of ibackup put, I want a `--fofn <name>` flag, so that I can apply
`ibackup:fofn` metadata to every uploaded file.

**Package:** `cmd/`
**Test file:** `main_test.go`

**Acceptance tests:**

1. Given a 2-column put file with 2 files, when I run `ibackup put --fofn
   project1 -b -f <file>`, then both uploaded files have `ibackup:fofn` metadata
   set to `"project1"`.

2. Given `--fofn project1` and `--meta "colour=red"`, when I run `ibackup put`,
   then uploaded files have both `ibackup:fofn=project1` and
   `ibackup:user:colour=red` metadata.

---

## Section D: Put Report Output

### D1: Report line formatting and parsing

As a developer using the fofn package, I want functions to format and parse
report lines, so that `ibackup put` and the fofn server can exchange per-file
status data reliably even when paths contain special characters.

**Package:** `fofn/`
**File:** `fofn/report.go`
**Test file:** `fofn/report_test.go`

**Acceptance tests:**

1. Given an entry with Local="/a/b", Remote="/c/d", Status="uploaded", Error="",
   when I call `fofn.FormatReportLine(entry)`, then I get
   `"/a/b"\t"/c/d"\tuploaded\t""`.

2. Given an entry with a path containing a tab Local="/a\tb", Remote="/c\td",
   Status="failed", Error="conn\treset", when I call
   `fofn.FormatReportLine(entry)`, then I get
   `"/a\tb"\t"/c\td"\tfailed\t"conn\treset"` (tabs within quoted strings are
   escaped).

3. Given the line `"/a/b"\t"/c/d"\tuploaded\t""`, when I call
   `fofn.ParseReportLine(line)`, then I get a `fofn.ReportEntry` with
   Local="/a/b", Remote="/c/d", Status="uploaded", Error="".

4. Given the line `"/a\tb"\t"/c\td"\tfailed\t"conn\treset"`, when I call
   `fofn.ParseReportLine(line)`, then I get a `fofn.ReportEntry` with
   Local="/a\tb", Remote="/c\td", Status="failed", Error="conn\treset".

5. Given a malformed line with too few fields, when I call
   `fofn.ParseReportLine(line)`, then I get a non-nil error.

### D2: Write and read a complete report file

As a developer using the fofn package, I want to write report entries
streamingly via a writer, and read them back streamingly via a callback, so that
the report file format is a reliable interchange that works at any scale.

The write API accepts an `io.Writer`:

```go
func WriteReportEntry(w io.Writer, entry ReportEntry) error
```

This is called once per entry as results become available (e.g. from `ibackup
put`). There is no `WriteReport(path, []entries)` function that requires a
complete slice.

The read API is callback-based (same function used by E1):

```go
func ParseReportCallback(
    path string, cb func(ReportEntry) error,
) error
```

A convenience `fofn.CollectReport(path) ([]ReportEntry, error)` may exist for
tests and small files.

**Package:** `fofn/`
**File:** `fofn/report.go`
**Test file:** `fofn/report_test.go`

**Acceptance tests:**

1. Given 3 entries with various statuses and paths (including special
   characters), when I write them using `fofn.WriteReportEntry(w, entry)` for
   each and then read them back with `fofn.ParseReportCallback(path, cb)`, then
   the callback receives the same 3 `fofn.ReportEntry` values with identical
   fields.

2. Given a non-existent report file path, when I call
   `fofn.ParseReportCallback(path, cb)`, then a non-nil error is returned.

3. Given an empty report file, when I call `fofn.ParseReportCallback(path, cb)`,
   then the callback is never invoked and no error is returned.

### D3: --report CLI flag for ibackup put

As a user of ibackup put, I want a `--report` flag that writes a
machine-parsable per-file status file, so that automated tools can reliably
determine the outcome for every file.

**Package:** `cmd/`
**Test file:** `main_test.go`

**Acceptance tests:**

1. Given a 2-column put file with 3 files (one new, one with matching mtime, one
   missing locally), when I run `ibackup put --report report.tsv -b -f <file>`,
   then `report.tsv` exists and contains 3 lines in the report format defined in
   D1 (4-column TSV with `strconv.Quote`-escaped paths, status, and error), with
   statuses "uploaded", "unmodified", and "missing" respectively. The file can
   be round-tripped through `fofn.CollectReport()`.

2. Given a 2-column put file with 1 file and `--no_replace` and `--report`, when
   the remote file exists with a different mtime, then the report contains a
   `fofn.ReportEntry` with Status="frozen" when parsed with
   `fofn.CollectReport()`.

---

## Section E: Status File Generation

### E1: Generate status file from chunk and report files

As a developer using the fofn package, I want a single function that reads all
chunk files and their corresponding report files in a run directory, identifies
unprocessed files from buried/failed chunks, and streams the combined results
into a status file with a SUMMARY line - without loading everything into memory.

The key type is `fofn.ReportEntry`:

```go
type ReportEntry struct {
    Local  string
    Remote string
    Status string // e.g. "uploaded", "frozen", "not_processed"
    Error  string
}
```

**Package:** `fofn/`
**File:** `fofn/status.go`
**Test file:** `fofn/status_test.go`

The implementation should be composed of independently testable parts:

- `fofn.ParseReportCallback` (defined in `fofn/report.go`, tested in D2) streams
  `fofn.ReportEntry` values from a report file to a callback, without
  accumulating them.
- `fofn.WriteStatusFromRun` (in `fofn/status.go`) given a run directory and a
  set of buried chunk names, iterates each chunk: for non-buried chunks it
  streams entries from the report file; for buried chunks it reads the chunk
  file and emits `fofn.ReportEntry` values with Status="not_processed" for any
  file not in the (possibly partial) report. It writes each entry to the status
  file as it goes, tallying counts, and appends a SUMMARY line at the end.

**Acceptance tests:**

1. Given a run directory with 3 chunk files and 3 corresponding report files
   (10, 10, 5 `fofn.ReportEntry` values respectively, all with various
   statuses), and no buried chunks, when I call `fofn.WriteStatusFromRun(runDir,
   statusPath, nil)`, then the status file contains exactly 25
   `fofn.ReportEntry`-format lines (one per file, matching the report file
   format from D1) followed by a SUMMARY line with correct counts for each
   status.

2. Given a run directory with 3 chunk files, 2 complete report files (10 entries
   each), and 1 buried chunk ("chunk.000002") whose report file does not exist,
   when I call `fofn.WriteStatusFromRun(runDir, statusPath, ["chunk.000002"])`,
   then the status file contains 20 entries from the reports plus 5 entries
   (matching chunk.000002's contents) with Status="not_processed", and the
   SUMMARY line reflects all 25 entries.

3. Given a buried chunk whose report file exists but is incomplete (5 of 10
   files reported), when I call `fofn.WriteStatusFromRun(...)` with that chunk
   marked as buried, then the status file contains the 5 reported entries with
   their original statuses, plus 5 entries with Status="not_processed" for the
   files in the chunk but missing from the report.

4. Given a run directory with 0 chunk files, when I call
   `fofn.WriteStatusFromRun(runDir, statusPath, nil)`, then the status file
   contains only a SUMMARY line with all counts equal to 0.

5. Given the status file from test 1, when I call
   `fofn.ParseStatus(statusPath)`, then I get back 25 `fofn.ReportEntry` values
   and a `fofn.StatusCounts` struct with correct counts for each status.

6. The streaming callback `fofn.ParseReportCallback(path, func(ReportEntry))`,
   when given a report file with 10 entries, calls the callback exactly 10 times
   with the correct `fofn.ReportEntry` values, and returns no error.

7. (**Memory test**) Given a run directory with 100 chunk files, each with a
   10,000-entry report file (1,000,000 entries total), when I call
   `fofn.WriteStatusFromRun(runDir, statusPath, nil)`, then
   `runtime.MemStats.HeapInuse` does not increase by more than 20 MB above the
   baseline (proving entries are streamed, not accumulated in slices).

---

## Section F: wr Job Management

### F1: Create put job commands

As a developer using the fofn package, I want to create the shell command string
for an `ibackup put` job given a chunk file, so that I can submit it to wr.

**Package:** `fofn/`
**File:** `fofn/jobs.go`
**Test file:** `fofn/jobs_test.go`

**Acceptance tests:**

1. Given chunkPath="chunk.000000", noReplace=false, fofnName="project1", and no
   user metadata, when I call `fofn.BuildPutCommand("chunk.000000", false,
   "project1", "")`, then the result is: `ibackup put -v -l chunk.000000.log
   --report chunk.000000.report --fofn project1 -b -f chunk.000000 >
   chunk.000000.out 2>&1`.

2. Given noReplace=true, when I call `fofn.BuildPutCommand("chunk.000000", true,
   "project1", "")`, then the result includes `--no_replace` in the command.

3. Given userMeta="colour=red;size=large", when I call
   `fofn.BuildPutCommand("chunk.000000", false, "project1",
   "colour=red;size=large")`, then the result includes `--meta
   "colour=red;size=large"` in the command.

4. Given an empty fofnName, when I call `fofn.BuildPutCommand("chunk.000000",
   false, "", "")`, then the result does not include `--fofn` in the command.

### F2: Create wr jobs for all chunks in a run

As a developer using the fofn package, I want to create wr Job structs for all
chunks in a run directory, so that I can submit them to wr in one batch.

**Package:** `fofn/`
**File:** `fofn/jobs.go`
**Test file:** `fofn/jobs_test.go`

The `fofn.CreateJobs` function takes a `fofn.RunConfig`:

```go
type RunConfig struct {
    RunDir      string        // absolute path to run dir
    ChunkPaths  []string      // relative chunk filenames
    SubDirName  string        // basename of fofn's parent dir
    FofnMtime   int64         // Unix seconds mtime of fofn
    NoReplace   bool          // .freeze mode
    FofnName    string        // basename of subdir, for ibackup:fofn metadata
    UserMeta    string        // semicolon-separated key=value from .metadata
    RAM         int           // MB, default 1024
    Time        time.Duration // default 8h
    Retries     uint8         // default 3
    LimitGroups []string      // default ["irods"]
    ReqGroup    string        // default "ibackup"
}
```

**Acceptance tests:**

1. Given a RunConfig with RunDir="/watch/proj/123", ChunkPaths=["chunk.000000",
   "chunk.000001"], SubDirName="proj", FofnMtime=123, when I call
   `fofn.CreateJobs(cfg)`, then I get 2 `*jobqueue.Job` values where:
   - Each Cmd matches `BuildPutCommand(chunk, false, "proj", "")`.
   - Each Cwd equals "/watch/proj/123".
   - Each CwdMatters is true.
   - Each RepGroup is "ibackup_fofn_proj_123".
   - Each ReqGroup is "ibackup".
   - Each Requirements.RAM is 1024.
   - Each Requirements.Time is 8h.
   - Each Retries is 3.
   - Each LimitGroups is ["irods"].

2. Given a RunConfig with NoReplace=true, when I call `fofn.CreateJobs(cfg)`,
   then each Cmd contains `--no_replace`.

3. Given a RunConfig with custom RAM=2048 and Time=4h, when I call
   `fofn.CreateJobs(cfg)`, then each Requirements.RAM is 2048 and Time is 4h.

### F3: Submit jobs to wr

As a developer using the fofn package, I want to submit created jobs to wr via
the JobSubmitter interface, so that they get scheduled and executed.

**Package:** `fofn/`
**File:** `fofn/jobs.go`
**Test file:** `fofn/jobs_test.go`

**Acceptance tests:**

1. Given a mock JobSubmitter and 3 jobs, when I call
   `submitter.SubmitJobs(jobs)`, then the mock records 3 submitted jobs and
   returns no error.

2. Given a mock JobSubmitter that returns an error, when I call
   `submitter.SubmitJobs(jobs)`, then the error is propagated to the caller.

### F4: Check if all jobs for a run are complete

As a developer using the fofn package, I want to check whether all wr jobs for a
given run are in a terminal state (complete or buried), so that I know when to
generate the status file.

**Package:** `fofn/`
**File:** `fofn/jobs.go`
**Test file:** `fofn/jobs_test.go`

**Acceptance tests:**

1. Given a mock JobSubmitter where
   FindIncompleteJobsByRepGroup("ibackup_fofn_proj_123") returns 0 jobs, when I
   call `fofn.IsRunComplete(submitter, "ibackup_fofn_proj_123")`, then it
   returns true.

2. Given a mock JobSubmitter where
   FindIncompleteJobsByRepGroup("ibackup_fofn_proj_123") returns 2 jobs (still
   running), when I call `fofn.IsRunComplete(submitter,
   "ibackup_fofn_proj_123")`, then it returns false.

### F5: Identify buried (failed) jobs

As a developer using the fofn package, I want to identify which jobs were buried
(failed all retries), so that I can mark their files as not_processed or delete
them on fofn update.

**Package:** `fofn/`
**File:** `fofn/jobs.go`
**Test file:** `fofn/jobs_test.go`

**Acceptance tests:**

1. Given a mock JobSubmitter where
   FindBuriedJobsByRepGroup("ibackup_fofn_proj_123") returns 1 buried job, when
   I call `fofn.FindBuriedChunks(submitter, "ibackup_fofn_proj_123", runDir)`,
   then I get the chunk path corresponding to the buried job (extracted from its
   Cmd).

2. Given a mock JobSubmitter where
   FindBuriedJobsByRepGroup("ibackup_fofn_proj_123") returns 0 jobs, when I call
   `fofn.FindBuriedChunks(submitter, "ibackup_fofn_proj_123", runDir)`, then I
   get an empty slice.

### F6: Delete buried jobs

As a developer using the fofn package, I want to delete buried wr jobs, so that
when a fofn is updated I can clean up the old failed jobs before starting a
fresh run.

**Package:** `fofn/`
**File:** `fofn/jobs.go`
**Test file:** `fofn/jobs_test.go`

**Acceptance tests:**

1. Given a mock JobSubmitter with 2 buried jobs, when I call
   `fofn.DeleteBuriedJobs(submitter, "ibackup_fofn_proj_123")`, then the mock's
   DeleteJobs method is called with those 2 jobs and no error is returned.

2. Given a mock JobSubmitter where DeleteJobs returns an error, when I call
   `fofn.DeleteBuriedJobs(submitter, "ibackup_fofn_proj_123")`, then the error
   is propagated.

---

## Section G: Directory Scanning and Group Ownership

### G1: Discover subdirectories with fofn files

As a developer using the fofn package, I want to scan a watch directory and find
all immediate subdirectories that contain a file named `fofn`, so that each one
can be processed for backup.

**Package:** `fofn/`
**File:** `fofn/scan.go`
**Test file:** `fofn/scan_test.go`

**Acceptance tests:**

1. Given a directory with 3 subdirectories, 2 of which contain a file named
   "fofn", when I call `fofn.ScanForFOFNs(watchDir)`, then I get a
   `[]fofn.SubDir` of length 2, where each `fofn.SubDir` has `.Path` set to the
   subdirectory's absolute path.

2. Given a directory with no subdirectories, when I call
   `fofn.ScanForFOFNs(watchDir)`, then I get an empty `[]fofn.SubDir` and no
   error.

3. Given a directory with a subdirectory containing a file named "fofn" and a
   subdirectory containing only other files, when I call
   `fofn.ScanForFOFNs(watchDir)`, then I get a `[]fofn.SubDir` of length 1.

4. Given a non-existent watch directory, when I call
   `fofn.ScanForFOFNs(watchDir)`, then I get a non-nil error.

### G2: Read transformer name from .transformer file

As a developer using the fofn package, I want to read the transformer name from
a `.transformer` file in a subdirectory, so that I know which transformer to use
for the fofn.

**Package:** `fofn/`
**File:** `fofn/scan.go`
**Test file:** `fofn/scan_test.go`

**Acceptance tests:**

1. Given a `.transformer` file containing "humgen\n", when I call
   `fofn.ReadTransformerName(dir)`, then I get "humgen" (trimmed) and no error.

2. Given a `.transformer` file containing "  humgen  \n", when I call
   `fofn.ReadTransformerName(dir)`, then I get "humgen" (whitespace trimmed) and
   no error.

3. Given no `.transformer` file in the directory, when I call
   `fofn.ReadTransformerName(dir)`, then I get a non-nil error.

4. Given an empty `.transformer` file, when I call
   `fofn.ReadTransformerName(dir)`, then I get a non-nil error (transformer name
   is required).

### G3: Detect .freeze file

As a developer using the fofn package, I want to check whether a `.freeze` file
exists in a subdirectory, so that I can enable no-replace mode for that fofn.

**Package:** `fofn/`
**File:** `fofn/scan.go`
**Test file:** `fofn/scan_test.go`

**Acceptance tests:**

1. Given a subdirectory containing a `.freeze` file (contents irrelevant), when
   I call `fofn.IsFrozen(dir)`, then it returns true.

2. Given a subdirectory without a `.freeze` file, when I call
   `fofn.IsFrozen(dir)`, then it returns false.

### G4: Detect fofn that needs processing

As a developer using the fofn package, I want to determine whether a fofn needs
processing, based on whether its mtime differs from the last processed mtime
(determined by the newest run directory), so that I only process updated fofns.

**Package:** `fofn/`
**File:** `fofn/scan.go`
**Test file:** `fofn/scan_test.go`

**Acceptance tests:**

1. Given a fofn with mtime 1000 and no existing run directories, when I call
   `fofn.NeedsProcessing(subDir)`, then it returns true and mtime 1000.

2. Given a fofn with mtime 1000 and an existing run directory named "1000", when
   I call `fofn.NeedsProcessing(subDir)`, then it returns false.

3. Given a fofn with mtime 2000 and an existing run directory named "1000", when
   I call `fofn.NeedsProcessing(subDir)`, then it returns true and mtime 2000.

### G5: Read user metadata from .metadata file

As a developer using the fofn package, I want to read optional user-supplied
metadata from a `.metadata` file in a subdirectory, so that the fofnserver can
apply it (in the `ibackup:user:` namespace) to every uploaded file.

The `.metadata` file is a TSV with one key-value pair per line (`key\tvalue`).
Keys must not contain colons (they will be prefixed with `ibackup:user:` by
`ibackup put`). Blank lines and lines starting with `#` are ignored.

**Package:** `fofn/`
**File:** `fofn/scan.go`
**Test file:** `fofn/scan_test.go`

**Acceptance tests:**

1. Given a `.metadata` file containing `colour\tred\nsize\tlarge\n`, when I call
   `fofn.ReadUserMetadata(dir)`, then I get `"colour=red;size=large"` and no
   error.

2. Given no `.metadata` file in the directory, when I call
   `fofn.ReadUserMetadata(dir)`, then I get `""` and no error (metadata is
   optional).

3. Given an empty `.metadata` file, when I call `fofn.ReadUserMetadata(dir)`,
   then I get `""` and no error.

4. Given a `.metadata` file with blank lines and comment lines (`#
   comment\n\ncolour\tred\n`), when I call `fofn.ReadUserMetadata(dir)`, then I
   get `"colour=red"` (comments and blanks skipped) and no error.

5. Given a `.metadata` file with a line that has no tab separator, when I call
   `fofn.ReadUserMetadata(dir)`, then I get a non-nil error.

### G6: Determine and apply group ownership

As a developer, I want generic utilities that determine the GID of a directory
and create files/directories with a specific GID, so that all fofnserver outputs
are readable by the same unix group as the watch directory.

**Package:** `internal/ownership/`
**File:** `internal/ownership/ownership.go`
**Test file:** `internal/ownership/ownership_test.go`

**Acceptance tests:**

1. Given a directory owned by group "testgrp" (GID 12345), when I call
   `ownership.GetDirGID(dir)`, then I get GID 12345 and no error.

2. Given a GID, when I call `ownership.CreateDirWithGID(path, gid)`, then the
   directory is created with mode 0750 and group ownership equal to the given
   GID.

3. Given a GID, when I call `ownership.WriteFileWithGID(path, content, gid)`,
   then the file is created with mode 0640 and group ownership equal to the
   given GID.

4. Given a non-existent directory path, when I call `ownership.GetDirGID(path)`,
   then I get a non-nil error.

---

## Section H: Server Orchestration

### H1: Process a single subdirectory end-to-end

As a developer using the fofn package, I want a `fofn.ProcessSubDir` function
that performs the full streaming pipeline for one subdirectory: scan fofn via
`scanner.ScanNullTerminated`, transform each path with
`transfer.NewRequestWithTransformedLocal`, and stream directly into
`fofn.WriteShuffledChunks`, then submit jobs with metadata flags derived from
the subdirectory name (`ibackup:fofn`) and any `.metadata` file (`ibackup:user:`
keys) - never holding all paths in memory.

**Package:** `fofn/`
**File:** `fofn/server.go`
**Test file:** `fofn/server_test.go`

**Acceptance tests:**

1. Given a subdirectory with a fofn containing 25 null-terminated paths, a
   .transformer file with a valid transformer name, no .freeze file, and a mock
   JobSubmitter, when I call `fofn.ProcessSubDir(subDir, submitter, cfg)`, then:
   - A run directory named after the fofn mtime is created.
   - 3 chunk files exist in the run directory (for chunkSize=10).
   - 3 jobs were submitted to the mock with correct RepGroup, Cwd, and Cmd (each
     Cmd includes `--fofn <dirname>`).
   - No error is returned.
   - The returned `fofn.RunState` has RepGroup set to
     "ibackup_fofn_<dirname>_<mtime>" and the run directory path.

2. Given the same setup but with a .freeze file present, when I call
   `fofn.ProcessSubDir(subDir, submitter, cfg)`, then each submitted job's Cmd
   includes `--no_replace`.

3. Given a subdirectory with a fofn but missing the .transformer file, when I
   call `fofn.ProcessSubDir(subDir, submitter, cfg)`, then a non-nil error is
   returned and no jobs are submitted.

4. Given a subdirectory with a fofn containing 0 paths, when I call
   `fofn.ProcessSubDir(subDir, submitter, cfg)`, then no jobs are submitted, no
   run directory is created, and no error is returned.

5. Given the same setup as test 1, when I examine the created run directory and
   chunk files, then they have group ownership matching the watch directory's
   GID and are group-readable.

6. Given a subdirectory with a `.metadata` file containing `colour\tred`, when I
   call `fofn.ProcessSubDir(subDir, submitter, cfg)`, then each submitted job's
   Cmd includes `--meta "colour=red"`.

7. Given a subdirectory without a `.metadata` file, when I call
   `fofn.ProcessSubDir(subDir, submitter, cfg)`, then each submitted job's Cmd
   does not include `--meta`.

### H2: Generate status file after run completion

As a developer using the fofn package, I want a `fofn.GenerateStatus` function
that parses all chunk report files in a run directory, handles buried jobs, and
writes the status file, so that users have a comprehensive backup status.

**Package:** `fofn/`
**File:** `fofn/server.go`
**Test file:** `fofn/server_test.go`

**Acceptance tests:**

1. Given a run directory with 3 chunk files and 3 corresponding report files
   (all complete), when I call `fofn.GenerateStatus(runDir, subDir,
   buriedChunks)` with no buried chunks, then a "status" file is written in the
   subdirectory containing all entries from all reports plus a correct SUMMARY
   line, and no error is returned.

2. Given a run directory with 3 chunk files but only 2 report files (simulating
   1 buried job whose chunk had 10 files), when I call
   `fofn.GenerateStatus(runDir, subDir, ["chunk.000002"])`, then the status file
   contains entries from the 2 reports plus 10 entries with status
   "not_processed" for the missing chunk's files, and the SUMMARY line reflects
   the correct counts.

3. Given a run directory with 1 chunk file and 1 report file that is incomplete
   (5 of 10 files), when I call `fofn.GenerateStatus(runDir, subDir,
   ["chunk.000000"])`, then the 5 unreported files from chunk.000000 appear as
   "not_processed" in the status file.

4. Given any of the above, when I examine the created status file, then it has
   group ownership matching the watch directory's GID and is group-readable.

### H3: Handle fofn update while jobs are running or buried

As a developer using the fofn package, I want the server to detect that a fofn
has been updated while jobs from a previous run are still in progress or buried,
and handle each case appropriately, so that we either wait or clean up and start
fresh.

**Package:** `fofn/`
**File:** `fofn/server.go`
**Test file:** `fofn/server_test.go`

The `fofn.Server` type maintains a map of active runs per subdirectory. On each
poll cycle it:

1. Scans for subdirectories with fofns.
2. For each subdirectory, checks if an active run exists.
3. If an active run exists, checks if it is complete (all jobs in a terminal
   state - complete or buried).
   - If not complete (jobs still running), skip - wait for next cycle.
   - If complete with NO buried jobs: generate status file, delete any older run
     directories (clean up successful history), clear active run, then check if
     fofn mtime has changed and start new run if so.
   - If complete with SOME buried jobs AND fofn mtime has changed: generate
     status file for the old run, delete the buried jobs from wr, clear active
     run, and start a new run with the updated fofn.
   - If complete with SOME buried jobs AND fofn mtime has NOT changed: generate
     status file (with not_processed entries for buried chunks) and leave active
     run in place. Do not retry - user must update the fofn to trigger a fresh
     run.
4. If no active run exists, check if fofn needs processing.
5. If fofn needs processing, start a new run.

**Acceptance tests:**

1. Given a Server with a mock JobSubmitter, and a subdirectory with a fofn, when
   I call `server.Poll()` the first time, then jobs are submitted and an active
   run is recorded.

2. Given an active run where all jobs are still incomplete (mock returns running
   jobs), when I call `server.Poll()` again, then no new jobs are submitted.

3. Given an active run where all jobs completed successfully (no buried jobs),
   when I call `server.Poll()`, then the status file is generated, the active
   run is cleared, and if the fofn mtime has changed since the run started, a
   new run begins.

4. Given an active run where all jobs completed successfully and the fofn has
   NOT changed, when I call `server.Poll()`, then the status file is generated
   and no new run begins.

5. Given an active run where all jobs are in terminal state but 1 is buried, and
   the fofn mtime has NOT changed, when I call `server.Poll()`, then the status
   file is generated with "not_processed" entries for the buried chunk's files.
   The active run remains in place (no resubmission).

6. Given an active run where all jobs are in terminal state but 1 is buried, and
   the fofn mtime HAS changed, when I call `server.Poll()`, then: the status
   file for the old run is generated, the buried job is deleted from wr, the
   active run is cleared, and a new run is started with the updated fofn.

7. Given an active run where all jobs completed successfully (run directory
   "1000") and a *previous* run directory ("500") still exists on disk, when I
   call `server.Poll()`, then the "500" directory is deleted (cleanup of old
   successful/historic runs).

### H4: Restart resilience

As a developer using the fofn package, I want the server on startup to detect
existing wr jobs from a previous instance, so that it waits for them to complete
rather than starting duplicate jobs.

**Package:** `fofn/`
**File:** `fofn/server.go`
**Test file:** `fofn/server_test.go`

**Acceptance tests:**

1. Given a run directory "1000" with chunk files, and a mock JobSubmitter that
   reports incomplete jobs for RepGroup "ibackup_fofn_proj_1000", when a new
   Server initialises and calls `Poll()`, then it does NOT submit new jobs but
   records the active run and waits for its completion.

2. Given a run directory "1000" with chunk files and completed report files, and
   a mock JobSubmitter that reports all jobs for RepGroup
   "ibackup_fofn_proj_1000" are complete, when a new Server initialises and
   calls `Poll()`, then it generates the status file and checks if the fofn has
   been updated since mtime 1000.

### H5: Parallel processing of multiple subdirectories

As a developer using the fofn package, I want the server to process multiple
subdirectories in parallel during a single poll cycle, so that throughput is
maximised (wr handles resource scheduling).

**Package:** `fofn/`
**File:** `fofn/server.go`
**Test file:** `fofn/server_test.go`

**Acceptance tests:**

1. Given a watch directory with 3 subdirectories each containing a fofn, when I
   call `server.Poll()`, then all 3 subdirectories are processed (jobs
   submitted) in the same poll cycle.

2. Given 3 subdirectories, 2 with active running jobs and 1 with a new fofn,
   when I call `server.Poll()`, then only the new subdirectory has jobs
   submitted; the other 2 are left to complete.

---

## Section I: CLI Command

### I1: ibackup fofnserver subcommand

As a user, I want an `ibackup fofnserver` subcommand, so that I can start the
fofnserver with appropriate configuration.

**Package:** `cmd/`
**File:** `cmd/fofnserver.go`

The command accepts these flags:

| Flag              | Type     | Default    | Description            |
|-------------------|----------|------------|------------------------|
| `--dir`           | string   | required   | Watch directory        |
| `--interval`      | duration | 5m         | Poll interval          |
| `--chunk-size`    | int      | 10000      | Files per chunk        |
| `--wr-deployment` | string   | production | wr deployment name     |
| `--ram`           | int      | 1024       | RAM (MB) per put job   |
| `--time`          | duration | 8h         | Time limit per put job |
| `--retries`       | int      | 3          | wr job retries         |
| `--limit-group`   | string   | irods      | wr limit group         |

The command:
1. Validates that `--dir` is provided and exists.
2. Validates that the IBACKUP_CONFIG environment variable is set (required for
   named transformers).
3. Creates a `fofn.Server` with the configured options.
4. Calls `fofn.Server.Run()` which blocks, polling at the configured interval.
5. On SIGINT/SIGTERM, exits immediately (wr jobs continue independently).

**Acceptance tests:**

1. Given no `--dir` flag, when I run `ibackup fofnserver`, then it exits with a
   non-zero code and an error message.

2. Given a valid `--dir` pointing to an existing directory, when I run `ibackup
   fofnserver --dir /tmp/test --interval 1s` and send SIGINT after 2 seconds,
   then it exits cleanly with code 0.

---

## Section J: Integration Test

### J1: End-to-end fofnserver with PretendSubmissions

As a developer, I want an integration test that exercises the full fofnserver
pipeline from CLI to status file, so that I can verify all components work
together.

**Package:** `main`
**File:** `main_test.go`

This test uses the wr client's `PretendSubmissions` mode so that no real wr
server is needed. It also uses the `internal.LocalHandler` mock so that no real
iRODS is needed. Since `ibackup put` jobs are "submitted" but not actually
executed via wr, the test must also directly invoke `ibackup put` (with the mock
handler) to generate report files, simulating what wr would do.

**Acceptance tests:**

1. **New fofn processing:**
   Given a watch directory with a subdirectory containing:
   - A fofn with 5 null-terminated paths to real temporary local files,
   - A .transformer file with a registered transformer,
   - A .metadata file with `colour\tred`,
   When the fofnserver processes this directory (simulated by calling the fofn
   package directly or building and running the binary), Then:
   - A run directory is created named after the fofn mtime.
   - Chunk files exist in the run directory.
   - Jobs were submitted (recorded by PretendSubmissions).
   - After simulating job execution (running `ibackup put` directly for each
     chunk), report files are written.
   - After calling status generation, a "status" file exists in the
     subdirectory.
   - The status file contains 5 data lines and a correct SUMMARY line.
   - All created files and directories have the correct group ownership.
   - Each submitted put command includes `--fofn <dirname>` and `--meta
     "colour=red"`.

2. **Freeze mode:**
   Given the same setup but with a .freeze file, and some files already
   "uploaded" (mock handler has them with an older mtime), when the fofnserver
   processes this directory, then the submitted put commands include
   `--no_replace`, and the status file shows "frozen" for the pre-existing files
   and "uploaded" for the new ones.

3. **Restart resilience:**
   Given a previously created run directory with chunk files, when a new fofn
   server instance starts and finds existing wr jobs (via PretendSubmissions),
   then it does not submit duplicate jobs. (Since wr itself rejects duplicates,
   this is mostly a behavioural test that the server doesn't error out.)

4. **Buried jobs with fofn update:**
   Given a completed run with 1 buried job, and the fofn has been updated (new
   mtime), when the server polls, then: the status file for the old run is
   written, the buried job is deleted from wr, and a new run is started with the
   updated fofn.

---

## Implementation Order

The following order ensures each step builds on tested foundations. Each step
corresponds to a set of user stories that should be implemented using TDD.

### Phase 1: Report and status format (fofn/report.go)

1. **D1** - Report line formatting and parsing
2. **D2** - Streaming report write and callback read

These are pure data format functions with no external dependencies. They form
the foundation for everything else.

### Phase 2: Null-terminated scanning (internal/scanner/)

3. **A1** - `scanner.ScanNullTerminated` (streaming, memory test)

A2 requires no new code; uses existing `transformer`/`transfer` packages.

### Phase 3: Streaming shuffle and chunking (fofn/chunk.go)

4. **B1** - `fofn.WriteShuffledChunks` (streaming, memory test)

### Phase 4: Put enhancements (transfer/ + cmd/)

5. **C1** - RequestStatusFrozen constant
6. **C2** - Putter no-replace mode
7. **C3** - --no_replace CLI flag (main_test.go)
8. **C4** - MetaKeyFofn constant
9. **C5** - --fofn CLI flag (main_test.go)

### Phase 5: Put report output (cmd/)

10. **D3** - --report CLI flag (main_test.go)

### Phase 6: wr job management (fofn/jobs.go)

11. **F1** - Create put job commands
12. **F2** - Create wr jobs for chunks
13. **F3** - Submit jobs via interface
14. **F4** - Check run completion
15. **F5** - Identify buried chunks
16. **F6** - Delete buried jobs

### Phase 7: Status generation (fofn/status.go)

17. **E1** - `fofn.WriteStatusFromRun` (streaming, memory test)

### Phase 8: Directory scanning and group ownership

18. **G6** - Group ownership (internal/ownership/)
19. **G1** - Discover subdirectories with fofn files (fofn/scan.go)
20. **G2** - Read transformer name
21. **G3** - Detect .freeze file
22. **G4** - Detect fofn needing processing
23. **G5** - Read user metadata from .metadata file

### Phase 9: Orchestration (fofn/server.go)

24. **H1** - Process single subdirectory end-to-end
25. **H2** - Generate status after run completion
26. **H3** - Handle fofn update while jobs running or buried
27. **H4** - Restart resilience
28. **H5** - Parallel processing

### Phase 10: CLI and integration (cmd/ + main_test.go)

29. **I1** - fofnserver CLI subcommand
30. **J1** - End-to-end integration test

---

## Appendix: Key Decisions

### Existing code reuse

- **Path transformation:** Use `transformer.MakePathTransformer()` and
  `transfer.NewRequestWithTransformedLocal()` directly. No new transformation
  code.
- **Null scanning:** `scanNulls` and `fofnLineSplitter` in `cmd/put.go` are
  moved to `internal/scanner/`. Both `cmd/` and `fofn/` import from there.
- **Test helpers:** Use `internal.RegisterDefaultTransformers()`,
  `internal.CreateTestFile()`, and `internal.LocalHandler` from the existing
  `internal/` package.

### Streaming and memory

Fofns may contain 10s of millions of file paths. All code that touches the full
set of entries must use streaming (callback or channel) patterns; no function
may accumulate all entries in a slice. Specifically:

- `scanner.ScanNullTerminated` streams via callback.
- `fofn.WriteShuffledChunks` streams from scanner to chunk files.
- `fofn.WriteReportEntry` writes one entry at a time.
- `fofn.ParseReportCallback` streams via callback.
- `fofn.WriteStatusFromRun` streams from report files to status file.
- `fofn.ProcessSubDir` (H1) never holds all paths in memory; it pipes the
  scanner output through transform and into chunk writing.

Each of A1, B1, and E1 includes a memory-bounded acceptance test that creates
1,000,000 entries and asserts heap growth stays under a threshold (10–20 MB).
These tests use `runtime.ReadMemStats` before and after the operation, with a
forced `runtime.GC()` before each measurement.

Note: `fofn.CollectReport` and `scanner.CollectNullTerminated` convenience
wrappers (returning slices) exist for tests and small config files, but must not
be used in the production pipeline.

### Error handling

- If a fofn cannot be parsed, log a warning and skip that subdirectory until the
  next poll.
- If the transformer is invalid or unregistered, log a warning and skip.
- If wr job submission fails, log an error and retry on the next poll cycle.
- If report parsing fails for a completed chunk, treat all files in that chunk
  as "not_processed".

### Testing strategy

- **internal/scanner/ tests:** Pure unit tests with `t.TempDir()`. Include
  memory-bounded test with 1M entries.

- **internal/ownership/ tests:** Pure unit tests with `t.TempDir()`.

- **fofn/ package tests:** Use a mock `JobSubmitter` (defined in fofn/ test
  files) that records submitted jobs and allows tests to control job state
  responses. Use `t.TempDir()` for filesystem operations. Register test
  transformers using `transformer.Register()`. Include memory-bounded tests for
  B1 and E1.

- **cmd/put.go tests:** Use the existing `internal.LocalHandler` mock for
  iRODS operations. Test --no_replace, --report, and --fofn flags via
  `main_test.go` integration tests.

- **main_test.go integration:** Use `client.PretendSubmissions` to avoid needing
  a real wr server. Simulate job execution by directly running put commands
  against mock handlers.

### Memory-bounded test pattern

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
    // 5. Assert:
    growth := after.HeapInuse - before.HeapInuse
    So(growth, ShouldBeLessThan, 20*1024*1024)
}
```

The threshold is generous (10–20 MB) to avoid flakiness. A non-streaming
implementation holding 1M entries would use ~200+ MB, so the test clearly
distinguishes the two approaches.

### Boilerplate

All new source files must start with:

```
/*******************************************************************************
 * Copyright (c) 2026 Genome Research Ltd.
 *
 * Authors:
 *	- Sendu Bala <sb10@sanger.ac.uk>
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

### TDD cycle

For each acceptance test:

1. Write a failing test (GoConvey style).
2. Run: `CGO_ENABLED=1 go test -tags netgo --count 1 ./<path> -v -run`
   `<TestFunc>`
3. Write minimal implementation to pass.
4. Refactor (short functions, low complexity, self-documenting names,
   100-col line wrap, 80-col comment wrap).
5. Run `cleanorder <file>` on every edited .go file.
6. Run `golangci-lint run --fix` and fix remaining issues.
7. Re-run the test to confirm it still passes.
