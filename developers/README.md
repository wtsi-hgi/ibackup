# Developer tools

## Speed gate
`make speed` checks the critical upload path for speed regressions. It must
pass when you change that path (see the main
[README](../README.md#development)).

### What it measures
The benchmarks in [speed](speed) drive an in-process server over HTTPS with
real clients, using the local mock storage handler instead of iRODS and
pretend put job submission instead of wr:

- `BenchmarkUpload`: a set of 2000 small files (10% of them hardlinks) goes
  through discovery, request reservation by 4 concurrent put clients,
  transfer, result reporting with `SendPutResultsToServer` and set
  completion.
- `BenchmarkRemove`: `ibackup remove` (trashing) of all files of an uploaded
  set of 1000 files.

Each op gets a new server and database, so every op does the same work. Time
outside the measured steps, such as server start-up, is excluded.

After each op, outside the timed part, the benchmarks check that the work was
really done, so that a broken path that skips work cannot look fast. After
an upload, every file must be counted as uploaded, none as skipped, replaced,
failed or missing; every file must have a remote copy of the right size
(empty for a hardlink, with one inode file per group of hardlinks); and the
server must have submitted put jobs. After a removal, the set must have no
files left, and every remote copy's sets metadata must name the set's trash
set (`.trash-speed`) instead of the set. A failed check fails the benchmark
and so the gate.

An upload that stops getting requests before its set is complete also fails
the benchmark. Once more than 10 rounds of put clients in a row get no
requests, which takes about 4 seconds, the benchmark stops starting new
clients and fails.

The one exception is for base revisions from before the server stopped
separate clients uploading a shared hardlink inode at the same time, such as
develop at 1727e87. There, a client may skip a hardlink whose inode another
client is uploading. The gate tells base's benchmarks, but never head's, to
accept skipped files as long as each is a hardlink.

### How it compares
[speedgate](speedgate) checks out the base revision in a temporary detached
worktree, copies the benchmark package into it, and builds a test binary
from each tree. It then runs the two binaries in turn for several rounds,
alternating which goes first, and compares the median ns/op of each
benchmark. Head is the working tree you run it in, uncommitted changes
included.

A benchmark more than the threshold slower on head is not yet a failure. The
gate runs just the benchmarks that were over it for another `SPEED_ROUNDS`
rounds, pools those results with the first ones, and fails only if the pooled
medians are still over the threshold. This stops one noisy burst on a shared
host from failing the gate.

The benchmarks only use APIs present in both trees. Where base requires the
pre-v3 `github.com/inconshreveable/log15` module, the gate rewrites the
benchmarks' `log15/v3` import to it; the two have the same API for what the
benchmarks use. If you change or remove an API that the benchmarks call, you
must update the benchmarks too, and base's copy will then fail to compile:
the gate exits 2 with the compiler error. In that case set `SPEED_BASE` to a
revision that has the new API, such as a commit of yours that only changes
the API, and compare against that.

Benchmark files go under /dev/shm when it exists. On a shared disk, database
fsync latency varies too much for a 10% threshold, so the gate measures CPU
and code path cost rather than disk speed.

### Running it
```bash
make speed
SPEED_BASE=origin/master SPEED_ROUNDS=4 make speed
```

The gate takes about 3 minutes, or twice that if it needs to confirm a
regression. It does not fetch, so run `git fetch origin` first if
origin/develop may be stale. Avoid running other heavy work at the same time;
the gate prints a warning above its table if the 1 minute load average was
over 0.5 per CPU when it started.

| Variable          | Default          | Meaning                                    |
| ----------------- | ---------------- | ------------------------------------------ |
| `SPEED_BASE`      | `origin/develop` | git ref to compare against                 |
| `SPEED_THRESHOLD` | `10`             | percent slowdown that fails the gate       |
| `SPEED_ROUNDS`    | `10`             | interleaved rounds of both trees           |
| `SPEED_COUNT`     | `1`              | `-test.count` per run                      |
| `SPEED_BENCHTIME` | `1x`             | `-test.benchtime` per run                  |
| `SPEED_BENCH`     | `.`              | `-test.bench` regexp                       |
| `SPEED_TMPDIR`    | `/dev/shm`       | where the worktree and benchmark files go  |

To run the benchmarks alone:

```bash
CGO_ENABLED=1 go test -tags netgo -run '^$' -bench . -benchtime 1x ./developers/speed
```

### Reading the results
Progress for each run goes to stderr. At the end the gate prints a table:

```text
benchmark          base median  head median  change  base spread  head spread  base allocs/op  head allocs/op  samples
BenchmarkUpload-8  4.542s       4.493s       -1.1%   11.5%        12.1%        6810435         6795220         10/10
BenchmarkRemove-8  352.116ms    317.429ms    -9.9%   30.0%        13.8%        1020712         753582          10/10
```

`change` is how much slower (positive) or faster (negative) head's median is
than base's. Spread is (max - min) / median of a side's samples; large
spreads mean the host was busy. Allocation counts include the server's, and
are for information only.

If a benchmark needed confirmation, a second table shows the pooled results.
For each such benchmark the gate then prints either a `FAIL:` line with both
pooled medians and the change in the first and pooled samples, or a line
saying it was within the threshold once pooled.

The speedgate program exits 0 if it passed, 1 if any benchmark regressed, and
2 if it could not run or could not judge a benchmark. That includes a failed
build or benchmark, a `SPEED_BENCH` that matched no benchmark, a benchmark
with results from only one tree, and a bad `SPEED_*` value: `SPEED_ROUNDS`
and `SPEED_COUNT` must be positive integers, and `SPEED_THRESHOLD` a positive
number. make shows the code as `Error 1` or `Error 2` on its
`make: *** [Makefile:...: speed]` line, then itself exits 2 for any failure,
so check that line, or for a script, the table and verdict.

Comparing a revision with itself on a busy shared host has given changes
within 5%. If a result is near the threshold and spreads are large, run it
again when the host is quieter before treating it as a regression.

### Interrupting it
Ctrl-C (or SIGTERM) stops the benchmarks and removes the base worktree and
the gate's work dir; make then reports `Error 130` on its
`make: *** [Makefile:...: speed]` line. If the gate is killed without a
chance to clean up, its next run removes the work dirs that earlier runs on
the same host left behind, with `git worktree remove --force` on each one's
base worktree. Only if git can't remove a base worktree that is still
registered does the gate run `git worktree prune`, which drops the
registrations of all the repo's worktrees whose directories no longer exist.
To clean up by hand instead:

```bash
for d in /dev/shm/ibackup-speed-*; do git worktree remove --force "$d/base"; rm -rf "$d"; done
```

Only remove the work dirs of runs that have finished; the gate checks this by
the process ID it records in each.
