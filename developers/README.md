# Developer tools

## Test zone
`make test-container` and `make race-container` run `make test` and
`make race` against a throwaway iRODS 4.3.5 zone and wr manager of their own,
whatever your iRODS and wr settings are, and stop them afterwards, even after
a failure, Ctrl-C or SIGTERM. They use [testzone.sh](testzone.sh), which you
can also run yourself:

```bash
developers/testzone.sh run go test -tags netgo -count 1 -run TestPuts .
developers/testzone.sh up /tmp/myzone && source /tmp/myzone/env.sh
developers/testzone.sh down /tmp/myzone
```

`up` needs a new or empty directory, which it makes private to you, and
`down` deletes it. On a signal, `run` sends SIGTERM to all of its command's
processes and waits for them before tearing down, then exits with 130 for
SIGINT, 143 for SIGTERM or 129 for SIGHUP; otherwise it exits with the
command's exit code.

You need wr on your PATH and Singularity or Apptainer, which runs the
iCommands and `baton-do`. If neither is on your PATH, the script runs
`module load ISG/singularity/3.11.4` (set `IBACKUP_TEST_SINGULARITY_MODULE`
for another module). The iRODS server runs in Singularity too, unless you set
`IBACKUP_TEST_RUNTIME` to `docker` or `podman`. Only Singularity has been run
locally; CI's `test` job runs a smoke test of the docker server.

The images are the ones CI uses, read from
[tests.yml](../.github/workflows/tests.yml). The first run pulls about 300MB
of them into `~/.cache/ibackup/testzone` (or under `$XDG_CACHE_HOME`); after
that a zone takes about 10 seconds to start.

The clients use the wrappers' shared Singularity instance, named
`ghcr.io--wtsi-npg--ub-22.04-irods-clients-4.3.5--9.8`. The script starts it
from the cached image if it is not already running, but, like the wrappers
themselves, never stops it, as anything else using those wrappers may be
using it. Stop it yourself with `singularity instance stop` once nothing
needs it.

The clients run inside that instance, so they only see the host paths it
binds: by default your home, `/tmp` and the directory it was started from,
plus any your site's Singularity config adds. The zone's directory must be
one of those, so use a `TMPDIR` that the instance binds, and an `up` directory
under your home or `/tmp`, or the clients won't find the zone's iRODS
environment files. The tests' own temp files are under `TMPDIR` too.

The zone runs as you, with its server and work files in its directory. Its
tests log in as iRODS user `ibackup`, and the admin is `irods`, both with
random passwords, and the server's control plane key is random too. Your
`~/.irods` is not used. The tests get `IRODS_ENVIRONMENT_FILE`,
`IBACKUP_TEST_IRODS_ADMIN_ENVIRONMENT_FILE`, `IBACKUP_TEST_COLLECTION`,
`IBACKUP_TEST_SCHEDULER`, `WR_DEPLOYMENT` and the `WR_MANAGER*` settings for
the zone. Any other `IRODS_*`, `WR_*` and `IBACKUP_TEST_COLLECTION*`
variables you have, and any `CONFIGOR_*` ones (wr reads its config with
configor), are unset, by `run`, `up` and sourcing `env.sh` (from bash).

Non-empty wr env vars win over wr config files, and the zone sets every
manager setting that its wr clients read (`WR_MANAGERHOST`, `_PORT`, `_WEB`,
`_DIR`, `_TOKENFILE`, `_CAFILE` and `_CERTDOMAIN`), so no `.wr_config*.yml`
of yours, in any directory, can point the tests at another manager. The
zone's manager itself is started and stopped in the zone's directory with a
HOME of its own, so your config files can't reach its other settings either.

Each zone takes a block of 260 ports from 21000 up, the first block that no
other zone holds and that has no sockets in use. A zone holds its block with
a lock file, `/tmp/ibackup-testzone-<port>.lock`, so you and others can run
several zones on one host at once. If the script is killed before it can tear
down, run `down` on its directory, which for `run` is
`${TMPDIR:-/tmp}/ibackup-testzone.*`.

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

### How it compares
[speedgate](speedgate) extracts the base revision's files into a temporary
directory with `git archive | tar -x`, copies the benchmark package into it,
and builds a test binary from each tree. Its only other use of git is
`git rev-parse` to find the repository and resolve `SPEED_BASE`, so it leaves
the repository unchanged. It then runs the two binaries in turn for several
rounds, alternating which goes first, and compares the median ns/op of each
benchmark. Head is the working tree you run it in, uncommitted changes
included.

A benchmark more than the threshold slower on head is not yet a failure. The
gate runs just the benchmarks that were over it for another `SPEED_ROUNDS`
rounds, pools those results with the first ones, and fails only if the pooled
medians are still over the threshold. This stops one noisy burst on a shared
host from failing the gate.

Base must have the APIs and dependencies that head's benchmarks use. If you
change or remove an API that the benchmarks call, you must update the
benchmarks too, and base's copy will then fail to compile: the gate exits 2,
saying that the benchmarks do not build in base, with the compiler error. In
that case set `SPEED_BASE` to a revision that has the new API, such as a
commit of yours that only changes the API, and compare against that.

The benchmarks need the `log15/v3` and wr v0.38.0 dependencies of PR #193, so
comparing against develop works once that PR has merged. That PR's own
comparison with develop is recorded in its description.

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
| `SPEED_TMPDIR`    | `/dev/shm`       | where the base tree and benchmark files go |

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
Ctrl-C, SIGTERM or SIGHUP sent to make's process group stops the benchmarks,
removes the gate's work dir, which holds the base tree, and removes make's
build dir for the gate. For each of the three, make reports `Error 130` on its
`make: *** [Makefile:...: speed]` line and then dies by the same signal, so
the calling shell may add its own report, such as `Hangup`, and sees an exit
status of 130, 143 or 129. If the gate is killed without a chance to clean
up, its work dir stays behind; once no gate is running, remove it with:

```bash
rm -rf /dev/shm/ibackup-speed-*
```
