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

// Command speedgate is the `make speed` regression gate. It runs the
// developers/speed benchmarks against the working tree (head) and a baseline
// revision (base) interleaved on the same host, and fails if head is more than
// a threshold percentage slower than base on any benchmark. See
// developers/README.md.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const (
	defaultBase      = "origin/develop"
	defaultThreshold = 10
	defaultRounds    = 10
	defaultCount     = 1
	defaultBenchtime = "1x"
	defaultBench     = "."
	tmpfsDir         = "/dev/shm"

	// waitDelay is how long a cancelled command's output pipes may stay open
	// after it is killed, in case a child of it holds them.
	waitDelay = 5 * time.Second

	exitPassed     = 0
	exitRegression = 1
	exitError      = 2
)

var errBadEnv = errors.New("invalid environment variable")

// command runs the named program in dir, returning its trimmed stdout, and on
// failure an error that includes its stderr. The program is killed if ctx is
// cancelled.
func command(ctx context.Context, dir string, env []string, name string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, name, args...)

	cmd.Dir = dir
	cmd.WaitDelay = waitDelay

	cmd.Env = append(os.Environ(), env...)

	var stderr strings.Builder

	cmd.Stderr = &stderr

	out, err := cmd.Output()

	if ctx.Err() != nil {
		return "", fmt.Errorf("%s interrupted: %w", name, ctx.Err())
	}

	if err != nil {
		err = fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, stderr.String())
	}

	return strings.TrimSpace(string(out)), err
}

// config holds the gate's settings, taken from SPEED_* environment variables.
type config struct {
	base      string
	threshold float64
	rounds    int
	count     int
	benchtime string
	bench     string
	tmpDir    string
}

func configFromEnv() (config, error) {
	cfg := config{
		base:      envString("SPEED_BASE", defaultBase),
		benchtime: envString("SPEED_BENCHTIME", defaultBenchtime),
		bench:     envString("SPEED_BENCH", defaultBench),
		tmpDir:    envString("SPEED_TMPDIR", defaultTmpDir()),
	}

	var err error

	cfg.threshold, err = envNumber("SPEED_THRESHOLD", defaultThreshold)
	if err != nil {
		return cfg, err
	}

	cfg.rounds, err = envInt("SPEED_ROUNDS", defaultRounds)
	if err != nil {
		return cfg, err
	}

	cfg.count, err = envInt("SPEED_COUNT", defaultCount)

	return cfg, err
}

func configAndRun(ctx context.Context) (bool, error) {
	cfg, err := configFromEnv()
	if err != nil {
		return false, err
	}

	return run(ctx, cfg)
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)

	code := exitCode(configAndRun(ctx))

	stop()
	os.Exit(code)
}

// exitCode returns the gate's exit code for the given outcome of run(),
// logging any error: 2 if it could not run or was interrupted, 1 if head
// regressed, otherwise 0.
func exitCode(regressed bool, err error) int {
	switch {
	case errors.Is(err, context.Canceled):
		slog.Error("speed gate interrupted", "err", err)

		return exitError
	case err != nil:
		slog.Error("speed gate could not run", "err", err)

		return exitError
	case regressed:
		return exitRegression
	default:
		return exitPassed
	}
}

func envString(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}

	return def
}

// envNumber returns the positive number in the given environment variable, or
// def if it is unset.
func envNumber(key string, def float64) (float64, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}

	n, err := strconv.ParseFloat(v, 64)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("%w: %s=%q is not a positive number", errBadEnv, key, v)
	}

	return n, nil
}

// envInt returns the positive integer in the given environment variable, or
// def if it is unset.
func envInt(key string, def int) (int, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}

	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("%w: %s=%q is not a positive integer", errBadEnv, key, v)
	}

	return n, nil
}

// defaultTmpDir returns tmpfsDir if it exists, so that benchmark databases
// avoid the fsync latency of a shared disk, which is too noisy for the gate.
func defaultTmpDir() string {
	if info, err := os.Stat(tmpfsDir); err == nil && info.IsDir() {
		return tmpfsDir
	}

	return os.TempDir()
}
