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

package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/wtsi-hgi/ibackup/internal"
)

const benchTimeout = "30m"

var errBaseBuild = errors.New("head's benchmarks do not build in base, which may lack APIs they use; " +
	"set SPEED_BASE to a revision that has them (see developers/README.md)")

// sideID identifies which of the two trees being compared a side is.
type sideID int

const (
	baseSide sideID = iota
	headSide
)

// buildStatter builds the statter the benchmarks use for discovery into dir,
// returning the environment that tells them where it is. Both sides use the
// same statter, since it is a separate program.
func buildStatter(dir string) ([]string, error) {
	if err := internal.BuildStatter(dir); err != nil {
		return nil, err
	}

	return []string{"IBACKUP_TEST_STATTER=" + filepath.Join(dir, "statter")}, nil
}

func (id sideID) String() string {
	if id == baseSide {
		return "base"
	}

	return "head"
}

// result is the outcome of one benchmark run.
type result struct {
	nsPerOp     float64
	allocsPerOp float64
}

type namedResult struct {
	name string
	result
}

// parseBenchLine parses a result line like:
//
//	BenchmarkUpload-8  1  4511231463 ns/op  943523424 B/op  6792500 allocs/op
func parseBenchLine(line string) (namedResult, bool) {
	fields := strings.Fields(line)
	if len(fields) < 4 || !strings.HasPrefix(fields[0], "Benchmark") { //nolint:mnd
		return namedResult{}, false
	}

	r := namedResult{name: fields[0]}

	for i := 2; i+1 < len(fields); i += 2 {
		if v, err := strconv.ParseFloat(fields[i], 64); err == nil {
			r.setMetric(fields[i+1], v)
		}
	}

	return r, r.nsPerOp > 0
}

func (r *namedResult) setMetric(unit string, v float64) {
	switch unit {
	case "ns/op":
		r.nsPerOp = v
	case "allocs/op":
		r.allocsPerOp = v
	}
}

// side is one of the two trees being compared.
type side struct {
	id  sideID
	dir string
	bin string
}

// build compiles the benchmark package of the given side's tree.
func (s side) build(ctx context.Context) error {
	_, err := command(ctx, s.dir, []string{"CGO_ENABLED=1"}, "go", "test", "-c", "-tags", "netgo",
		"-o", s.bin, "./"+benchPkgDir)
	if err != nil && s.id == baseSide {
		return fmt.Errorf("%w: building base benchmarks: %w", errBaseBuild, err)
	}

	if err != nil {
		return fmt.Errorf("building head benchmarks: %w", err)
	}

	return nil
}

// run runs the benchmarks matching the bench regexp with the side's benchmark
// binary once in workDir, with env on top of our own environment.
func (s side) run(ctx context.Context, cfg config, bench, workDir string, env []string) ([]namedResult, error) {
	out, err := command(ctx, workDir, env, s.bin, "-test.run=^$", "-test.bench="+bench,
		"-test.benchtime="+cfg.benchtime, "-test.count="+strconv.Itoa(cfg.count),
		"-test.benchmem", "-test.timeout="+benchTimeout)
	if err != nil {
		return nil, fmt.Errorf("running %s benchmarks: %w\n%s", s.id, err, out)
	}

	return parseBenchOutput(out), nil
}

// parseBenchOutput returns the results in the given `go test -bench` output.
func parseBenchOutput(out string) []namedResult {
	var results []namedResult

	for line := range strings.Lines(out) {
		if r, ok := parseBenchLine(line); ok {
			results = append(results, r)
		}
	}

	return results
}

// samples holds the results of each benchmark for each side, with the
// benchmark names in order of first appearance.
type samples struct {
	names   []string
	results [2]map[string][]result
}

func newSamples() *samples {
	return &samples{results: [2]map[string][]result{make(map[string][]result), make(map[string][]result)}}
}

func (s *samples) add(id sideID, results []namedResult) {
	for _, r := range results {
		if _, seen := s.results[baseSide][r.name]; !seen {
			if _, seen = s.results[headSide][r.name]; !seen {
				s.names = append(s.names, r.name)
			}
		}

		s.results[id][r.name] = append(s.results[id][r.name], r.result)
	}
}

// rounds runs both sides' benchmarks in a work dir.
type rounds struct {
	cfg     config
	sides   [2]side
	workDir string
	env     []string
}

// runAndCompare does run(), then returns the comparisons of all the results,
// or an error if they can't be judged.
func (r rounds) runAndCompare(ctx context.Context, all *samples, label, bench string) ([]comparison, error) {
	if err := r.run(ctx, all, label, bench); err != nil {
		return nil, err
	}

	comparisons := compare(all, r.cfg.threshold)

	return comparisons, checkMeasured(comparisons, bench)
}

// run runs the benchmarks matching the bench regexp on both sides interleaved
// for cfg.rounds rounds, adding the results to all. It alternates which side
// goes first, so that changes in load on a shared host affect both sides
// alike.
func (r rounds) run(ctx context.Context, all *samples, label, bench string) error {
	for round := range r.cfg.rounds {
		order := r.sides
		if round%2 == 1 {
			order = [2]side{r.sides[1], r.sides[0]}
		}

		for _, s := range order {
			results, err := s.run(ctx, r.cfg, bench, r.workDir, r.env)
			if err != nil {
				return err
			}

			all.add(s.id, results)
			reportProgress(label, round, r.cfg.rounds, s, results)
		}
	}

	return nil
}

func reportProgress(label string, round, rounds int, s side, results []namedResult) {
	for _, r := range results {
		fmt.Fprintf(os.Stderr, "%s %d/%d %s %s %s\n", label, round+1, rounds, s.id, r.name, fmtNs(r.nsPerOp))
	}
}

// benchPattern returns a -test.bench regexp matching only the given
// benchmarks, named as in `go test` output.
func benchPattern(names []string) string {
	quoted := make([]string, len(names))

	for i, name := range names {
		quoted[i] = regexp.QuoteMeta(trimCPUSuffix(name))
	}

	return "^(" + strings.Join(quoted, "|") + ")$"
}

// trimCPUSuffix removes the -GOMAXPROCS suffix `go test` adds to benchmark
// names.
func trimCPUSuffix(name string) string {
	i := strings.LastIndexByte(name, '-')
	if i < 0 {
		return name
	}

	if _, err := strconv.Atoi(name[i+1:]); err != nil {
		return name
	}

	return name[:i]
}
