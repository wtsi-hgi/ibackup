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
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

const (
	benchA      = "BenchmarkA"
	benchUpload = "BenchmarkUpload-8"
)

func TestParseBenchOutput(t *testing.T) {
	Convey("parseBenchOutput returns the time and allocs of each result line", t, func() {
		out := `goos: linux
BenchmarkUpload-8   	       1	4511231463 ns/op	943523424 B/op	 6792500 allocs/op
--- FAIL: BenchmarkOther
BenchmarkRemove-8   	       2	 163686436 ns/op
PASS`

		results := parseBenchOutput(out)
		So(results, ShouldResemble, []namedResult{
			{name: benchUpload, result: result{nsPerOp: 4511231463, allocsPerOp: 6792500}},
			{name: "BenchmarkRemove-8", result: result{nsPerOp: 163686436}},
		})
	})
}

func TestMedian(t *testing.T) {
	Convey("median returns the middle value, or the mean of the middle two", t, func() {
		So(median(nil), ShouldEqual, 0)
		So(median([]float64{5}), ShouldEqual, 5)
		So(median([]float64{9, 1, 5}), ShouldEqual, 5)
		So(median([]float64{9, 1, 3, 5}), ShouldEqual, 4)
	})
}

func TestFmtNs(t *testing.T) {
	Convey("fmtNs shows durations to at least 4 significant figures", t, func() {
		So(fmtNs(111), ShouldEqual, "111ns")
		So(fmtNs(163686436), ShouldEqual, "163.686ms")
		So(fmtNs(4511231463), ShouldEqual, "4.511s")
	})
}

func TestCompare(t *testing.T) {
	Convey("Given base results with a median of 100ns", t, func() {
		all := newSamples()
		all.add(baseSide, []namedResult{
			{name: benchA, result: result{nsPerOp: 90}},
			{name: benchA, result: result{nsPerOp: 100}},
			{name: benchA, result: result{nsPerOp: 500}},
		})

		addHead := func(ns ...float64) {
			for _, n := range ns {
				all.add(headSide, []namedResult{{name: benchA, result: result{nsPerOp: n}}})
			}
		}

		Convey("head exactly at the threshold passes", func() {
			addHead(110, 1, 1000)

			comparisons := compare(all, 10)
			So(comparisons, ShouldHaveLength, 1)
			So(comparisons[0].change, ShouldAlmostEqual, 10)
			So(comparisons[0].regressed, ShouldBeFalse)
			So(comparisons[0].base.spread, ShouldAlmostEqual, 410)

			So(regressedNames(comparisons), ShouldBeEmpty)

			var sb strings.Builder
			So(writeVerdict(&sb, comparisons, comparisons, 10, "base x"), ShouldBeFalse)
			So(sb.String(), ShouldContainSubstring, "speed gate passed")
		})

		Convey("head over the threshold needs confirmation", func() {
			addHead(111, 120, 105)

			first := compare(all, 10)
			So(first[0].change, ShouldAlmostEqual, 11)
			So(first[0].regressed, ShouldBeTrue)
			So(regressedNames(first), ShouldResemble, []string{benchA})

			var sb strings.Builder
			writeTable(&sb, first)
			So(sb.String(), ShouldContainSubstring, "+11.0%")

			Convey("and fails if the pooled results still exceed it, reporting first and pooled", func() {
				all.add(baseSide, []namedResult{{name: benchA, result: result{nsPerOp: 100}}})
				addHead(115)

				final := compare(all, 10)
				So(final[0].change, ShouldAlmostEqual, 13)
				So(final[0].regressed, ShouldBeTrue)

				sb.Reset()
				So(writeVerdict(&sb, first, final, 10, "base x"), ShouldBeTrue)
				So(sb.String(), ShouldContainSubstring,
					"FAIL: BenchmarkA: head 113ns is 13.0% slower than base 100ns (threshold 10%); "+
						"first 3/3 samples +11.0%, pooled 4/4 samples +13.0%")
				So(sb.String(), ShouldContainSubstring, "speed gate FAILED against base x")
			})

			Convey("but passes if the pooled results are within it, reporting first and pooled", func() {
				all.add(baseSide, []namedResult{
					{name: benchA, result: result{nsPerOp: 100}},
					{name: benchA, result: result{nsPerOp: 100}},
					{name: benchA, result: result{nsPerOp: 100}},
				})
				addHead(100, 101, 102)

				final := compare(all, 10)
				So(final[0].change, ShouldAlmostEqual, 3.5)
				So(final[0].regressed, ShouldBeFalse)

				sb.Reset()
				So(writeVerdict(&sb, first, final, 10, "base x"), ShouldBeFalse)
				So(sb.String(), ShouldContainSubstring,
					"BenchmarkA was 11.0% slower in the first 3/3 samples, but +3.5% in the pooled "+
						"6/6 samples, within the threshold")
				So(sb.String(), ShouldContainSubstring, "speed gate passed")
			})
		})

		Convey("a faster head passes", func() {
			addHead(50, 50, 50)

			comparisons := compare(all, 10)
			So(comparisons[0].change, ShouldAlmostEqual, -50)
			So(comparisons[0].regressed, ShouldBeFalse)
		})

		Convey("a benchmark with no head results cannot be judged", func() {
			err := checkMeasured(compare(all, 10), ".")
			So(err, ShouldWrap, errOneSided)
			So(err.Error(), ShouldContainSubstring, "BenchmarkA has 3 base and 0 head results")
		})

		Convey("a benchmark with results on both sides can be judged", func() {
			addHead(100)
			So(checkMeasured(compare(all, 10), "."), ShouldBeNil)
		})
	})

	Convey("No benchmark results cannot be judged", t, func() {
		comparisons := compare(newSamples(), 10)
		So(comparisons, ShouldBeEmpty)

		err := checkMeasured(comparisons, "NoSuchBench")
		So(err, ShouldWrap, errNothingMeasured)
		So(err.Error(), ShouldContainSubstring, `SPEED_BENCH "NoSuchBench"`)
	})
}

func TestBenchPattern(t *testing.T) {
	Convey("benchPattern matches only the given benchmarks, without their CPU suffix", t, func() {
		So(benchPattern([]string{benchUpload}), ShouldEqual, "^(BenchmarkUpload)$")
		So(benchPattern([]string{benchUpload, "BenchmarkRemove"}), ShouldEqual,
			"^(BenchmarkUpload|BenchmarkRemove)$")
		So(benchPattern([]string{"BenchmarkA-b-16"}), ShouldEqual, "^(BenchmarkA-b)$")
	})
}

func TestExitCode(t *testing.T) {
	Convey("exitCode is 2 if the gate could not run, 1 for a regression, else 0", t, func() {
		So(exitCode(false, nil), ShouldEqual, 0)
		So(exitCode(true, nil), ShouldEqual, 1)
		So(exitCode(false, errNothingMeasured), ShouldEqual, 2)
		So(exitCode(true, errNothingMeasured), ShouldEqual, 2)
		So(exitCode(false, fmt.Errorf("go interrupted: %w", context.Canceled)), ShouldEqual, 2)
	})

	Convey("The built gate exits 2 for a bad SPEED_ROUNDS before doing any work", t, func() {
		bin := filepath.Join(t.TempDir(), "speedgate")

		build := exec.CommandContext(t.Context(), "go", "build", "-o", bin, ".")
		out, err := build.CombinedOutput()
		So(err, ShouldBeNil)
		So(string(out), ShouldBeEmpty)

		gate := exec.CommandContext(t.Context(), bin)
		gate.Env = append(os.Environ(), "SPEED_ROUNDS=1.5", "SPEED_TMPDIR="+t.TempDir())
		out, err = gate.CombinedOutput()

		var exitErr *exec.ExitError
		So(errors.As(err, &exitErr), ShouldBeTrue)
		So(exitErr.ExitCode(), ShouldEqual, 2)
		So(string(out), ShouldContainSubstring, `SPEED_ROUNDS=\"1.5\" is not a positive integer`)
	})
}

func TestLoadWarning(t *testing.T) {
	Convey("loadWarning warns only when the load average per CPU is over 0.5", t, func() {
		So(loadWarning("4.00 3.00 2.00 1/100 1234\n", 8), ShouldBeEmpty)
		So(loadWarning("4.01 3.00 2.00 1/100 1234\n", 8), ShouldContainSubstring,
			"WARNING: load average 4.01 on 8 CPUs")
		So(loadWarning("", 8), ShouldBeEmpty)
		So(loadWarning("x", 8), ShouldBeEmpty)
		So(loadWarning("4.01 3.00 2.00 1/100 1234\n", 0), ShouldBeEmpty)
	})
}

func TestConfigFromEnv(t *testing.T) {
	Convey("configFromEnv uses defaults, SPEED_* overrides, and rejects bad numbers", t, func() {
		for _, key := range []string{"SPEED_BASE", "SPEED_THRESHOLD", "SPEED_ROUNDS", "SPEED_COUNT",
			"SPEED_BENCHTIME", "SPEED_BENCH", "SPEED_TMPDIR"} {
			t.Setenv(key, "")
		}

		cfg, err := configFromEnv()
		So(err, ShouldBeNil)
		So(cfg.base, ShouldEqual, defaultBase)
		So(cfg.threshold, ShouldEqual, defaultThreshold)
		So(cfg.rounds, ShouldEqual, defaultRounds)

		t.Setenv("SPEED_BASE", "v1.0.0")
		t.Setenv("SPEED_THRESHOLD", "2.5")

		cfg, err = configFromEnv()
		So(err, ShouldBeNil)
		So(cfg.base, ShouldEqual, "v1.0.0")
		So(cfg.threshold, ShouldEqual, 2.5)

		for _, bad := range []string{"-1", "0", "0.5", "1.5", "x"} {
			t.Setenv("SPEED_ROUNDS", bad)

			_, err = configFromEnv()
			So(err, ShouldWrap, errBadEnv)
		}

		t.Setenv("SPEED_ROUNDS", "3")
		t.Setenv("SPEED_COUNT", "0.5")

		_, err = configFromEnv()
		So(err, ShouldWrap, errBadEnv)

		t.Setenv("SPEED_COUNT", "2")

		cfg, err = configFromEnv()
		So(err, ShouldBeNil)
		So(cfg.rounds, ShouldEqual, 3)
		So(cfg.count, ShouldEqual, 2)
	})
}

func TestResolveBase(t *testing.T) {
	Convey("resolveBase gives a clear error for a missing ref", t, func() {
		root, err := headRoot(t.Context())
		So(err, ShouldBeNil)

		_, err = resolveBase(t.Context(), root, "no-such-ref-for-speedgate")
		So(err, ShouldWrap, errBaseMissing)
		So(err.Error(), ShouldContainSubstring, "SPEED_BASE")

		sha, err := resolveBase(t.Context(), root, "HEAD")
		So(err, ShouldBeNil)
		So(sha, ShouldHaveLength, 40)
	})
}

func TestExtractBase(t *testing.T) {
	Convey("extractBase writes a commit's files, without git metadata, plus head's benchmarks", t, func() {
		root, err := headRoot(t.Context())
		So(err, ShouldBeNil)

		// the first commit, which has no benchmarks, unless this is a shallow clone
		roots, err := command(t.Context(), root, nil, "git", "rev-list", "--max-parents=0", "HEAD")
		So(err, ShouldBeNil)

		sha := roots[strings.LastIndexByte(roots, '\n')+1:]

		dir := filepath.Join(t.TempDir(), baseDirName)
		So(extractBase(t.Context(), root, sha, dir), ShouldBeNil)

		license, err := command(t.Context(), root, nil, "git", "show", sha+":LICENSE")
		So(err, ShouldBeNil)

		got, err := os.ReadFile(filepath.Join(dir, "LICENSE"))
		So(err, ShouldBeNil)
		So(strings.TrimSpace(string(got)), ShouldEqual, license)

		_, err = os.Stat(filepath.Join(dir, ".git"))
		So(errors.Is(err, os.ErrNotExist), ShouldBeTrue)

		want, err := os.ReadFile(filepath.Join(root, benchPkgDir, "upload_test.go"))
		So(err, ShouldBeNil)

		got, err = os.ReadFile(filepath.Join(dir, benchPkgDir, "upload_test.go"))
		So(err, ShouldBeNil)
		So(string(got), ShouldEqual, string(want))

		Convey("but not for a missing commit", func() {
			err = extractBase(t.Context(), root, strings.Repeat("0", 40), filepath.Join(t.TempDir(), baseDirName))
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "git archive")
		})
	})
}

func TestBuildBase(t *testing.T) {
	Convey("A base whose benchmarks do not build fails with advice to set SPEED_BASE", t, func() {
		dir := t.TempDir()
		pkg := filepath.Join(dir, benchPkgDir)

		So(os.MkdirAll(pkg, dirPerms), ShouldBeNil)
		So(os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module example.com/base\n\ngo 1.25\n"),
			filePerms), ShouldBeNil)
		So(os.WriteFile(filepath.Join(pkg, "speed_test.go"),
			[]byte("package speed\n\nimport _ \"example.com/missing\"\n"), filePerms), ShouldBeNil)

		base := side{id: baseSide, dir: dir, bin: filepath.Join(dir, "base.test")}
		err := base.build(t.Context())
		So(err, ShouldWrap, errBaseBuild)
		So(err.Error(), ShouldContainSubstring, "SPEED_BASE")
		So(err.Error(), ShouldContainSubstring, "example.com/missing")

		head := side{id: headSide, dir: dir, bin: filepath.Join(dir, "head.test")}
		err = head.build(t.Context())
		So(err, ShouldNotBeNil)
		So(errors.Is(err, errBaseBuild), ShouldBeFalse)
	})
}
