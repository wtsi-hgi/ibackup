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
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"
)

const (
	percent = 100

	// maxLoadPerCPU is the load average per CPU above which the host is too
	// busy for reliable results.
	maxLoadPerCPU = 0.5
)

var (
	errNothingMeasured = errors.New("no benchmark results")
	errOneSided        = errors.New("benchmark lacks results")
)

// loadWarning returns a warning if the given /proc/loadavg content shows a
// 1 minute load average per CPU over maxLoadPerCPU, or else "".
func loadWarning(loadavg string, cpus int) string {
	fields := strings.Fields(loadavg)
	if len(fields) == 0 || cpus <= 0 {
		return ""
	}

	load, err := strconv.ParseFloat(fields[0], 64)
	if err != nil || load/float64(cpus) <= maxLoadPerCPU {
		return ""
	}

	return fmt.Sprintf("WARNING: load average %g on %d CPUs at start; the host is busy, "+
		"so results may be noisy\n", load, cpus)
}

// stats summarises one side's results for a benchmark.
type stats struct {
	n      int
	median float64
	spread float64
	allocs float64
}

func summarise(results []result) stats {
	ns := make([]float64, len(results))
	allocs := make([]float64, len(results))

	for i, r := range results {
		ns[i], allocs[i] = r.nsPerOp, r.allocsPerOp
	}

	s := stats{n: len(results), median: median(ns), allocs: median(allocs)}

	if s.median > 0 {
		s.spread = (slices.Max(ns) - slices.Min(ns)) / s.median * percent
	}

	return s
}

// compare compares the median time of each benchmark on head with base. A
// benchmark has regressed if head is more than threshold percent slower.
func compare(all *samples, threshold float64) []comparison {
	comparisons := make([]comparison, 0, len(all.names))

	for _, name := range all.names {
		c := comparison{
			name: name,
			base: summarise(all.results[baseSide][name]),
			head: summarise(all.results[headSide][name]),
		}
		c.hasBothSides = c.base.n > 0 && c.head.n > 0

		if c.hasBothSides && c.base.median > 0 {
			c.change = (c.head.median - c.base.median) / c.base.median * percent
			c.regressed = c.change > threshold
		}

		comparisons = append(comparisons, c)
	}

	return comparisons
}

// comparison is the outcome of comparing one benchmark's base and head
// results.
type comparison struct {
	name         string
	base, head   stats
	change       float64
	regressed    bool
	hasBothSides bool
}

// checkMeasured returns an error if there are no comparisons, or any lacks
// results on one side, since the gate can't then judge head.
func checkMeasured(comparisons []comparison, bench string) error {
	if len(comparisons) == 0 {
		return fmt.Errorf("%w: SPEED_BENCH %q matched no benchmark that ran on either tree",
			errNothingMeasured, bench)
	}

	for _, c := range comparisons {
		if !c.hasBothSides {
			return fmt.Errorf("%w on one side: %s has %d base and %d head results",
				errOneSided, c.name, c.base.n, c.head.n)
		}
	}

	return nil
}

// regressedNames returns the names of the regressed benchmarks.
func regressedNames(comparisons []comparison) []string {
	var names []string

	for _, c := range comparisons {
		if c.regressed {
			names = append(names, c.name)
		}
	}

	return names
}

// writeTable writes a table of the comparisons to w.
func writeTable(w io.Writer, comparisons []comparison) {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0) //nolint:mnd

	fmt.Fprintln(tw, "benchmark\tbase median\thead median\tchange\tbase spread\thead spread\t"+
		"base allocs/op\thead allocs/op\tsamples")

	for _, c := range comparisons {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%+.1f%%\t%.1f%%\t%.1f%%\t%.0f\t%.0f\t%d/%d\n",
			c.name, fmtNs(c.base.median), fmtNs(c.head.median), c.change,
			c.base.spread, c.head.spread, c.base.allocs, c.head.allocs, c.base.n, c.head.n)
	}

	tw.Flush()
}

// fmtNs formats a number of nanoseconds as a duration, keeping at least 4
// significant figures.
func fmtNs(ns float64) string {
	d := time.Duration(ns)

	// Multiplying by time.Millisecond and time.Microsecond scales by 1e6 and
	// 1e3: precision grows 1000-fold while d is over a million of it.
	precision := time.Nanosecond
	for precision*time.Millisecond < d { //nolint:durationcheck
		precision *= time.Microsecond //nolint:durationcheck
	}

	return d.Round(precision).String()
}

// writeVerdict writes which benchmarks regressed, if any, and returns true if
// any did. A benchmark regressed if it did so in both the first comparisons
// and the final ones, which pool the first results with those of a
// confirmation run of the benchmarks that regressed at first.
func writeVerdict(w io.Writer, first, final []comparison, threshold float64, baseDesc string) bool {
	failed := false

	firstByName := make(map[string]comparison, len(first))
	for _, f := range first {
		firstByName[f.name] = f
	}

	for _, c := range final {
		if writeConfirmation(w, firstByName[c.name], c, threshold) {
			failed = true
		}
	}

	if failed {
		fmt.Fprintf(w, "speed gate FAILED against %s\n", baseDesc)
	} else {
		fmt.Fprintf(w, "speed gate passed: no benchmark is more than %g%% slower than %s\n", threshold, baseDesc)
	}

	return failed
}

// writeConfirmation writes the outcome for a benchmark that regressed in its
// first comparison f, given its final comparison c, and returns true if it
// still regressed. It writes nothing for one that did not regress at first.
func writeConfirmation(w io.Writer, f, c comparison, threshold float64) bool {
	switch {
	case !f.regressed:
		return false
	case c.regressed:
		fmt.Fprintf(w, "FAIL: %s: head %s is %.1f%% slower than base %s (threshold %g%%); "+
			"first %d/%d samples %+.1f%%, pooled %d/%d samples %+.1f%%\n",
			c.name, fmtNs(c.head.median), c.change, fmtNs(c.base.median), threshold,
			f.base.n, f.head.n, f.change, c.base.n, c.head.n, c.change)

		return true
	default:
		fmt.Fprintf(w, "%s was %.1f%% slower in the first %d/%d samples, but %+.1f%% "+
			"in the pooled %d/%d samples, within the threshold\n",
			c.name, f.change, f.base.n, f.head.n, c.change, c.base.n, c.head.n)

		return false
	}
}

// median returns the median of the given values, or 0 if there are none.
func median(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}

	sorted := slices.Sorted(slices.Values(values))
	mid := len(sorted) / 2 //nolint:mnd

	if len(sorted)%2 == 0 {
		return (sorted[mid-1] + sorted[mid]) / 2 //nolint:mnd
	}

	return sorted[mid]
}
