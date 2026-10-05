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
	"runtime"
)

const (
	loadavgPath = "/proc/loadavg"

	// allowHardlinkSkipsVar, when not empty, tells the benchmarks to accept
	// the hardlink skips of revisions from before the server claimed
	// hardlinks. Only base gets it set; head gets it explicitly empty, even if
	// it is set in our own environment, so head must upload every file.
	allowHardlinkSkipsVar = "IBACKUP_SPEED_ALLOW_HARDLINK_SKIPS"

	baseDirName = "base"
)

// run carries out the gate as configured, printing a comparison table and
// verdict to stdout. It returns true if head regressed.
func run(ctx context.Context, cfg config) (regressed bool, err error) {
	root, err := headRoot(ctx)
	if err != nil {
		return false, err
	}

	sha, err := resolveBase(ctx, root, cfg.base)
	if err != nil {
		return false, err
	}

	if err = removeStaleWorkDirs(ctx, root, cfg.tmpDir); err != nil {
		return false, err
	}

	workDir, err := makeWorkDir(cfg.tmpDir)
	if workDir != "" {
		defer func() { err = errors.Join(err, os.RemoveAll(workDir)) }()
	}

	if err != nil {
		return false, err
	}

	return runInWorkDir(ctx, cfg, root, sha, workDir)
}

// runInWorkDir does the work of run() with a temporary workDir to hold the
// base worktree, test binaries and benchmark files.
func runInWorkDir(ctx context.Context, cfg config, root, sha, workDir string) (regressed bool, err error) {
	sides := newSides(root, workDir)

	removeWorktree, err := addBaseWorktree(ctx, root, sha, sides[baseSide].dir)
	if removeWorktree != nil {
		defer func() { err = errors.Join(err, removeWorktree()) }()
	}

	if err != nil {
		return false, err
	}

	return measure(ctx, cfg, sides, workDir, fmt.Sprintf("base %s (%.12s)", cfg.base, sha))
}

// newSides returns the base side, with its worktree in workDir, and the head
// side, the tree at root.
func newSides(root, workDir string) [2]side {
	return [2]side{
		{
			id: baseSide, dir: filepath.Join(workDir, baseDirName), bin: filepath.Join(workDir, "base.test"),
			env: []string{allowHardlinkSkipsVar + "=1"},
		},
		{
			id: headSide, dir: root, bin: filepath.Join(workDir, "head.test"),
			env: []string{allowHardlinkSkipsVar + "="},
		},
	}
}

// measure builds and runs both sides' benchmarks and reports the comparison.
// Benchmarks that regress are run again for as many rounds, and only fail the
// gate if they still regress with all their results pooled.
func measure(ctx context.Context, cfg config, sides [2]side, workDir, baseDesc string) (bool, error) {
	warning := hostLoadWarning()

	env, err := build(ctx, sides, workDir)
	if err != nil {
		return false, err
	}

	r := rounds{cfg: cfg, sides: sides, workDir: workDir, env: env}
	all := newSamples()

	first, err := r.runAndCompare(ctx, all, "round", cfg.bench)
	if err != nil {
		return false, err
	}

	fmt.Fprintf(os.Stdout, "\n%shead: working tree at %s\n%s; %d rounds, -benchtime %s, -count %d\n\n",
		warning, sides[headSide].dir, baseDesc, cfg.rounds, cfg.benchtime, cfg.count)
	writeTable(os.Stdout, first)

	final, err := confirm(ctx, r, all, first)
	if err != nil {
		return false, err
	}

	fmt.Fprintln(os.Stdout)

	return writeVerdict(os.Stdout, first, final, cfg.threshold, baseDesc), nil
}

// hostLoadWarning returns a warning if the host is busy, or else "".
func hostLoadWarning() string {
	content, err := os.ReadFile(loadavgPath)
	if err != nil {
		return ""
	}

	return loadWarning(string(content), runtime.NumCPU())
}

// build builds the statter and both sides' benchmarks in workDir, returning
// the environment the benchmarks need.
func build(ctx context.Context, sides [2]side, workDir string) ([]string, error) {
	env, err := buildStatter(workDir)
	if err != nil {
		return nil, err
	}

	for _, s := range sides {
		if err = s.build(ctx); err != nil {
			return nil, err
		}
	}

	return env, nil
}

// confirm runs the benchmarks that regressed in the first comparisons for
// another set of rounds, adding their results to all, and returns the
// comparisons of the pooled results.
func confirm(ctx context.Context, r rounds, all *samples, first []comparison) ([]comparison, error) {
	names := regressedNames(first)
	if len(names) == 0 {
		return first, nil
	}

	fmt.Fprintf(os.Stdout, "\nconfirming %v with %d more rounds\n", names, r.cfg.rounds)

	final, err := r.runAndCompare(ctx, all, "confirmation round", benchPattern(names))
	if err != nil {
		return nil, err
	}

	fmt.Fprintln(os.Stdout, "\npooled results:")
	writeTable(os.Stdout, final)

	return final, nil
}
