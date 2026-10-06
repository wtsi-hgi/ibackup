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
	"strings"
)

const (
	benchPkgDir   = "developers/speed"
	dirPerms      = 0o700
	filePerms     = 0o600
	workDirPrefix = "ibackup-speed-"
)

var errBaseMissing = errors.New("base ref not found")

// headRoot returns the top directory of the git working tree we're run in.
func headRoot(ctx context.Context) (string, error) {
	return command(ctx, "", nil, "git", "rev-parse", "--show-toplevel")
}

// resolveBase returns the commit the given ref names in the repo at root.
func resolveBase(ctx context.Context, root, ref string) (string, error) {
	sha, err := command(ctx, root, nil, "git", "rev-parse", "--verify", "--quiet", ref+"^{commit}")
	if err != nil {
		return "", fmt.Errorf("%w: %q; fetch it (eg. git fetch origin develop) or set SPEED_BASE",
			errBaseMissing, ref)
	}

	return sha, nil
}

// extractBase writes the files of commit sha in the repo at root to the new
// directory dir, with `git archive | tar -x`, and copies head's benchmark
// package into it.
func extractBase(ctx context.Context, root, sha, dir string) error {
	if err := os.Mkdir(dir, dirPerms); err != nil {
		return err
	}

	pipeR, pipeW, err := os.Pipe()
	if err != nil {
		return err
	}

	archive := newCommand(ctx, root, nil, "git", "archive", sha)
	archive.Stdout = pipeW
	extract := newCommand(ctx, dir, nil, "tar", "-x")
	extract.Stdin = pipeR

	err = errors.Join(archive.wrapErr(ctx, archive.Start()), extract.wrapErr(ctx, extract.Start()))

	// the children have their own copies, so that if one exits, the other sees
	// EOF or EPIPE instead of waiting on us
	err = errors.Join(err, pipeR.Close(), pipeW.Close())

	err = errors.Join(err, waitIfStarted(ctx, archive), waitIfStarted(ctx, extract))
	if err != nil {
		return err
	}

	return copyBenchPkg(filepath.Join(root, benchPkgDir), filepath.Join(dir, benchPkgDir))
}

// waitIfStarted waits for the given command if it was started, returning its
// wrapped error.
func waitIfStarted(ctx context.Context, c *stderrCmd) error {
	if c.Process == nil {
		return nil
	}

	return c.wrapErr(ctx, c.Wait())
}

// copyBenchPkg copies the Go files of the benchmark package from src to dst.
func copyBenchPkg(src, dst string) error {
	entries, err := os.ReadDir(src)
	if err != nil {
		return err
	}

	if err = os.MkdirAll(dst, dirPerms); err != nil {
		return err
	}

	for _, entry := range entries {
		if err = copyGoFile(src, dst, entry); err != nil {
			return err
		}
	}

	return nil
}

func copyGoFile(src, dst string, entry os.DirEntry) error {
	if !entry.Type().IsRegular() || !strings.HasSuffix(entry.Name(), ".go") {
		return nil
	}

	content, err := os.ReadFile(filepath.Join(src, entry.Name()))
	if err != nil {
		return err
	}

	// entry is from our own listing of src, so its name has no path elements
	return os.WriteFile(filepath.Join(dst, entry.Name()), content, filePerms) //nolint:gosec
}
