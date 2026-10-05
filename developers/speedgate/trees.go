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
	"slices"
	"strconv"
	"strings"
	"syscall"
)

const (
	benchPkgDir = "developers/speed"
	dirPerms    = 0o700
	filePerms   = 0o600

	log15V3   = "github.com/inconshreveable/log15/v3"
	log15Prev = "github.com/inconshreveable/log15"

	workDirPrefix = "ibackup-speed-"
	ownerFile     = "owner"
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

// makeWorkDir creates a new work directory in tmpDir, recording this process
// as its owner so that later runs can tell if it is stale.
func makeWorkDir(tmpDir string) (string, error) {
	dir, err := os.MkdirTemp(tmpDir, workDirPrefix)
	if err != nil {
		return "", err
	}

	host, err := os.Hostname()
	if err == nil {
		err = os.WriteFile(filepath.Join(dir, ownerFile),
			[]byte(host+" "+strconv.Itoa(os.Getpid())), filePerms)
	}

	return dir, err
}

// removeStaleWorkDirs removes the work directories in tmpDir that isStale(),
// left by gate runs on this host that were killed before they could clean up,
// along with their base worktrees' registrations in the repo at root.
func removeStaleWorkDirs(ctx context.Context, root, tmpDir string) error {
	dirs, err := filepath.Glob(filepath.Join(tmpDir, workDirPrefix+"*"))
	if err != nil {
		return err
	}

	host, err := os.Hostname()
	if err != nil {
		return err
	}

	for _, dir := range dirs {
		if !isStale(dir, host) {
			continue
		}

		fmt.Fprintf(os.Stderr, "removing %s left by an earlier speed gate run\n", dir)

		if err = removeStaleWorkDir(ctx, root, dir); err != nil {
			return err
		}
	}

	return nil
}

// isStale returns true if the given work dir's owner file says it was made on
// the given host by a process that no longer exists. Dirs we can't read, eg.
// those of other users, are not stale.
func isStale(dir, host string) bool {
	content, err := os.ReadFile(filepath.Join(dir, ownerFile))
	if err != nil {
		return false
	}

	ownerHost, pidStr, ok := strings.Cut(string(content), " ")
	if !ok || ownerHost != host {
		return false
	}

	pid, err := strconv.Atoi(pidStr)
	if err != nil {
		return false
	}

	return errors.Is(syscall.Kill(pid, 0), syscall.ESRCH)
}

// removeStaleWorkDir removes the given work dir and its base worktree. If git
// can't remove the worktree, but it is still registered in the repo at root
// once the dir is gone, it falls back to pruning all the repo's registrations
// of worktrees that no longer exist.
func removeStaleWorkDir(ctx context.Context, root, dir string) error {
	base := filepath.Join(dir, baseDirName)

	_, removeErr := command(ctx, root, nil, "git", "worktree", "remove", "--force", base)

	if err := os.RemoveAll(dir); err != nil || removeErr == nil {
		return err
	}

	registered, err := isWorktree(ctx, root, base)
	if err != nil || !registered {
		return err
	}

	_, err = command(ctx, root, nil, "git", "worktree", "prune")

	return err
}

// isWorktree returns true if dir is registered as a worktree of the repo at
// root.
func isWorktree(ctx context.Context, root, dir string) (bool, error) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return false, err
	}

	out, err := command(ctx, root, nil, "git", "worktree", "list", "--porcelain")
	if err != nil {
		return false, err
	}

	return slices.Contains(strings.Split(out, "\n"), "worktree "+abs), nil
}

// addBaseWorktree checks out sha in a detached worktree at dir and copies the
// head's benchmark package into it. It returns a function that removes the
// worktree, which works even after ctx is cancelled.
func addBaseWorktree(ctx context.Context, root, sha, dir string) (func() error, error) {
	if _, err := command(ctx, root, nil, "git", "worktree", "add", "--detach", dir, sha); err != nil {
		return nil, err
	}

	remove := func() error {
		_, err := command(context.WithoutCancel(ctx), root, nil, "git", "worktree", "remove", "--force", dir)

		return err
	}

	err := copyBenchPkg(filepath.Join(root, benchPkgDir), filepath.Join(dir, benchPkgDir), dir)
	if err != nil {
		return remove, err
	}

	return remove, nil
}

// copyBenchPkg copies the benchmark package from src to dst, adjusting its
// imports to compile against the module at baseRoot.
func copyBenchPkg(src, dst, baseRoot string) error {
	gomod, err := os.ReadFile(filepath.Join(baseRoot, "go.mod"))
	if err != nil {
		return err
	}

	entries, err := os.ReadDir(src)
	if err != nil {
		return err
	}

	if err = os.MkdirAll(dst, dirPerms); err != nil {
		return err
	}

	for _, entry := range entries {
		if err = copyGoFile(src, dst, entry, string(gomod)); err != nil {
			return err
		}
	}

	return nil
}

func copyGoFile(src, dst string, entry os.DirEntry, gomod string) error {
	if !entry.Type().IsRegular() || !strings.HasSuffix(entry.Name(), ".go") {
		return nil
	}

	content, err := os.ReadFile(filepath.Join(src, entry.Name()))
	if err != nil {
		return err
	}

	// entry is from our own listing of src, so its name has no path elements
	return os.WriteFile(filepath.Join(dst, entry.Name()), //nolint:gosec
		[]byte(adaptImports(string(content), gomod)), filePerms)
}

// adaptImports rewrites the log15/v3 import of the given Go source to the
// pre-v3 module path if that is what the given go.mod requires. The two
// versions have the same API for the parts the benchmarks use.
func adaptImports(source, gomod string) string {
	if strings.Contains(gomod, log15V3+" ") || !strings.Contains(gomod, log15Prev+" ") {
		return source
	}

	return strings.ReplaceAll(source, `"`+log15V3+`"`, `"`+log15Prev+`"`)
}
