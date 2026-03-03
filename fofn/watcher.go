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

package fofn

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/wtsi-hgi/ibackup/transformer"
	"golang.org/x/sync/errgroup"
)

const (
	statusFilename = "status"
	maxPollWorkers = 10
	dirMode        = 0750
)

// ProcessSubDirConfig holds configuration for processing a subdirectory.
// RandSeed controls chunk shuffling: 0 means use time-based randomness; a
// non-zero value gives deterministic shuffling (useful for tests).
type ProcessSubDirConfig struct {
	MinChunk  int
	MaxChunk  int
	RandSeed  int64
	RunConfig RunConfig
}

// prepareChunks creates a run directory and writes shuffled chunk files.
// Returns empty runDir and nil chunks if the fofn is empty. Group ownership is
// inherited from the parent directory's setgid bit.
func prepareChunks(
	subDir SubDir,
	transform func(string) (string, error),
	mtime int64,
	cfg ProcessSubDirConfig,
) (string, []string, error) {
	runDir := filepath.Join(subDir.Path, strconv.FormatInt(mtime, 10))

	if err := os.Mkdir(runDir, dirMode); err != nil {
		return "", nil, err
	}

	fofnPath := filepath.Join(subDir.Path, fofnFilename)

	chunks, err := WriteShuffledChunks(fofnPath, transform, runDir, cfg.MinChunk, cfg.MaxChunk, cfg.RandSeed)
	if err != nil {
		_ = os.RemoveAll(runDir)

		return "", nil, err
	}

	if chunks == nil {
		_ = os.Remove(runDir)

		return "", nil, nil
	}

	return runDir, chunks, nil
}

// RunState tracks the state of an active run for a subdirectory.
type RunState struct {
	RepGroup string
	RunDir   string
	Mtime    int64
}

// ProcessSubDir reads config.yml, looks up the named transformer, writes
// shuffled chunks, and submits jobs. SubDir.FofnMtime is used as the run
// directory name (eliminating a redundant stat). Returns a RunState describing
// the active run, or an error.
func ProcessSubDir(subDir SubDir, submitter JobSubmitter, cfg ProcessSubDirConfig) (RunState, error) {
	if subDir.FofnMtime == 0 {
		return RunState{}, nil
	}

	sdCfg, err := ReadConfig(subDir.Path)
	if err != nil {
		return RunState{}, err
	}

	transform, err := transformer.MakePathTransformer(sdCfg.Transformer)
	if err != nil {
		return RunState{}, err
	}

	runDir, chunks, err := prepareChunks(subDir, transform, subDir.FofnMtime, cfg)
	if err != nil || chunks == nil {
		return RunState{}, err
	}

	return submitChunkJobs(
		submitter, sdCfg, cfg.RunConfig,
		runDir, chunks,
		filepath.Base(subDir.Path), subDir.FofnMtime,
	)
}

// submitChunkJobs builds a RunConfig, creates jobs from the chunks, and submits
// them via the given submitter. Chunk paths are converted to basenames since
// the job's Cwd is set to runDir where the chunks reside.
func submitChunkJobs(
	submitter JobSubmitter,
	sdCfg SubDirConfig,
	baseCfg RunConfig,
	runDir string,
	chunks []string,
	dirName string,
	mtime int64,
) (RunState, error) {
	relChunks := make([]string, len(chunks))
	for i, c := range chunks {
		relChunks[i] = filepath.Base(c)
	}

	jobCfg := buildRunConfig(sdCfg, baseCfg, runDir, relChunks, dirName, mtime)

	jobs := CreateJobs(jobCfg)

	if err := submitter.SubmitJobs(jobs); err != nil {
		return RunState{}, err
	}

	repGroup := fmt.Sprintf("%s%s_%d", RepGroupPrefix, dirName, mtime)

	return RunState{
		RepGroup: repGroup,
		RunDir:   runDir,
		Mtime:    mtime,
	}, nil
}

// Watcher monitors a watch directory for fofn changes and manages backup runs
// using a state machine driven by the filesystem and wr job status. On each
// poll, wr is queried once for all fofn jobs (via RepGroupPrefix), eliminating
// the need for in-memory caching or filesystem artefact signalling.
type Watcher struct {
	watchDir  string
	submitter JobSubmitter
	cfg       ProcessSubDirConfig
	logger    *slog.Logger
}

// NewWatcher creates a new Watcher for the given watch directory.
func NewWatcher(watchDir string, submitter JobSubmitter, cfg ProcessSubDirConfig) *Watcher {
	return &Watcher{
		watchDir:  watchDir,
		submitter: submitter,
		cfg:       cfg,
		logger:    slog.Default(),
	}
}

// Poll performs one poll cycle: scan for subdirectories, query wr once for all
// job status, then reconcile each subdirectory through the state machine.
func (w *Watcher) Poll() error {
	subDirs, err := ScanForFOFNs(w.watchDir)
	if err != nil {
		return err
	}

	allStatus, err := ClassifyAllJobs(w.submitter)
	if err != nil {
		return err
	}

	return w.reconcileAll(subDirs, allStatus)
}

// reconcileAll classifies and dispatches all subdirectories concurrently with
// bounded parallelism. Returns the first error encountered.
func (w *Watcher) reconcileAll(subDirs []SubDir, allStatus map[string]RunJobStatus) error {
	g := new(errgroup.Group)
	g.SetLimit(maxPollWorkers)

	for _, sd := range subDirs {
		g.Go(func() error {
			return w.reconcile(sd, allStatus)
		})
	}

	return g.Wait()
}

// reconcile determines and executes the appropriate action for a subdirectory.
// scanRunDirs reads the directory once, yielding both the current run dir and
// any stale dirs. Stale-dir cleanup is always performed after dispatch,
// eliminating the common bug class of "handler X forgets to clean stale dirs."
// The status map is looked up once and shared between classify and the handler,
// eliminating redundant map lookups and ReadDir calls.
func (w *Watcher) reconcile(sd SubDir, allStatus map[string]RunJobStatus) error {
	scan, err := scanRunDirs(sd.Path)
	if err != nil {
		return err
	}

	var status RunJobStatus
	if scan.found {
		status = allStatus[makeRepGroup(sd.Path, scan.runMtime)]
	}

	if err := w.dispatch(sd, scan, status); err != nil {
		return err
	}

	return removeDirs(scan.staleDirs)
}

// scanRunDirs reads subDirPath once and partitions numeric subdirectories into
// the current run dir (highest number) and stale dirs (everything else).
// Non-existence of subDirPath is treated as "not found" to tolerate races where
// a directory is removed between scan and classification.
func scanRunDirs(subDirPath string) (runDirScan, error) {
	entries, err := os.ReadDir(subDirPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return runDirScan{}, nil
		}

		return runDirScan{}, fmt.Errorf("read dir for run dir: %w", err)
	}

	return partitionRunDirs(subDirPath, entries), nil
}

// dispatch executes the state-appropriate handler for a subdirectory.
func (w *Watcher) dispatch(sd SubDir, scan runDirScan, status RunJobStatus) error {
	if !scan.found {
		return w.startNewRun(sd)
	}

	if status.HasRunning {
		return nil
	}

	if sd.FofnMtime != scan.runMtime {
		return w.teardownAndRestart(sd, scan.runDir, status)
	}

	return w.settle(sd, scan.runDir, status)
}

// ensureArtefacts is the single function responsible for status file and
// symlink correctness. It regenerates the status file only when wr reports a
// more recent completion than the current status (avoiding the infinite-regen
// problem from issue #171), or when the status file is missing. The symlink is
// always checked and repaired if needed.
//
// Centralising both operations makes it structurally impossible to update one
// without the other — the class of bug where "we generated status in path A but
// forgot the symlink in path B" cannot occur. Having both settle and
// teardownAndRestart use this single function also eliminates the class of bug
// where "we added a new artefact to one path but forgot the other."
//
// Runs with buried chunks still get a valid status file and symlink showing
// which chunks completed and which remain. Since wr is queried every poll
// cycle, buried-then-retried chunks are detected naturally.
func ensureArtefacts(runDir string, subDir SubDir, status RunJobStatus) error {
	if needsStatusRegen(runDir, status.LastCompletedTime) {
		if err := GenerateStatus(runDir, status.BuriedChunks); err != nil {
			return err
		}
	}

	return ensureStatusSymlink(runDir, subDir.Path)
}

// removeDirs removes a list of directories. Returns the first error
// encountered. The stale-dir list is pre-computed by scanRunDirs, so no
// additional ReadDir call is needed.
func removeDirs(dirs []string) error {
	for _, dir := range dirs {
		if err := os.RemoveAll(dir); err != nil {
			return fmt.Errorf("remove old run dir: %w", err)
		}
	}

	return nil
}

// runDirScan holds the result of scanning a subdirectory for numeric run
// directories. It combines run-dir lookup with stale-dir enumeration in a
// single ReadDir call, eliminating the second ReadDir that deleteOldRunDirs
// previously required.
type runDirScan struct {
	runDir    string   // path to highest-numbered numeric dir, or ""
	runMtime  int64    // mtime value parsed from the dir name, or 0
	found     bool     // true if a numeric run dir exists
	staleDirs []string // absolute paths of numeric dirs other than the current
}

// partitionRunDirs separates numeric directories into the highest (current run)
// and all others (stale). Non-numeric and non-directory entries are ignored.
func partitionRunDirs(subDirPath string, entries []os.DirEntry) runDirScan {
	var scan runDirScan

	for _, entry := range entries {
		n, ok := numericDirValue(entry)
		if !ok {
			continue
		}

		dirPath := filepath.Join(subDirPath, entry.Name())

		if !shouldPromoteRunDir(scan, entry.Name(), n) {
			scan.staleDirs = append(scan.staleDirs, dirPath)

			continue
		}

		// Demote the previous best to stale.
		if scan.found {
			scan.staleDirs = append(scan.staleDirs, scan.runDir)
		}

		scan.runDir = dirPath
		scan.runMtime = n
		scan.found = true
	}

	return scan
}

func shouldPromoteRunDir(scan runDirScan, dirName string, mtime int64) bool {
	if !scan.found {
		return true
	}

	if mtime > scan.runMtime {
		return true
	}

	if mtime < scan.runMtime {
		return false
	}

	return dirName > filepath.Base(scan.runDir)
}

// Run polls immediately and then at the given interval until the context is
// cancelled. Returns nil on cancellation. The initial poll error is returned
// immediately; subsequent poll errors are logged and polling continues.
func (w *Watcher) Run(ctx context.Context, interval time.Duration) error {
	if err := w.Poll(); err != nil {
		return err
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := w.Poll(); err != nil {
				w.logger.Error("poll failed", "err", err)
			}
		}
	}
}

// makeRepGroup derives a deterministic repgroup name from a subdirectory path
// and fofn mtime, matching the format used by ProcessSubDir/submitChunkJobs.
func makeRepGroup(subDirPath string, mtime int64) string {
	return fmt.Sprintf("%s%s_%d", RepGroupPrefix, filepath.Base(subDirPath), mtime)
}

// settle ensures artefacts are correct for the current run. Stale-dir cleanup
// is handled by reconcile after dispatch, making it structurally impossible to
// forget cleanup in any handler.
func (w *Watcher) settle(subDir SubDir, runDir string, status RunJobStatus) error {
	return ensureArtefacts(runDir, subDir, status)
}

// numericDirValue returns the parsed int64 value and true for a directory entry
// whose name is a valid base-10 integer, or (0, false) otherwise.
func numericDirValue(entry os.DirEntry) (int64, bool) {
	if !entry.IsDir() {
		return 0, false
	}

	n, err := strconv.ParseInt(entry.Name(), 10, 64)
	if err != nil {
		return 0, false
	}

	return n, true
}

// needsStatusRegen returns true when the status file should be (re)generated.
// The decision compares wr's most recent job completion time against the status
// file's mtime: if a job completed more recently than the status was written,
// the status is stale and must be regenerated. A missing status file always
// triggers regeneration.
func needsStatusRegen(runDir string, lastCompleted time.Time) bool {
	statusPath := filepath.Join(runDir, statusFilename)

	info, err := os.Stat(statusPath)
	if err != nil {
		return true // no status file → generate
	}

	return !lastCompleted.IsZero() && lastCompleted.After(info.ModTime())
}

// statusSymlinkCurrent returns true when the subDir/status symlink exists and
// points to the expected relative target (runDirName/status).
func statusSymlinkCurrent(runDir, subDirPath string) bool {
	symlinkPath := filepath.Join(subDirPath, statusFilename)

	target, err := os.Readlink(symlinkPath)
	if err != nil {
		return false
	}

	expected := filepath.Join(filepath.Base(runDir), statusFilename)

	return target == expected
}

// ensureStatusSymlink checks whether the status symlink in subDirPath points to
// the correct status file in runDir. If absent or incorrect, it creates or
// updates the symlink. When the symlink is already correct, the cost is a
// single os.Readlink call.
func ensureStatusSymlink(runDir string, subDirPath string) error {
	if statusSymlinkCurrent(runDir, subDirPath) {
		return nil
	}

	return createStatusSymlink(runDir, subDirPath)
}

// teardownAndRestart generates any needed status artefacts for the old run,
// cleans up buried jobs, and starts a fresh run for the updated fofn.
// Stale-dir cleanup is handled by reconcile after dispatch.
func (w *Watcher) teardownAndRestart(subDir SubDir, runDir string, status RunJobStatus) error {
	if err := ensureArtefacts(runDir, subDir, status); err != nil {
		return err
	}

	if len(status.BuriedJobs) > 0 {
		if err := w.submitter.DeleteJobs(status.BuriedJobs); err != nil {
			return err
		}
	}

	return w.startNewRun(subDir)
}

func (w *Watcher) startNewRun(subDir SubDir) error {
	_, err := ProcessSubDir(subDir, w.submitter, w.cfg)

	return err
}

// GenerateStatus writes a combined status file from all chunk reports in
// runDir. Group ownership is inherited from the parent directory's setgid bit.
func GenerateStatus(runDir string, buriedChunks []string) error {
	statusPath := filepath.Join(runDir, statusFilename)

	return WriteStatusFromRun(runDir, statusPath, buriedChunks)
}

// createStatusSymlink atomically creates or updates a symlink at subDir/status
// pointing to runDirName/status (a relative path), using a temp symlink +
// rename. Relative symlinks are portable if the directory tree is moved.
func createStatusSymlink(runDir, subDirPath string) error {
	symlinkPath := filepath.Join(subDirPath, statusFilename)
	relTarget := filepath.Join(filepath.Base(runDir), statusFilename)
	tmpLink := symlinkPath + ".tmp"

	_ = os.Remove(tmpLink)

	if err := os.Symlink(relTarget, tmpLink); err != nil {
		return fmt.Errorf("create temp status symlink: %w", err)
	}

	if err := os.Rename(tmpLink, symlinkPath); err != nil {
		return fmt.Errorf("rename status symlink: %w", err)
	}

	return nil
}

func buildRunConfig(
	sdCfg SubDirConfig,
	baseCfg RunConfig,
	runDir string,
	chunks []string,
	dirName string,
	mtime int64,
) RunConfig {
	return RunConfig{
		RunDir:      runDir,
		ChunkPaths:  chunks,
		SubDirName:  dirName,
		FofnMtime:   mtime,
		NoReplace:   sdCfg.Freeze,
		UserMeta:    sdCfg.UserMetaString(),
		RAM:         baseCfg.RAM,
		Time:        baseCfg.Time,
		Retries:     baseCfg.Retries,
		LimitGroups: baseCfg.LimitGroups,
		ReqGroup:    baseCfg.ReqGroup,
	}
}
