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
	"sync"
	"time"

	"github.com/wtsi-hgi/ibackup/internal/ownership"
	"github.com/wtsi-hgi/ibackup/transformer"
)

const (
	statusFilename = "status"
	maxPollWorkers = 10
)

// ProcessSubDirConfig holds configuration for processing a subdirectory.
// RandSeed controls chunk shuffling: 0 means use time-based randomness;
// a non-zero value gives deterministic shuffling (useful for tests).
type ProcessSubDirConfig struct {
	MinChunk  int
	MaxChunk  int
	RandSeed  int64
	RunConfig RunConfig
}

// prepareChunks creates a run directory, writes shuffled chunk files, and sets
// their GID. Returns empty runDir and nil chunks if the fofn is empty.
func prepareChunks(
	subDir SubDir,
	transform func(string) (string, error),
	mtime int64,
	cfg ProcessSubDirConfig,
) (string, []string, error) {
	gid, runDir, err := createRunDir(subDir, mtime)
	if err != nil {
		return "", nil, err
	}

	chunks, err := writeChunksWithGID(
		subDir.Path, runDir, transform,
		gid, cfg.MinChunk, cfg.MaxChunk, cfg.RandSeed,
	)
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

// createRunDir creates a run directory named after the given mtime, with group
// ownership matching the watch directory.
func createRunDir(
	subDir SubDir, mtime int64,
) (int, string, error) {
	watchDir := filepath.Dir(subDir.Path)

	gid, err := ownership.GetDirGID(watchDir)
	if err != nil {
		return 0, "", err
	}

	runDir := filepath.Join(subDir.Path, strconv.FormatInt(mtime, 10))

	if err := ownership.CreateDirWithGID(
		runDir, gid,
	); err != nil {
		return 0, "", err
	}

	return gid, runDir, nil
}

// writeChunksWithGID writes shuffled chunk files and sets
// their group ownership to the given GID.
func writeChunksWithGID(
	subDirPath, runDir string,
	transform func(string) (string, error),
	gid, minChunk, maxChunk int,
	randSeed int64,
) ([]string, error) {
	fofnPath := filepath.Join(subDirPath, fofnFilename)

	chunks, err := WriteShuffledChunks(fofnPath, transform, runDir, minChunk, maxChunk, randSeed)
	if err != nil {
		return nil, err
	}

	for _, chunk := range chunks {
		if err := os.Chown(chunk, -1, gid); err != nil {
			return nil, fmt.Errorf("chown chunk: %w", err)
		}
	}

	return chunks, nil
}

// RunState tracks the state of an active run for a subdirectory.
type RunState struct {
	RepGroup string
	RunDir   string
	Mtime    int64
}

// ProcessSubDir reads config.yml, looks up the named transformer, writes
// shuffled chunks, and submits jobs. Returns a RunState describing the active
// run, or an error.
func ProcessSubDir(
	subDir SubDir,
	submitter JobSubmitter,
	cfg ProcessSubDirConfig,
) (RunState, error) {
	sdCfg, mtime, err := readAndCheckConfig(subDir)
	if err != nil || mtime == 0 {
		return RunState{}, err
	}

	transform, err := transformer.MakePathTransformer(sdCfg.Transformer)
	if err != nil {
		return RunState{}, err
	}

	runDir, chunks, err := prepareChunks(subDir, transform, mtime, cfg)
	if err != nil || chunks == nil {
		return RunState{}, err
	}

	return submitChunkJobs(
		submitter, sdCfg, cfg.RunConfig,
		runDir, chunks,
		filepath.Base(subDir.Path), mtime,
	)
}

// submitChunkJobs builds a RunConfig, creates jobs from
// the chunks, and submits them via the given submitter.
func submitChunkJobs(
	submitter JobSubmitter,
	sdCfg SubDirConfig,
	baseCfg RunConfig,
	runDir string,
	chunks []string,
	dirName string,
	mtime int64,
) (RunState, error) {
	jobCfg := buildRunConfig(sdCfg, baseCfg, runDir, chunks, dirName, mtime)

	jobs := CreateJobs(jobCfg)

	if err := submitter.SubmitJobs(jobs); err != nil {
		return RunState{}, err
	}

	repGroup := fmt.Sprintf("ibackup_fofn_%s_%d", dirName, mtime)

	return RunState{
		RepGroup: repGroup,
		RunDir:   runDir,
		Mtime:    mtime,
	}, nil
}

// buildRunFromNewest finds the newest numeric run
// directory and constructs the corresponding RunState.
func buildRunFromNewest(
	subDirPath string,
) (RunState, bool, error) {
	mtime, found, err := newestRunDir(subDirPath)
	if err != nil || !found {
		return RunState{}, false, err
	}

	dirName := filepath.Base(subDirPath)
	repGroup := fmt.Sprintf("ibackup_fofn_%s_%d", dirName, mtime)
	runDir := filepath.Join(subDirPath, strconv.FormatInt(mtime, 10))

	return RunState{
		RepGroup: repGroup,
		RunDir:   runDir,
		Mtime:    mtime,
	}, true, nil
}

// Watcher monitors a watch directory for fofn changes and manages backup runs.
type Watcher struct {
	watchDir   string
	submitter  JobSubmitter
	cfg        ProcessSubDirConfig
	logger     *slog.Logger
	mu         sync.Mutex
	activeRuns map[string]RunState // key: subDir.Path
}

// NewWatcher creates a new Watcher for the given watch directory.
func NewWatcher(
	watchDir string,
	submitter JobSubmitter,
	cfg ProcessSubDirConfig,
) *Watcher {
	return &Watcher{
		watchDir:   watchDir,
		submitter:  submitter,
		cfg:        cfg,
		logger:     slog.Default(),
		activeRuns: make(map[string]RunState),
	}
}

// getActiveRun returns the RunState for the given path
// and whether it exists, with thread-safe map access.
func (w *Watcher) getActiveRun(
	path string,
) (RunState, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()

	r, ok := w.activeRuns[path]

	return r, ok
}

// setActiveRun stores a RunState for the given path
// with thread-safe map access.
func (w *Watcher) setActiveRun(
	path string, state RunState,
) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.activeRuns[path] = state
}

// clearActiveRun removes the RunState for the given
// path with thread-safe map access.
func (w *Watcher) clearActiveRun(path string) {
	w.mu.Lock()
	defer w.mu.Unlock()

	delete(w.activeRuns, path)
}

// Poll performs one poll cycle: scan for subdirectories, check active runs,
// start new runs as needed. Subdirectories are processed in parallel.
func (w *Watcher) Poll() error {
	subDirs, err := ScanForFOFNs(w.watchDir)
	if err != nil {
		return err
	}

	return w.pollSubDirsParallel(subDirs)
}

// Run polls immediately and then at the given interval until the context is
// cancelled. Returns nil on cancellation. The initial poll error is returned
// immediately; subsequent poll errors are logged and polling continues.
func (w *Watcher) Run(
	ctx context.Context, interval time.Duration,
) error {
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
				w.logger.Error("poll failed",
					"err", err)
			}
		}
	}
}

// pollSubDirsParallel processes all subdirectories
// concurrently with bounded parallelism, collecting
// all errors.
func (w *Watcher) pollSubDirsParallel(
	subDirs []SubDir,
) error {
	var (
		wg    sync.WaitGroup
		errMu sync.Mutex
		errs  []error
		sem   = make(chan struct{}, maxPollWorkers)
	)

	for _, subDir := range subDirs {
		wg.Add(1)

		sem <- struct{}{}

		go w.pollSubDirCollect(subDir, sem, &wg, &errMu, &errs)
	}

	wg.Wait()

	return errors.Join(errs...)
}

func (w *Watcher) pollSubDirCollect(
	sd SubDir, sem chan struct{},
	wg *sync.WaitGroup, errMu *sync.Mutex, errs *[]error,
) {
	defer wg.Done()
	defer func() { <-sem }()

	if err := w.pollSubDir(sd); err != nil {
		errMu.Lock()

		*errs = append(*errs, err)

		errMu.Unlock()
	}
}

func (w *Watcher) pollSubDir(
	subDir SubDir,
) error {
	run, active := w.getActiveRun(subDir.Path)
	if active {
		return w.handleActiveRun(subDir, run)
	}

	return w.handleNewSubDir(subDir)
}

func (w *Watcher) handleActiveRun(
	subDir SubDir, run RunState,
) error {
	complete, err := IsRunComplete(w.submitter, run.RepGroup)
	if err != nil {
		return err
	}

	if !complete {
		return nil
	}

	buriedChunks, err := FindBuriedChunks(w.submitter, run.RepGroup, run.RunDir)
	if err != nil {
		return err
	}

	if len(buriedChunks) == 0 {
		return w.handleSuccessfulRun(subDir, run)
	}

	return w.handleBuriedRun(subDir, run, buriedChunks)
}

func (w *Watcher) handleSuccessfulRun(
	subDir SubDir, run RunState,
) error {
	if err := GenerateStatus(
		run.RunDir, subDir, nil,
	); err != nil {
		return err
	}

	if err := deleteOldRunDirs(
		subDir.Path, run.Mtime,
	); err != nil {
		return err
	}

	w.clearActiveRun(subDir.Path)

	return w.startNewRun(subDir)
}

// GenerateStatus writes a combined status file from all chunk reports in
// runDir, handles buried chunks, and creates/updates a symlink at subDir/status
// pointing to runDir/status.
func GenerateStatus(
	runDir string,
	subDir SubDir,
	buriedChunks []string,
) error {
	statusPath := filepath.Join(runDir, statusFilename)

	if err := WriteStatusFromRun(
		runDir, statusPath, buriedChunks,
	); err != nil {
		return err
	}

	if err := setStatusGID(
		statusPath, subDir.Path,
	); err != nil {
		return err
	}

	return createStatusSymlink(statusPath, subDir.Path)
}

// deleteOldRunDirs removes all numeric directories in
// subDirPath except the one matching keepMtime.
func deleteOldRunDirs(
	subDirPath string, keepMtime int64,
) error {
	entries, err := os.ReadDir(subDirPath)
	if err != nil {
		return fmt.Errorf("read dir for cleanup: %w", err)
	}

	for _, entry := range entries {
		if err := removeIfOldRunDir(
			subDirPath, entry, keepMtime,
		); err != nil {
			return err
		}
	}

	return nil
}

func (w *Watcher) handleBuriedRun(
	subDir SubDir,
	run RunState,
	buriedChunks []string,
) error {
	buriedBases := toBaseNames(buriedChunks)

	needed, _, err := NeedsProcessing(subDir)
	if err != nil {
		return err
	}

	if err := GenerateStatus(
		run.RunDir, subDir, buriedBases,
	); err != nil {
		return err
	}

	if !needed {
		return nil
	}

	if err := DeleteBuriedJobs(
		w.submitter, run.RepGroup,
	); err != nil {
		return err
	}

	w.clearActiveRun(subDir.Path)

	return w.startNewRun(subDir)
}

// toBaseNames converts a slice of full paths to their base names.
func toBaseNames(paths []string) []string {
	bases := make([]string, len(paths))

	for i, p := range paths {
		bases[i] = filepath.Base(p)
	}

	return bases
}

func (w *Watcher) handleNewSubDir(
	subDir SubDir,
) error {
	recovered, err := w.detectExistingRun(subDir)
	if err != nil || recovered {
		return err
	}

	return w.startNewRun(subDir)
}

// detectExistingRun checks for a run directory left by a previous Watcher
// instance. If an existing run has incomplete jobs, it records the active run
// and returns true. If the existing run is complete, it processes the
// completion immediately and returns true.
func (w *Watcher) detectExistingRun(
	subDir SubDir,
) (bool, error) {
	run, found, err := buildRunFromNewest(subDir.Path)
	if err != nil || !found {
		return false, err
	}

	complete, err := IsRunComplete(w.submitter, run.RepGroup)
	if err != nil {
		return false, err
	}

	w.setActiveRun(subDir.Path, run)

	if !complete {
		return true, nil
	}

	return true, w.handleActiveRun(subDir, run)
}

func (w *Watcher) startNewRun(subDir SubDir) error {
	state, err := ProcessSubDir(subDir, w.submitter, w.cfg)
	if err != nil {
		return err
	}

	if state.RepGroup != "" {
		w.setActiveRun(subDir.Path, state)
	}

	return nil
}

// setStatusGID sets the group ownership of the status
// file to match the watch directory's GID.
func setStatusGID(
	statusPath, subDirPath string,
) error {
	watchDir := filepath.Dir(subDirPath)

	gid, err := ownership.GetDirGID(watchDir)
	if err != nil {
		return err
	}

	if err := os.Chown(
		statusPath, -1, gid,
	); err != nil {
		return fmt.Errorf("chown status: %w", err)
	}

	return nil
}

// createStatusSymlink atomically creates or updates a symlink at subDir/status
// pointing to the given statusPath, using a temp symlink + rename.
func createStatusSymlink(
	statusPath, subDirPath string,
) error {
	symlinkPath := filepath.Join(subDirPath, statusFilename)
	tmpLink := symlinkPath + ".tmp"

	_ = os.Remove(tmpLink)

	if err := os.Symlink(
		statusPath, tmpLink,
	); err != nil {
		return fmt.Errorf("create temp status symlink: %w", err)
	}

	if err := os.Rename(tmpLink, symlinkPath); err != nil {
		return fmt.Errorf("rename status symlink: %w", err)
	}

	return nil
}

// readAndCheckConfig checks if processing is needed, and if so reads the
// config. Returns zero mtime when processing is not needed.
func readAndCheckConfig(
	subDir SubDir,
) (SubDirConfig, int64, error) {
	needed, mtime, err := NeedsProcessing(subDir)
	if err != nil {
		return SubDirConfig{}, 0, err
	}

	if !needed {
		return SubDirConfig{}, 0, nil
	}

	sdCfg, err := ReadConfig(subDir.Path)
	if err != nil {
		return SubDirConfig{}, 0, err
	}

	return sdCfg, mtime, nil
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

func removeIfOldRunDir(
	subDirPath string,
	entry os.DirEntry,
	keepMtime int64,
) error {
	if !entry.IsDir() {
		return nil
	}

	n, err := strconv.ParseInt(entry.Name(), 10, 64)
	if err != nil {
		return nil //nolint:nilerr
	}

	if n == keepMtime {
		return nil
	}

	dirPath := filepath.Join(subDirPath, entry.Name())

	if err := os.RemoveAll(dirPath); err != nil {
		return fmt.Errorf("remove old run dir: %w", err)
	}

	return nil
}
