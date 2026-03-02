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
	"slices"
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

// stateDesc maps each state to a human-readable description.
// The array is sized by numStates, so adding a state without a description
// is caught by the exhaustive test.
var stateDesc = [numStates]string{ //nolint:gochecknoglobals
	stateNewRun:  "start new run",
	stateRunning: "wait for jobs",
	stateSettle:  "settle artefacts",
	stateRestart: "teardown and restart",
}

// subDirState classifies the observable state of a subdirectory. Every
// reachable combination of filesystem and wr state maps to exactly one value.
// The dispatch switch and exhaustive test verify all values are handled,
// making missed-case bugs structurally impossible.
type subDirState int

const (
	stateNewRun  subDirState = iota // no run dir → start new
	stateRunning                    // wr jobs still active → wait
	stateSettle                     // jobs done, fofn unchanged → settle artefacts
	stateRestart                    // jobs done, fofn changed → teardown + restart
	numStates                       // sentinel for exhaustive checks
)

// classify determines the subdirectory state from run directory presence, wr
// job status, and fofn mtime comparison. Since wr is queried on every poll
// cycle, classification needs no filesystem artefact checks or caching.
func classify(
	sd SubDir, hasRunDir bool, runMtime int64,
	allStatus map[string]RunJobStatus,
) subDirState {
	if !hasRunDir {
		return stateNewRun
	}

	status := allStatus[makeRepGroup(sd.Path, runMtime)]

	if status.HasRunning {
		return stateRunning
	}

	if sd.FofnMtime != runMtime {
		return stateRestart
	}

	return stateSettle
}

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
func createRunDir(subDir SubDir, mtime int64) (int, string, error) {
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
// shuffled chunks, and submits jobs. SubDir.FofnMtime is used as the run
// directory name (eliminating a redundant stat). Returns a RunState describing
// the active run, or an error.
func ProcessSubDir(subDir SubDir, submitter JobSubmitter, cfg ProcessSubDirConfig) (RunState, error) {
	if subDir.FofnMtime == 0 {
		return RunState{}, nil
	}

	sdCfg, err := readSubDirConfig(subDir)
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
// bounded parallelism, collecting errors.
func (w *Watcher) reconcileAll(subDirs []SubDir, allStatus map[string]RunJobStatus) error {
	var (
		wg    sync.WaitGroup
		errMu sync.Mutex
		errs  []error
		sem   = make(chan struct{}, maxPollWorkers)
	)

	for _, sd := range subDirs {
		wg.Add(1)

		sem <- struct{}{}

		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			if err := w.reconcile(sd, allStatus); err != nil {
				errMu.Lock()

				errs = append(errs, err)

				errMu.Unlock()
			}
		}()
	}

	wg.Wait()

	return errors.Join(errs...)
}

// reconcile determines and executes the appropriate action for a subdirectory.
// classify returns the state; the switch dispatches the action. Every state has
// an explicit case; the default panics.
func (w *Watcher) reconcile(sd SubDir, allStatus map[string]RunJobStatus) error {
	runDir, runMtime, found, err := findRunDir(sd.Path)
	if err != nil {
		return err
	}

	state := classify(sd, found, runMtime, allStatus)

	switch state {
	case stateNewRun:
		return w.startNewRun(sd)
	case stateRunning:
		return nil
	case stateSettle:
		return w.settle(sd, runDir, runMtime, allStatus[makeRepGroup(sd.Path, runMtime)])
	case stateRestart:
		return w.teardownAndRestart(sd, runDir, runMtime, allStatus[makeRepGroup(sd.Path, runMtime)])
	default:
		panic(fmt.Sprintf("watchfofns: unhandled state: %d (%s)", state, stateDesc[state]))
	}
}

// findRunDir finds the current run directory inside subDirPath by looking for
// the highest-numbered numeric subdirectory. Returns the path, its mtime
// value, and true if found. Returns ("", 0, false, nil) if no run directory
// exists. Non-existence errors (e.g. directory removed between scan and
// classification) are treated as "not found"; other errors are propagated.
func findRunDir(subDirPath string) (string, int64, bool, error) {
	entries, err := os.ReadDir(subDirPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", 0, false, nil
		}

		return "", 0, false, fmt.Errorf("read dir for run dir: %w", err)
	}

	if bestMtime := highestNumericDir(entries); bestMtime != 0 {
		return filepath.Join(subDirPath, strconv.FormatInt(bestMtime, 10)), bestMtime, true, nil
	}

	return "", 0, false, nil
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
				w.logger.Error("poll failed",
					"err", err)
			}
		}
	}
}

// makeRepGroup derives a deterministic repgroup name from a subdirectory path
// and fofn mtime, matching the format used by ProcessSubDir/submitChunkJobs.
func makeRepGroup(subDirPath string, mtime int64) string {
	return fmt.Sprintf("%s%s_%d", RepGroupPrefix, filepath.Base(subDirPath), mtime)
}

// settle is the single convergent artefact reconciler. It ensures the status
// file and symlink are correct for the current run state. Status regeneration
// is driven by comparing wr's last completed job time against the status file's
// mtime, avoiding the endless-regeneration problem (issue #171).
//
// Runs with buried chunks still get a valid status file and symlink showing
// which chunks completed and which remain. Since wr is queried every poll
// cycle, buried-then-retried chunks are detected naturally without needing
// symlink-removal signalling.
func (w *Watcher) settle(subDir SubDir, runDir string, runMtime int64, status RunJobStatus) error {
	buriedChunks := sortedBuriedChunks(status)

	if needsStatusRegen(runDir, status.LastCompletedTime) {
		if err := GenerateStatus(runDir, subDir, buriedChunks); err != nil {
			return err
		}
	}

	if err := ensureStatusSymlink(runDir, subDir.Path); err != nil {
		return err
	}

	return deleteOldRunDirs(subDir.Path, runMtime)
}

// sortedBuriedChunks extracts and sorts the buried chunk names from the job
// status. Buried chunks are sorted for deterministic comparison.
func sortedBuriedChunks(status RunJobStatus) []string {
	if len(status.BuriedChunks) > 0 {
		sorted := make([]string, len(status.BuriedChunks))
		copy(sorted, status.BuriedChunks)
		slices.Sort(sorted)

		return sorted
	}

	return nil
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

// highestNumericDir returns the largest numeric directory name from entries, or
// 0 if none exist.
func highestNumericDir(entries []os.DirEntry) int64 {
	var best int64

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		if n, err := strconv.ParseInt(entry.Name(), 10, 64); err == nil && n > best {
			best = n
		}
	}

	return best
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

// ensureStatusSymlink checks whether the status symlink in subDirPath points
// to the correct status file in runDir. If absent or incorrect, it creates or
// updates the symlink. When the symlink is already correct, the cost is a
// single os.Readlink call.
func ensureStatusSymlink(runDir string, subDirPath string) error {
	if statusSymlinkCurrent(runDir, subDirPath) {
		return nil
	}

	return createStatusSymlink(runDir, subDirPath)
}

// teardownAndRestart generates a final status snapshot for the old run, cleans
// up buried jobs and stale directories, and starts a fresh run for the updated
// fofn.
func (w *Watcher) teardownAndRestart(subDir SubDir, runDir string, runMtime int64, status RunJobStatus) error {
	buriedChunks := sortedBuriedChunks(status)

	if err := GenerateStatus(runDir, subDir, buriedChunks); err != nil {
		return err
	}

	// Create symlink for the final status snapshot before teardown.
	if err := ensureStatusSymlink(runDir, subDir.Path); err != nil {
		return err
	}

	if len(status.BuriedJobs) > 0 {
		if err := w.submitter.DeleteJobs(status.BuriedJobs); err != nil {
			return err
		}
	}

	if err := deleteOldRunDirs(subDir.Path, runMtime); err != nil {
		return err
	}

	return w.startNewRun(subDir)
}

func (w *Watcher) startNewRun(subDir SubDir) error {
	_, err := ProcessSubDir(subDir, w.submitter, w.cfg)

	return err
}

// GenerateStatus writes a combined status file from all chunk reports in
// runDir, handles buried chunks, and creates/updates a symlink at subDir/status
// pointing to runDir/status.
func GenerateStatus(runDir string, subDir SubDir, buriedChunks []string) error {
	statusPath := filepath.Join(runDir, statusFilename)

	if err := WriteStatusFromRun(runDir, statusPath, buriedChunks); err != nil {
		return err
	}

	return setStatusGID(statusPath, subDir.Path)
}

// setStatusGID sets the group ownership of the status file to match the watch
// directory's GID.
func setStatusGID(statusPath, subDirPath string) error {
	watchDir := filepath.Dir(subDirPath)

	gid, err := ownership.GetDirGID(watchDir)
	if err != nil {
		return err
	}

	if err := os.Chown(statusPath, -1, gid); err != nil {
		return fmt.Errorf("chown status: %w", err)
	}

	issuesPath := statusPath + issuesSuffix

	if err := os.Chown(issuesPath, -1, gid); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("chown status issues: %w", err)
	}

	return nil
}

// deleteOldRunDirs removes all numeric directories in
// subDirPath except the one matching keepMtime.
func deleteOldRunDirs(subDirPath string, keepMtime int64) error {
	entries, err := os.ReadDir(subDirPath)
	if err != nil {
		return fmt.Errorf("read dir for cleanup: %w", err)
	}

	for _, entry := range entries {
		if err := removeIfOldRunDir(subDirPath, entry, keepMtime); err != nil {
			return err
		}
	}

	return nil
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

// readSubDirConfig reads config.yml for the given subdirectory.
func readSubDirConfig(subDir SubDir) (SubDirConfig, error) {
	return ReadConfig(subDir.Path)
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

func removeIfOldRunDir(subDirPath string, entry os.DirEntry, keepMtime int64) error {
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
