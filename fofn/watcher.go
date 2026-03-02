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
	stateDone:    "skip (done)",
	stateChanged: "teardown and restart (fofn changed)",
	stateDamaged: "repair artefacts",
	stateRunning: "wait for jobs",
	stateSettled: "settle artefacts",
	stateRestart: "teardown and restart (obsolete run)",
}

// subDirState classifies the complete observable state of a subdirectory.
// Every reachable combination of filesystem and wr state maps to exactly one
// value. The dispatch switch and the exhaustive test verify all values are
// handled, making missed-case bugs structurally impossible.
type subDirState int

const (
	stateNewRun  subDirState = iota // no run dir → start new
	stateDone                       // artefacts intact, fofn unchanged → skip
	stateChanged                    // artefacts intact, fofn changed → teardown + restart
	stateDamaged                    // cached done, artefacts damaged → repair
	stateRunning                    // wr jobs still active → wait
	stateSettled                    // wr jobs done, fofn unchanged → settle
	stateRestart                    // wr jobs done, fofn changed → teardown + restart
	numStates                       // sentinel for exhaustive checks
)

// jobResult classifies wr job status.
type jobResult int

const (
	jobsRunning  jobResult = iota // at least one job still active
	jobsComplete                  // all complete, none buried
	jobsBuried                    // at least one job buried
)

// classifyWR determines the state for an active run using wr data.
func classifyWR(snap snapshot, status RunJobStatus) subDirState {
	if classifyJobResult(status) == jobsRunning {
		return stateRunning
	}

	if snap.MtimeMatch {
		return stateSettled
	}

	return stateRestart
}

// classifyJobResult determines the jobResult from the wr job status.
func classifyJobResult(status RunJobStatus) jobResult {
	if status.HasRunning {
		return jobsRunning
	}

	if len(status.BuriedChunks) > 0 {
		return jobsBuried
	}

	return jobsComplete
}

// snapshot captures filesystem-observable state for a subdirectory, gathered
// once during classification. Downstream processing reads only this snapshot,
// eliminating redundant filesystem reads (e.g. the duplicate findRunDir call
// that previously occurred on the wr-query path).
type snapshot struct {
	RunDir     string
	RunMtime   int64
	HasRunDir  bool
	MtimeMatch bool // RunMtime == SubDir.FofnMtime
	Intact     bool // status file exists + symlink correct
	CachedDone bool // in-memory done-cache hit for current fofn mtime
}

// pollEntry holds the per-subdirectory state computed during a poll cycle.
type pollEntry struct {
	sd      SubDir
	snap    snapshot
	state   subDirState
	needsWR bool
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
// using a state machine driven by the filesystem and wr job status. All state
// is derived from:
//   - run directory existence (subdir/<mtime>/)
//   - status file + symlink presence (artefacts)
//   - wr job query (running / complete / buried)
//
// A lightweight in-memory doneRuns cache tracks confirmed-done directories,
// allowing the expensive wr query to be skipped when all directories are done.
type Watcher struct {
	watchDir  string
	submitter JobSubmitter
	cfg       ProcessSubDirConfig
	logger    *slog.Logger
	mu        sync.Mutex
	doneRuns  map[string]int64 // subdir path → fofn mtime of confirmed-done run
}

// NewWatcher creates a new Watcher for the given watch directory.
func NewWatcher(watchDir string, submitter JobSubmitter, cfg ProcessSubDirConfig) *Watcher {
	return &Watcher{
		watchDir:  watchDir,
		submitter: submitter,
		cfg:       cfg,
		logger:    slog.Default(),
		doneRuns:  make(map[string]int64),
	}
}

// isDone returns true if the subdirectory has a confirmed-done run matching the
// given fofn mtime.
func (w *Watcher) isDone(path string, fofnMtime int64) bool {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.doneRuns[path] == fofnMtime
}

// markDone records that a subdirectory's run is confirmed done.
func (w *Watcher) markDone(path string, fofnMtime int64) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.doneRuns[path] = fofnMtime
}

// clearDone removes the done marker for a subdirectory.
func (w *Watcher) clearDone(path string) {
	w.mu.Lock()
	defer w.mu.Unlock()

	delete(w.doneRuns, path)
}

// Poll performs one poll cycle: scan for subdirectories, classify each using
// filesystem state, optionally query wr for unresolved entries, and dispatch
// the appropriate action for each subdirectory through the unified state
// machine.
//
// In the common steady state where all directories are done (artefacts intact
// + fofn unchanged), the expensive wr query is skipped entirely.
func (w *Watcher) Poll() error {
	subDirs, err := ScanForFOFNs(w.watchDir)
	if err != nil {
		return err
	}

	w.pruneDone(subDirs)

	entries, anyNeedWR := w.classifyAll(subDirs)

	var allStatus map[string]RunJobStatus

	if anyNeedWR {
		allStatus, err = ClassifyAllJobs(w.submitter)
		if err != nil {
			return err
		}

		resolveWR(entries, allStatus)
	}

	return w.dispatchAll(entries, allStatus)
}

// resolveWR refines unresolved pollEntries using wr job status data.
func resolveWR(entries []pollEntry, allStatus map[string]RunJobStatus) {
	for i := range entries {
		if !entries[i].needsWR {
			continue
		}

		repGroup := makeRepGroup(entries[i].sd.Path, entries[i].snap.RunMtime)
		status := allStatus[repGroup]

		entries[i].state = classifyWR(entries[i].snap, status)
		entries[i].needsWR = false
	}
}

// pruneDone removes done-cache entries for subdirectories no longer present.
func (w *Watcher) pruneDone(subDirs []SubDir) {
	present := make(map[string]struct{}, len(subDirs))
	for _, sd := range subDirs {
		present[sd.Path] = struct{}{}
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	for path := range w.doneRuns {
		if _, ok := present[path]; !ok {
			delete(w.doneRuns, path)
		}
	}
}

// classifyAll snapshots every subdirectory and classifies it using filesystem
// state. Returns the entries and whether any require a wr query to resolve.
func (w *Watcher) classifyAll(subDirs []SubDir) ([]pollEntry, bool) {
	entries := make([]pollEntry, len(subDirs))
	anyNeedWR := false

	for i, sd := range subDirs {
		snap := w.takeSnapshot(sd)
		state, needsWR := w.classifyFS(sd, snap)

		entries[i] = pollEntry{
			sd:      sd,
			snap:    snap,
			state:   state,
			needsWR: needsWR,
		}

		if needsWR {
			anyNeedWR = true
		}
	}

	return entries, anyNeedWR
}

// takeSnapshot captures filesystem state for a subdirectory in a single pass.
// The resulting snapshot is used for all subsequent classification and dispatch,
// eliminating redundant directory reads. If the in-memory done cache points to
// a run directory that no longer exists, the cache is cleared (fixing an edge
// case where external deletion caused an error loop).
func (w *Watcher) takeSnapshot(sd SubDir) snapshot {
	// Check in-memory done cache first.
	if w.isDone(sd.Path, sd.FofnMtime) {
		runDir := filepath.Join(sd.Path, strconv.FormatInt(sd.FofnMtime, 10))

		fi, err := os.Stat(runDir)
		if err == nil && fi.IsDir() {
			return snapshot{
				RunDir:     runDir,
				RunMtime:   sd.FofnMtime,
				HasRunDir:  true,
				MtimeMatch: true,
				Intact:     artefactsIntact(runDir, sd.Path),
				CachedDone: true,
			}
		}

		// Cached run dir vanished — clear cache and scan from disk.
		w.clearDone(sd.Path)
	}

	// Scan for the latest run directory on disk.
	runDir, runMtime, found := findRunDir(sd.Path)
	if !found {
		return snapshot{}
	}

	return snapshot{
		RunDir:     runDir,
		RunMtime:   runMtime,
		HasRunDir:  true,
		MtimeMatch: runMtime == sd.FofnMtime,
		Intact:     artefactsIntact(runDir, sd.Path),
	}
}

// findRunDir finds the current run directory inside subDirPath by looking for
// the highest-numbered numeric subdirectory. Returns the path, its mtime
// value, and true if found; returns ("", 0, false) if no run directory exists.
func findRunDir(subDirPath string) (string, int64, bool) {
	entries, err := os.ReadDir(subDirPath)
	if err != nil {
		return "", 0, false
	}

	var bestMtime int64

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		n, parseErr := strconv.ParseInt(entry.Name(), 10, 64)
		if parseErr != nil {
			continue
		}

		if n > bestMtime {
			bestMtime = n
		}
	}

	if bestMtime == 0 {
		return "", 0, false
	}

	return filepath.Join(subDirPath, strconv.FormatInt(bestMtime, 10)), bestMtime, true
}

// classifyFS determines the subdirectory state from filesystem-only data.
// Returns needsWR=true when wr data is required to determine the final state;
// the returned state value is meaningless in that case.
func (w *Watcher) classifyFS(sd SubDir, snap snapshot) (subDirState, bool) {
	if !snap.HasRunDir {
		return stateNewRun, false
	}

	if snap.Intact {
		if snap.MtimeMatch {
			w.markDone(sd.Path, sd.FofnMtime)

			return stateDone, false
		}

		return stateChanged, false
	}

	if snap.CachedDone {
		return stateDamaged, false
	}

	return 0, true // needs wr
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

// dispatchAll processes all entries concurrently with bounded parallelism,
// collecting errors. stateDone and stateRunning entries are skipped.
func (w *Watcher) dispatchAll(entries []pollEntry, allStatus map[string]RunJobStatus) error {
	var (
		wg    sync.WaitGroup
		errMu sync.Mutex
		errs  []error
		sem   = make(chan struct{}, maxPollWorkers)
	)

	for i := range entries {
		if entries[i].state == stateDone || entries[i].state == stateRunning {
			continue
		}

		wg.Add(1)

		sem <- struct{}{}

		go w.dispatchCollect(entries[i], allStatus, sem, &wg, &errMu, &errs)
	}

	wg.Wait()

	return errors.Join(errs...)
}

func (w *Watcher) dispatchCollect(
	e pollEntry, allStatus map[string]RunJobStatus,
	sem chan struct{}, wg *sync.WaitGroup, errMu *sync.Mutex, errs *[]error,
) {
	defer wg.Done()
	defer func() { <-sem }()

	if err := w.dispatch(e, allStatus); err != nil {
		errMu.Lock()
		*errs = append(*errs, err)
		errMu.Unlock()
	}
}

// dispatch executes the action for a single subdirectory based on its state.
// Every subDirState value has an explicit case; the default panics, making
// unhandled states a crash rather than a silent bug.
func (w *Watcher) dispatch(e pollEntry, allStatus map[string]RunJobStatus) error {
	switch e.state { //nolint:exhaustive
	case stateNewRun:
		return w.startNewRun(e.sd)
	case stateChanged:
		return w.teardownAndRestart(e.sd, e.snap.RunDir, e.snap.RunMtime, RunJobStatus{})
	case stateDamaged:
		return w.settle(e.sd, e.snap.RunDir, e.snap.RunMtime, RunJobStatus{})
	case stateSettled:
		return w.settle(e.sd, e.snap.RunDir, e.snap.RunMtime,
			allStatus[makeRepGroup(e.sd.Path, e.snap.RunMtime)])
	case stateRestart:
		return w.teardownAndRestart(e.sd, e.snap.RunDir, e.snap.RunMtime,
			allStatus[makeRepGroup(e.sd.Path, e.snap.RunMtime)])
	default:
		panic(fmt.Sprintf("watchfofns: unhandled state: %d (%s)", e.state, stateDesc[e.state]))
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
// After settling, if no buried chunks remain, the subdirectory is marked done
// in the in-memory cache so future polls can skip the wr query.
func (w *Watcher) settle(subDir SubDir, runDir string, runMtime int64, status RunJobStatus) error {
	buriedChunks := sortedBuriedChunks(status)

	if needsStatusRegen(runDir, status.LastCompletedTime) {
		if err := GenerateStatus(runDir, subDir, buriedChunks); err != nil {
			return err
		}
	}

	if len(buriedChunks) > 0 {
		// Remove symlink for runs with buried chunks so artefactsIntact
		// returns false, ensuring future polls query wr to detect retry
		// completion.
		removeStatusSymlink(subDir.Path)
		w.clearDone(subDir.Path)

		return nil
	}

	if err := ensureStatusSymlink(runDir, subDir.Path); err != nil {
		return err
	}

	if err := deleteOldRunDirs(subDir.Path, runMtime); err != nil {
		return err
	}

	w.markDone(subDir.Path, subDir.FofnMtime)

	return nil
}

// artefactsIntact returns true when the status file exists in runDir and
// the status symlink in subDirPath points to it.
func artefactsIntact(runDir, subDirPath string) bool {
	statusPath := filepath.Join(runDir, statusFilename)

	if _, err := os.Stat(statusPath); err != nil {
		return false
	}

	return statusSymlinkCurrent(statusPath, subDirPath)
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

// removeStatusSymlink removes the status symlink from subDirPath, if present.
// Errors are ignored since the symlink may not exist.
func removeStatusSymlink(subDirPath string) {
	_ = os.Remove(filepath.Join(subDirPath, statusFilename))
}

// statusSymlinkCurrent returns true when the subDir/status symlink exists and
// points to the given statusPath.
func statusSymlinkCurrent(statusPath, subDirPath string) bool {
	symlinkPath := filepath.Join(subDirPath, statusFilename)

	target, err := os.Readlink(symlinkPath)
	if err != nil {
		return false
	}

	return target == statusPath
}

// ensureStatusSymlink checks whether the status symlink in subDirPath points
// to the correct status file in runDir. If absent or incorrect, it creates or
// updates the symlink. When the symlink is already correct, the cost is a
// single os.Readlink call.
func ensureStatusSymlink(runDir string, subDirPath string) error {
	statusPath := filepath.Join(runDir, statusFilename)

	if statusSymlinkCurrent(statusPath, subDirPath) {
		return nil
	}

	return createStatusSymlink(statusPath, subDirPath)
}

// teardownAndRestart generates a final status snapshot for the old run, cleans
// up buried jobs and stale directories, and starts a fresh run for the updated
// fofn. For done-phase runs where no wr query was made, the zero-value
// RunJobStatus correctly indicates no buried jobs.
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

	w.clearDone(subDir.Path)

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
// pointing to the given statusPath, using a temp symlink + rename.
func createStatusSymlink(statusPath, subDirPath string) error {
	symlinkPath := filepath.Join(subDirPath, statusFilename)
	tmpLink := symlinkPath + ".tmp"

	_ = os.Remove(tmpLink)

	if err := os.Symlink(statusPath, tmpLink); err != nil {
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
