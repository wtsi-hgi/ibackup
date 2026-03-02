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

// jobResult classifies wr job status for the state machine.
type jobResult int

const (
	jobsRunning  jobResult = iota // at least one job still active
	jobsComplete                  // all complete, none buried
	jobsBuried                    // at least one job buried
)

// actionType describes the action the state machine selects.
type actionType int

const (
	actionWait    actionType = iota // do nothing; jobs still running
	actionSettle                    // ensure artefacts match current state
	actionRestart                   // teardown old run + start new
)

// transitionKey uniquely identifies a state-machine input. Phase is derived
// from the filesystem (run-dir existence + status file) and wr, so the table
// uses only two dimensions: whether the fofn changed and the wr job result.
type transitionKey struct {
	fofnChanged bool
	jobs        jobResult
}

// transitions is the explicit state machine. Every reachable (fofnChanged,
// jobResult) pair MUST have an entry. A missing entry panics at runtime,
// making missed-case bugs structurally impossible rather than merely unlikely.
//
// The table applies only to subdirectories with an existing run directory.
// "Quick done" directories (artefacts intact + fofn unchanged) are handled
// before the wr query and never reach the table.
var transitions = map[transitionKey]actionType{ //nolint:gochecknoglobals
	// fofn unchanged
	{false, jobsRunning}:  actionWait,
	{false, jobsComplete}: actionSettle,
	{false, jobsBuried}:   actionSettle,
	// fofn changed
	{true, jobsRunning}:  actionWait,
	{true, jobsComplete}: actionRestart,
	{true, jobsBuried}:   actionRestart,
}

// quickResult records the fast-path classification of a subdirectory.
type quickResult int

const (
	quickNeedsPoll   quickResult = iota // needs wr query + state machine
	quickDone                           // confirmed done — skip entirely
	quickNewRun                         // no run dir at all — start new
	quickDoneRestart                    // done but fofn changed — restart
	quickDoneRepair                     // cached done, artefacts damaged — repair without wr
)

// classifyJobResult determines the jobResult for the transition table from
// the wr job status.
func classifyJobResult(status RunJobStatus) jobResult {
	if status.HasRunning {
		return jobsRunning
	}

	if len(status.BuriedChunks) > 0 {
		return jobsBuried
	}

	return jobsComplete
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

// Poll performs one poll cycle: scan for subdirectories, optionally query wr
// for job statuses, and process each subdirectory through the state machine.
//
// In the common steady state where all directories are done (artefacts intact
// + fofn unchanged), the expensive wr query is skipped entirely.
func (w *Watcher) Poll() error {
	subDirs, err := ScanForFOFNs(w.watchDir)
	if err != nil {
		return err
	}

	w.pruneDone(subDirs)

	needWR, quickResults := w.classifySubDirs(subDirs)

	var allStatus map[string]RunJobStatus

	if needWR {
		allStatus, err = ClassifyAllJobs(w.submitter)
		if err != nil {
			return err
		}
	}

	return w.pollSubDirsParallel(subDirs, quickResults, allStatus)
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

// classifySubDirs determines which subdirectories need a wr query by checking
// the done cache and filesystem state. Returns whether wr is needed and
// per-subdir quick results.
func (w *Watcher) classifySubDirs(subDirs []SubDir) (bool, map[string]quickResult) {
	needWR := false
	results := make(map[string]quickResult, len(subDirs))

	for _, sd := range subDirs {
		qr := w.quickClassify(sd)
		results[sd.Path] = qr

		if qr == quickNeedsPoll {
			needWR = true
		}
	}

	return needWR, results
}

// quickClassify performs a fast filesystem check to determine if a subdirectory
// can be handled without a wr query.
func (w *Watcher) quickClassify(subDir SubDir) quickResult {
	// Check the in-memory done cache first.
	if w.isDone(subDir.Path, subDir.FofnMtime) {
		runDir := filepath.Join(subDir.Path, strconv.FormatInt(subDir.FofnMtime, 10))

		if artefactsIntact(runDir, subDir.Path) {
			return quickDone
		}

		// Artefacts damaged — repair without wr since we know it was done.
		return quickDoneRepair
	}

	// Not in cache. Check if a run dir exists at all.
	_, runMtime, found := findRunDir(subDir.Path)
	if !found {
		return quickNewRun
	}

	// Run dir exists. Check artefact state once.
	runDir := filepath.Join(subDir.Path, strconv.FormatInt(runMtime, 10))
	if !artefactsIntact(runDir, subDir.Path) {
		return quickNeedsPoll
	}

	// Artefacts intact. If mtime matches fofn, this is a done run we
	// haven't cached yet (e.g. first poll after restart).
	if runMtime == subDir.FofnMtime {
		w.markDone(subDir.Path, subDir.FofnMtime)

		return quickDone
	}

	// Old done run with changed fofn — restart without wr.
	return quickDoneRestart
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

// pollSubDirsParallel processes all subdirectories concurrently with bounded
// parallelism, collecting all errors. quickDone subdirectories are skipped.
func (w *Watcher) pollSubDirsParallel(
	subDirs []SubDir, quickResults map[string]quickResult, allStatus map[string]RunJobStatus,
) error {
	var (
		wg    sync.WaitGroup
		errMu sync.Mutex
		errs  []error
		sem   = make(chan struct{}, maxPollWorkers)
	)

	for _, subDir := range subDirs {
		qr := quickResults[subDir.Path]
		if qr == quickDone {
			continue
		}

		wg.Add(1)

		sem <- struct{}{}

		go w.pollSubDirCollect(subDir, qr, allStatus, sem, &wg, &errMu, &errs)
	}

	wg.Wait()

	return errors.Join(errs...)
}

func (w *Watcher) pollSubDirCollect(
	sd SubDir,
	qr quickResult,
	allStatus map[string]RunJobStatus,
	sem chan struct{},
	wg *sync.WaitGroup, errMu *sync.Mutex, errs *[]error,
) {
	defer wg.Done()
	defer func() { <-sem }()

	if err := w.pollSubDir(sd, qr, allStatus); err != nil {
		errMu.Lock()

		*errs = append(*errs, err)

		errMu.Unlock()
	}
}

// pollSubDir processes a single subdirectory. State is derived entirely from
// the filesystem (run directory existence, status file) and wr job status.
func (w *Watcher) pollSubDir(subDir SubDir, qr quickResult, allStatus map[string]RunJobStatus) error {
	switch qr {
	case quickNewRun:
		return w.startNewRun(subDir)
	case quickDoneRestart:
		runDir, runMtime, _ := findRunDir(subDir.Path)

		return w.teardownAndRestart(subDir, runDir, runMtime, RunJobStatus{})
	case quickDoneRepair:
		// Cached-done entry with damaged artefacts — repair without wr query.
		runDir := filepath.Join(subDir.Path, strconv.FormatInt(subDir.FofnMtime, 10))

		return w.settle(subDir, runDir, subDir.FofnMtime, RunJobStatus{})
	default:
		// quickNeedsPoll: has a run dir, needs wr to determine action
		return w.reconcile(subDir, allStatus)
	}
}

// reconcile uses the explicit transition table to determine and execute the
// correct action for a subdirectory with an existing run directory. State is
// derived from the filesystem and wr; every (fofnChanged, jobResult) pair is
// accounted for in the table; a missing entry panics, making missed-case bugs
// structurally impossible.
func (w *Watcher) reconcile(subDir SubDir, allStatus map[string]RunJobStatus) error {
	runDir, runMtime, found := findRunDir(subDir.Path)
	if !found {
		return w.startNewRun(subDir)
	}

	fofnChanged := subDir.FofnMtime != runMtime
	repGroup := makeRepGroup(subDir.Path, runMtime)
	status := allStatus[repGroup]

	key := transitionKey{
		fofnChanged: fofnChanged,
		jobs:        classifyJobResult(status),
	}

	action, ok := transitions[key]
	if !ok {
		panic(fmt.Sprintf("watchfofns: unhandled transition: fofnChanged=%t jobs=%d",
			key.fofnChanged, key.jobs))
	}

	switch action {
	case actionWait:
		return nil
	case actionSettle:
		return w.settle(subDir, runDir, runMtime, status)
	case actionRestart:
		return w.teardownAndRestart(subDir, runDir, runMtime, status)
	default:
		panic(fmt.Sprintf("watchfofns: unknown action: %d", action))
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
