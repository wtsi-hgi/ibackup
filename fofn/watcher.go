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
	"encoding/json"
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
	statusFilename    = "status"
	maxPollWorkers    = 10
	runRecordFilename = "run.json"
	phaseRunning      = "running"
	phaseDone         = "done"
	phaseBuried       = "buried"
	ownerReadWrite    = 0o600
)

// jobResult classifies wr job status for the state machine.
type jobResult int

const (
	jobsNA       jobResult = iota // done phase; no wr query needed
	jobsRunning                   // at least one job still active
	jobsComplete                  // all complete, none buried
	jobsBuried                    // at least one job buried
)

// actionType describes the action the state machine selects.
type actionType int

const (
	actionWait    actionType = iota // do nothing; jobs still running
	actionSettle                    // ensure artefacts match current phase
	actionRestart                   // teardown old run + start new
)

// transitionKey uniquely identifies a state-machine input.
type transitionKey struct {
	phase       string
	fofnChanged bool
	jobs        jobResult
}

// transition holds the result of a state-machine lookup.
type transition struct {
	action   actionType
	newPhase string // meaningful only for actionSettle
}

// transitions is the explicit state machine. Every reachable (phase,
// fofnChanged, jobResult) triple MUST have an entry. A missing entry panics at
// runtime, making missed-case bugs structurally impossible rather than merely
// unlikely.
var transitions = map[transitionKey]transition{ //nolint:gochecknoglobals
	// running + fofn unchanged
	{phaseRunning, false, jobsRunning}:  {actionWait, ""},
	{phaseRunning, false, jobsComplete}: {actionSettle, phaseDone},
	{phaseRunning, false, jobsBuried}:   {actionSettle, phaseBuried},
	// running + fofn changed
	{phaseRunning, true, jobsRunning}:  {actionWait, ""},
	{phaseRunning, true, jobsComplete}: {actionRestart, ""},
	{phaseRunning, true, jobsBuried}:   {actionRestart, ""},
	// done + fofn unchanged (no wr query needed)
	{phaseDone, false, jobsNA}: {actionSettle, phaseDone},
	// done + fofn changed (no wr query needed)
	{phaseDone, true, jobsNA}: {actionRestart, ""},
	// buried + fofn unchanged
	{phaseBuried, false, jobsRunning}:  {actionWait, ""},
	{phaseBuried, false, jobsComplete}: {actionSettle, phaseDone},
	{phaseBuried, false, jobsBuried}:   {actionSettle, phaseBuried},
	// buried + fofn changed
	{phaseBuried, true, jobsRunning}:  {actionWait, ""},
	{phaseBuried, true, jobsComplete}: {actionRestart, ""},
	{phaseBuried, true, jobsBuried}:   {actionRestart, ""},
}

// classifyJobResult determines the jobResult for the transition table from the
// current phase and wr job status. Done-phase runs return jobsNA because no wr
// query is made for them.
func classifyJobResult(phase string, status RunJobStatus) jobResult {
	if phase == phaseDone {
		return jobsNA
	}

	if status.HasRunning {
		return jobsRunning
	}

	if len(status.BuriedChunks) > 0 {
		return jobsBuried
	}

	return jobsComplete
}

// RunRecord is the persisted state for one subdirectory's current run. It is
// written atomically to run.json in the subdirectory.
type RunRecord struct {
	FofnMtime    int64    `json:"fofn_mtime"`
	RunDir       string   `json:"run_dir"`
	RepGroup     string   `json:"rep_group"`
	Phase        string   `json:"phase"`
	BuriedChunks []string `json:"buried_chunks,omitempty"`
}

// readRunRecord reads run.json from subDirPath. Returns found=false if the
// file does not exist.
func readRunRecord(subDirPath string) (RunRecord, bool, error) {
	data, err := os.ReadFile(filepath.Join(subDirPath, runRecordFilename))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return RunRecord{}, false, nil
		}

		return RunRecord{}, false, fmt.Errorf("read run record: %w", err)
	}

	var rec RunRecord
	if err := json.Unmarshal(data, &rec); err != nil {
		return RunRecord{}, false, fmt.Errorf("parse run record: %w", err)
	}

	return rec, true, nil
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
// shuffled chunks, and submits jobs. Returns a RunState describing the active
// run, or an error.
func ProcessSubDir(subDir SubDir, submitter JobSubmitter, cfg ProcessSubDirConfig) (RunState, error) {
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

// preRead holds a cached run record, fofn mtime, and any read error for one
// subdirectory. Centralising all reads into the pre-scan lets pollSubDir make
// decisions without additional I/O.
type preRead struct {
	rec       *RunRecord
	err       error
	fofnMtime int64
	mtimeErr  error
}

// preReadOne reads the run record and fofn mtime for a single subdirectory.
// Returns nil when no record exists (new directory). The active flag indicates
// whether a wr query is required for this entry.
func preReadOne(sd SubDir) (*preRead, bool) {
	rec, found, err := readRunRecord(sd.Path)
	if err != nil {
		return &preRead{err: err}, true
	}

	if !found {
		return nil, false
	}

	r := rec
	pr := preRead{rec: &r}

	pr.fofnMtime, pr.mtimeErr = fofnMtimeFromPath(sd.Path)

	return &pr, rec.Phase != phaseDone
}

// Watcher monitors a watch directory for fofn changes and manages backup runs
// using a persisted run.json state machine.
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

// Poll performs one poll cycle: scan for subdirectories, optionally query wr
// for job statuses, and process each subdirectory through a linear pipeline.
//
// A pre-scan reads each subdirectory's run.json and fofn mtime once into a
// cache, both to determine whether wr must be queried and to supply each
// goroutine with its data without further file reads. Done directories whose
// fofn mtime has not changed and whose artefacts are intact are skipped
// entirely — zero additional I/O. In the common steady state where all
// directories are done, the expensive wr query is also skipped, reducing poll
// cost to O(N) ReadFile + Stat calls.
func (w *Watcher) Poll() error {
	subDirs, err := ScanForFOFNs(w.watchDir)
	if err != nil {
		return err
	}

	cache, needWR := preReadAll(subDirs)

	var allStatus map[string]RunJobStatus

	if needWR {
		allStatus, err = ClassifyAllJobs(w.submitter)
		if err != nil {
			return err
		}
	}

	return w.pollSubDirsParallel(subDirs, cache, allStatus)
}

// preReadAll reads all run.json files and fofn mtimes in one pass, caching the
// results and determining whether any subdirectory requires a wr query.
// Subdirectories without run.json (no entry in cache) need no wr query;
// subdirectories with a read error are cached and wr is queried
// conservatively.
func preReadAll(subDirs []SubDir) (map[string]preRead, bool) {
	cache := make(map[string]preRead, len(subDirs))
	needWR := false

	for _, sd := range subDirs {
		pr, active := preReadOne(sd)
		if pr == nil {
			continue
		}

		if active {
			needWR = true
		}

		cache[sd.Path] = *pr
	}

	return cache, needWR
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
// parallelism, collecting all errors.
func (w *Watcher) pollSubDirsParallel(
	subDirs []SubDir, cache map[string]preRead, allStatus map[string]RunJobStatus) error {
	var (
		wg    sync.WaitGroup
		errMu sync.Mutex
		errs  []error
		sem   = make(chan struct{}, maxPollWorkers)
	)

	for _, subDir := range subDirs {
		wg.Add(1)

		sem <- struct{}{}

		go w.pollSubDirCollect(subDir, cache[subDir.Path], allStatus, sem, &wg, &errMu, &errs)
	}

	wg.Wait()

	return errors.Join(errs...)
}

func (w *Watcher) pollSubDirCollect(
	sd SubDir,
	cached preRead,
	allStatus map[string]RunJobStatus,
	sem chan struct{},
	wg *sync.WaitGroup, errMu *sync.Mutex, errs *[]error,
) {
	defer wg.Done()
	defer func() { <-sem }()

	if err := w.pollSubDir(sd, cached, allStatus); err != nil {
		errMu.Lock()

		*errs = append(*errs, err)

		errMu.Unlock()
	}
}

// pollSubDir processes a single subdirectory through a linear decision
// pipeline using the cached run record and fofn mtime from pre-scan:
//
//  1. Pre-read error       → propagate
//  2. No run record        → start a new run
//  3. Mtime read error     → propagate
//  4. Existing run record  → reconcile state
func (w *Watcher) pollSubDir(subDir SubDir, cached preRead, allStatus map[string]RunJobStatus) error {
	if cached.err != nil {
		return cached.err
	}

	if cached.rec == nil {
		return w.startNewRun(subDir)
	}

	if cached.mtimeErr != nil {
		return cached.mtimeErr
	}

	return w.reconcile(subDir, cached, allStatus)
}

// reconcile uses the explicit transition table to determine and execute the
// correct action for a subdirectory with an existing run record. Every
// (phase, fofnChanged, jobResult) triple is accounted for in the table;
// a missing entry panics, making missed-case bugs structurally impossible.
func (w *Watcher) reconcile(subDir SubDir, cached preRead, allStatus map[string]RunJobStatus) error {
	rec := cached.rec
	status := allStatus[rec.RepGroup]

	key := transitionKey{
		phase:       rec.Phase,
		fofnChanged: cached.fofnMtime != rec.FofnMtime,
		jobs:        classifyJobResult(rec.Phase, status),
	}

	tr, ok := transitions[key]
	if !ok {
		panic(fmt.Sprintf("watchfofns: unhandled transition: phase=%s fofnChanged=%t jobs=%d",
			key.phase, key.fofnChanged, key.jobs))
	}

	switch tr.action {
	case actionWait:
		return nil
	case actionSettle:
		buried := sortedBuriedChunks(status)

		return w.settle(subDir, rec, tr.newPhase, buried)
	case actionRestart:
		return w.teardownAndRestart(subDir, *rec, status)
	default:
		panic(fmt.Sprintf("watchfofns: unknown action: %d", tr.action))
	}
}

// settle is the single convergent artefact reconciler. Every code path that
// must ensure status file, symlink, and run-record consistency calls this
// function. It is idempotent: when artefacts are already correct and the phase
// has not changed, the cost is a single Stat + Readlink.
func (w *Watcher) settle(subDir SubDir, rec *RunRecord, newPhase string, buriedChunks []string) error {
	phaseChanged := newPhase != rec.Phase || !slices.Equal(buriedChunks, rec.BuriedChunks)

	rec.Phase = newPhase
	rec.BuriedChunks = buriedChunks

	if !phaseChanged && artefactsIntact(rec, subDir.Path) {
		return nil
	}

	if err := repairArtefacts(rec.RunDir, subDir, buriedChunks, phaseChanged); err != nil {
		return err
	}

	if !phaseChanged {
		return nil
	}

	if err := deleteOldRunDirs(subDir.Path, rec.FofnMtime); err != nil {
		return err
	}

	return WriteRunRecord(subDir.Path, *rec)
}

// artefactsIntact returns true when the status file exists in rec.RunDir and
// the status symlink in subDirPath points to it.
func artefactsIntact(rec *RunRecord, subDirPath string) bool {
	statusPath := filepath.Join(rec.RunDir, statusFilename)

	if _, err := os.Stat(statusPath); err != nil {
		return false
	}

	return statusSymlinkCurrent(statusPath, subDirPath)
}

// repairArtefacts ensures the status file and symlink are correct. Extracted
// from settle to keep cyclomatic complexity low.
func repairArtefacts(
	runDir string, subDir SubDir,
	buriedChunks []string, phaseChanged bool,
) error {
	if err := ensureStatusFile(runDir, subDir, buriedChunks, phaseChanged); err != nil {
		return err
	}

	return ensureStatusSymlink(runDir, subDir.Path)
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

// ensureStatusFile checks whether the status file exists in runDir. If absent
// or if forceRegenerate is true (phase transition), it regenerates the status
// from reports. When the file already exists and no regeneration is forced, the
// cost is a single os.Stat call.
func ensureStatusFile(runDir string, subDir SubDir, buriedChunks []string, forceRegenerate bool) error {
	if !forceRegenerate {
		statusPath := filepath.Join(runDir, statusFilename)

		if _, err := os.Stat(statusPath); err == nil {
			return nil
		}
	}

	return GenerateStatus(runDir, subDir, buriedChunks)
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
// up buried jobs and stale directories, removes run.json, and starts a fresh
// run for the updated fofn. Called by the state machine's actionRestart; for
// done-phase runs where no wr query was made, the zero-value RunJobStatus
// correctly indicates no buried jobs.
func (w *Watcher) teardownAndRestart(subDir SubDir, rec RunRecord, status RunJobStatus) error {
	buriedChunks := sortedBuriedChunks(status)

	if err := GenerateStatus(rec.RunDir, subDir, buriedChunks); err != nil {
		return err
	}

	if len(status.BuriedJobs) > 0 {
		if err := w.submitter.DeleteJobs(status.BuriedJobs); err != nil {
			return err
		}
	}

	if err := deleteOldRunDirs(subDir.Path, rec.FofnMtime); err != nil {
		return err
	}

	if err := removeRunRecord(subDir.Path); err != nil {
		return err
	}

	return w.startNewRun(subDir)
}

// WriteRunRecord writes run.json atomically via temp file + rename. Sets GID
// to match the watch directory (parent of subDirPath).
func WriteRunRecord(subDirPath string, rec RunRecord) error {
	data, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal run record: %w", err)
	}

	recordPath := filepath.Join(subDirPath, runRecordFilename)

	return writeAtomicWithGID(recordPath, data, subDirPath)
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

// removeRunRecord removes run.json from subDirPath, ignoring ErrNotExist.
func removeRunRecord(subDirPath string) error {
	err := os.Remove(filepath.Join(subDirPath, runRecordFilename))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove run record: %w", err)
	}

	return nil
}

func (w *Watcher) startNewRun(subDir SubDir) error {
	state, err := ProcessSubDir(subDir, w.submitter, w.cfg)
	if err != nil {
		return err
	}

	if state.RepGroup == "" {
		return nil
	}

	rec := RunRecord{
		FofnMtime: state.Mtime,
		RunDir:    state.RunDir,
		RepGroup:  state.RepGroup,
		Phase:     phaseRunning,
	}

	return WriteRunRecord(subDir.Path, rec)
}

// fofnMtimeFromPath returns the mtime of the fofn file in the given
// subdirectory path as a Unix timestamp.
func fofnMtimeFromPath(subDirPath string) (int64, error) {
	fofnPath := filepath.Join(subDirPath, fofnFilename)

	info, err := os.Stat(fofnPath)
	if err != nil {
		return 0, err
	}

	return info.ModTime().Unix(), nil
}

// GenerateStatus writes a combined status file from all chunk reports in
// runDir, handles buried chunks, and creates/updates a symlink at subDir/status
// pointing to runDir/status.
func GenerateStatus(runDir string, subDir SubDir, buriedChunks []string) error {
	statusPath := filepath.Join(runDir, statusFilename)

	if err := WriteStatusFromRun(runDir, statusPath, buriedChunks); err != nil {
		return err
	}

	if err := setStatusGID(statusPath, subDir.Path); err != nil {
		return err
	}

	return createStatusSymlink(statusPath, subDir.Path)
}

// writeAtomicWithGID writes data to destPath via a temp file, sets GID from
// the watch directory (parent of subDirPath), and renames into place.
func writeAtomicWithGID(destPath string, data []byte, subDirPath string) error {
	tmpPath := destPath + ".tmp"

	if err := os.WriteFile(tmpPath, data, ownerReadWrite); err != nil {
		return fmt.Errorf("write temp: %w", err)
	}

	gid, err := ownership.GetDirGID(filepath.Dir(subDirPath))
	if err != nil {
		_ = os.Remove(tmpPath)

		return err
	}

	if err := os.Chown(tmpPath, -1, gid); err != nil {
		_ = os.Remove(tmpPath)

		return fmt.Errorf("chown temp: %w", err)
	}

	if err := os.Rename(tmpPath, destPath); err != nil {
		_ = os.Remove(tmpPath)

		return fmt.Errorf("rename temp: %w", err)
	}

	return nil
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

// readAndCheckConfig reads the config and returns the fofn mtime. Returns zero
// mtime if the fofn file does not exist.
func readAndCheckConfig(subDir SubDir) (SubDirConfig, int64, error) {
	fofnPath := filepath.Join(subDir.Path, fofnFilename)

	info, err := os.Stat(fofnPath)
	if err != nil {
		return SubDirConfig{}, 0, err
	}

	mtime := info.ModTime().Unix()

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
