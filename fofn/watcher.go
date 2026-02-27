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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/VertebrateResequencing/wr/jobqueue"
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

// RunRecord is the persisted state for one subdirectory's current run. It is
// written atomically to run.json in the subdirectory.
type RunRecord struct {
	FofnMtime    int64  `json:"fofn_mtime"`
	RunDir       string `json:"run_dir"`
	RepGroup     string `json:"rep_group"`
	Phase        string `json:"phase"`
	ReportDigest string `json:"report_digest"`
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

// Poll performs one poll cycle: scan for subdirectories, query wr once for all
// fofn job statuses, and process each subdirectory through a linear pipeline.
func (w *Watcher) Poll() error {
	subDirs, err := ScanForFOFNs(w.watchDir)
	if err != nil {
		return err
	}

	allStatus, err := ClassifyAllJobs(w.submitter)
	if err != nil {
		return err
	}

	return w.pollSubDirsParallel(subDirs, allStatus)
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
func (w *Watcher) pollSubDirsParallel(subDirs []SubDir, allStatus map[string]RunJobStatus) error {
	var (
		wg    sync.WaitGroup
		errMu sync.Mutex
		errs  []error
		sem   = make(chan struct{}, maxPollWorkers)
	)

	for _, subDir := range subDirs {
		wg.Add(1)

		sem <- struct{}{}

		go w.pollSubDirCollect(subDir, allStatus, sem, &wg, &errMu, &errs)
	}

	wg.Wait()

	return errors.Join(errs...)
}

func (w *Watcher) pollSubDirCollect(
	sd SubDir,
	allStatus map[string]RunJobStatus,
	sem chan struct{},
	wg *sync.WaitGroup, errMu *sync.Mutex, errs *[]error,
) {
	defer wg.Done()
	defer func() { <-sem }()

	if err := w.pollSubDir(sd, allStatus); err != nil {
		errMu.Lock()

		*errs = append(*errs, err)

		errMu.Unlock()
	}
}

// pollSubDir processes a single subdirectory using the run.json state machine:
//  1. No run.json → start a new run
//  2. Jobs still running → wait
//  3. All jobs finished → ensure status is current, then check for fofn changes
func (w *Watcher) pollSubDir(subDir SubDir, allStatus map[string]RunJobStatus) error {
	rec, found, err := readRunRecord(subDir.Path)
	if err != nil {
		return err
	}

	if !found {
		return w.startNewRun(subDir)
	}

	status := allStatus[rec.RepGroup]

	if status.HasRunning {
		return nil
	}

	err = w.ensureStatusCurrent(subDir, rec, status)
	if err != nil {
		return err
	}

	mtime, fofnErr := fofnMtime(subDir)
	if fofnErr != nil {
		return fofnErr
	}

	if mtime != rec.FofnMtime {
		return w.cleanupAndRestart(subDir, rec.FofnMtime, status.BuriedJobs)
	}

	return nil
}

// ensureStatusCurrent ensures the status file, symlink, run record, and old
// run directories are up to date for a subdirectory whose jobs have all
// finished. It is phase-agnostic: it computes the correct phase from the
// current job status rather than branching on the persisted phase.
func (w *Watcher) ensureStatusCurrent(subDir SubDir, rec RunRecord, status RunJobStatus) error {
	digest, err := reportDigest(rec.RunDir)
	if err != nil {
		return err
	}

	phase, buriedChunks := classifyPhase(status)

	if statusNeedsRegeneration(digest, phase, rec, subDir.Path) {
		if err := GenerateStatus(rec.RunDir, subDir, buriedChunks); err != nil {
			return err
		}
	}

	if err := w.updateRunRecordIfChanged(subDir.Path, rec, phase, digest); err != nil {
		return err
	}

	if phase == phaseDone {
		return deleteOldRunDirs(subDir.Path, rec.FofnMtime)
	}

	return nil
}

// reportDigest computes a hex SHA-256 digest of report file names and mtimes
// in the run directory. Returns empty string if no report files exist.
func reportDigest(runDir string) (string, error) {
	names, err := collectReportNames(runDir)
	if err != nil {
		return "", err
	}

	if len(names) == 0 {
		return "", nil
	}

	h := sha256.New()

	for _, name := range names {
		info, err := os.Stat(filepath.Join(runDir, name))
		if err != nil {
			return "", fmt.Errorf("stat report %s: %w", name, err)
		}

		fmt.Fprintf(h, "%s:%d\n", name, info.ModTime().UnixNano())
	}

	return hex.EncodeToString(h.Sum(nil)), nil
}

// classifyPhase determines the correct phase and buried chunks from current
// job status.
func classifyPhase(status RunJobStatus) (string, []string) {
	if len(status.BuriedChunks) > 0 {
		return phaseBuried, status.BuriedChunks
	}

	return phaseDone, nil
}

// statusNeedsRegeneration returns true when the status file or symlink must be
// rewritten because the report digest changed, the phase changed, or the
// symlink is stale.
func statusNeedsRegeneration(digest, phase string, rec RunRecord, subDirPath string) bool {
	return digest != rec.ReportDigest ||
		phase != rec.Phase ||
		!statusSymlinkCurrent(filepath.Join(rec.RunDir, statusFilename), subDirPath)
}

// updateRunRecordIfChanged writes the run record when phase or digest differs
// from what is persisted.
func (w *Watcher) updateRunRecordIfChanged(subDirPath string, rec RunRecord, phase, digest string) error {
	if digest == rec.ReportDigest && phase == rec.Phase {
		return nil
	}

	rec.Phase = phase
	rec.ReportDigest = digest

	return WriteRunRecord(subDirPath, rec)
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

// cleanupAndRestart deletes buried jobs (if any), cleans old run dirs, removes
// run.json, and starts a fresh run.
func (w *Watcher) cleanupAndRestart(
	subDir SubDir, keepMtime int64, buriedJobs []*jobqueue.Job,
) error {
	if len(buriedJobs) > 0 {
		if err := w.submitter.DeleteJobs(buriedJobs); err != nil {
			return err
		}
	}

	if err := deleteOldRunDirs(subDir.Path, keepMtime); err != nil {
		return err
	}

	if err := removeRunRecord(subDir.Path); err != nil {
		return err
	}

	return w.startNewRun(subDir)
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

// fofnMtime returns the mtime of the fofn file in the given subdirectory as a
// Unix timestamp.
func fofnMtime(subDir SubDir) (int64, error) {
	fofnPath := filepath.Join(subDir.Path, fofnFilename)

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

// collectReportNames returns the sorted names of .report files in runDir.
func collectReportNames(runDir string) ([]string, error) {
	entries, err := os.ReadDir(runDir)
	if err != nil {
		return nil, fmt.Errorf("read run dir for digest: %w", err)
	}

	var names []string

	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".report") {
			names = append(names, e.Name())
		}
	}

	slices.Sort(names)

	return names, nil
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

	if err := os.Chown(
		statusPath, -1, gid,
	); err != nil {
		return fmt.Errorf("chown status: %w", err)
	}

	issuesPath := statusPath + issuesSuffix

	if err := os.Chown(
		issuesPath, -1, gid,
	); err != nil && !errors.Is(err, os.ErrNotExist) {
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
		if err := removeIfOldRunDir(
			subDirPath, entry, keepMtime,
		); err != nil {
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
