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
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/VertebrateResequencing/wr/jobqueue"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/ibackup/set"
	"github.com/wtsi-hgi/ibackup/transfer"
	"github.com/wtsi-hgi/ibackup/transformer"
	"pgregory.net/rapid"
)

// filePair represents a local/remote path pair.
type filePair struct {
	Local  string
	Remote string
}

// writeChunkAndReport writes both a chunk file and a complete report file for
// all pairs with status "uploaded".
func writeChunkAndReport(runDir, chunkName string, pairs []filePair) {
	writeChunkFile(runDir, chunkName, pairs)
	writeReportFile(runDir, chunkName, pairs, transfer.RequestStatusUploaded)
}

// makeFilePairs creates n file pairs with sequential
// indices starting from startIdx.
func makeFilePairs(startIdx, endIdx int) []filePair {
	pairs := make([]filePair, endIdx-startIdx)

	for i := range pairs {
		idx := startIdx + i
		pairs[i] = filePair{
			Local:  fmt.Sprintf("/tmp/file/%06d", idx),
			Remote: fmt.Sprintf("/irods/file/%06d", idx),
		}
	}

	return pairs
}

// writeChunkFile writes a chunk file with base64-encoded local/remote pairs.
func writeChunkFile(runDir, chunkName string, pairs []filePair) {
	path := filepath.Join(runDir, chunkName)

	f, err := os.Create(path)
	So(err, ShouldBeNil)

	for _, p := range pairs {
		line := base64.StdEncoding.EncodeToString([]byte(p.Local)) + "\t" +
			base64.StdEncoding.EncodeToString([]byte(p.Remote)) + "\n"

		_, writeErr := f.WriteString(line)
		So(writeErr, ShouldBeNil)
	}

	So(f.Close(), ShouldBeNil)
}

// readChunkPairs decodes a chunk file and returns its file pairs.
func readChunkPairs(chunkPath string) []filePair {
	content, err := os.ReadFile(chunkPath)
	So(err, ShouldBeNil)

	lines := strings.Split(strings.TrimSpace(string(content)), "\n")

	pairs := make([]filePair, 0, len(lines))

	for _, line := range lines {
		if line == "" {
			continue
		}

		local, remote := decodeChunkTestLine(line)
		pairs = append(pairs, filePair{
			Local:  local,
			Remote: remote,
		})
	}

	return pairs
}

// decodeChunkTestLine decodes a base64-encoded chunk
// line into local and remote paths.
func decodeChunkTestLine(line string) (string, string) {
	parts := strings.SplitN(line, "\t", 2)
	So(len(parts), ShouldEqual, 2)

	local, err := base64.StdEncoding.DecodeString(parts[0])
	So(err, ShouldBeNil)

	remote, err := base64.StdEncoding.DecodeString(parts[1])
	So(err, ShouldBeNil)

	return string(local), string(remote)
}

// writeReportFile writes a report file (.report suffix)
// for the given chunk using FormatReportLine.
func writeReportFile(runDir, chunkName string, pairs []filePair, status transfer.RequestStatus) {
	path := filepath.Join(runDir, chunkName+".report")

	f, err := os.Create(path)
	So(err, ShouldBeNil)

	for _, p := range pairs {
		line := formatReportLine(ReportEntry{
			Local:  p.Local,
			Remote: p.Remote,
			Status: status,
		})

		_, writeErr := fmt.Fprintln(f, line)
		So(writeErr, ShouldBeNil)
	}

	So(f.Close(), ShouldBeNil)
}

// mustWriteChunkT writes a chunk file without GoConvey assertions,
// for use inside rapid property checks.
func mustWriteChunkT(t *rapid.T, runDir, chunkName string, pairs []filePair) {
	t.Helper()

	path := filepath.Join(runDir, chunkName)

	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}

	for _, p := range pairs {
		line := base64.StdEncoding.EncodeToString([]byte(p.Local)) + "\t" +
			base64.StdEncoding.EncodeToString([]byte(p.Remote)) + "\n"

		if _, err := f.WriteString(line); err != nil {
			f.Close()
			t.Fatal(err)
		}
	}

	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

// mustWriteReportT writes a report file without GoConvey assertions,
// for use inside rapid property checks.
func mustWriteReportT(t *rapid.T, runDir, chunkName string, pairs []filePair, status transfer.RequestStatus) {
	t.Helper()

	path := filepath.Join(runDir, chunkName+".report")

	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}

	for _, p := range pairs {
		line := formatReportLine(ReportEntry{
			Local:  p.Local,
			Remote: p.Remote,
			Status: status,
		})

		if _, err := fmt.Fprintln(f, line); err != nil {
			f.Close()
			t.Fatal(err)
		}
	}

	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestProcessSubDir(t *testing.T) {
	Convey("ProcessSubDir", t, func() {
		So(transformer.Register("test", `^/tmp/(.*)$`, "/irods/$1"), ShouldBeNil)

		watchDir := t.TempDir()

		Convey("creates run dir and submits jobs for 25 paths", func() {
			paths := generateTmpPaths(25)
			ssubDir := setupSubDir(watchDir, paths, subDirConfig{Transformer: "test"})

			mock := &mockJobSubmitter{}
			cfg := ProcessSubDirConfig{
				MinChunk: 10,
				MaxChunk: 10,
				RandSeed: 1,
			}

			state, err := processSubDir(ssubDir, mock, cfg, 0)
			So(err, ShouldBeNil)

			fofnInfo, statErr := os.Stat(filepath.Join(ssubDir.Path, fofnFilename))
			So(statErr, ShouldBeNil)

			expectedMtime := fofnInfo.ModTime().Unix()
			So(state.Mtime, ShouldEqual, expectedMtime)

			expectedRunDir := filepath.Join(ssubDir.Path, strconv.FormatInt(expectedMtime, 10))
			So(state.RunDir, ShouldEqual, expectedRunDir)

			_, statErr = os.Stat(state.RunDir)
			So(statErr, ShouldBeNil)

			entries, readErr := os.ReadDir(state.RunDir)
			So(readErr, ShouldBeNil)
			So(entries, ShouldHaveLength, 3)

			So(mock.submitted, ShouldHaveLength, 3)

			dirName := filepath.Base(ssubDir.Path)
			expectedRG := fmt.Sprintf("ibackup_fofn_%s_%d", dirName, expectedMtime)
			So(state.RepGroup, ShouldEqual, expectedRG)

			for _, job := range mock.submitted {
				So(job.RepGroup, ShouldEqual, expectedRG)
				So(job.Cwd, ShouldEqual, state.RunDir)
				So(job.Cmd, ShouldContainSubstring, fmt.Sprintf("--fofn '%s'", dirName))
			}
		})

		Convey("includes --no_replace when freeze is true", func() {
			paths := generateTmpPaths(5)
			ssubDir := setupSubDir(watchDir, paths, subDirConfig{Transformer: "test", Freeze: true})

			mock := &mockJobSubmitter{}
			cfg := ProcessSubDirConfig{
				MinChunk: 10,
				MaxChunk: 10,
				RandSeed: 1,
			}

			_, err := processSubDir(ssubDir, mock, cfg, 0)
			So(err, ShouldBeNil)
			So(mock.submitted, ShouldNotBeEmpty)

			for _, job := range mock.submitted {
				So(job.Cmd, ShouldContainSubstring, "--no_replace")
			}
		})

		Convey("returns error when config.yml is missing", func() {
			sd := writeFofn(watchDir, generateTmpPaths(5), subDirConfig{Transformer: "test"})
			sd = subDirWithMtime(sd)
			mock := &mockJobSubmitter{}
			cfg := ProcessSubDirConfig{
				MinChunk: 10,
				MaxChunk: 10,
				RandSeed: 1,
			}

			So(os.Remove(filepath.Join(sd.Path, configFilename)), ShouldBeNil)

			_, err := processSubDir(sd, mock, cfg, 0)
			So(err, ShouldNotBeNil)
			So(mock.submitted, ShouldBeEmpty)
		})

		Convey("returns zero state for empty fofn", func() {
			ssubDir := setupSubDir(watchDir, nil, subDirConfig{Transformer: "test"})

			mock := &mockJobSubmitter{}
			cfg := ProcessSubDirConfig{
				MinChunk: 10,
				MaxChunk: 10,
				RandSeed: 1,
			}

			state, err := processSubDir(ssubDir, mock, cfg, 0)
			So(err, ShouldBeNil)
			So(state, ShouldResemble, runState{})
			So(mock.submitted, ShouldBeEmpty)

			entries, readErr := os.ReadDir(ssubDir.Path)
			So(readErr, ShouldBeNil)

			for _, e := range entries {
				So(e.IsDir(), ShouldBeFalse)
			}
		})

		Convey("includes --meta when config has metadata", func() {
			paths := generateTmpPaths(5)
			ssubDir := setupSubDir(
				watchDir, paths,
				subDirConfig{Transformer: "test", Metadata: map[string]string{"colour": "red"}},
			)

			mock := &mockJobSubmitter{}
			cfg := ProcessSubDirConfig{
				MinChunk: 10,
				MaxChunk: 10,
				RandSeed: 1,
			}

			_, err := processSubDir(ssubDir, mock, cfg, 0)
			So(err, ShouldBeNil)
			So(mock.submitted, ShouldNotBeEmpty)

			for _, job := range mock.submitted {
				So(job.Cmd, ShouldContainSubstring, `--meta 'colour=red'`)
			}
		})

		Convey("omits --meta when config has no metadata", func() {
			paths := generateTmpPaths(5)
			ssubDir := setupSubDir(watchDir, paths, subDirConfig{Transformer: "test"})

			mock := &mockJobSubmitter{}
			cfg := ProcessSubDirConfig{
				MinChunk: 10,
				MaxChunk: 10,
				RandSeed: 1,
			}

			_, err := processSubDir(ssubDir, mock, cfg, 0)
			So(err, ShouldBeNil)
			So(mock.submitted, ShouldNotBeEmpty)

			for _, job := range mock.submitted {
				So(job.Cmd, ShouldNotContainSubstring, "--meta")
			}
		})

		Convey("creates 100 chunks and 100 jobs for 50000 paths with default bounds", func() {
			paths := generateTmpPaths(50000)
			ssubDir := setupSubDir(watchDir, paths, subDirConfig{Transformer: "test"})

			mock := &mockJobSubmitter{}
			cfg := ProcessSubDirConfig{
				MinChunk: 250,
				MaxChunk: 10000,
				RandSeed: 1,
			}

			state, err := processSubDir(ssubDir, mock, cfg, 0)
			So(err, ShouldBeNil)
			So(state.RunDir, ShouldNotBeEmpty)

			entries, readErr := os.ReadDir(state.RunDir)
			So(readErr, ShouldBeNil)
			So(entries, ShouldHaveLength, 100)

			So(mock.submitted, ShouldHaveLength, 100)
		})
	})
}

func TestGenerateStatus(t *testing.T) {
	Convey("GenerateStatus", t, func() {
		watchDir := t.TempDir()

		Convey("writes status file for 3 complete reports", func() {
			runDir := setupRunDir(watchDir, "proj1")

			writeChunkAndReport(runDir, "chunk.000000", makeFilePairs(0, 5))
			writeChunkAndReport(runDir, "chunk.000001", makeFilePairs(5, 10))
			writeChunkAndReport(runDir, "chunk.000002", makeFilePairs(10, 15))

			err := generateStatus(runDir, nil)
			So(err, ShouldBeNil)

			statusPath := filepath.Join(runDir, statusFilename)
			entries, counts, parseErr := parseStatus(statusPath)
			So(parseErr, ShouldBeNil)
			So(entries, ShouldHaveLength, 15)
			So(counts.Uploaded, ShouldEqual, 15)
		})

		Convey("handles buried chunk with no report file", func() {
			runDir := setupRunDir(watchDir, "proj2")

			writeChunkAndReport(runDir, "chunk.000000", makeFilePairs(0, 5))
			writeChunkAndReport(runDir, "chunk.000001", makeFilePairs(5, 10))
			writeChunkFile(runDir, "chunk.000002", makeFilePairs(10, 20))

			err := generateStatus(
				runDir,
				[]string{"chunk.000002"},
			)
			So(err, ShouldBeNil)

			statusPath := filepath.Join(runDir, statusFilename)
			entries, counts, parseErr := parseStatus(statusPath)
			So(parseErr, ShouldBeNil)
			So(entries, ShouldHaveLength, 20)
			So(counts.Uploaded, ShouldEqual, 10)
			So(counts.NotProcessed, ShouldEqual, 10)
		})

		Convey("handles buried chunk with incomplete report", func() {
			runDir := setupRunDir(watchDir, "proj3")

			pairs := makeFilePairs(0, 10)
			writeChunkFile(runDir, "chunk.000000", pairs)
			writeReportFile(runDir, "chunk.000000", pairs[:5], transfer.RequestStatusUploaded)

			err := generateStatus(
				runDir,
				[]string{"chunk.000000"},
			)
			So(err, ShouldBeNil)

			statusPath := filepath.Join(runDir, statusFilename)
			entries, counts, parseErr := parseStatus(statusPath)
			So(parseErr, ShouldBeNil)
			So(entries, ShouldHaveLength, 10)
			So(counts.Uploaded, ShouldEqual, 5)
			So(counts.NotProcessed, ShouldEqual, 5)
		})
	})
}

// setupRunDir creates a watch directory, subdirectory,
// and run directory; returns the run path.
func setupRunDir(
	watchDir, name string,
) string {
	subPath := filepath.Join(watchDir, name)
	So(os.MkdirAll(subPath, dirMode), ShouldBeNil)

	runDir := filepath.Join(subPath, "12345")
	So(os.MkdirAll(runDir, dirMode), ShouldBeNil)

	return runDir
}

func TestWatcherPoll(t *testing.T) {
	Convey("Watcher.Poll", t, func() {
		So(transformer.Register("test", `^/tmp/(.*)$`, "/irods/$1"), ShouldBeNil)

		watchDir := t.TempDir()
		cfg := ProcessSubDirConfig{
			MinChunk: 10,
			MaxChunk: 10,
			RandSeed: 1,
		}

		Convey("first poll submits jobs and creates run dir", func() {
			paths := generateTmpPaths(25)
			ssubDir := setupSubDir(watchDir, paths, subDirConfig{Transformer: "test"})

			mock := &mockJobSubmitter{}

			w, err := NewWatcher(watchDir, mock, cfg)
			So(err, ShouldBeNil)

			err = w.poll()
			So(err, ShouldBeNil)
			So(mock.submitted, ShouldHaveLength, 3)

			// Run dir should exist with the fofn mtime as name
			runScan, findErr := scanRunDirs(ssubDir.Path)
			So(findErr, ShouldBeNil)
			So(runScan.found, ShouldBeTrue)
			So(runScan.runMtime, ShouldEqual, ssubDir.FofnMtime)
			So(runScan.runDir, ShouldNotBeEmpty)
		})

		Convey("skips when active run has incomplete jobs", func() {
			paths := generateTmpPaths(25)
			ssubDir := setupSubDir(watchDir, paths, subDirConfig{Transformer: "test"})

			mock := &mockJobSubmitter{}

			w, err := NewWatcher(watchDir, mock, cfg)
			So(err, ShouldBeNil)

			err = w.poll()
			So(err, ShouldBeNil)

			submitCount := len(mock.submitted)
			repGroup := makeRepGroup(ssubDir.Path, ssubDir.FofnMtime)

			mock.allJobs = []*jobqueue.Job{
				{RepGroup: repGroup, Cmd: "running"},
			}

			err = w.poll()
			So(err, ShouldBeNil)
			So(mock.submitted, ShouldHaveLength, submitCount)
		})

		Convey("completes successful run and starts new run when fofn changed", func() {
			paths := generateTmpPaths(25)
			ssubDir := setupSubDir(watchDir, paths, subDirConfig{Transformer: "test"})

			mock := &mockJobSubmitter{}

			w, err := NewWatcher(watchDir, mock, cfg)
			So(err, ShouldBeNil)

			err = w.poll()
			So(err, ShouldBeNil)

			firstCount := len(mock.submitted)
			runScan, findErr := scanRunDirs(ssubDir.Path)
			So(findErr, ShouldBeNil)
			So(runScan.found, ShouldBeTrue)

			runDir := runScan.runDir
			runMtime := runScan.runMtime

			writeReportsForChunks(runDir)

			mock.allJobs = nil

			updateFofnMtime(watchDir, generateTmpPaths(15), runMtime+1000)

			err = w.poll()
			So(err, ShouldBeNil)

			statusPath := filepath.Join(runDir, statusFilename)
			_, statErr := os.Stat(statusPath)
			So(statErr, ShouldBeNil)

			symlinkPath := filepath.Join(ssubDir.Path, statusFilename)
			_, readErr := os.Readlink(symlinkPath)
			So(readErr, ShouldBeNil)

			So(len(mock.submitted), ShouldBeGreaterThan, firstCount)

			newRunScan, newFindErr := scanRunDirs(ssubDir.Path)
			So(newFindErr, ShouldBeNil)
			So(newRunScan.found, ShouldBeTrue)
			So(newRunScan.runDir, ShouldNotEqual, runDir)
			So(newRunScan.runMtime, ShouldEqual, runMtime+1000)
		})

		Convey("completes successful run with no new run when fofn unchanged", func() {
			paths := generateTmpPaths(25)
			ssubDir := setupSubDir(watchDir, paths, subDirConfig{Transformer: "test"})

			mock := &mockJobSubmitter{}

			w, err := NewWatcher(watchDir, mock, cfg)
			So(err, ShouldBeNil)

			err = w.poll()
			So(err, ShouldBeNil)

			submitCount := len(mock.submitted)
			runScan, findErr := scanRunDirs(ssubDir.Path)
			So(findErr, ShouldBeNil)
			So(runScan.found, ShouldBeTrue)

			runDir := runScan.runDir

			writeReportsForChunks(runDir)

			mock.allJobs = nil

			err = w.poll()
			So(err, ShouldBeNil)

			statusPath := filepath.Join(runDir, statusFilename)
			_, statErr := os.Stat(statusPath)
			So(statErr, ShouldBeNil)

			So(mock.submitted, ShouldHaveLength, submitCount)
		})

		Convey("generates not_processed status for buried chunk when fofn unchanged", func() {
			paths := generateTmpPaths(25)
			ssubDir := setupSubDir(watchDir, paths, subDirConfig{Transformer: "test"})

			mock := &mockJobSubmitter{}

			w, err := NewWatcher(watchDir, mock, cfg)
			So(err, ShouldBeNil)

			err = w.poll()
			So(err, ShouldBeNil)

			submitCount := len(mock.submitted)
			runScan, findErr := scanRunDirs(ssubDir.Path)
			So(findErr, ShouldBeNil)
			So(runScan.found, ShouldBeTrue)

			runDir := runScan.runDir

			repGroup := makeRepGroup(ssubDir.Path, ssubDir.FofnMtime)

			buriedPairs := readChunkPairs(filepath.Join(runDir, "chunk.000002"))
			buriedCount := len(buriedPairs)
			uploadedCount := 25 - buriedCount

			writeReportsExcept(runDir, "chunk.000002")

			mock.allJobs = []*jobqueue.Job{
				{
					RepGroup: repGroup,
					State:    jobqueue.JobStateBuried,
					Cmd:      "ibackup put -f chunk.000002",
					EndTime:  time.Now(),
				},
			}

			err = w.poll()
			So(err, ShouldBeNil)

			statusPath := filepath.Join(runDir, statusFilename)
			entries, counts, parseErr := parseStatus(statusPath)
			So(parseErr, ShouldBeNil)
			So(entries, ShouldHaveLength, 25)
			So(counts.Uploaded, ShouldEqual, uploadedCount)
			So(counts.NotProcessed, ShouldEqual, buriedCount)

			So(mock.submitted, ShouldHaveLength, submitCount)
		})

		Convey("does not regenerate status repeatedly for unchanged buried run", func() {
			paths := generateTmpPaths(25)
			ssubDir := setupSubDir(watchDir, paths, subDirConfig{Transformer: "test"})

			mock := &mockJobSubmitter{}

			w, err := NewWatcher(watchDir, mock, cfg)
			So(err, ShouldBeNil)

			err = w.poll()
			So(err, ShouldBeNil)

			runScan, findErr := scanRunDirs(ssubDir.Path)
			So(findErr, ShouldBeNil)
			So(runScan.found, ShouldBeTrue)

			runDir := runScan.runDir

			repGroup := makeRepGroup(ssubDir.Path, ssubDir.FofnMtime)

			writeReportsExcept(runDir, "chunk.000002")

			// Buried job with EndTime in the past.
			buriedTime := time.Now().Add(-time.Hour)
			mock.allJobs = []*jobqueue.Job{{
				RepGroup: repGroup,
				State:    jobqueue.JobStateBuried,
				Cmd:      "ibackup put -f chunk.000002",
				EndTime:  buriedTime,
			}}

			err = w.poll()
			So(err, ShouldBeNil)

			statusPath := filepath.Join(runDir, statusFilename)
			knownTime := time.Unix(900, 0)
			So(os.Chtimes(statusPath, knownTime, knownTime), ShouldBeNil)

			// Third poll: same buried state, same LastCompletedTime.
			// Status mtime is now in the future relative to buriedTime,
			// so needsStatusRegen returns false → no regen.
			futureTime := time.Now().Add(time.Hour)
			So(os.Chtimes(statusPath, futureTime, futureTime), ShouldBeNil)

			err = w.poll()
			So(err, ShouldBeNil)

			statusInfoAfter, statErr := os.Stat(statusPath)
			So(statErr, ShouldBeNil)
			So(statusInfoAfter.ModTime().Unix(), ShouldEqual, futureTime.Unix())

			// Buried runs still have symlink — since wr is queried every
			// poll cycle, buried-then-retried chunks are detected naturally.
			target, readErr := os.Readlink(filepath.Join(ssubDir.Path, statusFilename))
			So(readErr, ShouldBeNil)
			So(target, ShouldEqual, filepath.Join(filepath.Base(runDir), statusFilename))
		})

		Convey("deletes buried jobs and starts new run when fofn changed", func() {
			paths := generateTmpPaths(25)
			ssubDir := setupSubDir(watchDir, paths, subDirConfig{Transformer: "test"})

			mock := &mockJobSubmitter{}

			w, err := NewWatcher(watchDir, mock, cfg)
			So(err, ShouldBeNil)

			err = w.poll()
			So(err, ShouldBeNil)

			firstCount := len(mock.submitted)
			runScan, findErr := scanRunDirs(ssubDir.Path)
			So(findErr, ShouldBeNil)
			So(runScan.found, ShouldBeTrue)

			runDir := runScan.runDir
			runMtime := runScan.runMtime

			repGroup := makeRepGroup(ssubDir.Path, runMtime)

			writeReportsExcept(runDir, "chunk.000002")

			buriedJob := &jobqueue.Job{
				RepGroup: repGroup,
				State:    jobqueue.JobStateBuried,
				Cmd:      "ibackup put -f chunk.000002",
			}
			mock.allJobs = []*jobqueue.Job{buriedJob}

			updateFofnMtime(watchDir, generateTmpPaths(15), runMtime+1000)

			err = w.poll()
			So(err, ShouldBeNil)

			statusPath := filepath.Join(runDir, statusFilename)
			_, statErr := os.Stat(statusPath)
			So(statErr, ShouldBeNil)

			So(mock.deleted, ShouldNotBeEmpty)

			So(len(mock.submitted), ShouldBeGreaterThan, firstCount)

			newRunScan, newFindErr := scanRunDirs(ssubDir.Path)
			So(newFindErr, ShouldBeNil)
			So(newRunScan.found, ShouldBeTrue)
			So(newRunScan.runMtime, ShouldEqual, runMtime+1000)
			So(newRunScan.runDir, ShouldNotEqual, runDir)
		})

		Convey("deletes old run directories on successful completion", func() {
			paths := generateTmpPaths(25)
			ssubDir := setupSubDir(watchDir, paths, subDirConfig{Transformer: "test"})

			mock := &mockJobSubmitter{}

			w, err := NewWatcher(watchDir, mock, cfg)
			So(err, ShouldBeNil)

			err = w.poll()
			So(err, ShouldBeNil)

			runScan, findErr := scanRunDirs(ssubDir.Path)
			So(findErr, ShouldBeNil)
			So(runScan.found, ShouldBeTrue)

			runDir := runScan.runDir

			oldRunDir := filepath.Join(ssubDir.Path, "500")
			So(os.MkdirAll(oldRunDir, dirMode), ShouldBeNil)

			writeReportsForChunks(runDir)

			mock.allJobs = nil

			err = w.poll()
			So(err, ShouldBeNil)

			_, statErr := os.Stat(oldRunDir)
			So(os.IsNotExist(statErr), ShouldBeTrue)

			_, statErr = os.Stat(runDir)
			So(statErr, ShouldBeNil)
		})

		Convey("updates status symlink after second run completes", func() {
			paths := generateTmpPaths(25)
			ssubDir := setupSubDir(watchDir, paths, subDirConfig{Transformer: "test"})
			mock := &mockJobSubmitter{}

			w, err := NewWatcher(watchDir, mock, cfg)
			So(err, ShouldBeNil)

			So(w.poll(), ShouldBeNil)

			firstRunScan, findErr := scanRunDirs(ssubDir.Path)
			So(findErr, ShouldBeNil)
			So(firstRunScan.found, ShouldBeTrue)

			firstRunDir := firstRunScan.runDir
			firstMtime := firstRunScan.runMtime

			writeReportsForChunks(firstRunDir)

			mock.allJobs = nil

			updateFofnMtime(watchDir, paths[:15], firstMtime+1000)

			So(w.poll(), ShouldBeNil)

			secondRunScan, findErr2 := scanRunDirs(ssubDir.Path)
			So(findErr2, ShouldBeNil)
			So(secondRunScan.found, ShouldBeTrue)

			secondRunDir := secondRunScan.runDir

			writeReportsForChunks(secondRunDir)

			mock.allJobs = nil
			mock.completionMap = map[string]time.Time{
				makeRepGroup(ssubDir.Path, firstMtime+1000): time.Now(),
			}

			So(w.poll(), ShouldBeNil)

			statusPath := filepath.Join(secondRunDir, statusFilename)
			entries, counts, parseErr := parseStatus(statusPath)
			So(parseErr, ShouldBeNil)
			So(entries, ShouldHaveLength, 15)
			So(counts.Uploaded, ShouldEqual, 15)

			symlinkPath := filepath.Join(ssubDir.Path, statusFilename)
			target, readErr := os.Readlink(symlinkPath)
			So(readErr, ShouldBeNil)

			expectedTarget := filepath.Join(filepath.Base(secondRunDir), statusFilename)
			So(target, ShouldEqual, expectedTarget)
		})

		Convey("jobs are created with the correct unix group set", func() {
			paths := generateTmpPaths(25)
			cfg.RunConfig.Group = "some-group"

			setupSubDir(watchDir, paths, subDirConfig{Transformer: "test"})

			mock := &mockJobSubmitter{}

			w, err := NewWatcher(watchDir, mock, cfg)
			So(err, ShouldBeNil)

			err = w.poll()
			So(err, ShouldBeNil)
			So(mock.submitted, ShouldHaveLength, 3)
			So(mock.submitted[0].Group, ShouldEqual, "some-group")
		})
	})
}

func TestWatcherRestart(t *testing.T) {
	Convey("Watcher restart resilience", t, func() {
		So(transformer.Register("test", `^/tmp/(.*)$`, "/irods/$1"), ShouldBeNil)

		watchDir := t.TempDir()
		cfg := ProcessSubDirConfig{
			MinChunk: 10,
			MaxChunk: 10,
			RandSeed: 1,
		}

		Convey("detects existing run with incomplete jobs and does not submit new jobs", func() {
			subPath := filepath.Join(watchDir, "proj")
			So(os.MkdirAll(subPath, dirMode), ShouldBeNil)

			fofnPath := writeFofn(subPath, generateTmpPaths(10)).FOFNPath()
			fofnTime := time.Unix(1000, 0)
			So(os.Chtimes(fofnPath, fofnTime, fofnTime), ShouldBeNil)

			So(writeConfig(subPath, subDirConfig{Transformer: "test"}), ShouldBeNil)

			runDir := filepath.Join(subPath, "1000")
			So(os.MkdirAll(runDir, dirMode), ShouldBeNil)

			writeChunkFile(runDir, "chunk.000000", makeFilePairs(0, 10))

			mock := &mockJobSubmitter{
				allJobs: []*jobqueue.Job{
					{
						RepGroup: "ibackup_fofn_proj_1000",
						Cmd:      "running",
					},
				},
			}

			w, err := NewWatcher(watchDir, mock, cfg)
			So(err, ShouldBeNil)

			err = w.poll()
			So(err, ShouldBeNil)
			So(mock.submitted, ShouldBeEmpty)

			// Run dir unchanged
			runScan, findErr := scanRunDirs(subPath)
			So(findErr, ShouldBeNil)
			So(runScan.found, ShouldBeTrue)
			So(runScan.runMtime, ShouldEqual, 1000)
		})

		Convey("returns error when completion status cannot be queried", func() {
			subPath := filepath.Join(watchDir, "proj")
			So(os.MkdirAll(subPath, dirMode), ShouldBeNil)

			writeFofn(subPath, generateTmpPaths(10), subDirConfig{Transformer: "test"})

			runDir := filepath.Join(subPath, "1000")
			So(os.MkdirAll(runDir, dirMode), ShouldBeNil)
			writeChunkFile(runDir, "chunk.000000", makeFilePairs(0, 10))

			mock := &mockJobSubmitter{allJobsErr: errTest}

			w, err := NewWatcher(watchDir, mock, cfg)
			So(err, ShouldBeNil)

			err = w.poll()
			So(err, ShouldNotBeNil)
			So(mock.submitted, ShouldBeEmpty)
		})

		Convey("detects completed existing run and generates status file", func() {
			fofnPath := writeFofn(watchDir, generateTmpPaths(10), subDirConfig{Transformer: "test"}).FOFNPath()
			subPath := filepath.Dir(fofnPath)

			fofnTime := time.Unix(1000, 0)
			So(os.Chtimes(fofnPath, fofnTime, fofnTime), ShouldBeNil)

			runDir := filepath.Join(subPath, "1000")
			So(os.MkdirAll(runDir, dirMode), ShouldBeNil)

			writeChunkAndReport(runDir, "chunk.000000", makeFilePairs(0, 10))

			// No status file yet — active run with all jobs complete.
			mock := &mockJobSubmitter{}

			w, err := NewWatcher(watchDir, mock, cfg)
			So(err, ShouldBeNil)

			err = w.poll()
			So(err, ShouldBeNil)

			statusPath := filepath.Join(runDir, statusFilename)
			_, statErr := os.Stat(statusPath)
			So(statErr, ShouldBeNil)

			symlinkPath := filepath.Join(subPath, statusFilename)
			target, readErr := os.Readlink(symlinkPath)
			So(readErr, ShouldBeNil)
			So(target, ShouldEqual, filepath.Join(filepath.Base(runDir), statusFilename))

			So(mock.submitted, ShouldBeEmpty)
		})

		Convey("regenerates status when symlink is missing", func() {
			fofnPath := writeFofn(watchDir, generateTmpPaths(10), subDirConfig{Transformer: "test"}).FOFNPath()
			fofnTime := time.Unix(1000, 0)

			So(os.Chtimes(fofnPath, fofnTime, fofnTime), ShouldBeNil)

			subPath := filepath.Dir(fofnPath)

			runDir := filepath.Join(subPath, "1000")
			So(os.MkdirAll(runDir, dirMode), ShouldBeNil)

			writeChunkAndReport(runDir, "chunk.000000", makeFilePairs(0, 10))
			generateDoneStatus(runDir, subDir{Path: subPath})

			symlinkPath := filepath.Join(subPath, statusFilename)
			So(os.Remove(symlinkPath), ShouldBeNil)

			mock := &mockJobSubmitter{}

			w, err := NewWatcher(watchDir, mock, cfg)
			So(err, ShouldBeNil)

			err = w.poll()
			So(err, ShouldBeNil)
			So(mock.submitted, ShouldBeEmpty)

			target, readErr := os.Readlink(symlinkPath)
			So(readErr, ShouldBeNil)
			So(target, ShouldEqual, filepath.Join(filepath.Base(runDir), statusFilename))
		})

		Convey("regenerates status when symlink points to wrong status file", func() {
			fofnPath := writeFofn(watchDir, generateTmpPaths(10), subDirConfig{Transformer: "test"}).FOFNPath()
			fofnTime := time.Unix(1000, 0)

			So(os.Chtimes(fofnPath, fofnTime, fofnTime), ShouldBeNil)

			subPath := filepath.Dir(fofnPath)

			runDir := filepath.Join(subPath, "1000")
			So(os.MkdirAll(runDir, dirMode), ShouldBeNil)

			writeChunkAndReport(runDir, "chunk.000000", makeFilePairs(0, 10))
			generateDoneStatus(runDir, subDir{Path: subPath})

			wrongRunDir := filepath.Join(subPath, "900")
			So(os.MkdirAll(wrongRunDir, dirMode), ShouldBeNil)
			wrongStatusPath := filepath.Join(wrongRunDir, statusFilename)
			So(os.WriteFile(wrongStatusPath, []byte("wrong"), 0600), ShouldBeNil)

			symlinkPath := filepath.Join(subPath, statusFilename)
			So(os.Remove(symlinkPath), ShouldBeNil)
			So(os.Symlink(wrongStatusPath, symlinkPath), ShouldBeNil)

			mock := &mockJobSubmitter{}

			w, err := NewWatcher(watchDir, mock, cfg)
			So(err, ShouldBeNil)

			err = w.poll()
			So(err, ShouldBeNil)
			So(mock.submitted, ShouldBeEmpty)

			target, readErr := os.Readlink(symlinkPath)
			So(readErr, ShouldBeNil)
			So(target, ShouldEqual, filepath.Join(filepath.Base(runDir), statusFilename))
		})

		Convey("does not rewrite status artefacts for same fofn mtime after completion", func() {
			fofnPath := writeFofn(watchDir, generateTmpPaths(10), subDirConfig{Transformer: "test"}).FOFNPath()

			fofnTime := time.Unix(1000, 0)
			So(os.Chtimes(fofnPath, fofnTime, fofnTime), ShouldBeNil)

			subPath := filepath.Dir(fofnPath)

			runDir := filepath.Join(subPath, "1000")
			So(os.MkdirAll(runDir, dirMode), ShouldBeNil)

			writeChunkAndReport(runDir, "chunk.000000", makeFilePairs(0, 10))
			generateDoneStatus(runDir, subDir{Path: subPath})

			statusPath := filepath.Join(runDir, statusFilename)
			knownTime := time.Unix(900, 0)
			So(os.Chtimes(statusPath, knownTime, knownTime), ShouldBeNil)

			statusInfoBefore, statErr := os.Stat(statusPath)
			So(statErr, ShouldBeNil)
			So(statusInfoBefore.ModTime(), ShouldEqual, knownTime)

			symlinkPath := filepath.Join(subPath, statusFilename)
			symlinkInfoBefore, lstatErr := os.Lstat(symlinkPath)
			So(lstatErr, ShouldBeNil)

			beforeStat, ok := symlinkInfoBefore.Sys().(*syscall.Stat_t)
			So(ok, ShouldBeTrue)

			mock := &mockJobSubmitter{}
			So(mock.submitted, ShouldBeEmpty)

			w, err := NewWatcher(watchDir, mock, cfg)
			So(err, ShouldBeNil)

			err = w.poll()
			So(err, ShouldBeNil)

			statusInfoAfter, statErr := os.Stat(statusPath)
			So(statErr, ShouldBeNil)

			symlinkInfoAfter, lstatErr := os.Lstat(symlinkPath)
			So(lstatErr, ShouldBeNil)

			afterStat, ok := symlinkInfoAfter.Sys().(*syscall.Stat_t)
			So(ok, ShouldBeTrue)

			So(statusInfoAfter.ModTime(), ShouldEqual, statusInfoBefore.ModTime())
			So(afterStat.Ino, ShouldEqual, beforeStat.Ino)

			target, readErr := os.Readlink(symlinkPath)
			So(readErr, ShouldBeNil)
			So(target, ShouldEqual, filepath.Join(filepath.Base(runDir), statusFilename))
		})

		Convey("does not regenerate status for externally modified reports in stable done run", func() {
			fofnPath := writeFofn(watchDir, generateTmpPaths(10), subDirConfig{Transformer: "test"}).FOFNPath()
			fofnTime := time.Unix(1000, 0)

			So(os.Chtimes(fofnPath, fofnTime, fofnTime), ShouldBeNil)

			subPath := filepath.Dir(fofnPath)

			runDir := filepath.Join(subPath, "1000")
			So(os.MkdirAll(runDir, dirMode), ShouldBeNil)

			pairs := makeFilePairs(0, 10)
			writeChunkAndReport(runDir, "chunk.000000", pairs)
			generateDoneStatus(runDir, subDir{Path: subPath})

			// Externally overwrite the report with different statuses.
			writeReportFile(runDir, "chunk.000000", pairs, transfer.RequestStatusMissing)

			statusPath := filepath.Join(runDir, statusFilename)
			knownTime := time.Unix(900, 0)
			So(os.Chtimes(statusPath, knownTime, knownTime), ShouldBeNil)

			mock := &mockJobSubmitter{}

			w, err := NewWatcher(watchDir, mock, cfg)
			So(err, ShouldBeNil)

			err = w.poll()
			So(err, ShouldBeNil)
			So(mock.submitted, ShouldBeEmpty)

			// Status file was NOT regenerated — known mtime preserved.
			statusInfo, statErr := os.Stat(statusPath)
			So(statErr, ShouldBeNil)
			So(statusInfo.ModTime(), ShouldEqual, knownTime)

			// Stale status still shows original uploaded counts.
			_, counts, parseErr := parseStatus(statusPath)
			So(parseErr, ShouldBeNil)
			So(counts.Uploaded, ShouldEqual, 10)
			So(counts.Missing, ShouldEqual, 0)
		})

		Convey("cleans stale run directories during phase transition", func() {
			fofnPath := writeFofn(watchDir, generateTmpPaths(10), subDirConfig{Transformer: "test"}).FOFNPath()
			fofnTime := time.Unix(1000, 0)

			So(os.Chtimes(fofnPath, fofnTime, fofnTime), ShouldBeNil)

			subPath := filepath.Dir(fofnPath)

			runDir := filepath.Join(subPath, "1000")
			So(os.MkdirAll(runDir, dirMode), ShouldBeNil)

			oldRunDir := filepath.Join(subPath, "500")
			So(os.MkdirAll(oldRunDir, dirMode), ShouldBeNil)

			writeChunkAndReport(runDir, "chunk.000000", makeFilePairs(0, 10))

			// No status file — active run. wr returns no jobs → complete.
			mock := &mockJobSubmitter{}

			w, err := NewWatcher(watchDir, mock, cfg)
			So(err, ShouldBeNil)

			err = w.poll()
			So(err, ShouldBeNil)
			So(mock.submitted, ShouldBeEmpty)

			// status was generated (transition to done), old dir cleaned
			_, statErr := os.Stat(oldRunDir)
			So(os.IsNotExist(statErr), ShouldBeTrue)
			_, statErr = os.Stat(runDir)
			So(statErr, ShouldBeNil)
		})

		Convey("returns error when stale-run cleanup fails during phase transition", func() {
			fofnPath := writeFofn(watchDir, generateTmpPaths(10), subDirConfig{Transformer: "test"}).FOFNPath()
			fofnTime := time.Unix(1000, 0)

			So(os.Chtimes(fofnPath, fofnTime, fofnTime), ShouldBeNil)

			subPath := filepath.Dir(fofnPath)

			runDir := filepath.Join(subPath, "1000")
			So(os.MkdirAll(runDir, dirMode), ShouldBeNil)

			// Create old run dir with undeletable content so
			// os.RemoveAll fails on the nested directory.
			oldRunDir := filepath.Join(subPath, "500")
			nestedDir := filepath.Join(oldRunDir, "nested")
			So(os.MkdirAll(nestedDir, dirMode), ShouldBeNil)
			So(os.WriteFile(filepath.Join(nestedDir, "file"), []byte("x"), 0600), ShouldBeNil)
			So(os.Chmod(nestedDir, 0550), ShouldBeNil)

			defer func() {
				restoreErr := os.Chmod(nestedDir, dirMode)
				So(restoreErr, ShouldBeNil)
			}()

			writeChunkAndReport(runDir, "chunk.000000", makeFilePairs(0, 10))

			// No status file — active run. wr returns no jobs → complete.
			mock := &mockJobSubmitter{}

			w, err := NewWatcher(watchDir, mock, cfg)
			So(err, ShouldBeNil)

			err = w.poll()
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "remove old run dir")

			_, statErr := os.Stat(oldRunDir)
			So(statErr, ShouldBeNil)
		})

		Convey("processes updated fofn when completed status artefacts already exist", func() {
			fofnPath := writeFofn(watchDir, generateTmpPaths(10), subDirConfig{Transformer: "test"}).FOFNPath()
			fofnTime := time.Unix(1000, 0)
			So(os.Chtimes(fofnPath, fofnTime, fofnTime), ShouldBeNil)

			subPath := filepath.Dir(fofnPath)

			runDir := filepath.Join(subPath, "1000")
			So(os.MkdirAll(runDir, dirMode), ShouldBeNil)

			writeChunkAndReport(runDir, "chunk.000000", makeFilePairs(0, 10))
			generateDoneStatus(runDir, subDir{Path: subPath})

			mock := &mockJobSubmitter{}

			w, err := NewWatcher(watchDir, mock, cfg)
			So(err, ShouldBeNil)

			updateFofnMtime(watchDir, generateTmpPaths(15), 2000)

			err = w.poll()
			So(err, ShouldBeNil)
			So(mock.submitted, ShouldNotBeEmpty)

			// New run dir created for mtime 2000
			newRunScan, findErr := scanRunDirs(subPath)
			So(findErr, ShouldBeNil)
			So(newRunScan.found, ShouldBeTrue)
			So(newRunScan.runMtime, ShouldEqual, 2000)
		})

		Convey("refreshes status after buried chunk is retried successfully", func() {
			fofnPath := writeFofn(watchDir, generateTmpPaths(10), subDirConfig{Transformer: "test"}).FOFNPath()
			fofnTime := time.Unix(1000, 0)
			So(os.Chtimes(fofnPath, fofnTime, fofnTime), ShouldBeNil)

			subPath := filepath.Dir(fofnPath)

			runDir := filepath.Join(subPath, "1000")
			So(os.MkdirAll(runDir, dirMode), ShouldBeNil)

			writeChunkAndReport(runDir, "chunk.000000", makeFilePairs(0, 5))
			writeChunkFile(runDir, "chunk.000001", makeFilePairs(5, 10))

			So(generateStatus(runDir, []string{"chunk.000001"}), ShouldBeNil)

			statusPath := filepath.Join(runDir, statusFilename)
			_, initialCounts, parseErr := parseStatus(statusPath)
			So(parseErr, ShouldBeNil)
			So(initialCounts.Uploaded, ShouldEqual, 5)
			So(initialCounts.NotProcessed, ShouldEqual, 5)

			// Simulate retry: write the previously missing report
			writeReportFile(runDir, "chunk.000001", makeFilePairs(5, 10), transfer.RequestStatusUploaded)

			// mock returns no buried jobs (retry succeeded), but completion lookup
			// reports a completion time after status mtime to trigger regen.
			mock := &mockJobSubmitter{
				completionMap: map[string]time.Time{
					"ibackup_fofn_" + new(set.Set).ID() + "_1000": time.Now().Add(time.Hour),
				},
			}

			w, err := NewWatcher(watchDir, mock, cfg)
			So(err, ShouldBeNil)

			So(w.poll(), ShouldBeNil)
			So(mock.submitted, ShouldBeEmpty)

			_, counts, parseErr := parseStatus(statusPath)
			So(parseErr, ShouldBeNil)
			So(counts.Uploaded, ShouldEqual, 10)
			So(counts.NotProcessed, ShouldEqual, 0)
		})

		Convey("done run submits no new jobs", func() {
			subPath := filepath.Join(watchDir, "proj")
			So(os.MkdirAll(subPath, dirMode), ShouldBeNil)

			fofnPath := writeFofn(subPath, generateTmpPaths(10)).FOFNPath()
			fofnTime := time.Unix(1000, 0)
			So(os.Chtimes(fofnPath, fofnTime, fofnTime), ShouldBeNil)

			So(writeConfig(subPath, subDirConfig{Transformer: "test"}), ShouldBeNil)

			runDir := filepath.Join(subPath, "1000")
			So(os.MkdirAll(runDir, dirMode), ShouldBeNil)

			writeChunkAndReport(runDir, "chunk.000000", makeFilePairs(0, 10))
			generateDoneStatus(runDir, subDir{Path: subPath})

			mock := &mockJobSubmitter{}

			w, err := NewWatcher(watchDir, mock, cfg)
			So(err, ShouldBeNil)

			err = w.poll()
			So(err, ShouldBeNil)
			So(mock.submitted, ShouldBeEmpty)
		})

		Convey("done run with intact artefacts does not regenerate status on subsequent polls", func() {
			fofnPath := writeFofn(watchDir, generateTmpPaths(10), subDirConfig{Transformer: "test"}).FOFNPath()
			fofnTime := time.Unix(1000, 0)

			So(os.Chtimes(fofnPath, fofnTime, fofnTime), ShouldBeNil)

			subPath := filepath.Dir(fofnPath)

			runDir := filepath.Join(subPath, "1000")
			So(os.MkdirAll(runDir, dirMode), ShouldBeNil)

			writeChunkAndReport(runDir, "chunk.000000", makeFilePairs(0, 10))
			generateDoneStatus(runDir, subDir{Path: subPath})

			statusPath := filepath.Join(runDir, statusFilename)
			knownTime := time.Unix(900, 0)
			So(os.Chtimes(statusPath, knownTime, knownTime), ShouldBeNil)

			mock := &mockJobSubmitter{}

			w, err := NewWatcher(watchDir, mock, cfg)
			So(err, ShouldBeNil)

			// First poll: artefacts intact, status file not regenerated.
			err = w.poll()
			So(err, ShouldBeNil)

			statusInfo, statErr := os.Stat(statusPath)
			So(statErr, ShouldBeNil)
			So(statusInfo.ModTime(), ShouldEqual, knownTime)

			// Break the symlink → next poll repairs it.
			symlinkPath := filepath.Join(subPath, statusFilename)
			So(os.Remove(symlinkPath), ShouldBeNil)

			err = w.poll()
			So(err, ShouldBeNil)

			// Symlink was repaired.
			target, readErr := os.Readlink(symlinkPath)
			So(readErr, ShouldBeNil)
			So(target, ShouldEqual, filepath.Join(filepath.Base(runDir), statusFilename))

			// Status file was NOT regenerated — mtime preserved.
			statusInfo, statErr = os.Stat(statusPath)
			So(statErr, ShouldBeNil)
			So(statusInfo.ModTime(), ShouldEqual, knownTime)
		})

		Convey("done run with intact artefacts restarts when fofn changes", func() {
			fofnPath := writeFofn(watchDir, generateTmpPaths(10), subDirConfig{Transformer: "test"}).FOFNPath()
			fofnTime := time.Unix(1000, 0)
			So(os.Chtimes(fofnPath, fofnTime, fofnTime), ShouldBeNil)

			subPath := filepath.Dir(fofnPath)

			runDir := filepath.Join(subPath, "1000")
			So(os.MkdirAll(runDir, dirMode), ShouldBeNil)

			writeChunkAndReport(runDir, "chunk.000000", makeFilePairs(0, 10))
			generateDoneStatus(runDir, subDir{Path: subPath})

			updateFofnMtime(watchDir, generateTmpPaths(15), 2000)

			mock := &mockJobSubmitter{}

			w, err := NewWatcher(watchDir, mock, cfg)
			So(err, ShouldBeNil)

			err = w.poll()
			So(err, ShouldBeNil)
			So(mock.submitted, ShouldNotBeEmpty)

			// New run dir created with updated mtime.
			newRunScan, findErr := scanRunDirs(subPath)
			So(findErr, ShouldBeNil)
			So(newRunScan.found, ShouldBeTrue)
			So(newRunScan.runMtime, ShouldEqual, 2000)
			So(newRunScan.runDir, ShouldNotEqual, runDir)
		})

		Convey("repairs symlink without regenerating status file", func() {
			fofnPath := writeFofn(watchDir, generateTmpPaths(10), subDirConfig{Transformer: "test"}).FOFNPath()
			fofnTime := time.Unix(1000, 0)
			So(os.Chtimes(fofnPath, fofnTime, fofnTime), ShouldBeNil)

			subPath := filepath.Dir(fofnPath)

			runDir := filepath.Join(subPath, "1000")
			So(os.MkdirAll(runDir, dirMode), ShouldBeNil)

			writeChunkAndReport(runDir, "chunk.000000", makeFilePairs(0, 10))
			generateDoneStatus(runDir, subDir{Path: subPath})

			// Break the symlink
			symlinkPath := filepath.Join(subPath, statusFilename)
			So(os.Remove(symlinkPath), ShouldBeNil)

			// Mark status file mtime to verify it's not regenerated
			statusPath := filepath.Join(runDir, statusFilename)
			knownTime := time.Unix(900, 0)
			So(os.Chtimes(statusPath, knownTime, knownTime), ShouldBeNil)

			mock := &mockJobSubmitter{}

			w, err := NewWatcher(watchDir, mock, cfg)
			So(err, ShouldBeNil)

			err = w.poll()
			So(err, ShouldBeNil)
			// wr is always queried once per poll cycle.
			So(mock.findCallCount, ShouldEqual, 1)

			// Symlink was repaired
			target, readErr := os.Readlink(symlinkPath)
			So(readErr, ShouldBeNil)
			So(target, ShouldEqual, filepath.Join(filepath.Base(runDir), statusFilename))

			// Status file was NOT regenerated — mtime preserved
			statusInfo, statErr := os.Stat(statusPath)
			So(statErr, ShouldBeNil)
			So(statusInfo.ModTime(), ShouldEqual, knownTime)
		})

		Convey("waits for running jobs before restarting when fofn changes during active phase", func() {
			fofnPath := writeFofn(watchDir, generateTmpPaths(10), subDirConfig{Transformer: "test"}).FOFNPath()
			fofnTime := time.Unix(1000, 0)
			So(os.Chtimes(fofnPath, fofnTime, fofnTime), ShouldBeNil)

			subPath := filepath.Dir(fofnPath)

			runDir := filepath.Join(subPath, "1000")
			So(os.MkdirAll(runDir, dirMode), ShouldBeNil)

			writeChunkFile(runDir, "chunk.000000", makeFilePairs(0, 10))

			// fofn has changed
			updateFofnMtime(watchDir, generateTmpPaths(15), 2000)

			// But jobs are still running
			mock := &mockJobSubmitter{
				allJobs: []*jobqueue.Job{
					{RepGroup: makeRepGroup(subPath, 1000), Cmd: "running"},
				},
			}

			w, err := NewWatcher(watchDir, mock, cfg)
			So(err, ShouldBeNil)

			err = w.poll()
			So(err, ShouldBeNil)
			So(mock.submitted, ShouldBeEmpty)

			// Run dir unchanged — still waiting for old run.
			runScan, findErr := scanRunDirs(subPath)
			So(findErr, ShouldBeNil)
			So(runScan.found, ShouldBeTrue)
			So(runScan.runMtime, ShouldEqual, 1000)
		})

		Convey("regenerates status when status file deleted from done run", func() {
			fofnPath := writeFofn(watchDir, generateTmpPaths(10), subDirConfig{Transformer: "test"}).FOFNPath()
			fofnTime := time.Unix(1000, 0)
			So(os.Chtimes(fofnPath, fofnTime, fofnTime), ShouldBeNil)

			subPath := filepath.Dir(fofnPath)

			runDir := filepath.Join(subPath, "1000")
			So(os.MkdirAll(runDir, dirMode), ShouldBeNil)

			writeChunkAndReport(runDir, "chunk.000000", makeFilePairs(0, 10))
			generateDoneStatus(runDir, subDir{Path: subPath})

			// Delete the status file
			So(os.Remove(filepath.Join(runDir, statusFilename)), ShouldBeNil)

			mock := &mockJobSubmitter{}

			w, err := NewWatcher(watchDir, mock, cfg)
			So(err, ShouldBeNil)

			err = w.poll()
			So(err, ShouldBeNil)
			So(mock.submitted, ShouldBeEmpty)
			So(mock.findCallCount, ShouldEqual, 1)

			// Status file regenerated
			statusPath := filepath.Join(runDir, statusFilename)
			entries, counts, parseErr := parseStatus(statusPath)
			So(parseErr, ShouldBeNil)
			So(entries, ShouldHaveLength, 10)
			So(counts.Uploaded, ShouldEqual, 10)

			// Symlink updated
			symlinkPath := filepath.Join(subPath, statusFilename)
			target, readErr := os.Readlink(symlinkPath)
			So(readErr, ShouldBeNil)
			So(target, ShouldEqual, filepath.Join(filepath.Base(runDir), statusFilename))
		})
	})
}

func TestWatcherParallel(t *testing.T) {
	Convey("Watcher.Poll parallel processing", t, func() {
		So(transformer.Register("test", `^/tmp/(.*)$`, "/irods/$1"), ShouldBeNil)

		watchDir := t.TempDir()
		cfg := ProcessSubDirConfig{
			MinChunk: 10,
			MaxChunk: 10,
			RandSeed: 1,
		}

		Convey("processes all 3 subdirectories in a single poll cycle", func() {
			for _, name := range []string{
				"proj1", "proj2", "proj3",
			} {
				setupSubDir(watchDir, generateTmpPaths(5), subDirConfig{Name: name, Transformer: "test"})
			}

			mock := &mockJobSubmitter{}

			w, err := NewWatcher(watchDir, mock, cfg)
			So(err, ShouldBeNil)

			err = w.poll()
			So(err, ShouldBeNil)
			So(mock.submitted, ShouldHaveLength, 3)

			for _, name := range []string{
				"proj1", "proj2", "proj3",
			} {
				runScan, findErr := scanRunDirs(filepath.Join(watchDir, (&set.Set{Name: name}).ID()))
				So(findErr, ShouldBeNil)
				So(runScan.found, ShouldBeTrue)
			}
		})

		Convey("only submits jobs for new subdirectory when 2 have active runs", func() {
			for _, name := range []string{
				"proj1", "proj2",
			} {
				setupSubDir(watchDir, generateTmpPaths(5), subDirConfig{Name: name, Transformer: "test"})
			}

			mock := &mockJobSubmitter{}

			w, err := NewWatcher(watchDir, mock, cfg)
			So(err, ShouldBeNil)

			err = w.poll()
			So(err, ShouldBeNil)

			initialCount := len(mock.submitted)
			So(initialCount, ShouldEqual, 2)

			// Build running jobs from the run dirs that were created.
			var runningJobs []*jobqueue.Job

			for _, name := range []string{
				"proj1", "proj2",
			} {
				subPath := filepath.Join(watchDir, (&set.Set{Name: name}).ID())
				runScan, findErr := scanRunDirs(subPath)
				So(findErr, ShouldBeNil)
				So(runScan.found, ShouldBeTrue)

				runningJobs = append(runningJobs,
					&jobqueue.Job{
						RepGroup: makeRepGroup(subPath, runScan.runMtime),
						Cmd:      "running",
					})
			}

			mock.allJobs = runningJobs

			setupSubDir(watchDir, generateTmpPaths(5), subDirConfig{Name: "proj3", Transformer: "test"})

			err = w.poll()
			So(err, ShouldBeNil)
			So(mock.submitted, ShouldHaveLength, initialCount+1)

			for _, name := range []string{
				"proj1", "proj2", "proj3",
			} {
				runScan, findErr := scanRunDirs(filepath.Join(watchDir, (&set.Set{Name: name}).ID()))
				So(findErr, ShouldBeNil)
				So(runScan.found, ShouldBeTrue)
			}
		})
	})
}

// setupSubDir creates a subdirectory with a fofn and
// config.yml inside the watch directory.
func setupSubDir(watchDir string, paths []string, cfg subDirConfig) subDir {
	return subDirWithMtime(writeFofn(watchDir, paths, cfg))
}

// generateTmpPaths creates n paths matching the test
// transformer pattern (^/tmp/.*).
func generateTmpPaths(n int) []string {
	paths := make([]string, n)

	for i := range n {
		paths[i] = fmt.Sprintf("/tmp/file/%06d", i)
	}

	return paths
}

// subDirWithMtime creates a SubDir by statting the fofn to get FofnMtime.
func subDirWithMtime(s subDir) subDir {
	info, err := os.Stat(s.FOFNPath())
	So(err, ShouldBeNil)

	s.FofnMtime = info.ModTime().Unix()

	return s
}

// generateDoneStatus creates a complete "done" state: status file + symlink.
// Use this when setting up a test that expects artefacts to be intact.
// The symlink uses a relative target (runDirName/status) matching the
// watcher's createStatusSymlink convention.
func generateDoneStatus(runDir string, subDir subDir) {
	So(generateStatus(runDir, nil), ShouldBeNil)

	symlinkPath := filepath.Join(subDir.Path, statusFilename)
	relTarget := filepath.Join(filepath.Base(runDir), statusFilename)

	_ = os.Remove(symlinkPath)
	So(os.Symlink(relTarget, symlinkPath), ShouldBeNil)
}

// writeReportsForChunks writes "uploaded" report files
// for every chunk file in runDir.
func writeReportsForChunks(runDir string) {
	writeReportsExcept(runDir, "")
}

// updateFofnMtime writes new paths to the fofn and sets
// its mtime to the given Unix timestamp.
func updateFofnMtime(watchDir string, paths []string, mtime int64) {
	t := time.Unix(mtime, 0)

	So(os.Chtimes(writeFofn(watchDir, paths).FOFNPath(), t, t), ShouldBeNil)
}

// writeReportsExcept writes "uploaded" report files for
// every chunk file in runDir except the named one.
func writeReportsExcept(runDir, skip string) {
	matches, err := filepath.Glob(filepath.Join(runDir, "chunk.*"))
	So(err, ShouldBeNil)

	for _, m := range matches {
		base := filepath.Base(m)

		if isChunkAuxFile(base) || base == skip {
			continue
		}

		pairs := readChunkPairs(m)
		writeReportFile(runDir, base, pairs, transfer.RequestStatusUploaded)
	}
}

// TestSettleRepairsArtefacts uses property-based testing to verify that the
// settle function correctly repairs any combination of artefact damage.
// This catches the class of bugs where "we generated status/symlink in path
// A but forgot to in path B.".
func TestSettleRepairsArtefacts(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		watchDir := t.TempDir()
		subPath := filepath.Join(watchDir, "proj")

		if err := os.MkdirAll(subPath, dirMode); err != nil {
			rt.Fatal(err)
		}

		mtime := rapid.Int64Range(1000, 9999).Draw(rt, "mtime")
		numChunks := rapid.IntRange(1, 5).Draw(rt, "numChunks")
		numBuried := rapid.IntRange(0, numChunks).Draw(rt, "numBuried")

		runDir := filepath.Join(subPath, strconv.FormatInt(mtime, 10))
		if err := os.MkdirAll(runDir, dirMode); err != nil {
			rt.Fatal(err)
		}

		// Write chunks and reports
		var buriedChunkNames []string

		for i := range numChunks {
			chunkName := fmt.Sprintf("chunk.%06d", i)
			pairs := makeFilePairs(i*5, i*5+5)

			mustWriteChunkT(rt, runDir, chunkName, pairs)

			if i >= numChunks-numBuried {
				buriedChunkNames = append(buriedChunkNames, chunkName)

				continue
			}

			mustWriteReportT(rt, runDir, chunkName, pairs, transfer.RequestStatusUploaded)
		}

		// Build a RunJobStatus with buried info and a LastCompletedTime
		// that forces status generation.
		status := runJobStatus{
			LastCompletedTime: time.Now().Add(time.Hour),
		}
		status.BuriedChunks = append(status.BuriedChunks, buriedChunkNames...)
		sd := subDir{Path: subPath, FofnMtime: mtime}

		if err := ensureArtefacts(runDir, sd, status); err != nil {
			rt.Fatalf("initial settle: %v", err)
		}

		// Apply random damage
		statusPath := filepath.Join(runDir, statusFilename)
		symlinkPath := filepath.Join(subPath, statusFilename)

		damage := rapid.SampledFrom([]string{
			"none", "deleteStatus", "deleteSymlink",
			"corruptSymlink", "deleteBoth",
		}).Draw(rt, "damage")

		switch damage {
		case "deleteStatus":
			os.Remove(statusPath)

			issuesPath := statusPath + issuesSuffix
			os.Remove(issuesPath)
		case "deleteSymlink":
			os.Remove(symlinkPath)
		case "corruptSymlink":
			os.Remove(symlinkPath)

			if err := os.Symlink("/wrong/path", symlinkPath); err != nil {
				rt.Fatal(err)
			}
		case "deleteBoth":
			os.Remove(statusPath)
			os.Remove(symlinkPath)

			issuesPath := statusPath + issuesSuffix
			os.Remove(issuesPath)
		}

		// Call settle again — must repair. Use a LastCompletedTime in the
		// future so needsStatusRegen triggers for damaged status files.
		repairStatus := runJobStatus{
			LastCompletedTime: time.Now().Add(2 * time.Hour),
		}

		repairStatus.BuriedChunks = append(repairStatus.BuriedChunks, buriedChunkNames...)

		if err := ensureArtefacts(runDir, sd, repairStatus); err != nil {
			rt.Fatalf("repair settle: %v", err)
		}

		// Invariant 1: status file exists
		if _, err := os.Stat(statusPath); err != nil {
			rt.Fatalf("status file should exist after settle: %v", err)
		}

		// Invariant 2: symlink points to the current run status file.
		verifySymlink(rt, symlinkPath, statusPath)
	})
}

// verifySymlink checks that the symlink points to the correct relative status
// file target. Since wr is queried every poll cycle, buried runs also have a
// valid symlink showing which chunks completed.
func verifySymlink(rt *rapid.T, symlinkPath, statusPath string) {
	target, err := os.Readlink(symlinkPath)
	if err != nil {
		rt.Fatalf("symlink should exist after settle: %v", err)
	}

	// statusPath is absolute: runDir/status. We expect the symlink target
	// to be the relative form: runDirName/status.
	runDir := filepath.Dir(statusPath)
	expected := filepath.Join(filepath.Base(runDir), statusFilename)

	if target != expected {
		rt.Fatalf("symlink target: want %s, got %s", expected, target)
	}
}

// TestSettleIdempotent verifies that calling settle twice produces the same
// result — no redundant file operations on the second call.
func TestSettleIdempotent(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		watchDir := t.TempDir()
		subPath := filepath.Join(watchDir, "proj")

		if err := os.MkdirAll(subPath, dirMode); err != nil {
			rt.Fatal(err)
		}

		mtime := rapid.Int64Range(1000, 9999).Draw(rt, "mtime")
		numChunks := rapid.IntRange(1, 3).Draw(rt, "numChunks")

		runDir := filepath.Join(subPath, strconv.FormatInt(mtime, 10))
		if err := os.MkdirAll(runDir, dirMode); err != nil {
			rt.Fatal(err)
		}

		for i := range numChunks {
			chunkName := fmt.Sprintf("chunk.%06d", i)
			pairs := makeFilePairs(i*5, i*5+5)

			mustWriteChunkT(rt, runDir, chunkName, pairs)
			mustWriteReportT(rt, runDir, chunkName, pairs, transfer.RequestStatusUploaded)
		}

		sd := subDir{Path: subPath, FofnMtime: mtime}

		// First settle with a recent LastCompletedTime to force status generation.
		status := runJobStatus{
			LastCompletedTime: time.Now().Add(time.Hour),
		}

		if err := ensureArtefacts(runDir, sd, status); err != nil {
			rt.Fatalf("first settle: %v", err)
		}

		statusPath := filepath.Join(runDir, statusFilename)

		statusInfo, err := os.Stat(statusPath)
		if err != nil {
			rt.Fatal(err)
		}

		firstMtime := statusInfo.ModTime()

		// Second settle with zero LastCompletedTime — needsStatusRegen
		// returns false because status file exists and no new completions.
		idempotentStatus := runJobStatus{}

		settleErr := ensureArtefacts(runDir, sd, idempotentStatus)
		if settleErr != nil {
			rt.Fatalf("second settle: %v", settleErr)
		}

		statusInfo, err = os.Stat(statusPath)
		if err != nil {
			rt.Fatal(err)
		}

		if statusInfo.ModTime() != firstMtime {
			rt.Fatal("settle should not rewrite status file on idempotent call")
		}
	})
}

// TestPollRepairsDoneRun uses property-based testing to verify that a full
// poll cycle correctly repairs artefact damage for done runs with unchanged
// fofns. This is the exact invariant stated in the design: "if artefacts are
// intact and fofn unchanged, status file exists and symlink is correct.".
func TestPollRepairsDoneRun(t *testing.T) {
	if err := transformer.Register("test", `^/tmp/(.*)$`, "/irods/$1"); err != nil {
		t.Log("transformer already registered:", err)
	}

	rapid.Check(t, func(rt *rapid.T) {
		watchDir := t.TempDir()
		subPath := filepath.Join(watchDir, "proj")

		if err := os.MkdirAll(subPath, dirMode); err != nil {
			rt.Fatal(err)
		}

		// Write fofn with fixed mtime
		fofnPath := filepath.Join(subPath, fofnFilename)
		if err := os.WriteFile(fofnPath, []byte("/tmp/file/000000\n"), 0600); err != nil {
			rt.Fatal(err)
		}

		fofnTime := time.Unix(1000, 0)
		if err := os.Chtimes(fofnPath, fofnTime, fofnTime); err != nil {
			rt.Fatal(err)
		}

		if err := writeConfig(subPath, subDirConfig{Transformer: "test"}); err != nil {
			rt.Fatal(err)
		}

		// Create completed run
		runDir := filepath.Join(subPath, "1000")
		if err := os.MkdirAll(runDir, dirMode); err != nil {
			rt.Fatal(err)
		}

		numChunks := rapid.IntRange(1, 4).Draw(rt, "numChunks")
		for i := range numChunks {
			chunkName := fmt.Sprintf("chunk.%06d", i)
			pairs := makeFilePairs(i*5, i*5+5)

			mustWriteChunkT(rt, runDir, chunkName, pairs)
			mustWriteReportT(rt, runDir, chunkName, pairs, transfer.RequestStatusUploaded)
		}

		if err := generateStatus(runDir, nil); err != nil {
			rt.Fatal(err)
		}

		statusPath := filepath.Join(runDir, statusFilename)
		relTarget := filepath.Join(filepath.Base(runDir), statusFilename)

		if err := os.Symlink(relTarget, filepath.Join(subPath, statusFilename)); err != nil {
			rt.Fatal(err)
		}

		// Apply random damage
		symlinkPath := filepath.Join(subPath, statusFilename)

		damage := rapid.SampledFrom([]string{
			"none", "deleteStatus", "deleteSymlink",
			"corruptSymlink", "deleteBoth",
		}).Draw(rt, "damage")

		switch damage {
		case "deleteStatus":
			os.Remove(statusPath)

			issuesPath := statusPath + issuesSuffix
			os.Remove(issuesPath)
		case "deleteSymlink":
			os.Remove(symlinkPath)
		case "corruptSymlink":
			os.Remove(symlinkPath)

			if err := os.Symlink("/wrong/path", symlinkPath); err != nil {
				rt.Fatal(err)
			}
		case "deleteBoth":
			os.Remove(statusPath)
			os.Remove(symlinkPath)

			issuesPath := statusPath + issuesSuffix
			os.Remove(issuesPath)
		}

		// Full poll cycle
		mock := &mockJobSubmitter{}

		w, err := NewWatcher(watchDir, mock, ProcessSubDirConfig{
			MinChunk: 10,
			MaxChunk: 10,
			RandSeed: 1,
		})
		if err != nil {
			rt.Fatalf("failed to create Watcher: %v", err)
		}

		if err = w.poll(); err != nil {
			rt.Fatalf("Poll failed: %v", err)
		}

		// Invariant: status file exists
		if _, err = os.Stat(statusPath); err != nil {
			rt.Fatalf("status file missing after poll (damage=%s): %v", damage, err)
		}

		// Invariant: symlink correct (relative target)
		target, err := os.Readlink(symlinkPath)
		if err != nil {
			rt.Fatalf("symlink missing after poll (damage=%s): %v", damage, err)
		}

		expectedTarget := filepath.Join(filepath.Base(runDir), statusFilename)
		if target != expectedTarget {
			rt.Fatalf("symlink target wrong (damage=%s): want %s, got %s",
				damage, expectedTarget, target)
		}

		// Invariant: no new jobs submitted (fofn unchanged)
		if len(mock.submitted) != 0 {
			rt.Fatalf("expected no new jobs, got %d", len(mock.submitted))
		}
	})
}
