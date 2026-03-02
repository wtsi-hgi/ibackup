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
	"github.com/wtsi-hgi/ibackup/transformer"
	"pgregory.net/rapid"
)

func TestStateExhaustive(t *testing.T) {
	Convey("every subDirState has a description", t, func() {
		covered := 0

		for s := subDirState(0); s < numStates; s++ {
			So(stateDesc[s], ShouldNotBeEmpty)

			covered++
		}

		Convey("has exactly 4 states with no gaps", func() {
			So(covered, ShouldEqual, int(numStates))
			So(int(numStates), ShouldEqual, 4)
		})
	})
}

// filePair represents a local/remote path pair.
type filePair struct {
	Local  string
	Remote string
}

// writeChunkAndReport writes both a chunk file and a complete report file for
// all pairs with status "uploaded".
func writeChunkAndReport(
	runDir, chunkName string,
	pairs []filePair,
) {
	writeChunkFile(runDir, chunkName, pairs)
	writeReportFile(runDir, chunkName, pairs, "uploaded")
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

// writeChunkOnly writes a chunk file with no report.
func writeChunkOnly(
	runDir, chunkName string,
	pairs []filePair,
) {
	writeChunkFile(runDir, chunkName, pairs)
}

// writeChunkFile writes a chunk file with base64-encoded local/remote pairs.
func writeChunkFile(
	runDir, chunkName string, pairs []filePair,
) {
	path := filepath.Join(runDir, chunkName)

	f, err := os.Create(path)
	So(err, ShouldBeNil)

	for _, p := range pairs {
		line := base64.StdEncoding.EncodeToString(
			[]byte(p.Local),
		) + "\t" + base64.StdEncoding.EncodeToString(
			[]byte(p.Remote),
		) + "\n"

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
func decodeChunkTestLine(
	line string,
) (string, string) {
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
func writeReportFile(
	runDir, chunkName string,
	pairs []filePair, status string,
) {
	path := filepath.Join(runDir, chunkName+".report")

	f, err := os.Create(path)
	So(err, ShouldBeNil)

	for _, p := range pairs {
		line := FormatReportLine(ReportEntry{
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
func mustWriteChunkT(t fataler, runDir, chunkName string, pairs []filePair) {
	path := filepath.Join(runDir, chunkName)

	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}

	for _, p := range pairs {
		line := base64.StdEncoding.EncodeToString(
			[]byte(p.Local),
		) + "\t" + base64.StdEncoding.EncodeToString(
			[]byte(p.Remote),
		) + "\n"

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
func mustWriteReportT(t fataler, runDir, chunkName string, pairs []filePair, status string) {
	path := filepath.Join(runDir, chunkName+".report")

	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}

	for _, p := range pairs {
		line := FormatReportLine(ReportEntry{
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

// fataler is the subset of testing.TB needed by property-test helpers.
type fataler interface {
	Fatal(args ...any)
	Fatalf(format string, args ...any)
}

func TestProcessSubDir(t *testing.T) {
	Convey("ProcessSubDir", t, func() {
		So(transformer.Register("test", `^/tmp/(.*)$`, "/irods/$1"), ShouldBeNil)

		watchDir := t.TempDir()

		Convey("creates run dir and submits jobs for 25 paths", func() {
			paths := generateTmpPaths(25)
			subDir := setupSubDir(watchDir, "proj1", paths, SubDirConfig{Transformer: "test"})

			mock := &mockJobSubmitter{}
			cfg := ProcessSubDirConfig{
				MinChunk: 10,
				MaxChunk: 10,
				RandSeed: 1,
			}

			state, err := ProcessSubDir(subDir, mock, cfg)
			So(err, ShouldBeNil)

			fofnInfo, statErr := os.Stat(filepath.Join(subDir.Path, "fofn"))
			So(statErr, ShouldBeNil)

			expectedMtime := fofnInfo.ModTime().Unix()
			So(state.Mtime, ShouldEqual, expectedMtime)

			expectedRunDir := filepath.Join(
				subDir.Path,
				strconv.FormatInt(expectedMtime, 10),
			)
			So(state.RunDir, ShouldEqual,
				expectedRunDir)

			_, statErr = os.Stat(state.RunDir)
			So(statErr, ShouldBeNil)

			entries, readErr := os.ReadDir(state.RunDir)
			So(readErr, ShouldBeNil)
			So(entries, ShouldHaveLength, 3)

			So(mock.submitted, ShouldHaveLength, 3)

			dirName := filepath.Base(subDir.Path)
			expectedRG := fmt.Sprintf("ibackup_fofn_%s_%d", dirName, expectedMtime)
			So(state.RepGroup, ShouldEqual, expectedRG)

			for _, job := range mock.submitted {
				So(job.RepGroup, ShouldEqual,
					expectedRG)
				So(job.Cwd, ShouldEqual,
					state.RunDir)
				So(job.Cmd, ShouldContainSubstring,
					fmt.Sprintf("--fofn '%s'", dirName))
			}
		})

		Convey("includes --no_replace when freeze is true", func() {
			paths := generateTmpPaths(5)
			subDir := setupSubDir(
				watchDir, "proj2", paths,
				SubDirConfig{Transformer: "test", Freeze: true},
			)

			mock := &mockJobSubmitter{}
			cfg := ProcessSubDirConfig{
				MinChunk: 10,
				MaxChunk: 10,
				RandSeed: 1,
			}

			_, err := ProcessSubDir(subDir, mock, cfg)
			So(err, ShouldBeNil)
			So(mock.submitted, ShouldNotBeEmpty)

			for _, job := range mock.submitted {
				So(job.Cmd, ShouldContainSubstring,
					"--no_replace")
			}
		})

		Convey("returns error when config.yml is missing", func() {
			subPath := filepath.Join(watchDir, "proj3")
			So(os.MkdirAll(subPath, 0750),
				ShouldBeNil)

			writeFofn(subPath, generateTmpPaths(5))

			sd := subDirWithMtime(subPath)
			mock := &mockJobSubmitter{}
			cfg := ProcessSubDirConfig{
				MinChunk: 10,
				MaxChunk: 10,
				RandSeed: 1,
			}

			_, err := ProcessSubDir(sd, mock, cfg)
			So(err, ShouldNotBeNil)
			So(mock.submitted, ShouldBeEmpty)
		})

		Convey("returns zero state for empty fofn",
			func() {
				subDir := setupSubDir(watchDir, "proj4", nil, SubDirConfig{Transformer: "test"})

				mock := &mockJobSubmitter{}
				cfg := ProcessSubDirConfig{
					MinChunk: 10,
					MaxChunk: 10,
					RandSeed: 1,
				}

				state, err := ProcessSubDir(subDir, mock, cfg)
				So(err, ShouldBeNil)
				So(state, ShouldResemble, RunState{})
				So(mock.submitted, ShouldBeEmpty)

				entries, readErr := os.ReadDir(subDir.Path)
				So(readErr, ShouldBeNil)

				for _, e := range entries {
					So(e.IsDir(), ShouldBeFalse)
				}
			})

		Convey("includes --meta when config has metadata", func() {
			paths := generateTmpPaths(5)
			subDir := setupSubDir(
				watchDir, "proj6", paths,
				SubDirConfig{Transformer: "test", Metadata: map[string]string{"colour": "red"}},
			)

			mock := &mockJobSubmitter{}
			cfg := ProcessSubDirConfig{
				MinChunk: 10,
				MaxChunk: 10,
				RandSeed: 1,
			}

			_, err := ProcessSubDir(subDir, mock, cfg)
			So(err, ShouldBeNil)
			So(mock.submitted, ShouldNotBeEmpty)

			for _, job := range mock.submitted {
				So(job.Cmd, ShouldContainSubstring,
					`--meta 'colour=red'`)
			}
		})

		Convey("omits --meta when config has no metadata", func() {
			paths := generateTmpPaths(5)
			subDir := setupSubDir(watchDir, "proj7", paths, SubDirConfig{Transformer: "test"})

			mock := &mockJobSubmitter{}
			cfg := ProcessSubDirConfig{
				MinChunk: 10,
				MaxChunk: 10,
				RandSeed: 1,
			}

			_, err := ProcessSubDir(subDir, mock, cfg)
			So(err, ShouldBeNil)
			So(mock.submitted, ShouldNotBeEmpty)

			for _, job := range mock.submitted {
				So(job.Cmd, ShouldNotContainSubstring,
					"--meta")
			}
		})

		Convey("creates 100 chunks and 100 jobs for 50000 paths with default bounds", func() {
			paths := generateTmpPaths(50000)
			subDir := setupSubDir(watchDir, "proj_vc3", paths, SubDirConfig{Transformer: "test"})

			mock := &mockJobSubmitter{}
			cfg := ProcessSubDirConfig{
				MinChunk: 250,
				MaxChunk: 10000,
				RandSeed: 1,
			}

			state, err := ProcessSubDir(subDir, mock, cfg)
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

		Convey("writes status file and symlink for 3 complete reports", func() {
			subDir, runDir := setupRunDir(watchDir, "proj1")

			writeChunkAndReport(runDir, "chunk.000000", makeFilePairs(0, 5))
			writeChunkAndReport(runDir, "chunk.000001", makeFilePairs(5, 10))
			writeChunkAndReport(runDir, "chunk.000002", makeFilePairs(10, 15))

			err := GenerateStatus(runDir, nil)
			So(err, ShouldBeNil)

			statusPath := filepath.Join(runDir, "status")
			entries, counts, parseErr := ParseStatus(statusPath)
			So(parseErr, ShouldBeNil)
			So(entries, ShouldHaveLength, 15)
			So(counts.Uploaded, ShouldEqual, 15)

			// GenerateStatus writes the status file; symlink is managed
			// separately by the watcher's settle path.
			So(os.Symlink(statusPath, filepath.Join(subDir.Path, "status")), ShouldBeNil)

			symlinkPath := filepath.Join(subDir.Path, "status")
			target, readErr := os.Readlink(symlinkPath)
			So(readErr, ShouldBeNil)
			So(target, ShouldEqual, statusPath)
		})

		Convey("handles buried chunk with no report file", func() {
			_, runDir := setupRunDir(watchDir, "proj2")

			writeChunkAndReport(runDir, "chunk.000000", makeFilePairs(0, 5))
			writeChunkAndReport(runDir, "chunk.000001", makeFilePairs(5, 10))
			writeChunkOnly(runDir, "chunk.000002", makeFilePairs(10, 20))

			err := GenerateStatus(
				runDir,
				[]string{"chunk.000002"},
			)
			So(err, ShouldBeNil)

			statusPath := filepath.Join(runDir, "status")
			entries, counts, parseErr := ParseStatus(statusPath)
			So(parseErr, ShouldBeNil)
			So(entries, ShouldHaveLength, 20)
			So(counts.Uploaded, ShouldEqual, 10)
			So(counts.NotProcessed, ShouldEqual, 10)
		})

		Convey("handles buried chunk with incomplete report", func() {
			_, runDir := setupRunDir(watchDir, "proj3")

			pairs := makeFilePairs(0, 10)
			writeChunkFile(runDir, "chunk.000000", pairs)
			writeReportFile(runDir, "chunk.000000", pairs[:5], "uploaded")

			err := GenerateStatus(
				runDir,
				[]string{"chunk.000000"},
			)
			So(err, ShouldBeNil)

			statusPath := filepath.Join(runDir, "status")
			entries, counts, parseErr := ParseStatus(statusPath)
			So(parseErr, ShouldBeNil)
			So(entries, ShouldHaveLength, 10)
			So(counts.Uploaded, ShouldEqual, 5)
			So(counts.NotProcessed, ShouldEqual, 5)
		})

	})
}

// setupRunDir creates a watch directory, subdirectory,
// and run directory; returns the SubDir and run path.
func setupRunDir(
	watchDir, name string,
) (SubDir, string) {
	subPath := filepath.Join(watchDir, name)
	So(os.MkdirAll(subPath, 0750), ShouldBeNil)

	runDir := filepath.Join(subPath, "12345")
	So(os.MkdirAll(runDir, 0750), ShouldBeNil)

	return SubDir{Path: subPath}, runDir
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
			subDir := setupSubDir(watchDir, "proj1", paths, SubDirConfig{Transformer: "test"})

			mock := &mockJobSubmitter{}
			w := NewWatcher(watchDir, mock, cfg)

			err := w.Poll()
			So(err, ShouldBeNil)
			So(mock.submitted, ShouldHaveLength, 3)

			// Run dir should exist with the fofn mtime as name
			runDir, runMtime, found, findErr := findRunDir(subDir.Path)
			So(findErr, ShouldBeNil)
			So(found, ShouldBeTrue)
			So(runMtime, ShouldEqual, subDir.FofnMtime)
			So(runDir, ShouldNotBeEmpty)
		})

		Convey("skips when active run has incomplete jobs", func() {
			paths := generateTmpPaths(25)
			subDir := setupSubDir(watchDir, "proj2", paths, SubDirConfig{Transformer: "test"})

			mock := &mockJobSubmitter{}
			w := NewWatcher(watchDir, mock, cfg)

			err := w.Poll()
			So(err, ShouldBeNil)

			submitCount := len(mock.submitted)
			repGroup := makeRepGroup(subDir.Path, subDir.FofnMtime)

			mock.allJobs = []*jobqueue.Job{
				{RepGroup: repGroup, Cmd: "running"},
			}

			err = w.Poll()
			So(err, ShouldBeNil)
			So(mock.submitted, ShouldHaveLength,
				submitCount)
		})

		Convey("completes successful run and starts new run when fofn changed", func() {
			paths := generateTmpPaths(25)
			subDir := setupSubDir(watchDir, "proj3", paths, SubDirConfig{Transformer: "test"})

			mock := &mockJobSubmitter{}
			w := NewWatcher(watchDir, mock, cfg)

			err := w.Poll()
			So(err, ShouldBeNil)

			firstCount := len(mock.submitted)
			runDir, runMtime, found, findErr := findRunDir(subDir.Path)
			So(findErr, ShouldBeNil)
			So(found, ShouldBeTrue)

			writeReportsForChunks(runDir)

			mock.allJobs = nil

			updateFofnMtime(subDir.Path, generateTmpPaths(15), runMtime+1000)

			err = w.Poll()
			So(err, ShouldBeNil)

			statusPath := filepath.Join(runDir, "status")
			_, statErr := os.Stat(statusPath)
			So(statErr, ShouldBeNil)

			symlinkPath := filepath.Join(subDir.Path, "status")
			_, readErr := os.Readlink(symlinkPath)
			So(readErr, ShouldBeNil)

			So(len(mock.submitted),
				ShouldBeGreaterThan, firstCount)

			newRunDir, newMtime, newFound, newFindErr := findRunDir(subDir.Path)
			So(newFindErr, ShouldBeNil)
			So(newFound, ShouldBeTrue)
			So(newRunDir, ShouldNotEqual, runDir)
			So(newMtime, ShouldEqual, runMtime+1000)
		})

		Convey("completes successful run with no new run when fofn unchanged", func() {
			paths := generateTmpPaths(25)
			subDir := setupSubDir(watchDir, "proj4", paths, SubDirConfig{Transformer: "test"})

			mock := &mockJobSubmitter{}
			w := NewWatcher(watchDir, mock, cfg)

			err := w.Poll()
			So(err, ShouldBeNil)

			submitCount := len(mock.submitted)
			runDir, _, found, findErr := findRunDir(subDir.Path)
			So(findErr, ShouldBeNil)
			So(found, ShouldBeTrue)

			writeReportsForChunks(runDir)

			mock.allJobs = nil

			err = w.Poll()
			So(err, ShouldBeNil)

			statusPath := filepath.Join(runDir, "status")
			_, statErr := os.Stat(statusPath)
			So(statErr, ShouldBeNil)

			So(mock.submitted, ShouldHaveLength,
				submitCount)
		})

		Convey("generates not_processed status for buried chunk when fofn unchanged",
			func() {
				paths := generateTmpPaths(25)
				subDir := setupSubDir(
					watchDir, "proj5", paths,
					SubDirConfig{Transformer: "test"},
				)

				mock := &mockJobSubmitter{}
				w := NewWatcher(watchDir, mock, cfg)

				err := w.Poll()
				So(err, ShouldBeNil)

				submitCount := len(mock.submitted)
				runDir, _, found, findErr := findRunDir(subDir.Path)
				So(findErr, ShouldBeNil)
				So(found, ShouldBeTrue)

				repGroup := makeRepGroup(subDir.Path, subDir.FofnMtime)

				buriedPairs := readChunkPairs(
					filepath.Join(
						runDir, "chunk.000002",
					),
				)
				buriedCount := len(buriedPairs)
				uploadedCount := 25 - buriedCount

				writeReportsExcept(runDir, "chunk.000002")

				mock.allJobs = []*jobqueue.Job{
					{
						RepGroup: repGroup,
						State:    jobqueue.JobStateBuried,
						Cmd: "ibackup put " +
							"-f chunk.000002",
					},
				}

				err = w.Poll()
				So(err, ShouldBeNil)

				statusPath := filepath.Join(runDir, "status")
				entries, counts, parseErr :=
					ParseStatus(statusPath)
				So(parseErr, ShouldBeNil)
				So(entries, ShouldHaveLength, 25)
				So(counts.Uploaded, ShouldEqual,
					uploadedCount)
				So(counts.NotProcessed,
					ShouldEqual, buriedCount)

				So(mock.submitted, ShouldHaveLength,
					submitCount)
			})

		Convey("does not regenerate status repeatedly for unchanged buried run", func() {
			paths := generateTmpPaths(25)
			subDir := setupSubDir(
				watchDir, "proj5b", paths,
				SubDirConfig{Transformer: "test"},
			)

			mock := &mockJobSubmitter{}
			w := NewWatcher(watchDir, mock, cfg)

			err := w.Poll()
			So(err, ShouldBeNil)

			runDir, _, found, findErr := findRunDir(subDir.Path)
			So(findErr, ShouldBeNil)
			So(found, ShouldBeTrue)

			repGroup := makeRepGroup(subDir.Path, subDir.FofnMtime)

			writeReportsExcept(runDir, "chunk.000002")

			// Buried job with EndTime in the past.
			buriedTime := time.Now().Add(-time.Hour)
			mock.allJobs = []*jobqueue.Job{{
				RepGroup: repGroup,
				State:    jobqueue.JobStateBuried,
				Cmd:      "ibackup put -f chunk.000002",
			}}
			// Add a completed job to set LastCompletedTime.
			mock.allJobs = append(mock.allJobs, &jobqueue.Job{
				RepGroup: repGroup,
				State:    jobqueue.JobStateComplete,
				Cmd:      "ibackup put -f chunk.000000",
				EndTime:  buriedTime,
			})

			err = w.Poll()
			So(err, ShouldBeNil)

			statusPath := filepath.Join(runDir, "status")
			knownTime := time.Unix(900, 0)
			So(os.Chtimes(statusPath, knownTime, knownTime), ShouldBeNil)

			// Third poll: same buried state, same LastCompletedTime.
			// Status mtime is now in the future relative to buriedTime,
			// so needsStatusRegen returns false → no regen.
			futureTime := time.Now().Add(time.Hour)
			So(os.Chtimes(statusPath, futureTime, futureTime), ShouldBeNil)

			err = w.Poll()
			So(err, ShouldBeNil)

			statusInfoAfter, statErr := os.Stat(statusPath)
			So(statErr, ShouldBeNil)
			So(statusInfoAfter.ModTime().Unix(), ShouldEqual, futureTime.Unix())

			// Buried runs still have symlink — since wr is queried every
			// poll cycle, buried-then-retried chunks are detected naturally.
			target, readErr := os.Readlink(filepath.Join(subDir.Path, "status"))
			So(readErr, ShouldBeNil)
			So(target, ShouldEqual, filepath.Join(filepath.Base(runDir), "status"))
		})

		Convey("deletes buried jobs and starts new run when fofn changed", func() {
			paths := generateTmpPaths(25)
			subDir := setupSubDir(watchDir, "proj6", paths, SubDirConfig{Transformer: "test"})

			mock := &mockJobSubmitter{}
			w := NewWatcher(watchDir, mock, cfg)

			err := w.Poll()
			So(err, ShouldBeNil)

			firstCount := len(mock.submitted)
			runDir, runMtime, found, findErr := findRunDir(subDir.Path)
			So(findErr, ShouldBeNil)
			So(found, ShouldBeTrue)

			repGroup := makeRepGroup(subDir.Path, runMtime)

			writeReportsExcept(runDir, "chunk.000002")

			buriedJob := &jobqueue.Job{
				RepGroup: repGroup,
				State:    jobqueue.JobStateBuried,
				Cmd:      "ibackup put -f chunk.000002",
			}
			mock.allJobs = []*jobqueue.Job{buriedJob}

			updateFofnMtime(subDir.Path, generateTmpPaths(15), runMtime+1000)

			err = w.Poll()
			So(err, ShouldBeNil)

			statusPath := filepath.Join(runDir, "status")
			_, statErr := os.Stat(statusPath)
			So(statErr, ShouldBeNil)

			So(mock.deleted, ShouldNotBeEmpty)

			So(len(mock.submitted),
				ShouldBeGreaterThan, firstCount)

			newRunDir, newMtime, newFound, newFindErr := findRunDir(subDir.Path)
			So(newFindErr, ShouldBeNil)
			So(newFound, ShouldBeTrue)
			So(newMtime, ShouldEqual, runMtime+1000)
			So(newRunDir, ShouldNotEqual, runDir)
		})

		Convey("deletes old run directories on successful completion", func() {
			paths := generateTmpPaths(25)
			subDir := setupSubDir(watchDir, "proj7", paths, SubDirConfig{Transformer: "test"})

			mock := &mockJobSubmitter{}
			w := NewWatcher(watchDir, mock, cfg)

			err := w.Poll()
			So(err, ShouldBeNil)

			runDir, _, found, findErr := findRunDir(subDir.Path)
			So(findErr, ShouldBeNil)
			So(found, ShouldBeTrue)

			oldRunDir := filepath.Join(subDir.Path, "500")
			So(os.MkdirAll(oldRunDir, 0750),
				ShouldBeNil)

			writeReportsForChunks(runDir)

			mock.allJobs = nil

			err = w.Poll()
			So(err, ShouldBeNil)

			_, statErr := os.Stat(oldRunDir)
			So(os.IsNotExist(statErr), ShouldBeTrue)

			_, statErr = os.Stat(runDir)
			So(statErr, ShouldBeNil)
		})

		Convey("updates status symlink after second run completes", func() {
			paths := generateTmpPaths(25)
			subDir := setupSubDir(watchDir, "proj8", paths, SubDirConfig{Transformer: "test"})

			mock := &mockJobSubmitter{}
			w := NewWatcher(watchDir, mock, cfg)

			err := w.Poll()
			So(err, ShouldBeNil)

			firstRunDir, firstMtime, found, findErr := findRunDir(subDir.Path)
			So(findErr, ShouldBeNil)
			So(found, ShouldBeTrue)

			writeReportsForChunks(firstRunDir)

			mock.allJobs = nil

			updateFofnMtime(subDir.Path, generateTmpPaths(15), firstMtime+1000)

			err = w.Poll()
			So(err, ShouldBeNil)

			secondRunDir, _, found, findErr2 := findRunDir(subDir.Path)
			So(findErr2, ShouldBeNil)
			So(found, ShouldBeTrue)

			writeReportsForChunks(secondRunDir)

			mock.allJobs = nil

			err = w.Poll()
			So(err, ShouldBeNil)

			statusPath := filepath.Join(secondRunDir, "status")
			entries, counts, parseErr := ParseStatus(statusPath)
			So(parseErr, ShouldBeNil)
			So(entries, ShouldHaveLength, 15)
			So(counts.Uploaded, ShouldEqual, 15)

			symlinkPath := filepath.Join(subDir.Path, "status")
			target, readErr := os.Readlink(symlinkPath)
			So(readErr, ShouldBeNil)

			expectedTarget := filepath.Join(filepath.Base(secondRunDir), "status")
			So(target, ShouldEqual,
				expectedTarget)
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

		Convey("detects existing run with incomplete jobs and does not submit new jobs",
			func() {
				subPath := filepath.Join(watchDir, "proj")
				So(os.MkdirAll(subPath, 0750),
					ShouldBeNil)

				fofnPath := writeFofn(subPath, generateTmpPaths(10))
				fofnTime := time.Unix(1000, 0)
				So(os.Chtimes(fofnPath, fofnTime, fofnTime), ShouldBeNil)

				So(WriteConfig(subPath, SubDirConfig{Transformer: "test"}), ShouldBeNil)

				runDir := filepath.Join(subPath, "1000")
				So(os.MkdirAll(runDir, 0750),
					ShouldBeNil)

				writeChunkOnly(runDir, "chunk.000000", makeFilePairs(0, 10))

				mock := &mockJobSubmitter{
					allJobs: []*jobqueue.Job{
						{
							RepGroup: "ibackup_fofn_proj_1000",
							Cmd:      "running",
						},
					},
				}

				w := NewWatcher(watchDir, mock, cfg)

				err := w.Poll()
				So(err, ShouldBeNil)
				So(mock.submitted, ShouldBeEmpty)

				// Run dir unchanged
				_, runMtime, found, findErr := findRunDir(subPath)
				So(findErr, ShouldBeNil)
				So(found, ShouldBeTrue)
				So(runMtime, ShouldEqual, 1000)
			})

		Convey("returns error when completion status cannot be queried", func() {
			subPath := filepath.Join(watchDir, "proj")
			So(os.MkdirAll(subPath, 0750), ShouldBeNil)

			writeFofn(subPath, generateTmpPaths(10))
			So(WriteConfig(subPath, SubDirConfig{Transformer: "test"}), ShouldBeNil)

			runDir := filepath.Join(subPath, "1000")
			So(os.MkdirAll(runDir, 0750), ShouldBeNil)
			writeChunkOnly(runDir, "chunk.000000", makeFilePairs(0, 10))

			mock := &mockJobSubmitter{allJobsErr: errTest}
			w := NewWatcher(watchDir, mock, cfg)

			err := w.Poll()
			So(err, ShouldNotBeNil)
			So(mock.submitted, ShouldBeEmpty)
		})

		Convey("detects completed existing run and generates status file", func() {
			subPath := filepath.Join(watchDir, "proj")
			So(os.MkdirAll(subPath, 0750),
				ShouldBeNil)

			fofnPath := writeFofn(subPath, generateTmpPaths(10))

			fofnTime := time.Unix(1000, 0)
			So(os.Chtimes(fofnPath, fofnTime, fofnTime), ShouldBeNil)

			So(WriteConfig(subPath, SubDirConfig{Transformer: "test"}), ShouldBeNil)

			runDir := filepath.Join(subPath, "1000")
			So(os.MkdirAll(runDir, 0750),
				ShouldBeNil)

			writeChunkAndReport(runDir, "chunk.000000", makeFilePairs(0, 10))

			// No status file yet — active run with all jobs complete.
			mock := &mockJobSubmitter{}

			w := NewWatcher(watchDir, mock, cfg)

			err := w.Poll()
			So(err, ShouldBeNil)

			statusPath := filepath.Join(runDir, "status")
			_, statErr := os.Stat(statusPath)
			So(statErr, ShouldBeNil)

			symlinkPath := filepath.Join(subPath, "status")
			target, readErr := os.Readlink(symlinkPath)
			So(readErr, ShouldBeNil)
			So(target, ShouldEqual, filepath.Join(filepath.Base(runDir), "status"))

			So(mock.submitted, ShouldBeEmpty)
		})

		Convey("regenerates status when symlink is missing", func() {
			subPath := filepath.Join(watchDir, "proj")
			So(os.MkdirAll(subPath, 0750), ShouldBeNil)

			fofnPath := writeFofn(subPath, generateTmpPaths(10))
			fofnTime := time.Unix(1000, 0)
			So(os.Chtimes(fofnPath, fofnTime, fofnTime), ShouldBeNil)

			So(WriteConfig(subPath, SubDirConfig{Transformer: "test"}), ShouldBeNil)

			runDir := filepath.Join(subPath, "1000")
			So(os.MkdirAll(runDir, 0750), ShouldBeNil)

			writeChunkAndReport(runDir, "chunk.000000", makeFilePairs(0, 10))
			generateDoneStatus(runDir, SubDir{Path: subPath})

			symlinkPath := filepath.Join(subPath, "status")
			So(os.Remove(symlinkPath), ShouldBeNil)

			mock := &mockJobSubmitter{}
			w := NewWatcher(watchDir, mock, cfg)

			err := w.Poll()
			So(err, ShouldBeNil)
			So(mock.submitted, ShouldBeEmpty)

			target, readErr := os.Readlink(symlinkPath)
			So(readErr, ShouldBeNil)
			So(target, ShouldEqual, filepath.Join(filepath.Base(runDir), "status"))
		})

		Convey("regenerates status when symlink points to wrong status file", func() {
			subPath := filepath.Join(watchDir, "proj")
			So(os.MkdirAll(subPath, 0750), ShouldBeNil)

			fofnPath := writeFofn(subPath, generateTmpPaths(10))
			fofnTime := time.Unix(1000, 0)
			So(os.Chtimes(fofnPath, fofnTime, fofnTime), ShouldBeNil)

			So(WriteConfig(subPath, SubDirConfig{Transformer: "test"}), ShouldBeNil)

			runDir := filepath.Join(subPath, "1000")
			So(os.MkdirAll(runDir, 0750), ShouldBeNil)

			writeChunkAndReport(runDir, "chunk.000000", makeFilePairs(0, 10))
			generateDoneStatus(runDir, SubDir{Path: subPath})

			wrongRunDir := filepath.Join(subPath, "900")
			So(os.MkdirAll(wrongRunDir, 0750), ShouldBeNil)
			wrongStatusPath := filepath.Join(wrongRunDir, "status")
			So(os.WriteFile(wrongStatusPath, []byte("wrong"), 0600), ShouldBeNil)

			symlinkPath := filepath.Join(subPath, "status")
			So(os.Remove(symlinkPath), ShouldBeNil)
			So(os.Symlink(wrongStatusPath, symlinkPath), ShouldBeNil)

			mock := &mockJobSubmitter{}
			w := NewWatcher(watchDir, mock, cfg)

			err := w.Poll()
			So(err, ShouldBeNil)
			So(mock.submitted, ShouldBeEmpty)

			target, readErr := os.Readlink(symlinkPath)
			So(readErr, ShouldBeNil)
			So(target, ShouldEqual, filepath.Join(filepath.Base(runDir), "status"))
		})

		Convey("does not rewrite status artefacts for same fofn mtime after completion", func() {
			subPath := filepath.Join(watchDir, "proj")
			So(os.MkdirAll(subPath, 0750),
				ShouldBeNil)

			fofnPath := writeFofn(subPath, generateTmpPaths(10))

			fofnTime := time.Unix(1000, 0)
			So(os.Chtimes(fofnPath, fofnTime, fofnTime), ShouldBeNil)

			So(WriteConfig(subPath, SubDirConfig{Transformer: "test"}), ShouldBeNil)

			runDir := filepath.Join(subPath, "1000")
			So(os.MkdirAll(runDir, 0750),
				ShouldBeNil)

			writeChunkAndReport(runDir, "chunk.000000", makeFilePairs(0, 10))
			generateDoneStatus(runDir, SubDir{Path: subPath})

			statusPath := filepath.Join(runDir, "status")
			knownTime := time.Unix(900, 0)
			So(os.Chtimes(statusPath, knownTime, knownTime), ShouldBeNil)

			statusInfoBefore, statErr := os.Stat(statusPath)
			So(statErr, ShouldBeNil)
			So(statusInfoBefore.ModTime(), ShouldEqual, knownTime)

			symlinkPath := filepath.Join(subPath, "status")
			symlinkInfoBefore, lstatErr := os.Lstat(symlinkPath)
			So(lstatErr, ShouldBeNil)

			beforeStat, ok := symlinkInfoBefore.Sys().(*syscall.Stat_t)
			So(ok, ShouldBeTrue)

			mock := &mockJobSubmitter{}

			w := NewWatcher(watchDir, mock, cfg)

			So(mock.submitted, ShouldBeEmpty)

			err := w.Poll()
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
			So(target, ShouldEqual, filepath.Join(filepath.Base(runDir), "status"))
		})

		Convey("does not regenerate status for externally modified reports in stable done run", func() {
			subPath := filepath.Join(watchDir, "proj")
			So(os.MkdirAll(subPath, 0750), ShouldBeNil)

			fofnPath := writeFofn(subPath, generateTmpPaths(10))
			fofnTime := time.Unix(1000, 0)
			So(os.Chtimes(fofnPath, fofnTime, fofnTime), ShouldBeNil)

			So(WriteConfig(subPath, SubDirConfig{Transformer: "test"}), ShouldBeNil)

			runDir := filepath.Join(subPath, "1000")
			So(os.MkdirAll(runDir, 0750), ShouldBeNil)

			pairs := makeFilePairs(0, 10)
			writeChunkAndReport(runDir, "chunk.000000", pairs)
			generateDoneStatus(runDir, SubDir{Path: subPath})

			// Externally overwrite the report with different statuses.
			writeReportFile(runDir, "chunk.000000", pairs, "missing")

			statusPath := filepath.Join(runDir, "status")
			knownTime := time.Unix(900, 0)
			So(os.Chtimes(statusPath, knownTime, knownTime), ShouldBeNil)

			mock := &mockJobSubmitter{}
			w := NewWatcher(watchDir, mock, cfg)

			err := w.Poll()
			So(err, ShouldBeNil)
			So(mock.submitted, ShouldBeEmpty)

			// Status file was NOT regenerated — known mtime preserved.
			statusInfo, statErr := os.Stat(statusPath)
			So(statErr, ShouldBeNil)
			So(statusInfo.ModTime(), ShouldEqual, knownTime)

			// Stale status still shows original uploaded counts.
			_, counts, parseErr := ParseStatus(statusPath)
			So(parseErr, ShouldBeNil)
			So(counts.Uploaded, ShouldEqual, 10)
			So(counts.Missing, ShouldEqual, 0)
		})

		Convey("cleans stale run directories during phase transition", func() {
			subPath := filepath.Join(watchDir, "proj")
			So(os.MkdirAll(subPath, 0750), ShouldBeNil)

			fofnPath := writeFofn(subPath, generateTmpPaths(10))
			fofnTime := time.Unix(1000, 0)
			So(os.Chtimes(fofnPath, fofnTime, fofnTime), ShouldBeNil)

			So(WriteConfig(subPath, SubDirConfig{Transformer: "test"}), ShouldBeNil)

			runDir := filepath.Join(subPath, "1000")
			So(os.MkdirAll(runDir, 0750), ShouldBeNil)

			oldRunDir := filepath.Join(subPath, "500")
			So(os.MkdirAll(oldRunDir, 0750), ShouldBeNil)

			writeChunkAndReport(runDir, "chunk.000000", makeFilePairs(0, 10))

			// No status file — active run. wr returns no jobs → complete.
			mock := &mockJobSubmitter{}
			w := NewWatcher(watchDir, mock, cfg)

			err := w.Poll()
			So(err, ShouldBeNil)
			So(mock.submitted, ShouldBeEmpty)

			// status was generated (transition to done), old dir cleaned
			_, statErr := os.Stat(oldRunDir)
			So(os.IsNotExist(statErr), ShouldBeTrue)
			_, statErr = os.Stat(runDir)
			So(statErr, ShouldBeNil)
		})

		Convey("returns error when stale-run cleanup fails during phase transition", func() {
			subPath := filepath.Join(watchDir, "proj")
			So(os.MkdirAll(subPath, 0750), ShouldBeNil)

			fofnPath := writeFofn(subPath, generateTmpPaths(10))
			fofnTime := time.Unix(1000, 0)
			So(os.Chtimes(fofnPath, fofnTime, fofnTime), ShouldBeNil)

			So(WriteConfig(subPath, SubDirConfig{Transformer: "test"}), ShouldBeNil)

			runDir := filepath.Join(subPath, "1000")
			So(os.MkdirAll(runDir, 0750), ShouldBeNil)

			// Create old run dir with undeletable content so
			// os.RemoveAll fails on the nested directory.
			oldRunDir := filepath.Join(subPath, "500")
			nestedDir := filepath.Join(oldRunDir, "nested")
			So(os.MkdirAll(nestedDir, 0750), ShouldBeNil)
			So(os.WriteFile(filepath.Join(nestedDir, "file"), []byte("x"), 0600), ShouldBeNil)
			So(os.Chmod(nestedDir, 0550), ShouldBeNil)

			defer func() {
				restoreErr := os.Chmod(nestedDir, 0750)
				So(restoreErr, ShouldBeNil)
			}()

			writeChunkAndReport(runDir, "chunk.000000", makeFilePairs(0, 10))

			// No status file — active run. wr returns no jobs → complete.
			mock := &mockJobSubmitter{}
			w := NewWatcher(watchDir, mock, cfg)

			err := w.Poll()
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "remove old run dir")

			_, statErr := os.Stat(oldRunDir)
			So(statErr, ShouldBeNil)
		})

		Convey("processes updated fofn when completed status artefacts already exist", func() {
			subPath := filepath.Join(watchDir, "proj")
			So(os.MkdirAll(subPath, 0750), ShouldBeNil)

			fofnPath := writeFofn(subPath, generateTmpPaths(10))
			fofnTime := time.Unix(1000, 0)
			So(os.Chtimes(fofnPath, fofnTime, fofnTime), ShouldBeNil)

			So(WriteConfig(subPath, SubDirConfig{Transformer: "test"}), ShouldBeNil)

			runDir := filepath.Join(subPath, "1000")
			So(os.MkdirAll(runDir, 0750), ShouldBeNil)

			writeChunkAndReport(runDir, "chunk.000000", makeFilePairs(0, 10))
			generateDoneStatus(runDir, SubDir{Path: subPath})

			generateDoneStatus(runDir, SubDir{Path: subPath})

			mock := &mockJobSubmitter{}
			w := NewWatcher(watchDir, mock, cfg)

			updateFofnMtime(subPath, generateTmpPaths(15), 2000)

			err := w.Poll()
			So(err, ShouldBeNil)
			So(mock.submitted, ShouldNotBeEmpty)

			// New run dir created for mtime 2000
			_, newMtime, found, findErr := findRunDir(subPath)
			So(findErr, ShouldBeNil)
			So(found, ShouldBeTrue)
			So(newMtime, ShouldEqual, 2000)
		})

		Convey("refreshes status after buried chunk is retried successfully", func() {
			subPath := filepath.Join(watchDir, "proj")
			So(os.MkdirAll(subPath, 0750), ShouldBeNil)

			fofnPath := writeFofn(subPath, generateTmpPaths(10))
			fofnTime := time.Unix(1000, 0)
			So(os.Chtimes(fofnPath, fofnTime, fofnTime), ShouldBeNil)

			So(WriteConfig(subPath, SubDirConfig{Transformer: "test"}), ShouldBeNil)

			runDir := filepath.Join(subPath, "1000")
			So(os.MkdirAll(runDir, 0750), ShouldBeNil)

			writeChunkAndReport(runDir, "chunk.000000", makeFilePairs(0, 5))
			writeChunkOnly(runDir, "chunk.000001", makeFilePairs(5, 10))

			So(GenerateStatus(
				runDir,
				[]string{"chunk.000001"},
			), ShouldBeNil)

			statusPath := filepath.Join(runDir, "status")
			_, initialCounts, parseErr := ParseStatus(statusPath)
			So(parseErr, ShouldBeNil)
			So(initialCounts.Uploaded, ShouldEqual, 5)
			So(initialCounts.NotProcessed, ShouldEqual, 5)

			// Simulate retry: write the previously missing report
			writeReportFile(runDir, "chunk.000001", makeFilePairs(5, 10), "uploaded")

			// mock returns no buried jobs (retry succeeded), but a completed
			// job with EndTime after status mtime to trigger regen.
			mock := &mockJobSubmitter{
				allJobs: []*jobqueue.Job{{
					RepGroup: "ibackup_fofn_proj_1000",
					State:    jobqueue.JobStateComplete,
					Cmd:      "ibackup put -f chunk.000001",
					EndTime:  time.Now().Add(time.Hour),
				}},
			}
			w := NewWatcher(watchDir, mock, cfg)

			err := w.Poll()
			So(err, ShouldBeNil)
			So(mock.submitted, ShouldBeEmpty)

			_, counts, parseErr := ParseStatus(statusPath)
			So(parseErr, ShouldBeNil)
			So(counts.Uploaded, ShouldEqual, 10)
			So(counts.NotProcessed, ShouldEqual, 0)
		})

		Convey("done run submits no new jobs", func() {
			subPath := filepath.Join(watchDir, "proj")
			So(os.MkdirAll(subPath, 0750), ShouldBeNil)

			fofnPath := writeFofn(subPath, generateTmpPaths(10))
			fofnTime := time.Unix(1000, 0)
			So(os.Chtimes(fofnPath, fofnTime, fofnTime), ShouldBeNil)

			So(WriteConfig(subPath, SubDirConfig{Transformer: "test"}), ShouldBeNil)

			runDir := filepath.Join(subPath, "1000")
			So(os.MkdirAll(runDir, 0750), ShouldBeNil)

			writeChunkAndReport(runDir, "chunk.000000", makeFilePairs(0, 10))
			generateDoneStatus(runDir, SubDir{Path: subPath})

			mock := &mockJobSubmitter{}
			w := NewWatcher(watchDir, mock, cfg)

			err := w.Poll()
			So(err, ShouldBeNil)
			So(mock.submitted, ShouldBeEmpty)
		})

		Convey("done run with intact artefacts does not regenerate status on subsequent polls", func() {
			subPath := filepath.Join(watchDir, "proj")
			So(os.MkdirAll(subPath, 0750), ShouldBeNil)

			fofnPath := writeFofn(subPath, generateTmpPaths(10))
			fofnTime := time.Unix(1000, 0)
			So(os.Chtimes(fofnPath, fofnTime, fofnTime), ShouldBeNil)

			So(WriteConfig(subPath, SubDirConfig{Transformer: "test"}), ShouldBeNil)

			runDir := filepath.Join(subPath, "1000")
			So(os.MkdirAll(runDir, 0750), ShouldBeNil)

			writeChunkAndReport(runDir, "chunk.000000", makeFilePairs(0, 10))
			generateDoneStatus(runDir, SubDir{Path: subPath})

			statusPath := filepath.Join(runDir, "status")
			knownTime := time.Unix(900, 0)
			So(os.Chtimes(statusPath, knownTime, knownTime), ShouldBeNil)

			mock := &mockJobSubmitter{}
			w := NewWatcher(watchDir, mock, cfg)

			// First poll: artefacts intact, status file not regenerated.
			err := w.Poll()
			So(err, ShouldBeNil)

			statusInfo, statErr := os.Stat(statusPath)
			So(statErr, ShouldBeNil)
			So(statusInfo.ModTime(), ShouldEqual, knownTime)

			// Break the symlink → next poll repairs it.
			symlinkPath := filepath.Join(subPath, "status")
			So(os.Remove(symlinkPath), ShouldBeNil)

			err = w.Poll()
			So(err, ShouldBeNil)

			// Symlink was repaired.
			target, readErr := os.Readlink(symlinkPath)
			So(readErr, ShouldBeNil)
			So(target, ShouldEqual, filepath.Join(filepath.Base(runDir), "status"))

			// Status file was NOT regenerated — mtime preserved.
			statusInfo, statErr = os.Stat(statusPath)
			So(statErr, ShouldBeNil)
			So(statusInfo.ModTime(), ShouldEqual, knownTime)
		})

		Convey("done run with intact artefacts restarts when fofn changes", func() {
			subPath := filepath.Join(watchDir, "proj")
			So(os.MkdirAll(subPath, 0750), ShouldBeNil)

			fofnPath := writeFofn(subPath, generateTmpPaths(10))
			fofnTime := time.Unix(1000, 0)
			So(os.Chtimes(fofnPath, fofnTime, fofnTime), ShouldBeNil)

			So(WriteConfig(subPath, SubDirConfig{Transformer: "test"}), ShouldBeNil)

			runDir := filepath.Join(subPath, "1000")
			So(os.MkdirAll(runDir, 0750), ShouldBeNil)

			writeChunkAndReport(runDir, "chunk.000000", makeFilePairs(0, 10))
			generateDoneStatus(runDir, SubDir{Path: subPath})

			updateFofnMtime(subPath, generateTmpPaths(15), 2000)

			mock := &mockJobSubmitter{}
			w := NewWatcher(watchDir, mock, cfg)

			err := w.Poll()
			So(err, ShouldBeNil)
			So(mock.submitted, ShouldNotBeEmpty)

			// New run dir created with updated mtime.
			newRunDir, newMtime, found, findErr := findRunDir(subPath)
			So(findErr, ShouldBeNil)
			So(found, ShouldBeTrue)
			So(newMtime, ShouldEqual, 2000)
			So(newRunDir, ShouldNotEqual, runDir)
		})

		Convey("repairs symlink without regenerating status file", func() {
			subPath := filepath.Join(watchDir, "proj")
			So(os.MkdirAll(subPath, 0750), ShouldBeNil)

			fofnPath := writeFofn(subPath, generateTmpPaths(10))
			fofnTime := time.Unix(1000, 0)
			So(os.Chtimes(fofnPath, fofnTime, fofnTime), ShouldBeNil)

			So(WriteConfig(subPath, SubDirConfig{Transformer: "test"}), ShouldBeNil)

			runDir := filepath.Join(subPath, "1000")
			So(os.MkdirAll(runDir, 0750), ShouldBeNil)

			writeChunkAndReport(runDir, "chunk.000000", makeFilePairs(0, 10))
			generateDoneStatus(runDir, SubDir{Path: subPath})

			// Break the symlink
			symlinkPath := filepath.Join(subPath, "status")
			So(os.Remove(symlinkPath), ShouldBeNil)

			// Mark status file mtime to verify it's not regenerated
			statusPath := filepath.Join(runDir, "status")
			knownTime := time.Unix(900, 0)
			So(os.Chtimes(statusPath, knownTime, knownTime), ShouldBeNil)

			mock := &mockJobSubmitter{}
			w := NewWatcher(watchDir, mock, cfg)

			err := w.Poll()
			So(err, ShouldBeNil)
			// wr is always queried once per poll cycle.
			So(mock.findCallCount, ShouldEqual, 1)

			// Symlink was repaired
			target, readErr := os.Readlink(symlinkPath)
			So(readErr, ShouldBeNil)
			So(target, ShouldEqual, filepath.Join(filepath.Base(runDir), "status"))

			// Status file was NOT regenerated — mtime preserved
			statusInfo, statErr := os.Stat(statusPath)
			So(statErr, ShouldBeNil)
			So(statusInfo.ModTime(), ShouldEqual, knownTime)
		})

		Convey("waits for running jobs before restarting when fofn changes during active phase", func() {
			subPath := filepath.Join(watchDir, "proj")
			So(os.MkdirAll(subPath, 0750), ShouldBeNil)

			fofnPath := writeFofn(subPath, generateTmpPaths(10))
			fofnTime := time.Unix(1000, 0)
			So(os.Chtimes(fofnPath, fofnTime, fofnTime), ShouldBeNil)

			So(WriteConfig(subPath, SubDirConfig{Transformer: "test"}), ShouldBeNil)

			runDir := filepath.Join(subPath, "1000")
			So(os.MkdirAll(runDir, 0750), ShouldBeNil)

			writeChunkOnly(runDir, "chunk.000000", makeFilePairs(0, 10))

			// fofn has changed
			updateFofnMtime(subPath, generateTmpPaths(15), 2000)

			// But jobs are still running
			mock := &mockJobSubmitter{
				allJobs: []*jobqueue.Job{
					{RepGroup: makeRepGroup(subPath, 1000), Cmd: "running"},
				},
			}

			w := NewWatcher(watchDir, mock, cfg)

			err := w.Poll()
			So(err, ShouldBeNil)
			So(mock.submitted, ShouldBeEmpty)

			// Run dir unchanged — still waiting for old run.
			_, runMtime, found, findErr := findRunDir(subPath)
			So(findErr, ShouldBeNil)
			So(found, ShouldBeTrue)
			So(runMtime, ShouldEqual, 1000)
		})

		Convey("regenerates status when status file deleted from done run", func() {
			subPath := filepath.Join(watchDir, "proj")
			So(os.MkdirAll(subPath, 0750), ShouldBeNil)

			fofnPath := writeFofn(subPath, generateTmpPaths(10))
			fofnTime := time.Unix(1000, 0)
			So(os.Chtimes(fofnPath, fofnTime, fofnTime), ShouldBeNil)

			So(WriteConfig(subPath, SubDirConfig{Transformer: "test"}), ShouldBeNil)

			runDir := filepath.Join(subPath, "1000")
			So(os.MkdirAll(runDir, 0750), ShouldBeNil)

			writeChunkAndReport(runDir, "chunk.000000", makeFilePairs(0, 10))
			generateDoneStatus(runDir, SubDir{Path: subPath})

			// Delete the status file
			So(os.Remove(filepath.Join(runDir, "status")), ShouldBeNil)

			mock := &mockJobSubmitter{}
			w := NewWatcher(watchDir, mock, cfg)

			err := w.Poll()
			So(err, ShouldBeNil)
			So(mock.submitted, ShouldBeEmpty)
			So(mock.findCallCount, ShouldEqual, 1)

			// Status file regenerated
			statusPath := filepath.Join(runDir, "status")
			entries, counts, parseErr := ParseStatus(statusPath)
			So(parseErr, ShouldBeNil)
			So(entries, ShouldHaveLength, 10)
			So(counts.Uploaded, ShouldEqual, 10)

			// Symlink updated
			symlinkPath := filepath.Join(subPath, "status")
			target, readErr := os.Readlink(symlinkPath)
			So(readErr, ShouldBeNil)
			So(target, ShouldEqual, filepath.Join(filepath.Base(runDir), "status"))
		})
	})
}

func TestWatcherParallel(t *testing.T) {
	Convey("Watcher.Poll parallel processing",
		t, func() {
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
					setupSubDir(
						watchDir, name,
						generateTmpPaths(5),
						SubDirConfig{Transformer: "test"},
					)
				}

				mock := &mockJobSubmitter{}
				w := NewWatcher(watchDir, mock, cfg)

				err := w.Poll()
				So(err, ShouldBeNil)
				So(mock.submitted,
					ShouldHaveLength, 3)

				for _, name := range []string{
					"proj1", "proj2", "proj3",
				} {
					_, _, found, findErr := findRunDir(
						filepath.Join(watchDir, name),
					)
					So(findErr, ShouldBeNil)
					So(found, ShouldBeTrue)
				}
			})

			Convey("only submits jobs for new subdirectory when 2 have active runs", func() {
				for _, name := range []string{
					"proj1", "proj2",
				} {
					setupSubDir(
						watchDir, name,
						generateTmpPaths(5),
						SubDirConfig{Transformer: "test"},
					)
				}

				mock := &mockJobSubmitter{}
				w := NewWatcher(watchDir, mock, cfg)

				err := w.Poll()
				So(err, ShouldBeNil)

				initialCount := len(mock.submitted)
				So(initialCount, ShouldEqual, 2)

				// Build running jobs from the run dirs that were created.
				var runningJobs []*jobqueue.Job

				for _, name := range []string{
					"proj1", "proj2",
				} {
					subPath := filepath.Join(watchDir, name)
					_, mtime, found, findErr := findRunDir(subPath)
					So(findErr, ShouldBeNil)
					So(found, ShouldBeTrue)

					runningJobs = append(runningJobs,
						&jobqueue.Job{
							RepGroup: makeRepGroup(subPath, mtime),
							Cmd:      "running",
						})
				}

				mock.allJobs = runningJobs

				setupSubDir(
					watchDir, "proj3",
					generateTmpPaths(5),
					SubDirConfig{Transformer: "test"},
				)

				err = w.Poll()
				So(err, ShouldBeNil)
				So(mock.submitted,
					ShouldHaveLength,
					initialCount+1)

				for _, name := range []string{
					"proj1", "proj2", "proj3",
				} {
					_, _, found, findErr := findRunDir(
						filepath.Join(watchDir, name),
					)
					So(findErr, ShouldBeNil)
					So(found, ShouldBeTrue)
				}
			})
		})
}

// setupSubDir creates a subdirectory with a fofn and
// config.yml inside the watch directory.
func setupSubDir(
	watchDir, name string,
	paths []string,
	cfg SubDirConfig,
) SubDir {
	subPath := filepath.Join(watchDir, name)
	So(os.MkdirAll(subPath, 0750), ShouldBeNil)

	writeFofn(subPath, paths)

	So(WriteConfig(subPath, cfg), ShouldBeNil)

	return subDirWithMtime(subPath)
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
func subDirWithMtime(subPath string) SubDir {
	info, err := os.Stat(filepath.Join(subPath, fofnFilename))
	So(err, ShouldBeNil)

	return SubDir{Path: subPath, FofnMtime: info.ModTime().Unix()}
}

// generateDoneStatus creates a complete "done" state: status file + symlink.
// Use this when setting up a test that expects artefacts to be intact.
// The symlink uses a relative target (runDirName/status) matching the
// watcher's createStatusSymlink convention.
func generateDoneStatus(runDir string, subDir SubDir) {
	So(GenerateStatus(runDir, nil), ShouldBeNil)

	symlinkPath := filepath.Join(subDir.Path, "status")
	relTarget := filepath.Join(filepath.Base(runDir), "status")

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
func updateFofnMtime(
	subDirPath string, paths []string, mtime int64,
) {
	writeFofn(subDirPath, paths)

	t := time.Unix(mtime, 0)

	So(os.Chtimes(filepath.Join(subDirPath, "fofn"), t, t), ShouldBeNil)
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
		writeReportFile(runDir, base, pairs, "uploaded")
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

		if err := os.MkdirAll(subPath, 0750); err != nil {
			rt.Fatal(err)
		}

		mtime := rapid.Int64Range(1000, 9999).Draw(rt, "mtime")
		numChunks := rapid.IntRange(1, 5).Draw(rt, "numChunks")
		numBuried := rapid.IntRange(0, numChunks).Draw(rt, "numBuried")

		runDir := filepath.Join(subPath, strconv.FormatInt(mtime, 10))
		if err := os.MkdirAll(runDir, 0750); err != nil {
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

			mustWriteReportT(rt, runDir, chunkName, pairs, "uploaded")
		}

		// Build a RunJobStatus with buried info and a LastCompletedTime
		// that forces status generation.
		status := RunJobStatus{
			LastCompletedTime: time.Now().Add(time.Hour),
		}

		status.BuriedChunks = append(status.BuriedChunks, buriedChunkNames...)

		// Create valid initial state via settle
		w := NewWatcher(watchDir, &mockJobSubmitter{}, ProcessSubDirConfig{})
		sd := SubDir{Path: subPath, FofnMtime: mtime}

		if err := w.settle(sd, runDir, status); err != nil {
			rt.Fatalf("initial settle: %v", err)
		}

		// Apply random damage
		statusPath := filepath.Join(runDir, "status")
		symlinkPath := filepath.Join(subPath, "status")

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
		repairStatus := RunJobStatus{
			LastCompletedTime: time.Now().Add(2 * time.Hour),
		}

		repairStatus.BuriedChunks = append(repairStatus.BuriedChunks, buriedChunkNames...)

		if err := w.settle(sd, runDir, repairStatus); err != nil {
			rt.Fatalf("repair settle: %v", err)
		}

		// Invariant 1: status file exists
		if _, err := os.Stat(statusPath); err != nil {
			rt.Fatalf("status file should exist after settle: %v", err)
		}

		// Invariant 2: symlink semantics depend on burial state.
		verifySymlink(rt, numBuried, symlinkPath, statusPath)
	})
}

// verifySymlink checks that the symlink points to the correct relative status
// file target. Since wr is queried every poll cycle, buried runs also have a
// valid symlink showing which chunks completed.
func verifySymlink(rt *rapid.T, _ int, symlinkPath, statusPath string) {
	target, err := os.Readlink(symlinkPath)
	if err != nil {
		rt.Fatalf("symlink should exist after settle: %v", err)
	}

	// statusPath is absolute: runDir/status. We expect the symlink target
	// to be the relative form: runDirName/status.
	runDir := filepath.Dir(statusPath)
	expected := filepath.Join(filepath.Base(runDir), "status")

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

		if err := os.MkdirAll(subPath, 0750); err != nil {
			rt.Fatal(err)
		}

		mtime := rapid.Int64Range(1000, 9999).Draw(rt, "mtime")
		numChunks := rapid.IntRange(1, 3).Draw(rt, "numChunks")

		runDir := filepath.Join(subPath, strconv.FormatInt(mtime, 10))
		if err := os.MkdirAll(runDir, 0750); err != nil {
			rt.Fatal(err)
		}

		for i := range numChunks {
			chunkName := fmt.Sprintf("chunk.%06d", i)
			pairs := makeFilePairs(i*5, i*5+5)

			mustWriteChunkT(rt, runDir, chunkName, pairs)
			mustWriteReportT(rt, runDir, chunkName, pairs, "uploaded")
		}

		w := NewWatcher(watchDir, &mockJobSubmitter{}, ProcessSubDirConfig{})
		sd := SubDir{Path: subPath, FofnMtime: mtime}

		// First settle with a recent LastCompletedTime to force status generation.
		status := RunJobStatus{
			LastCompletedTime: time.Now().Add(time.Hour),
		}

		if err := w.settle(sd, runDir, status); err != nil {
			rt.Fatalf("first settle: %v", err)
		}

		statusPath := filepath.Join(runDir, "status")

		statusInfo, err := os.Stat(statusPath)
		if err != nil {
			rt.Fatal(err)
		}

		firstMtime := statusInfo.ModTime()

		// Second settle with zero LastCompletedTime — needsStatusRegen
		// returns false because status file exists and no new completions.
		idempotentStatus := RunJobStatus{}

		settleErr := w.settle(sd, runDir, idempotentStatus)
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

		if err := os.MkdirAll(subPath, 0750); err != nil {
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

		if err := WriteConfig(subPath, SubDirConfig{Transformer: "test"}); err != nil {
			rt.Fatal(err)
		}

		// Create completed run
		runDir := filepath.Join(subPath, "1000")
		if err := os.MkdirAll(runDir, 0750); err != nil {
			rt.Fatal(err)
		}

		numChunks := rapid.IntRange(1, 4).Draw(rt, "numChunks")
		for i := range numChunks {
			chunkName := fmt.Sprintf("chunk.%06d", i)
			pairs := makeFilePairs(i*5, i*5+5)

			mustWriteChunkT(rt, runDir, chunkName, pairs)
			mustWriteReportT(rt, runDir, chunkName, pairs, "uploaded")
		}

		if err := GenerateStatus(runDir, nil); err != nil {
			rt.Fatal(err)
		}

		statusPath := filepath.Join(runDir, "status")
		relTarget := filepath.Join(filepath.Base(runDir), "status")

		if err := os.Symlink(relTarget, filepath.Join(subPath, "status")); err != nil {
			rt.Fatal(err)
		}

		// Apply random damage
		symlinkPath := filepath.Join(subPath, "status")

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
		w := NewWatcher(watchDir, mock, ProcessSubDirConfig{
			MinChunk: 10,
			MaxChunk: 10,
			RandSeed: 1,
		})

		if err := w.Poll(); err != nil {
			rt.Fatalf("Poll failed: %v", err)
		}

		// Invariant: status file exists
		if _, err := os.Stat(statusPath); err != nil {
			rt.Fatalf("status file missing after poll (damage=%s): %v", damage, err)
		}

		// Invariant: symlink correct (relative target)
		target, err := os.Readlink(symlinkPath)
		if err != nil {
			rt.Fatalf("symlink missing after poll (damage=%s): %v", damage, err)
		}

		expectedTarget := filepath.Join(filepath.Base(runDir), "status")
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
