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
)

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

func TestProcessSubDir(t *testing.T) {
	Convey("ProcessSubDir", t, func() {
		So(transformer.Register("test", `^/tmp/(.*)$`, "/irods/$1"), ShouldBeNil)

		watchDir := t.TempDir()

		Convey("creates run dir and submits jobs for "+
			"25 paths", func() {
			paths := generateTmpPaths(25)
			subDir := setupSubDir(watchDir, "proj1", paths, "transformer: test\n")

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
					fmt.Sprintf("--fofn %q", dirName))
			}
		})

		Convey("includes --no_replace when freeze is "+
			"true", func() {
			paths := generateTmpPaths(5)
			subDir := setupSubDir(
				watchDir, "proj2", paths,
				"transformer: test\nfreeze: true\n",
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

		Convey("returns error when config.yml is "+
			"missing", func() {
			subPath := filepath.Join(watchDir, "proj3")
			So(os.MkdirAll(subPath, 0750),
				ShouldBeNil)

			writeFofn(subPath, generateTmpPaths(5))

			sd := SubDir{Path: subPath}
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
				subDir := setupSubDir(watchDir, "proj4", nil, "transformer: test\n")

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

		Convey("sets GID on run dir and chunk files",
			func() {
				paths := generateTmpPaths(25)
				subDir := setupSubDir(
					watchDir, "proj5", paths,
					"transformer: test\n",
				)

				mock := &mockJobSubmitter{}
				cfg := ProcessSubDirConfig{
					MinChunk: 10,
					MaxChunk: 10,
					RandSeed: 1,
				}

				state, err := ProcessSubDir(subDir, mock, cfg)
				So(err, ShouldBeNil)

				expectedGID := fileGID(watchDir)
				So(fileGID(state.RunDir), ShouldEqual,
					expectedGID)

				runInfo, statErr := os.Stat(state.RunDir)
				So(statErr, ShouldBeNil)
				So(runInfo.Mode()&0040,
					ShouldNotEqual, 0)

				entries, readErr := os.ReadDir(state.RunDir)
				So(readErr, ShouldBeNil)

				for _, e := range entries {
					cp := filepath.Join(state.RunDir, e.Name())
					So(fileGID(cp), ShouldEqual,
						expectedGID)

					ci, ciErr := os.Stat(cp)
					So(ciErr, ShouldBeNil)
					So(ci.Mode()&0040,
						ShouldNotEqual, 0)
				}
			})

		Convey("includes --meta when config has "+
			"metadata", func() {
			paths := generateTmpPaths(5)
			subDir := setupSubDir(
				watchDir, "proj6", paths,
				"transformer: test\nmetadata:\n"+
					"  colour: red\n",
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
					`--meta "colour=red"`)
			}
		})

		Convey("omits --meta when config has no "+
			"metadata", func() {
			paths := generateTmpPaths(5)
			subDir := setupSubDir(watchDir, "proj7", paths, "transformer: test\n")

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

		Convey("creates 100 chunks and 100 jobs for "+
			"50000 paths with default bounds", func() {
			paths := generateTmpPaths(50000)
			subDir := setupSubDir(watchDir, "proj_vc3", paths, "transformer: test\n")

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

		Convey("writes status file and symlink "+
			"for 3 complete reports", func() {
			subDir, runDir := setupRunDir(watchDir, "proj1")

			writeChunkAndReport(runDir, "chunk.000000", makeFilePairs(0, 5))
			writeChunkAndReport(runDir, "chunk.000001", makeFilePairs(5, 10))
			writeChunkAndReport(runDir, "chunk.000002", makeFilePairs(10, 15))

			err := GenerateStatus(runDir, subDir, nil)
			So(err, ShouldBeNil)

			statusPath := filepath.Join(runDir, "status")
			entries, counts, parseErr := ParseStatus(statusPath)
			So(parseErr, ShouldBeNil)
			So(entries, ShouldHaveLength, 15)
			So(counts.Uploaded, ShouldEqual, 15)

			symlinkPath := filepath.Join(subDir.Path, "status")
			target, readErr := os.Readlink(symlinkPath)
			So(readErr, ShouldBeNil)
			So(target, ShouldEqual, statusPath)
		})

		Convey("handles buried chunk with no "+
			"report file", func() {
			subDir, runDir := setupRunDir(watchDir, "proj2")

			writeChunkAndReport(runDir, "chunk.000000", makeFilePairs(0, 5))
			writeChunkAndReport(runDir, "chunk.000001", makeFilePairs(5, 10))
			writeChunkOnly(runDir, "chunk.000002", makeFilePairs(10, 20))

			err := GenerateStatus(
				runDir, subDir,
				[]string{"chunk.000002"},
			)
			So(err, ShouldBeNil)

			statusPath := filepath.Join(runDir, "status")
			entries, counts, parseErr := ParseStatus(statusPath)
			So(parseErr, ShouldBeNil)
			So(entries, ShouldHaveLength, 20)
			So(counts.Uploaded, ShouldEqual, 10)
			So(counts.NotProcessed, ShouldEqual, 10)

			symlinkPath := filepath.Join(subDir.Path, "status")
			target, readErr := os.Readlink(symlinkPath)
			So(readErr, ShouldBeNil)
			So(target, ShouldEqual, statusPath)
		})

		Convey("handles buried chunk with "+
			"incomplete report", func() {
			subDir, runDir := setupRunDir(watchDir, "proj3")

			pairs := makeFilePairs(0, 10)
			writeChunkFile(runDir, "chunk.000000", pairs)
			writeReportFile(runDir, "chunk.000000", pairs[:5], "uploaded")

			err := GenerateStatus(
				runDir, subDir,
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

		Convey("sets GID on status file matching "+
			"watch directory", func() {
			subDir, runDir := setupRunDir(watchDir, "proj4")

			writeChunkAndReport(runDir, "chunk.000000", makeFilePairs(0, 3))

			err := GenerateStatus(runDir, subDir, nil)
			So(err, ShouldBeNil)

			statusPath := filepath.Join(runDir, "status")
			expectedGID := fileGID(watchDir)
			So(fileGID(statusPath), ShouldEqual,
				expectedGID)

			info, statErr := os.Stat(statusPath)
			So(statErr, ShouldBeNil)
			So(info.Mode()&0040,
				ShouldNotEqual, 0)
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

		Convey("first poll submits jobs and records "+
			"active run", func() {
			paths := generateTmpPaths(25)
			subDir := setupSubDir(watchDir, "proj1", paths, "transformer: test\n")

			mock := &mockJobSubmitter{}
			w := NewWatcher(watchDir, mock, cfg)

			err := w.Poll()
			So(err, ShouldBeNil)
			So(mock.submitted, ShouldHaveLength, 3)

			run, ok := w.activeRuns[subDir.Path]
			So(ok, ShouldBeTrue)
			So(run.RepGroup, ShouldNotBeEmpty)
			So(run.RunDir, ShouldNotBeEmpty)
		})

		Convey("skips when active run has "+
			"incomplete jobs", func() {
			paths := generateTmpPaths(25)
			subDir := setupSubDir(watchDir, "proj2", paths, "transformer: test\n")

			mock := &mockJobSubmitter{}
			w := NewWatcher(watchDir, mock, cfg)

			err := w.Poll()
			So(err, ShouldBeNil)

			submitCount := len(mock.submitted)

			mock.incomplete = []*jobqueue.Job{
				{Cmd: "running"},
			}

			err = w.Poll()
			So(err, ShouldBeNil)
			So(mock.submitted, ShouldHaveLength,
				submitCount)

			_, ok := w.activeRuns[subDir.Path]
			So(ok, ShouldBeTrue)
		})

		Convey("completes successful run and starts "+
			"new run when fofn changed", func() {
			paths := generateTmpPaths(25)
			subDir := setupSubDir(watchDir, "proj3", paths, "transformer: test\n")

			mock := &mockJobSubmitter{}
			w := NewWatcher(watchDir, mock, cfg)

			err := w.Poll()
			So(err, ShouldBeNil)

			firstCount := len(mock.submitted)
			run := w.activeRuns[subDir.Path]

			writeReportsForChunks(run.RunDir)

			mock.incomplete = nil
			mock.buried = nil

			updateFofnMtime(subDir.Path, generateTmpPaths(15), run.Mtime+1000)

			err = w.Poll()
			So(err, ShouldBeNil)

			statusPath := filepath.Join(run.RunDir, "status")
			_, statErr := os.Stat(statusPath)
			So(statErr, ShouldBeNil)

			symlinkPath := filepath.Join(subDir.Path, "status")
			_, readErr := os.Readlink(symlinkPath)
			So(readErr, ShouldBeNil)

			So(len(mock.submitted),
				ShouldBeGreaterThan, firstCount)

			newRun, ok := w.activeRuns[subDir.Path]
			So(ok, ShouldBeTrue)
			So(newRun.RunDir, ShouldNotEqual,
				run.RunDir)
		})

		Convey("completes successful run with no "+
			"new run when fofn unchanged", func() {
			paths := generateTmpPaths(25)
			subDir := setupSubDir(watchDir, "proj4", paths, "transformer: test\n")

			mock := &mockJobSubmitter{}
			w := NewWatcher(watchDir, mock, cfg)

			err := w.Poll()
			So(err, ShouldBeNil)

			submitCount := len(mock.submitted)
			run := w.activeRuns[subDir.Path]

			writeReportsForChunks(run.RunDir)

			mock.incomplete = nil
			mock.buried = nil

			err = w.Poll()
			So(err, ShouldBeNil)

			statusPath := filepath.Join(run.RunDir, "status")
			_, statErr := os.Stat(statusPath)
			So(statErr, ShouldBeNil)

			So(mock.submitted, ShouldHaveLength,
				submitCount)

			_, ok := w.activeRuns[subDir.Path]
			So(ok, ShouldBeFalse)
		})

		Convey("generates not_processed status for "+
			"buried chunk when fofn unchanged",
			func() {
				paths := generateTmpPaths(25)
				subDir := setupSubDir(
					watchDir, "proj5", paths,
					"transformer: test\n",
				)

				mock := &mockJobSubmitter{}
				w := NewWatcher(watchDir, mock, cfg)

				err := w.Poll()
				So(err, ShouldBeNil)

				submitCount := len(mock.submitted)
				run := w.activeRuns[subDir.Path]

				buriedPairs := readChunkPairs(
					filepath.Join(
						run.RunDir, "chunk.000002",
					),
				)
				buriedCount := len(buriedPairs)
				uploadedCount := 25 - buriedCount

				writeReportsExcept(run.RunDir, "chunk.000002")

				mock.incomplete = nil
				mock.buried = []*jobqueue.Job{
					{Cmd: "ibackup put " +
						"-f chunk.000002"},
				}

				err = w.Poll()
				So(err, ShouldBeNil)

				statusPath := filepath.Join(run.RunDir, "status")
				entries, counts, parseErr :=
					ParseStatus(statusPath)
				So(parseErr, ShouldBeNil)
				So(entries, ShouldHaveLength, 25)
				So(counts.Uploaded, ShouldEqual,
					uploadedCount)
				So(counts.NotProcessed,
					ShouldEqual, buriedCount)

				_, ok := w.activeRuns[subDir.Path]
				So(ok, ShouldBeTrue)

				So(mock.submitted, ShouldHaveLength,
					submitCount)
			})

		Convey("deletes buried jobs and starts new "+
			"run when fofn changed", func() {
			paths := generateTmpPaths(25)
			subDir := setupSubDir(watchDir, "proj6", paths, "transformer: test\n")

			mock := &mockJobSubmitter{}
			w := NewWatcher(watchDir, mock, cfg)

			err := w.Poll()
			So(err, ShouldBeNil)

			firstCount := len(mock.submitted)
			run := w.activeRuns[subDir.Path]

			writeReportsExcept(run.RunDir, "chunk.000002")

			buriedJob := &jobqueue.Job{
				Cmd: "ibackup put -f chunk.000002",
			}
			mock.incomplete = nil
			mock.buried = []*jobqueue.Job{buriedJob}

			updateFofnMtime(subDir.Path, generateTmpPaths(15), run.Mtime+1000)

			err = w.Poll()
			So(err, ShouldBeNil)

			statusPath := filepath.Join(run.RunDir, "status")
			_, statErr := os.Stat(statusPath)
			So(statErr, ShouldBeNil)

			So(mock.deleted, ShouldNotBeEmpty)

			So(len(mock.submitted),
				ShouldBeGreaterThan, firstCount)

			newRun, ok := w.activeRuns[subDir.Path]
			So(ok, ShouldBeTrue)
			So(newRun.RunDir, ShouldNotEqual,
				run.RunDir)
		})

		Convey("deletes old run directories on "+
			"successful completion", func() {
			paths := generateTmpPaths(25)
			subDir := setupSubDir(watchDir, "proj7", paths, "transformer: test\n")

			mock := &mockJobSubmitter{}
			w := NewWatcher(watchDir, mock, cfg)

			err := w.Poll()
			So(err, ShouldBeNil)

			run := w.activeRuns[subDir.Path]

			oldRunDir := filepath.Join(subDir.Path, "500")
			So(os.MkdirAll(oldRunDir, 0750),
				ShouldBeNil)

			writeReportsForChunks(run.RunDir)

			mock.incomplete = nil
			mock.buried = nil

			err = w.Poll()
			So(err, ShouldBeNil)

			_, statErr := os.Stat(oldRunDir)
			So(os.IsNotExist(statErr), ShouldBeTrue)

			_, statErr = os.Stat(run.RunDir)
			So(statErr, ShouldBeNil)
		})

		Convey("updates status symlink after second "+
			"run completes", func() {
			paths := generateTmpPaths(25)
			subDir := setupSubDir(watchDir, "proj8", paths, "transformer: test\n")

			mock := &mockJobSubmitter{}
			w := NewWatcher(watchDir, mock, cfg)

			err := w.Poll()
			So(err, ShouldBeNil)

			firstRun := w.activeRuns[subDir.Path]

			writeReportsForChunks(firstRun.RunDir)

			mock.incomplete = nil
			mock.buried = nil

			updateFofnMtime(subDir.Path, generateTmpPaths(15), firstRun.Mtime+1000)

			err = w.Poll()
			So(err, ShouldBeNil)

			secondRun := w.activeRuns[subDir.Path]

			writeReportsForChunks(secondRun.RunDir)

			mock.incomplete = nil
			mock.buried = nil

			err = w.Poll()
			So(err, ShouldBeNil)

			symlinkPath := filepath.Join(subDir.Path, "status")
			target, readErr := os.Readlink(symlinkPath)
			So(readErr, ShouldBeNil)

			expectedTarget := filepath.Join(secondRun.RunDir, "status")
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

		Convey("detects existing run with incomplete "+
			"jobs and does not submit new jobs",
			func() {
				subPath := filepath.Join(watchDir, "proj")
				So(os.MkdirAll(subPath, 0750),
					ShouldBeNil)

				writeFofn(subPath, generateTmpPaths(10))

				So(os.WriteFile(
					filepath.Join(
						subPath, "config.yml",
					),
					[]byte("transformer: test\n"),
					0600,
				), ShouldBeNil)

				runDir := filepath.Join(subPath, "1000")
				So(os.MkdirAll(runDir, 0750),
					ShouldBeNil)

				writeChunkOnly(runDir, "chunk.000000", makeFilePairs(0, 10))

				mock := &mockJobSubmitter{
					incomplete: []*jobqueue.Job{
						{Cmd: "running"},
					},
				}

				w := NewWatcher(watchDir, mock, cfg)

				err := w.Poll()
				So(err, ShouldBeNil)
				So(mock.submitted, ShouldBeEmpty)

				_, ok := w.activeRuns[subPath]
				So(ok, ShouldBeTrue)

				run := w.activeRuns[subPath]
				So(run.Mtime, ShouldEqual, 1000)
				So(run.RepGroup, ShouldEqual,
					"ibackup_fofn_proj_1000")
				So(run.RunDir, ShouldEqual, runDir)
			})

		Convey("detects completed existing run and "+
			"generates status file", func() {
			subPath := filepath.Join(watchDir, "proj")
			So(os.MkdirAll(subPath, 0750),
				ShouldBeNil)

			fofnPath := writeFofn(subPath, generateTmpPaths(10))

			fofnTime := time.Unix(1000, 0)
			So(os.Chtimes(fofnPath, fofnTime, fofnTime), ShouldBeNil)

			So(os.WriteFile(
				filepath.Join(
					subPath, "config.yml",
				),
				[]byte("transformer: test\n"),
				0600,
			), ShouldBeNil)

			runDir := filepath.Join(subPath, "1000")
			So(os.MkdirAll(runDir, 0750),
				ShouldBeNil)

			writeChunkAndReport(runDir, "chunk.000000", makeFilePairs(0, 10))

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
			So(target, ShouldEqual, statusPath)

			So(mock.submitted, ShouldBeEmpty)

			_, ok := w.activeRuns[subPath]
			So(ok, ShouldBeFalse)
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

			Convey("processes all 3 subdirectories "+
				"in a single poll cycle", func() {
				for _, name := range []string{
					"proj1", "proj2", "proj3",
				} {
					setupSubDir(
						watchDir, name,
						generateTmpPaths(5),
						"transformer: test\n",
					)
				}

				mock := &mockJobSubmitter{}
				w := NewWatcher(watchDir, mock, cfg)

				err := w.Poll()
				So(err, ShouldBeNil)
				So(mock.submitted,
					ShouldHaveLength, 3)
				So(w.activeRuns,
					ShouldHaveLength, 3)
			})

			Convey("only submits jobs for new "+
				"subdirectory when 2 have active "+
				"runs", func() {
				for _, name := range []string{
					"proj1", "proj2",
				} {
					setupSubDir(
						watchDir, name,
						generateTmpPaths(5),
						"transformer: test\n",
					)
				}

				mock := &mockJobSubmitter{}
				w := NewWatcher(watchDir, mock, cfg)

				err := w.Poll()
				So(err, ShouldBeNil)

				initialCount := len(mock.submitted)
				So(initialCount, ShouldEqual, 2)

				mock.incomplete = []*jobqueue.Job{
					{Cmd: "running"},
				}

				setupSubDir(
					watchDir, "proj3",
					generateTmpPaths(5),
					"transformer: test\n",
				)

				err = w.Poll()
				So(err, ShouldBeNil)
				So(mock.submitted,
					ShouldHaveLength,
					initialCount+1)
				So(w.activeRuns,
					ShouldHaveLength, 3)
			})
		})
}

// setupSubDir creates a subdirectory with a fofn and
// config.yml inside the watch directory.
func setupSubDir(
	watchDir, name string,
	paths []string,
	configContent string,
) SubDir {
	subPath := filepath.Join(watchDir, name)
	So(os.MkdirAll(subPath, 0750), ShouldBeNil)

	writeFofn(subPath, paths)

	So(os.WriteFile(
		filepath.Join(subPath, "config.yml"),
		[]byte(configContent), 0600,
	), ShouldBeNil)

	return SubDir{Path: subPath}
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

// fileGID returns the group ID of the given path.
func fileGID(path string) int {
	info, err := os.Stat(path)
	So(err, ShouldBeNil)

	stat, ok := info.Sys().(*syscall.Stat_t)
	So(ok, ShouldBeTrue)

	return int(stat.Gid)
}
