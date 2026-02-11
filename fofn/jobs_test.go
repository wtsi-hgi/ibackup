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
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/VertebrateResequencing/wr/jobqueue"
	. "github.com/smartystreets/goconvey/convey"
)

var errTest = errors.New("test error")

func TestBuildPutCommand(t *testing.T) {
	Convey("BuildPutCommand", t, func() {
		Convey("builds a basic command without optional flags",
			func() {
				cmd := BuildPutCommand("chunk.000000", false, "project1", "")
				So(cmd, ShouldEqual,
					"ibackup put -v -l chunk.000000.log "+
						"--report chunk.000000.report "+
						`--fofn "project1" -b `+
						"-f chunk.000000 "+
						"> chunk.000000.out 2>&1")
			})

		Convey("includes --no_replace when noReplace is true",
			func() {
				cmd := BuildPutCommand("chunk.000000", true, "project1", "")
				So(cmd, ShouldContainSubstring,
					"--no_replace")
			})

		Convey("includes --meta with quoted value when "+
			"userMeta is set", func() {
			cmd := BuildPutCommand(
				"chunk.000000", false, "project1",
				"colour=red;size=large",
			)
			So(cmd, ShouldContainSubstring,
				`--meta "colour=red;size=large"`)
		})

		Convey("omits --fofn when fofnName is empty",
			func() {
				cmd := BuildPutCommand("chunk.000000", false, "", "")
				So(cmd, ShouldNotContainSubstring, "--fofn")
			})
	})
}

func TestCreateJobs(t *testing.T) {
	Convey("CreateJobs", t, func() {
		Convey("creates jobs with default config values",
			func() {
				cfg := RunConfig{
					RunDir: "/watch/proj/123",
					ChunkPaths: []string{
						"chunk.000000",
						"chunk.000001",
					},
					SubDirName: "proj",
					FofnMtime:  123,
					Retries:    3,
				}

				jobs := CreateJobs(cfg)
				So(jobs, ShouldHaveLength, 2)

				expectedCmd0 := BuildPutCommand("chunk.000000", false, "proj", "")
				So(jobs[0].Cmd, ShouldEqual, expectedCmd0)
				So(jobs[0].Cwd, ShouldEqual,
					"/watch/proj/123")
				So(jobs[0].CwdMatters, ShouldBeTrue)
				So(jobs[0].RepGroup, ShouldEqual,
					"ibackup_fofn_proj_123")
				So(jobs[0].ReqGroup, ShouldEqual,
					"ibackup")
				So(jobs[0].Requirements.RAM, ShouldEqual,
					1024)
				So(jobs[0].Requirements.Cores, ShouldEqual,
					0.1)
				So(jobs[0].Requirements.Time, ShouldEqual,
					8*time.Hour)
				So(jobs[0].Retries, ShouldEqual, uint8(3))
				So(jobs[0].LimitGroups, ShouldResemble,
					[]string{"irods"})

				expectedCmd1 := BuildPutCommand("chunk.000001", false, "proj", "")
				So(jobs[1].Cmd, ShouldEqual, expectedCmd1)
			})

		Convey("preserves Retries zero value",
			func() {
				cfg := RunConfig{
					RunDir:     "/watch/proj/123",
					ChunkPaths: []string{"chunk.000000"},
					SubDirName: "proj",
					FofnMtime:  123,
					Retries:    0,
				}

				jobs := CreateJobs(cfg)
				So(jobs, ShouldHaveLength, 1)
				So(jobs[0].Retries, ShouldEqual, uint8(0))
			})

		Convey("includes --no_replace when NoReplace is "+
			"true", func() {
			cfg := RunConfig{
				RunDir:     "/watch/proj/123",
				ChunkPaths: []string{"chunk.000000"},
				SubDirName: "proj",
				FofnMtime:  123,
				NoReplace:  true,
			}

			jobs := CreateJobs(cfg)
			So(jobs[0].Cmd, ShouldContainSubstring,
				"--no_replace")
		})

		Convey("uses custom RAM and Time", func() {
			cfg := RunConfig{
				RunDir:     "/watch/proj/123",
				ChunkPaths: []string{"chunk.000000"},
				SubDirName: "proj",
				FofnMtime:  123,
				RAM:        2048,
				Time:       4 * time.Hour,
			}

			jobs := CreateJobs(cfg)
			So(jobs[0].Requirements.RAM, ShouldEqual,
				2048)
			So(jobs[0].Requirements.Time, ShouldEqual,
				4*time.Hour)
		})
	})
}

type mockJobSubmitter struct {
	mu            sync.Mutex
	submitted     []*jobqueue.Job
	submitErr     error
	buried        []*jobqueue.Job
	buriedErr     error
	incomplete    []*jobqueue.Job
	incompleteErr error
	deleted       []*jobqueue.Job
	deleteErr     error
}

func (m *mockJobSubmitter) SubmitJobs(
	jobs []*jobqueue.Job,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.submitted = append(m.submitted, jobs...)

	return m.submitErr
}

func (m *mockJobSubmitter) FindBuriedJobsByRepGroup(
	_ string,
) ([]*jobqueue.Job, error) {
	return m.buried, m.buriedErr
}

func (m *mockJobSubmitter) FindIncompleteJobsByRepGroup(
	_ string,
) ([]*jobqueue.Job, error) {
	return m.incomplete, m.incompleteErr
}

func (m *mockJobSubmitter) DeleteJobs(
	jobs []*jobqueue.Job,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.deleted = append(m.deleted, jobs...)

	return m.deleteErr
}

func (m *mockJobSubmitter) Disconnect() error {
	return nil
}

func TestJobSubmitter(t *testing.T) {
	Convey("JobSubmitter mock", t, func() {
		Convey("records submitted jobs without error",
			func() {
				mock := &mockJobSubmitter{}
				jobs := []*jobqueue.Job{
					{Cmd: "cmd1"},
					{Cmd: "cmd2"},
					{Cmd: "cmd3"},
				}

				err := mock.SubmitJobs(jobs)
				So(err, ShouldBeNil)
				So(mock.submitted, ShouldHaveLength, 3)
			})

		Convey("propagates submit error", func() {
			mock := &mockJobSubmitter{
				submitErr: errTest,
			}
			jobs := []*jobqueue.Job{{Cmd: "cmd1"}}

			err := mock.SubmitJobs(jobs)
			So(err, ShouldNotBeNil)
			So(err, ShouldEqual, errTest)
		})
	})
}

func TestIsRunComplete(t *testing.T) {
	Convey("IsRunComplete", t, func() {
		Convey("returns true when no incomplete jobs",
			func() {
				mock := &mockJobSubmitter{
					incomplete: []*jobqueue.Job{},
				}

				complete, err := IsRunComplete(mock, "repgroup1")
				So(err, ShouldBeNil)
				So(complete, ShouldBeTrue)
			})

		Convey("returns false when there are incomplete "+
			"jobs", func() {
			mock := &mockJobSubmitter{
				incomplete: []*jobqueue.Job{
					{Cmd: "cmd1"},
					{Cmd: "cmd2"},
				},
			}

			complete, err := IsRunComplete(mock, "repgroup1")
			So(err, ShouldBeNil)
			So(complete, ShouldBeFalse)
		})
	})
}

func TestFindBuriedChunks(t *testing.T) {
	Convey("FindBuriedChunks", t, func() {
		Convey("extracts chunk path from buried job cmd",
			func() {
				mock := &mockJobSubmitter{
					buried: []*jobqueue.Job{
						{Cmd: "ibackup put -v " +
							"-l chunk.000000.log " +
							"--report chunk.000000.report " +
							`--fofn "project1" -b ` +
							"-f chunk.000000 " +
							"> chunk.000000.out 2>&1"},
					},
				}

				chunks, err := FindBuriedChunks(mock, "repgroup1", "/run/dir")
				So(err, ShouldBeNil)
				So(chunks, ShouldHaveLength, 1)
				So(chunks[0], ShouldEqual,
					"/run/dir/chunk.000000")
			})

		Convey("returns empty slice when no buried jobs",
			func() {
				mock := &mockJobSubmitter{
					buried: []*jobqueue.Job{},
				}

				chunks, err := FindBuriedChunks(mock, "repgroup1", "/run/dir")
				So(err, ShouldBeNil)
				So(chunks, ShouldBeEmpty)
			})
	})
}

func TestDeleteBuriedJobs(t *testing.T) {
	Convey("DeleteBuriedJobs", t, func() {
		Convey("deletes buried jobs and returns no error",
			func() {
				buried := []*jobqueue.Job{
					{Cmd: "cmd1"},
					{Cmd: "cmd2"},
				}
				mock := &mockJobSubmitter{
					buried: buried,
				}

				err := DeleteBuriedJobs(mock, "repgroup1")
				So(err, ShouldBeNil)
				So(mock.deleted, ShouldHaveLength, 2)
			})

		Convey("propagates DeleteJobs error", func() {
			buried := []*jobqueue.Job{
				{Cmd: "cmd1"},
			}
			mock := &mockJobSubmitter{
				buried:    buried,
				deleteErr: errTest,
			}

			err := DeleteBuriedJobs(mock, "repgroup1")
			So(err, ShouldNotBeNil)
			So(err, ShouldEqual, errTest)
		})
	})
}
