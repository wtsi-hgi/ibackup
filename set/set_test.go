/*******************************************************************************
 * Copyright (c) 2022, 2023 Genome Research Ltd.
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

package set

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/shirou/gopsutil/process"
	. "github.com/smartystreets/goconvey/convey"
	gas "github.com/wtsi-hgi/go-authserver"
	"github.com/wtsi-hgi/ibackup/internal"
	"github.com/wtsi-hgi/ibackup/put"
	"github.com/wtsi-hgi/ibackup/slack"
	"github.com/wtsi-ssg/wrstat/v4/walk"
	bolt "go.etcd.io/bbolt"
)

const userPerms = 0700

func TestSet(t *testing.T) {
	Convey("Set statuses convert nicely to strings", t, func() {
		So(PendingDiscovery.String(), ShouldEqual, "pending discovery")
		So(PendingUpload.String(), ShouldEqual, "pending upload")
		So(Uploading.String(), ShouldEqual, "uploading")
		So(Failing.String(), ShouldEqual, "failing")
		So(Complete.String(), ShouldEqual, "complete")
	})

	Convey("Entry statuses convert nicely to strings", t, func() {
		So(Pending.String(), ShouldEqual, "pending")
		So(Uploaded.String(), ShouldEqual, "uploaded")
		So(Failed.String(), ShouldEqual, "failed")
		So(Missing.String(), ShouldEqual, "missing")
	})

	Convey("Status methods are useful helpers", t, func() {
		s := &Set{Transformer: "humgen"}
		So(s.Incomplete(), ShouldBeTrue)
		So(s.HasProblems(), ShouldBeFalse)
		So(s.Queued(), ShouldBeTrue)

		s.Status = Complete
		So(s.Incomplete(), ShouldBeFalse)
		So(s.HasProblems(), ShouldBeFalse)
		So(s.Queued(), ShouldBeFalse)

		s.Failed = 1
		So(s.Incomplete(), ShouldBeTrue)
		So(s.HasProblems(), ShouldBeTrue)
		So(s.Queued(), ShouldBeFalse)

		s.Failed = 0
		s.Error = "error"
		So(s.Incomplete(), ShouldBeTrue)
		So(s.HasProblems(), ShouldBeTrue)
		So(s.Queued(), ShouldBeFalse)

		s.Error = ""
		s.Transformer = "invalid"
		So(s.Incomplete(), ShouldBeTrue)
		So(s.HasProblems(), ShouldBeTrue)
		So(s.Queued(), ShouldBeFalse)

		s.Transformer = "humgen"
		s.Status = PendingDiscovery
		So(s.Incomplete(), ShouldBeTrue)
		So(s.HasProblems(), ShouldBeFalse)
		So(s.Queued(), ShouldBeTrue)

		s.Status = PendingUpload
		So(s.Incomplete(), ShouldBeTrue)
		So(s.HasProblems(), ShouldBeFalse)
		So(s.Queued(), ShouldBeTrue)
	})

	Convey("Entry.ShouldUpload() gives good advice", t, func() {
		reuploadAfter := time.Now()

		e := &Entry{Status: Pending}
		So(e.ShouldUpload(reuploadAfter), ShouldBeTrue)

		e.Status = Missing
		So(e.ShouldUpload(reuploadAfter), ShouldBeTrue)

		e.Status = Failed
		So(e.ShouldUpload(reuploadAfter), ShouldBeTrue)

		e.Attempts = AttemptsToBeConsideredFailing
		So(e.ShouldUpload(reuploadAfter), ShouldBeTrue)

		e.Attempts = 1
		e.LastAttempt = reuploadAfter.Add(1 * time.Second)
		e.Status = Uploaded
		So(e.ShouldUpload(reuploadAfter), ShouldBeFalse)
		So(e.ShouldUpload(reuploadAfter.Add(2*time.Second)), ShouldBeTrue)
	})

	Convey("Discovered() returns friendly strings", t, func() {
		s := &Set{}
		So(s.Discovered(), ShouldEqual, "not started")

		t := time.Now()
		s.StartedDiscovery = t
		So(s.Discovered(), ShouldEqual, "started "+t.Format(dateFormat))

		t2 := t.Add(24 * time.Hour)
		s.LastDiscovery = t2
		So(s.Discovered(), ShouldEqual, "completed "+t2.Format(dateFormat))

		t3 := t2.Add(24 * time.Hour)
		s.StartedDiscovery = t3
		So(s.Discovered(), ShouldEqual, "started "+t3.Format(dateFormat))
	})

	Convey("Count() and Size() return friendly strings", t, func() {
		s := &Set{}
		So(s.Count(), ShouldEqual, "pending")
		So(s.Size(), ShouldEqual, "pending")

		s.NumFiles = 3

		So(s.Count(), ShouldEqual, "pending")
		So(s.Size(), ShouldEqual, "pending")

		s.LastDiscovery = time.Now()

		So(s.Count(), ShouldEqual, "3")
		So(s.Size(), ShouldEqual, "0 B (and counting)")

		s.SizeFiles = 30
		So(s.Size(), ShouldEqual, "30 B (and counting)")

		s.Status = Complete
		So(s.Count(), ShouldEqual, "3")
		So(s.Size(), ShouldEqual, "30 B")

		s.LastCompletedCount = 3
		s.LastCompletedSize = 30
		s.NumFiles = 0
		s.SizeFiles = 0
		s.Status = PendingDiscovery

		So(s.Count(), ShouldEqual, "3 (as of last completion)")
		So(s.Size(), ShouldEqual, "30 B (as of last completion)")
	})

	Convey("MakeTransformer and TransformPath work", t, func() {
		s := &Set{Transformer: "humgen"}
		trans, err := s.MakeTransformer()
		So(err, ShouldBeNil)

		dddLocalPath := "/lustre/scratch118/humgen/projects/ddd/file.txt"
		remote, err := trans(dddLocalPath)
		So(err, ShouldBeNil)
		dddRemotePath := "/humgen/projects/ddd/scratch118/file.txt"
		So(remote, ShouldEqual, dddRemotePath)

		dest, err := s.TransformPath(dddLocalPath)
		So(err, ShouldBeNil)
		So(dest, ShouldEqual, dddRemotePath)

		_, err = s.TransformPath("/invalid/path.txt")
		So(err, ShouldNotBeNil)

		s = &Set{Transformer: "gengen"}
		trans, err = s.MakeTransformer()
		So(err, ShouldBeNil)

		partsLocalPath := "/lustre/scratch126/gengen/teams/parts/sequencing/file.txt"
		remote, err = trans(partsLocalPath)
		So(err, ShouldBeNil)

		partsRemotePath := "/humgen/gengen/teams/parts/scratch126/sequencing/file.txt"
		So(remote, ShouldEqual, partsRemotePath)

		dest, err = s.TransformPath(partsLocalPath)
		So(err, ShouldBeNil)
		So(dest, ShouldEqual, partsRemotePath)

		_, err = s.TransformPath("/invalid/path.txt")
		So(err, ShouldNotBeNil)

		dir, err := os.Getwd()
		So(err, ShouldBeNil)

		s = &Set{Transformer: "prefix=" + dir + ":/zone"}
		trans, err = s.MakeTransformer()
		So(err, ShouldBeNil)
		remote, err = trans(filepath.Join(dir, "file.txt"))
		So(err, ShouldBeNil)
		So(remote, ShouldEqual, "/zone/file.txt")
	})

	Convey("UsageSummary returns summaries of all given sets", t, func() {
		nov23 := time.Unix(1700063826, 0)
		month := 730 * time.Hour
		sets := []*Set{
			{Name: "setA", Requester: "userA", LastCompleted: nov23,
				SizeFiles: 10, NumFiles: 1},
			{Name: "setB", Requester: "userA", LastCompleted: nov23.Add(month),
				SizeFiles: 20, NumFiles: 2},
			{Name: "setC", Requester: "userB", LastCompleted: nov23,
				SizeFiles: 40, NumFiles: 3},
			{Name: "setD", Requester: "userC", LastCompleted: nov23.Add(-1 * month),
				SizeFiles: 1, NumFiles: 4},
			{Name: "setE", Requester: "userD", LastCompleted: nov23.Add(-1 * month),
				SizeFiles: 1, NumFiles: 5},
			{Name: "setF", Requester: "userE", SizeFiles: 0, NumFiles: 1},
		}

		usage := UsageSummary(sets)
		So(usage, ShouldNotBeNil)

		So(usage.Total.Size, ShouldEqual, 72)
		So(usage.Total.Number, ShouldEqual, 16)

		br := usage.ByRequester
		So(len(br), ShouldEqual, 5)
		So(br[0].For, ShouldEqual, "userB")
		So(br[0].Size, ShouldEqual, 40)
		So(br[0].Number, ShouldEqual, 3)
		So(br[1].For, ShouldEqual, "userA")
		So(br[1].Size, ShouldEqual, 30)
		So(br[1].Number, ShouldEqual, 3)
		So(br[2].For, ShouldEqual, "userD")
		So(br[2].Size, ShouldEqual, 1)
		So(br[2].Number, ShouldEqual, 5)
		So(br[3].For, ShouldEqual, "userC")
		So(br[3].Size, ShouldEqual, 1)
		So(br[3].Number, ShouldEqual, 4)
		So(br[4].For, ShouldEqual, "userE")
		So(br[4].Size, ShouldEqual, 0)
		So(br[4].Number, ShouldEqual, 1)

		bs := usage.BySet
		So(len(bs), ShouldEqual, 6)
		So(bs[0].For, ShouldEqual, "userB.setC")
		So(bs[0].Size, ShouldEqual, 40)
		So(bs[1].For, ShouldEqual, "userA.setB")
		So(bs[1].Size, ShouldEqual, 20)
		So(bs[2].For, ShouldEqual, "userA.setA")
		So(bs[2].Size, ShouldEqual, 10)
		So(bs[3].For, ShouldEqual, "userD.setE")
		So(bs[3].Size, ShouldEqual, 1)
		So(bs[4].For, ShouldEqual, "userC.setD")
		So(bs[4].Size, ShouldEqual, 1)
		So(bs[5].For, ShouldEqual, "userE.setF")
		So(bs[5].Size, ShouldEqual, 0)

		bm := usage.ByMonth
		So(len(bm), ShouldEqual, 3)
		So(bm[0].For, ShouldEqual, "2023/10")
		So(bm[0].Size, ShouldEqual, 2)
		So(bm[1].For, ShouldEqual, "2023/11")
		So(bm[1].Size, ShouldEqual, 50)
		So(bm[2].For, ShouldEqual, "2023/12")
		So(bm[2].Size, ShouldEqual, 20)

		sets = []*Set{
			{Name: "setA", Requester: "userA",
				SizeFiles: 10 * uint64(bytesInTiB), NumFiles: 1},
		}

		usage = UsageSummary(sets)
		So(usage.Total.SizeTiB(), ShouldEqual, 10)
	})
}

func TestSetDB(t *testing.T) {
	Convey("Given a path", t, func() {
		tDir := t.TempDir()
		dbPath := filepath.Join(tDir, "set.db")

		Convey("You can create a new database", func() {
			slackWriter := gas.NewStringLogger()
			slacker := slack.NewMock(slackWriter)

			db, err := New(dbPath, "")
			So(err, ShouldBeNil)
			So(db, ShouldNotBeNil)

			db.LogSetChangesToSlack(slacker)

			Convey("And add Sets to it", func() {
				set := &Set{
					Name:        "set1",
					Requester:   "jim",
					Transformer: "prefix=/local:/remote",
					MonitorTime: 0,
					DeleteLocal: false,
				}

				set.LogChangesToSlack(slacker)

				err = db.AddOrUpdate(set)
				So(err, ShouldBeNil)

				So(slackWriter.String(), ShouldEqual, slack.BoxPrefixInfo+"`jim.set1` stored in db")
				slackWriter.Reset()

				err = db.SetFileEntries(set.ID(), []string{"/a/b.txt", "/c/d.txt", "/e/f.txt"})
				So(err, ShouldBeNil)

				err = db.SetDirEntries(set.ID(), createFileEnts([]string{"/g/h", "/g/i"}))
				So(err, ShouldBeNil)

				set.MonitorTime = 1 * time.Hour
				err = db.AddOrUpdate(set)
				So(err, ShouldBeNil)

				set2 := &Set{
					Name:        "set2",
					Requester:   "jane",
					Transformer: "prefix=/local:/remote",
					MonitorTime: 0,
					DeleteLocal: true,
				}

				set2.LogChangesToSlack(slacker)

				err = db.AddOrUpdate(set2)
				So(err, ShouldBeNil)

				err = db.SetFileEntries(set2.ID(), []string{"/a/b.txt", "/c/j.txt"})
				So(err, ShouldBeNil)

				err = db.SetFileEntries(set2.ID(), []string{"/a/b.txt", "/c/k.txt"})
				So(err, ShouldBeNil)

				Convey("Then get a particular Set", func() {
					retrieved := db.GetByID(set.ID())
					So(retrieved, ShouldNotBeNil)
					So(retrieved, ShouldResemble, set)

					So(db.GetByID("sdf"), ShouldBeNil)

					slackWriter.Reset()

					Convey("And set an Error and Warning for it, which are cleared when we start discovery again", func() {
						errMsg := "fooErr"
						err = db.SetError(set.ID(), errMsg)
						So(err, ShouldBeNil)
						So(slackWriter.String(), ShouldEqual, slack.BoxPrefixError+"`jim.set1` is invalid: "+errMsg)
						slackWriter.Reset()

						warnMsg := "fooWarn"
						err = db.SetWarning(set.ID(), warnMsg)
						So(err, ShouldBeNil)
						So(slackWriter.String(), ShouldEqual, slack.BoxPrefixWarn+"`jim.set1` has an issue: "+warnMsg)

						retrieved = db.GetByID(set.ID())
						So(retrieved, ShouldNotBeNil)
						So(retrieved.Error, ShouldEqual, errMsg)
						So(retrieved.Warning, ShouldEqual, warnMsg)

						retrieved, err = db.Discover(set.ID(), nil)
						So(err, ShouldBeNil)
						So(retrieved, ShouldNotBeNil)
						So(retrieved.Error, ShouldBeBlank)
						So(retrieved.Warning, ShouldBeBlank)
					})

					Convey("And set bool vals back to false on update", func() {
						retrieved2 := db.GetByID(set2.ID())
						So(retrieved2, ShouldNotBeNil)
						So(retrieved2, ShouldResemble, set2)
						So(retrieved2.DeleteLocal, ShouldBeTrue)

						retrieved2.DeleteLocal = false

						err = db.AddOrUpdate(retrieved2)
						So(err, ShouldBeNil)

						retrieved2 = db.GetByID(set2.ID())
						So(retrieved2.DeleteLocal, ShouldBeFalse)
					})
				})

				Convey("Then get all the Sets and their entries", func() {
					sets, errg := db.GetAll()
					So(errg, ShouldBeNil)
					So(sets, ShouldNotBeNil)
					So(len(sets), ShouldEqual, 2)
					So(sets, ShouldResemble, []*Set{set2, set})

					definedEntry, errg := db.GetDefinedFileEntry(sets[0].ID())
					So(errg, ShouldBeNil)
					So(definedEntry, ShouldNotBeNil)
					So(definedEntry.Path, ShouldEqual, "/a/b.txt")

					fEntries, errg := db.GetFileEntries(sets[1].ID())
					So(errg, ShouldBeNil)
					So(len(fEntries), ShouldEqual, 3)
					So(fEntries[0], ShouldResemble, &Entry{Path: "/a/b.txt"})
					So(fEntries[1], ShouldResemble, &Entry{Path: "/c/d.txt"})
					So(fEntries[2], ShouldResemble, &Entry{Path: "/e/f.txt"})

					dEntries, errg := db.GetDirEntries(sets[1].ID())
					So(errg, ShouldBeNil)
					So(len(dEntries), ShouldEqual, 2)
					So(dEntries[0], ShouldResemble, &Entry{Path: "/g/h"})
					So(dEntries[1], ShouldResemble, &Entry{Path: "/g/i"})

					fEntries, err = db.GetFileEntries(sets[0].ID())
					So(err, ShouldBeNil)
					So(len(fEntries), ShouldEqual, 2)
					So(fEntries[0], ShouldResemble, &Entry{Path: "/a/b.txt"})
					So(fEntries[1], ShouldResemble, &Entry{Path: "/c/k.txt"})

					dEntries, err = db.GetDirEntries(sets[0].ID())
					So(err, ShouldBeNil)
					So(len(dEntries), ShouldEqual, 0)
				})

				Convey("The get an particular entry from a set", func() {
					entry, errr := db.GetFileEntryForSet(set2.ID(), "/a/b.txt")
					So(errr, ShouldBeNil)
					So(entry, ShouldResemble, &Entry{Path: "/a/b.txt"})

					entry, errr = db.GetFileEntryForSet(set2.ID(), "/not/a/file.txt")
					So(errr, ShouldNotBeNil)
					So(entry, ShouldBeNil)
				})

				Convey("Then get all the Sets for a particular Requester", func() {
					sets, errg := db.GetByRequester("jim")
					So(errg, ShouldBeNil)
					So(sets, ShouldNotBeNil)
					So(len(sets), ShouldEqual, 1)
					So(sets, ShouldResemble, []*Set{set})

					Convey("And update Set status on discovering dir files and uploading all", func() {
						So(sets[0].Status, ShouldEqual, PendingDiscovery)
						So(sets[0].StartedDiscovery.IsZero(), ShouldBeTrue)
						So(sets[0].LastDiscovery.IsZero(), ShouldBeTrue)
						So(sets[0].Description, ShouldBeBlank)

						sets[0].LastDiscovery = time.Now()
						sets[0].Description = "desc"
						err = db.AddOrUpdate(sets[0])
						So(err, ShouldBeNil)

						sets, err = db.GetByRequester("jim")
						So(err, ShouldBeNil)

						So(sets[0].Status, ShouldEqual, PendingDiscovery)
						So(sets[0].LastDiscovery.IsZero(), ShouldBeTrue)
						So(sets[0].Description, ShouldEqual, "desc")

						tdir := t.TempDir()
						pureFiles := make([]string, 3)

						for i := range pureFiles {
							pureFiles[i] = filepath.Join(tdir, fmt.Sprintf("%d.txt", i))
							internal.CreateTestFile(t, pureFiles[i], "")
						}

						err = db.SetFileEntries(set.ID(), pureFiles)
						So(err, ShouldBeNil)

						slackWriter.Reset()

						discoverASet(db, sets[0], func() ([]*walk.Dirent, error) {
							return createFileEnts([]string{"/g/h/l.txt", "/g/i/m.txt"}), nil
						}, func() {
							sets, err = db.GetByRequester("jim")
							So(err, ShouldBeNil)
							So(sets[0].Status, ShouldEqual, PendingDiscovery)
							So(sets[0].StartedDiscovery.IsZero(), ShouldBeFalse)
						})

						bsets, errg := db.GetByRequester("jim")
						So(errg, ShouldBeNil)
						So(bsets[0].LastDiscovery, ShouldHappenAfter, sets[0].LastDiscovery)

						fEntries, errg := db.GetFileEntries(sets[0].ID())
						So(errg, ShouldBeNil)
						So(len(fEntries), ShouldEqual, 5)
						So(slackWriter.String(), ShouldEqual, fmt.Sprintf("%s`jim.set1` completed discovery: %d files",
							slack.BoxPrefixInfo, len(fEntries)))
						slackWriter.Reset()
						So(fEntries[0].Path, ShouldEqual, pureFiles[0])
						So(fEntries[1].Path, ShouldEqual, pureFiles[1])
						So(fEntries[2].Path, ShouldEqual, pureFiles[2])
						So(fEntries[3].Path, ShouldEqual, "/g/h/l.txt")
						So(fEntries[4].Path, ShouldEqual, "/g/i/m.txt")

						sets, err = db.GetByRequester("jim")
						So(err, ShouldBeNil)

						So(sets[0].Status, ShouldEqual, PendingUpload)
						So(sets[0].StartedDiscovery.IsZero(), ShouldBeFalse)
						So(sets[0].LastDiscovery.IsZero(), ShouldBeFalse)
						So(sets[0].NumFiles, ShouldEqual, 5)
						So(sets[0].SizeFiles, ShouldEqual, 0)

						setsAll, errg := db.GetAll()
						So(errg, ShouldBeNil)
						So(setsAll, ShouldNotBeNil)
						So(len(setsAll), ShouldEqual, 2)

						r := &put.Request{
							Local:     pureFiles[0],
							Requester: set.Requester,
							Set:       set.Name,
							Size:      3,
							Status:    put.RequestStatusUploading,
							Error:     "",
						}

						So(slackWriter.String(), ShouldBeBlank)
						e, errs := db.SetEntryStatus(r)
						So(errs, ShouldBeNil)
						So(e, ShouldNotBeNil)
						So(e.Path, ShouldEqual, fEntries[0].Path)
						So(e.Size, ShouldEqual, r.Size)
						So(e.Status, ShouldEqual, UploadingEntry)
						So(slackWriter.String(), ShouldEqual, (slack.BoxPrefixInfo + "`jim.set1` started uploading files"))
						slackWriter.Reset()

						sets, err = db.GetByRequester("jim")
						So(err, ShouldBeNil)

						So(sets[0].Status, ShouldEqual, Uploading)
						So(sets[0].NumFiles, ShouldEqual, 5)
						So(sets[0].SizeFiles, ShouldEqual, 3)
						So(sets[0].Uploaded, ShouldEqual, 0)
						So(sets[0].LastCompletedSize, ShouldEqual, 0)

						r = &put.Request{
							Local:     pureFiles[0],
							Requester: set.Requester,
							Set:       set.Name,
							Size:      3,
							Status:    put.RequestStatusUploaded,
							Error:     "",
						}

						err = db.db.Update(func(tx *bolt.Tx) error {
							eg, b, errge := db.getEntry(tx, set.ID(), r.Local)
							So(errge, ShouldBeNil)
							eg.Attempts = 2

							return b.Put([]byte(r.Local), db.encodeToBytes(eg))
						})
						So(err, ShouldBeNil)

						e, err = db.SetEntryStatus(r)
						So(err, ShouldBeNil)
						So(e, ShouldNotBeNil)
						So(e.Path, ShouldEqual, fEntries[0].Path)
						So(e.Size, ShouldEqual, r.Size)
						So(e.Status, ShouldEqual, Uploaded)

						sets, err = db.GetByRequester("jim")
						So(err, ShouldBeNil)

						So(sets[0].Status, ShouldEqual, Uploading)
						So(sets[0].NumFiles, ShouldEqual, 5)
						So(sets[0].SizeFiles, ShouldEqual, 3)
						So(sets[0].Uploaded, ShouldEqual, 1)
						So(sets[0].Failed, ShouldEqual, 0)
						So(sets[0].LastCompletedSize, ShouldEqual, 0)

						fEntries, err = db.GetFileEntries(sets[0].ID())
						So(err, ShouldBeNil)
						So(len(fEntries), ShouldEqual, 5)
						So(fEntries[0].Size, ShouldEqual, 3)
						So(fEntries[0].Status, ShouldEqual, Uploaded)
						So(fEntries[0].LastAttempt.IsZero(), ShouldBeFalse)

						r = &put.Request{
							Local:     pureFiles[1],
							Requester: set.Requester,
							Set:       set.Name,
							Size:      2,
							Status:    put.RequestStatusUnmodified,
							Error:     "",
						}

						_, err = db.SetEntryStatus(r)
						So(err, ShouldBeNil)

						sets, err = db.GetByRequester("jim")
						So(err, ShouldBeNil)

						So(sets[0].Status, ShouldEqual, Uploading)
						So(sets[0].SizeFiles, ShouldEqual, 5)
						So(sets[0].Uploaded, ShouldEqual, 2)

						fEntries, err = db.GetFileEntries(sets[0].ID())
						So(err, ShouldBeNil)
						So(len(fEntries), ShouldEqual, 5)
						So(fEntries[1].Size, ShouldEqual, 2)
						So(fEntries[1].Status, ShouldEqual, Uploaded)
						So(fEntries[1].LastAttempt.IsZero(), ShouldBeFalse)

						r = &put.Request{
							Local:     pureFiles[2],
							Requester: set.Requester,
							Set:       set.Name,
							Size:      4,
							Status:    put.RequestStatusUploading,
							Error:     "",
						}

						_, err = db.SetEntryStatus(r)
						So(err, ShouldBeNil)

						r.Status = put.RequestStatusReplaced
						_, err = db.SetEntryStatus(r)
						So(err, ShouldBeNil)

						sets, err = db.GetByRequester("jim")
						So(err, ShouldBeNil)

						So(sets[0].NumFiles, ShouldEqual, 5)
						So(sets[0].Uploaded, ShouldEqual, 3)
						So(sets[0].Status, ShouldEqual, Uploading)
						So(sets[0].SizeFiles, ShouldEqual, 9)

						fEntries, err = db.GetFileEntries(sets[0].ID())
						So(err, ShouldBeNil)
						So(len(fEntries), ShouldEqual, 5)
						So(fEntries[2].Size, ShouldEqual, 4)
						So(fEntries[2].Status, ShouldEqual, Uploaded)
						So(fEntries[2].LastAttempt.IsZero(), ShouldBeFalse)

						r = &put.Request{
							Local:     "/g/h/l.txt",
							Requester: set.Requester,
							Set:       set.Name,
							Size:      6,
							Status:    put.RequestStatusUploading,
							Error:     "",
						}

						_, err = db.SetEntryStatus(r)
						So(err, ShouldBeNil)

						fEntries, failSkips, errg := db.GetFailedEntries(sets[0].ID())
						So(errg, ShouldBeNil)
						So(len(fEntries), ShouldEqual, 0)
						So(failSkips, ShouldEqual, 0)

						r.Status = put.RequestStatusFailed
						errMsg := "upload failed"
						r.Error = errMsg
						_, err = db.SetEntryStatus(r)
						So(err, ShouldBeNil)

						sets, err = db.GetByRequester("jim")
						So(err, ShouldBeNil)

						So(sets[0].Status, ShouldEqual, Uploading)
						So(sets[0].SizeFiles, ShouldEqual, 15)
						So(sets[0].Uploaded, ShouldEqual, 3)
						So(sets[0].Failed, ShouldEqual, 1)

						fEntries, err = db.GetFileEntries(sets[0].ID())
						So(err, ShouldBeNil)
						So(len(fEntries), ShouldEqual, 5)
						So(fEntries[3].Size, ShouldEqual, 6)
						So(fEntries[3].Status, ShouldEqual, Failed)
						So(fEntries[3].Attempts, ShouldEqual, 1)
						So(fEntries[3].LastAttempt.IsZero(), ShouldBeFalse)
						So(fEntries[3].LastError, ShouldEqual, errMsg)

						fEntries, failSkips, err = db.GetFailedEntries(sets[0].ID())
						So(err, ShouldBeNil)
						So(len(fEntries), ShouldEqual, 1)
						So(failSkips, ShouldEqual, 0)

						r.Status = put.RequestStatusUploading
						_, err = db.SetEntryStatus(r)
						So(err, ShouldBeNil)
						r.Status = put.RequestStatusFailed
						r.Error = errMsg
						_, err = db.SetEntryStatus(r)
						So(err, ShouldBeNil)

						sets, err = db.GetByRequester("jim")
						So(err, ShouldBeNil)

						So(sets[0].Status, ShouldEqual, Uploading)
						So(sets[0].SizeFiles, ShouldEqual, 15)
						So(sets[0].Uploaded, ShouldEqual, 3)
						So(sets[0].Failed, ShouldEqual, 1)

						fEntries, err = db.GetFileEntries(sets[0].ID())
						So(err, ShouldBeNil)
						So(len(fEntries), ShouldEqual, 5)
						So(fEntries[3].Size, ShouldEqual, 6)
						So(fEntries[3].Status, ShouldEqual, Failed)
						So(fEntries[3].Attempts, ShouldEqual, 2)
						So(fEntries[3].LastAttempt.IsZero(), ShouldBeFalse)
						So(fEntries[3].LastError, ShouldEqual, errMsg)

						r.Status = put.RequestStatusUploading
						_, err = db.SetEntryStatus(r)
						So(err, ShouldBeNil)
						r.Status = put.RequestStatusFailed
						r.Error = errMsg
						_, err = db.SetEntryStatus(r)
						So(err, ShouldBeNil)

						sets, err = db.GetByRequester("jim")
						So(err, ShouldBeNil)

						So(sets[0].Status, ShouldEqual, Failing)
						So(sets[0].SizeFiles, ShouldEqual, 15)
						So(sets[0].Uploaded, ShouldEqual, 3)
						So(sets[0].Failed, ShouldEqual, 1)
						So(slackWriter.String(), ShouldEqual, slack.BoxPrefixError+"`jim.set1` has failed uploads")
						slackWriter.Reset()

						fEntries, err = db.GetFileEntries(sets[0].ID())
						So(err, ShouldBeNil)
						So(len(fEntries), ShouldEqual, 5)
						So(fEntries[3].Size, ShouldEqual, 6)
						So(fEntries[3].Status, ShouldEqual, Failed)
						So(fEntries[3].Attempts, ShouldEqual, 3)
						So(fEntries[3].LastAttempt.IsZero(), ShouldBeFalse)
						So(fEntries[3].LastError, ShouldEqual, errMsg)

						r = &put.Request{
							Local:     "/g/i/m.txt",
							Requester: set.Requester,
							Set:       set.Name,
							Size:      0,
							Status:    put.RequestStatusMissing,
							Error:     "",
						}

						_, err = db.SetEntryStatus(r)
						So(err, ShouldBeNil)

						sets, err = db.GetByRequester("jim")
						So(err, ShouldBeNil)

						So(sets[0].Status, ShouldEqual, Complete)
						So(sets[0].SizeFiles, ShouldEqual, 15)
						So(sets[0].Uploaded, ShouldEqual, 3)
						So(sets[0].Failed, ShouldEqual, 1)
						So(sets[0].Missing, ShouldEqual, 1)
						So(slackWriter.String(), ShouldEqual,
							fmt.Sprintf("%s`jim.set1` completed backup "+
								"(%d uploaded; %d failed; %d missing; %d abnormal; %s of data)",
								slack.BoxPrefixSuccess, sets[0].Uploaded, sets[0].Failed,
								sets[0].Missing, sets[0].Abnormal, sets[0].Size()))
						lastCompleted := sets[0].LastCompleted
						So(lastCompleted.IsZero(), ShouldBeFalse)
						So(sets[0].LastCompletedSize, ShouldEqual, 15)
						So(sets[0].LastCompletedCount, ShouldEqual, 4)

						fEntries, err = db.GetFileEntries(sets[0].ID())
						So(err, ShouldBeNil)
						So(len(fEntries), ShouldEqual, 5)
						So(fEntries[4].Size, ShouldEqual, 0)
						So(fEntries[4].Status, ShouldEqual, Missing)
						So(fEntries[4].LastAttempt.IsZero(), ShouldBeFalse)
						So(fEntries[4].LastError, ShouldBeBlank)

						r = &put.Request{
							Local:     "/g/h/l.txt",
							Requester: set.Requester,
							Set:       set.Name,
							Size:      6,
							Status:    put.RequestStatusUploading,
							Error:     "",
						}

						_, err = db.SetEntryStatus(r)
						So(err, ShouldBeNil)
						r.Status = put.RequestStatusUploaded
						_, err = db.SetEntryStatus(r)
						So(err, ShouldBeNil)

						sets, err = db.GetByRequester("jim")
						So(err, ShouldBeNil)

						So(sets[0].Status, ShouldEqual, Complete)
						So(sets[0].SizeFiles, ShouldEqual, 15)
						So(sets[0].Uploaded, ShouldEqual, 4)
						So(sets[0].Failed, ShouldEqual, 0)
						lastCompleted2 := sets[0].LastCompleted
						So(lastCompleted2.After(lastCompleted), ShouldBeTrue)
						So(sets[0].LastCompletedSize, ShouldEqual, 15)
						So(sets[0].LastCompletedCount, ShouldEqual, 4)

						fEntries, err = db.GetFileEntries(sets[0].ID())
						So(err, ShouldBeNil)
						So(len(fEntries), ShouldEqual, 5)
						So(fEntries[3].Size, ShouldEqual, 6)
						So(fEntries[3].Status, ShouldEqual, Uploaded)
						So(fEntries[3].Attempts, ShouldEqual, 4)
						So(fEntries[3].LastAttempt.IsZero(), ShouldBeFalse)
						So(fEntries[3].LastError, ShouldBeBlank)

						fEntries, failSkips, err = db.GetFailedEntries(sets[0].ID())
						So(err, ShouldBeNil)
						So(len(fEntries), ShouldEqual, 0)
						So(failSkips, ShouldEqual, 0)

						Convey("Finally, set status gets reset on new discovery", func() {
							oldStart := sets[0].StartedDiscovery
							oldDisc := sets[0].LastDiscovery

							discoverASet(db, sets[0], func() ([]*walk.Dirent, error) {
								return createFileEnts([]string{"/g/h/l.txt", "/g/i/m.txt", "/g/i/n.txt"}), nil
							}, func() {
								sets, err = db.GetByRequester("jim")
								So(err, ShouldBeNil)
								So(sets[0].Status, ShouldEqual, PendingDiscovery)
								So(sets[0].StartedDiscovery.After(oldStart), ShouldBeTrue)
								So(sets[0].NumFiles, ShouldEqual, 0)
								So(sets[0].SizeFiles, ShouldEqual, 0)
								So(sets[0].Uploaded, ShouldEqual, 0)
								So(sets[0].Failed, ShouldEqual, 0)
								So(sets[0].Missing, ShouldEqual, 0)
								So(sets[0].LastCompletedCount, ShouldEqual, 4)
								So(sets[0].LastCompletedSize, ShouldEqual, 15)
							})

							fEntries, errg := db.GetFileEntries(sets[0].ID())
							So(errg, ShouldBeNil)
							So(len(fEntries), ShouldEqual, 6)
							So(fEntries[5], ShouldResemble, &Entry{Path: "/g/i/n.txt"})

							sets, err = db.GetByRequester("jim")
							So(err, ShouldBeNil)

							So(sets[0].Status, ShouldEqual, PendingUpload)
							So(sets[0].LastDiscovery.After(oldDisc), ShouldBeTrue)
							So(sets[0].NumFiles, ShouldEqual, 6)
							So(sets[0].SizeFiles, ShouldEqual, 0)

							r = &put.Request{
								Local:     "/g/h/l.txt",
								Requester: set.Requester,
								Set:       set.Name,
								Size:      7,
								Status:    put.RequestStatusUploading,
								Error:     "",
							}

							_, err = db.SetEntryStatus(r)
							So(err, ShouldBeNil)
							r.Status = put.RequestStatusUploaded
							_, err = db.SetEntryStatus(r)
							So(err, ShouldBeNil)

							sets, err = db.GetByRequester("jim")
							So(err, ShouldBeNil)

							So(sets[0].Status, ShouldEqual, Uploading)
							So(sets[0].SizeFiles, ShouldEqual, 7)
							So(sets[0].Uploaded, ShouldEqual, 1)
							So(sets[0].Failed, ShouldEqual, 0)

							fEntries, err = db.GetFileEntries(sets[0].ID())
							So(err, ShouldBeNil)
							So(len(fEntries), ShouldEqual, 6)
							So(fEntries[3].Size, ShouldEqual, 7)
							So(fEntries[3].Status, ShouldEqual, Uploaded)
							So(fEntries[3].Attempts, ShouldEqual, 1)
							So(fEntries[3].LastError, ShouldBeBlank)

							fEntries, err = db.GetPureFileEntries(sets[0].ID())
							So(err, ShouldBeNil)
							So(len(fEntries), ShouldEqual, 3)
						})

						Convey("Set status becomes complete on new discovery with all missing files", func() {
							err = db.SetFileEntries(sets[0].ID(), []string{})
							So(err, ShouldBeNil)

							slackWriter.Reset()

							oldDisc := sets[0].LastDiscovery

							discoverASet(db, sets[0], func() ([]*walk.Dirent, error) {
								dirents := createFileEnts([]string{"/g/h/l.txt", "/g/i/m.txt", "/g/i/n.txt"})
								for _, dirent := range dirents {
									dirent.Inode = 0
									dirent.Type = os.ModeIrregular
								}

								return dirents, nil
							}, func() {})

							sets, err = db.GetByRequester("jim")
							So(err, ShouldBeNil)

							So(sets[0].Status, ShouldEqual, Complete)
							So(sets[0].LastDiscovery.After(oldDisc), ShouldBeTrue)
							So(sets[0].NumFiles, ShouldEqual, 3)
							So(sets[0].Missing, ShouldEqual, 3)
							So(sets[0].SizeFiles, ShouldEqual, 0)

							So(slackWriter.String(), ShouldEqual, slack.BoxPrefixWarn+
								"`jim.set1` completed discovery and backup due to no files")
						})

						Convey("Set status becomes complete on new discovery with no files", func() {
							err = db.SetFileEntries(sets[0].ID(), []string{})
							So(err, ShouldBeNil)
							err = db.SetDirEntries(sets[0].ID(), nil)
							So(err, ShouldBeNil)

							slackWriter.Reset()

							oldDisc := sets[0].LastDiscovery

							discoverASet(db, sets[0], func() ([]*walk.Dirent, error) {
								return nil, nil
							}, func() {})

							sets, err = db.GetByRequester("jim")
							So(err, ShouldBeNil)

							So(sets[0].Status, ShouldEqual, Complete)
							So(sets[0].LastDiscovery.After(oldDisc), ShouldBeTrue)
							So(sets[0].NumFiles, ShouldEqual, 0)
							So(sets[0].Missing, ShouldEqual, 0)
							So(sets[0].SizeFiles, ShouldEqual, 0)

							So(slackWriter.String(), ShouldEqual, slack.BoxPrefixWarn+
								"`jim.set1` completed discovery and backup due to no files")
						})
					})
				})

				Convey("Then get a single Set belonging to a particular Requester", func() {
					got, errg := db.GetByNameAndRequester(set.Name, set.Requester)
					So(errg, ShouldBeNil)
					So(got, ShouldNotBeNil)
					So(got, ShouldResemble, set)

					got, err = db.GetByNameAndRequester("nonexistant", set.Requester)
					So(err, ShouldBeNil)
					So(got, ShouldBeNil)

					got, err = db.GetByNameAndRequester(set.Name, "nonexistant")
					So(err, ShouldBeNil)
					So(got, ShouldBeNil)
				})

				Convey("Then fail to re-add a set while it is being discovered", func() {
					err = db.AddOrUpdate(set)
					So(err, ShouldBeNil)

					err = db.setDiscoveryStarted(set.ID())
					So(err, ShouldBeNil)

					err = db.AddOrUpdate(set)
					So(err, ShouldNotBeNil)
					So(err.Error(), ShouldStartWith, "can't add set while set is being discovered")
				})
			})

			Convey("And add a set with pure hardlinks to it", func() {
				setl1 := &Set{
					Name:        "setlink",
					Requester:   "jim",
					Transformer: "prefix=/local:/remote",
				}

				err = db.AddOrUpdate(setl1)
				So(err, ShouldBeNil)

				localDir := t.TempDir()
				path1 := filepath.Join(localDir, "file.link1")
				internal.CreateTestFile(t, path1, "")

				path2 := filepath.Join(localDir, "file.link2")
				err = os.Link(path1, path2)
				So(err, ShouldBeNil)

				info, errl := os.Lstat(path1)
				So(errl, ShouldBeNil)
				statt, ok := info.Sys().(*syscall.Stat_t)
				So(ok, ShouldBeTrue)

				confirmHardLinks := func(setID string) {
					err = db.SetFileEntries(setID, []string{path1, path2, "/zmissing"})
					So(err, ShouldBeNil)

					entries, errg := db.GetPureFileEntries(setID)
					So(errg, ShouldBeNil)
					So(len(entries), ShouldEqual, 3)
					So(entries[0].Status, ShouldEqual, Pending)
					So(entries[1].Status, ShouldEqual, Pending)
					So(entries[0].Type, ShouldEqual, Regular)
					So(entries[1].Type, ShouldEqual, Regular)

					_, err = db.Discover(setID, nil)
					So(err, ShouldBeNil)

					entries, err = db.GetPureFileEntries(setID)
					So(err, ShouldBeNil)
					So(len(entries), ShouldEqual, 3)
					So(entries[0].Status, ShouldEqual, Pending)
					So(entries[1].Status, ShouldEqual, Pending)
					So(entries[0].Type, ShouldEqual, Regular)
					So(entries[1].Type, ShouldEqual, Hardlink)
					So(entries[1].Inode, ShouldEqual, statt.Ino)

					got := db.GetByID(setID)
					So(got, ShouldNotBeNil)
					So(got.NumFiles, ShouldEqual, 3)
					So(got.Hardlinks, ShouldEqual, 1)
				}

				confirmHardLinks(setl1.ID())

				Convey("which can be added again by a different set", func() {
					setl2 := &Set{
						Name:        "setlink2",
						Requester:   "jim",
						Transformer: "prefix=/local:/remote",
					}

					err = db.AddOrUpdate(setl2)
					So(err, ShouldBeNil)

					confirmHardLinks(setl2.ID())
				})

				Convey("then change their status to uploaded", func() {
					entries, errg := db.GetPureFileEntries(setl1.ID())
					So(errg, ShouldBeNil)

					for _, entry := range entries {
						if entry.Path == "/zmissing" {
							continue
						}

						r := &put.Request{
							Local:     entry.Path,
							Status:    put.RequestStatusUploading,
							Requester: setl1.Requester,
							Set:       setl1.Name,
						}

						if entry.Type == Symlink {
							r.Symlink = entry.Dest
						}

						if entry.Type == Hardlink {
							r.Hardlink = entry.Dest
						}

						_, err = db.SetEntryStatus(r)
						So(err, ShouldBeNil)

						r.Status = put.RequestStatusUploaded

						_, err = db.SetEntryStatus(r)
						So(err, ShouldBeNil)
					}

					got, erra := db.GetByNameAndRequester(setl1.Name, setl1.Requester)
					So(erra, ShouldBeNil)
					So(got.NumFiles, ShouldEqual, len(entries))
					So(got.Symlinks, ShouldEqual, 0)
					So(got.Hardlinks, ShouldEqual, 1)
					So(got.Uploaded, ShouldEqual, 2)
					So(got.Missing, ShouldEqual, 1)
					So(got.Failed, ShouldEqual, 0)
				})

				Convey("then rediscover the set and still know about the hard links", func() {
					got, errd := db.Discover(setl1.ID(), nil)
					So(errd, ShouldBeNil)
					So(got.Hardlinks, ShouldEqual, 1)

					got = db.GetByID(setl1.ID())
					So(got, ShouldNotBeNil)
					So(err, ShouldBeNil)
					So(got.Hardlinks, ShouldEqual, 1)
				})
			})

			Convey("And add a set with pure symlinks to it", func() {
				setl1 := &Set{
					Name:        "setlink",
					Requester:   "jim",
					Transformer: "prefix=/local:/remote",
				}

				err = db.AddOrUpdate(setl1)
				So(err, ShouldBeNil)

				localDir := t.TempDir()
				path1 := filepath.Join(localDir, "file.source")
				internal.CreateTestFile(t, path1, "")

				path2 := filepath.Join(localDir, "file.dest")
				err = os.Symlink(path1, path2)
				So(err, ShouldBeNil)

				err = db.SetFileEntries(setl1.ID(), []string{path1, path2, "/zmissing"})
				So(err, ShouldBeNil)

				entries, errg := db.GetPureFileEntries(setl1.ID())
				So(errg, ShouldBeNil)
				So(len(entries), ShouldEqual, 3)
				So(entries[0].Status, ShouldEqual, Pending)
				So(entries[1].Status, ShouldEqual, Pending)
				So(entries[0].Type, ShouldEqual, Regular)
				So(entries[1].Type, ShouldEqual, Regular)

				got := db.GetByID(setl1.ID())
				So(got, ShouldNotBeNil)
				So(got.Status, ShouldEqual, PendingDiscovery)
				So(got.NumFiles, ShouldEqual, 0)

				got, err = db.Discover(setl1.ID(), nil)
				So(err, ShouldBeNil)
				So(got, ShouldNotBeNil)
				So(got.NumFiles, ShouldEqual, 3)
				So(got.Symlinks, ShouldEqual, 1)

				entries, err = db.GetPureFileEntries(setl1.ID())
				So(err, ShouldBeNil)
				So(len(entries), ShouldEqual, 3)
				So(entries[1].Status, ShouldEqual, Pending)
				So(entries[0].Status, ShouldEqual, Pending)
				So(entries[1].Type, ShouldEqual, Regular)
				So(entries[0].Type, ShouldEqual, Symlink)
				So(entries[0].Dest, ShouldEqual, path1)

				setEntryToUploaded(entries[1], setl1, db)

				got = db.GetByID(setl1.ID())
				So(got, ShouldNotBeNil)
				So(got.Status, ShouldEqual, Uploading)

				setEntryToUploaded(entries[0], setl1, db)

				got = db.GetByID(setl1.ID())
				So(got, ShouldNotBeNil)
				So(got.Status, ShouldEqual, Complete)
				So(got.NumFiles, ShouldEqual, len(entries))
				So(got.Symlinks, ShouldEqual, 1)
				So(got.Hardlinks, ShouldEqual, 0)
				So(got.Uploaded, ShouldEqual, 2)
				So(got.Missing, ShouldEqual, 1)
				So(got.Failed, ShouldEqual, 0)

				Convey("then rediscover the set and still know about the symlinks", func() {
					got, err = db.Discover(setl1.ID(), nil)
					So(got, ShouldNotBeNil)
					So(err, ShouldBeNil)
					So(got.Symlinks, ShouldEqual, 1)
				})
			})

			Convey("And add a set with directories containing hardlinks to it", func() {
				tdir := t.TempDir()
				dir := filepath.Join(tdir, "sub1")
				err = os.Mkdir(dir, userPerms)
				So(err, ShouldBeNil)

				dir2 := filepath.Join(tdir, "sub2")
				err = os.Mkdir(dir2, userPerms)
				So(err, ShouldBeNil)

				local := filepath.Join(dir, "file")
				link1 := filepath.Join(dir, "link1")
				link2 := filepath.Join(dir2, "link2")
				unlinked := filepath.Join(dir, "unlinked")

				internal.CreateTestFile(t, local, "a")
				err = os.Link(local, link1)
				So(err, ShouldBeNil)
				err = os.Link(link1, link2)
				So(err, ShouldBeNil)

				info, errs := os.Stat(local)
				So(errs, ShouldBeNil)
				stat, ok := info.Sys().(*syscall.Stat_t)
				So(ok, ShouldBeTrue)

				internal.CreateTestFile(t, unlinked, "a")
				info, errs = os.Stat(unlinked)
				So(errs, ShouldBeNil)
				statUnlinked, ok := info.Sys().(*syscall.Stat_t)
				So(ok, ShouldBeTrue)

				setl1 := &Set{
					Name:        "setlink",
					Requester:   "jim",
					Transformer: "prefix=" + tdir + ":/remote",
				}

				err = db.AddOrUpdate(setl1)
				So(err, ShouldBeNil)

				dirents := []*walk.Dirent{
					{
						Path:  local,
						Inode: stat.Ino,
					},
					{
						Path:  link1,
						Inode: stat.Ino,
					},
					{
						Path:  link2,
						Inode: stat.Ino,
					},
					{
						Path:  unlinked,
						Inode: statUnlinked.Ino,
					},
				}

				discoverCB := func(_ []*Entry) ([]*walk.Dirent, error) { //nolint:unparam
					return dirents, nil
				}

				got, errd := db.Discover(setl1.ID(), discoverCB)
				So(errd, ShouldBeNil)

				So(got, ShouldNotBeNil)
				So(got.Hardlinks, ShouldEqual, 2)
				So(got.NumFiles, ShouldEqual, 4)

				entries, errg := db.GetFileEntries(setl1.ID())
				So(errg, ShouldBeNil)
				So(len(entries), ShouldEqual, 4)
				So(entries[0].Status, ShouldEqual, Pending)
				So(entries[1].Status, ShouldEqual, Pending)
				So(entries[2].Status, ShouldEqual, Pending)
				So(entries[3].Status, ShouldEqual, Pending)
				So(entries[0].Type, ShouldEqual, Regular)
				So(entries[1].Type, ShouldEqual, Hardlink)
				So(entries[1].Inode, ShouldEqual, stat.Ino)
				So(entries[2].Type, ShouldEqual, Regular)
				So(entries[3].Type, ShouldEqual, Hardlink)
				So(entries[3].Inode, ShouldEqual, stat.Ino)

				So(entries[0].InodeStoragePath(), ShouldBeBlank)
				So(entries[1].InodeStoragePath(), ShouldStartWith, local)
				So(entries[1].InodeStoragePath(), ShouldEndWith, fmt.Sprintf("/%d", entries[1].Inode))
				So(entries[3].InodeStoragePath(), ShouldEqual, entries[1].InodeStoragePath())

				Convey("then rediscover the set and still know about the hardlinks", func() {
					got, errd = db.Discover(setl1.ID(), func(dirEntries []*Entry) ([]*walk.Dirent, error) {
						return dirents, nil
					})
					So(errd, ShouldBeNil)
					So(got.Hardlinks, ShouldEqual, 2)

					got = db.GetByID(setl1.ID())
					So(got, ShouldNotBeNil)
					So(err, ShouldBeNil)
					So(got.Hardlinks, ShouldEqual, 2)
				})

				Convey("then get back all known local paths for the hardlink", func() {
					paths, errh := db.HardlinkPaths(entries[1])
					So(errh, ShouldBeNil)
					So(paths, ShouldResemble, []string{dirents[0].Path, dirents[3].Path})

					paths, errh = db.HardlinkPaths(entries[0])
					So(errh, ShouldBeNil)
					So(paths, ShouldResemble, []string{dirents[1].Path, dirents[3].Path})
				})

				Convey("then get a remote path for the hardlink", func() {
					path, errh := db.HardlinkRemote(entries[1])
					So(errh, ShouldBeNil)
					So(path, ShouldEqual, "/remote/sub1/file")
				})

				Convey("then previously seen moved files don't get treated as hardlinks", func() {
					moved := filepath.Join(dir, "moved")
					err = os.Rename(unlinked, moved)
					So(err, ShouldBeNil)

					dirents[2].Path = moved

					got, errd = db.Discover(setl1.ID(), discoverCB)
					So(errd, ShouldBeNil)

					entries, errg = db.GetFileEntries(setl1.ID())
					So(errg, ShouldBeNil)
					So(len(entries), ShouldEqual, 4)
					So(entries[0].Type, ShouldEqual, Regular)
					So(entries[1].Type, ShouldEqual, Hardlink)
					So(entries[1].Inode, ShouldEqual, stat.Ino)
					So(entries[2].Type, ShouldEqual, Regular)
					So(entries[3].Type, ShouldEqual, Hardlink)
					So(entries[3].Inode, ShouldEqual, stat.Ino)

					So(got, ShouldNotBeNil)
					So(got.Hardlinks, ShouldEqual, 2)
					So(got.NumFiles, ShouldEqual, 4)
				})
			})

			Convey("And add a set with directories containing symlinks to it", func() {
				setl1 := &Set{
					Name:        "setlink",
					Requester:   "jim",
					Transformer: "prefix=/local:/remote",
				}

				err = db.AddOrUpdate(setl1)
				So(err, ShouldBeNil)

				dir := t.TempDir()

				path1 := filepath.Join(dir, "file")
				path2 := filepath.Join(dir, "link")

				internal.CreateTestFile(t, path1, "")
				err = os.Symlink(path1, path2)
				So(err, ShouldBeNil)

				got, errb := db.Discover(setl1.ID(), func(_ []*Entry) ([]*walk.Dirent, error) {
					return []*walk.Dirent{
						{
							Path:  path1,
							Inode: 1,
						},
						{
							Path:  path2,
							Type:  os.ModeSymlink,
							Inode: 2,
						},
					}, nil
				})
				So(errb, ShouldBeNil)
				So(got, ShouldNotBeNil)
				So(got.Symlinks, ShouldEqual, 1)
				So(got.NumFiles, ShouldEqual, 2)

				entries, errG := db.GetFileEntries(setl1.ID())
				So(errG, ShouldBeNil)
				So(len(entries), ShouldEqual, 2)
				So(entries[0].Status, ShouldEqual, Pending)
				So(entries[1].Status, ShouldEqual, Pending)
				So(entries[0].Type, ShouldEqual, Regular)
				So(entries[1].Type, ShouldEqual, Symlink)
				So(entries[1].Dest, ShouldEqual, path1)
			})

			Convey("And add a set with a missing file to it", func() {
				setl1 := &Set{
					Name:        "missing",
					Requester:   "jim",
					Transformer: "prefix=/local:/remote",
				}

				err = db.AddOrUpdate(setl1)
				So(err, ShouldBeNil)

				missing := "/non/existent/file"

				err = db.SetFileEntries(setl1.ID(), []string{missing})
				So(err, ShouldBeNil)

				entries, errg := db.GetPureFileEntries(setl1.ID())
				So(errg, ShouldBeNil)
				So(len(entries), ShouldEqual, 1)
				So(entries[0].Status, ShouldEqual, Pending)
				So(entries[0].Type, ShouldEqual, Regular)

				got, errb := db.Discover(setl1.ID(), nil)
				So(got, ShouldNotBeNil)
				So(errb, ShouldBeNil)
				So(got.Missing, ShouldEqual, 1)

				entries, err = db.GetPureFileEntries(setl1.ID())
				So(err, ShouldBeNil)
				So(len(entries), ShouldEqual, 1)
				So(entries[0].Status, ShouldEqual, Missing)
				So(entries[0].Type, ShouldEqual, Regular)

				Convey("then rediscover the set and still know about the missing file", func() {
					got, errb := db.Discover(setl1.ID(), nil)
					So(got, ShouldNotBeNil)
					So(errb, ShouldBeNil)
					So(got.Missing, ShouldEqual, 1)
				})
			})

			Convey("And add a set with a missing directory to it (which are just recorded and not checked)", func() {
				setl1 := &Set{
					Name:        "missingdir",
					Requester:   "jim",
					Transformer: "prefix=/local:/remote",
				}

				err = db.AddOrUpdate(setl1)
				So(err, ShouldBeNil)

				missing := "/non/existent/dir"

				err = db.SetDirEntries(setl1.ID(), []*walk.Dirent{{
					Path: missing,
					Type: os.ModeDir,
				}})
				So(err, ShouldBeNil)

				entries, errg := db.GetDirEntries(setl1.ID())
				So(errg, ShouldBeNil)
				So(len(entries), ShouldEqual, 1)
				So(entries[0].Status, ShouldEqual, Pending)
				So(entries[0].Type, ShouldEqual, Directory)

				got := db.GetByID(setl1.ID())
				So(got, ShouldNotBeNil)
				So(err, ShouldBeNil)
				So(got.Missing, ShouldEqual, 0)
			})

			Convey("And add a set with an abnormal file to it", func() {
				setl1 := &Set{
					Name:        "abnormal",
					Requester:   "jim",
					Transformer: "prefix=/local:/remote",
				}

				err = db.AddOrUpdate(setl1)
				So(err, ShouldBeNil)

				dir := t.TempDir()

				fifoPath := filepath.Join(dir, "fifo")
				err = syscall.Mkfifo(fifoPath, userPerms)
				So(err, ShouldBeNil)

				err = db.SetFileEntries(setl1.ID(), []string{fifoPath})
				So(err, ShouldBeNil)

				entries, errg := db.GetPureFileEntries(setl1.ID())
				So(errg, ShouldBeNil)
				So(len(entries), ShouldEqual, 1)
				So(entries[0].Status, ShouldEqual, Pending)
				So(entries[0].Type, ShouldEqual, Regular)

				got, errb := db.Discover(setl1.ID(), nil)
				So(got, ShouldNotBeNil)
				So(errb, ShouldBeNil)
				So(got.Missing, ShouldEqual, 0)
				So(got.Abnormal, ShouldEqual, 1)

				entries, err = db.GetPureFileEntries(setl1.ID())
				So(err, ShouldBeNil)
				So(len(entries), ShouldEqual, 1)
				So(entries[0].Status, ShouldEqual, AbnormalEntry)
				So(entries[0].Type, ShouldEqual, Abnormal)

				Convey("then rediscover the set and still know about the abnormal file", func() {
					got, errb := db.Discover(setl1.ID(), nil)
					So(got, ShouldNotBeNil)
					So(errb, ShouldBeNil)
					So(got.Abnormal, ShouldEqual, 1)
				})
			})
		})
	})
}

func discoverASet(db *DB, set *Set, discoveryFunc func() ([]*walk.Dirent, error), pendingTestsFunc func()) {
	errCh := make(chan error, 1)
	waitCh := make(chan struct{})

	go func() {
		_, errd := db.Discover(set.ID(), func(e []*Entry) ([]*walk.Dirent, error) {
			waitCh <- struct{}{}
			<-waitCh

			return discoveryFunc()
		})

		errCh <- errd
	}()

	<-waitCh

	pendingTestsFunc()

	waitCh <- struct{}{}

	err := <-errCh
	So(err, ShouldBeNil)
}

func setEntryToUploaded(entry *Entry, given *Set, db *DB) {
	transformer, err := given.MakeTransformer()
	So(err, ShouldBeNil)

	r, err := put.NewRequestWithTransformedLocal(entry.Path, transformer)
	So(err, ShouldBeNil)

	r.Set = given.Name
	r.Requester = given.Requester

	if entry.Type == Hardlink {
		r.Hardlink = entry.Dest
	}

	if entry.Type == Symlink {
		r.Symlink = entry.Dest
	}

	r.Status = put.RequestStatusUploading
	_, err = db.SetEntryStatus(r)
	So(err, ShouldBeNil)

	r.Status = put.RequestStatusUploaded
	_, err = db.SetEntryStatus(r)
	So(err, ShouldBeNil)
}

func TestBackup(t *testing.T) {
	Convey("Given a example database, Backup() backs it up", t, func() {
		dir := t.TempDir()

		backupFile := filepath.Join(dir, "backup")

		db, err := New(filepath.Join(dir, "db"), backupFile)
		So(err, ShouldBeNil)
		So(db, ShouldNotBeNil)

		db.SetMinimumTimeBetweenBackups(0)

		example := &Set{
			Name:      "checkme",
			Requester: "requester",
		}

		err = db.AddOrUpdate(example)
		So(err, ShouldBeNil)

		err = db.Backup()
		So(err, ShouldBeNil)

		testBackupOK := func(path string) {
			backupUpDB, errn := NewRO(path)
			So(errn, ShouldBeNil)
			So(backupUpDB, ShouldNotBeNil)

			got, errg := backupUpDB.GetByNameAndRequester("checkme", "requester")
			So(errg, ShouldBeNil)
			So(got, ShouldResemble, example)
		}

		testBackupOK(backupFile)

		Convey("Backup() again to the same path succeeds", func() {
			err = db.Backup()
			So(err, ShouldBeNil)

			testBackupOK(backupFile)

			_, err = os.Stat(backupFile + backupExt)
			So(err, ShouldNotBeNil)
		})

		Convey("Backup() again to the same path when it is not writable creates a temp file with the backup", func() {
			err = os.Remove(backupFile)
			So(err, ShouldBeNil)

			err = os.Mkdir(backupFile, userPerms)
			So(err, ShouldBeNil)

			err = db.Backup()
			So(err, ShouldNotBeNil)

			testBackupOK(backupFile + backupExt)
		})

		Convey("Backup()s queue if called multiple times simultaneously, resulting in 1 extra backup", func() {
			db.SetMinimumTimeBetweenBackups(1 * time.Second)
			n := 100
			errCh := make(chan error, n)

			watcher, errw := fsnotify.NewWatcher()
			So(errw, ShouldBeNil)
			defer watcher.Close()

			doBackupCalls := 0
			watchingDone := make(chan struct{})

			go func() {
				defer close(watchingDone)

				for {
					select {
					case event, ok := <-watcher.Events:
						if !ok {
							return
						}

						if event.Has(fsnotify.Rename) {
							doBackupCalls++
						}
					case _, ok := <-watcher.Errors:
						if !ok {
							return
						}
					}
				}
			}()

			err = watcher.Add(filepath.Dir(backupFile))
			So(err, ShouldBeNil)

			for i := 0; i < n; i++ {
				go func() {
					errb := db.Backup()
					errCh <- errb
				}()
			}

			for i := 0; i < n; i++ {
				So(<-errCh, ShouldBeNil)
			}

			close(errCh)
			err = watcher.Close()
			So(err, ShouldBeNil)
			<-watchingDone
			So(doBackupCalls, ShouldEqual, 2)
		})

		Convey("and can back it up with local handler as well", func() {
			remoteDir := filepath.Join(dir, "remote")
			err = os.Mkdir(remoteDir, userPerms)
			So(err, ShouldBeNil)
			remotePath := filepath.Join(remoteDir, "db")

			handler := put.GetLocalHandler()

			db.EnableRemoteBackups(remotePath, handler)

			err = db.Backup()
			So(err, ShouldBeNil)

			_, err := os.Stat(remotePath)
			So(err, ShouldBeNil)

			testBackupOK(remotePath)
		})

		Convey("and can back it up to iRODS as well", func() {
			remoteDir := os.Getenv("IBACKUP_TEST_COLLECTION")
			if remoteDir == "" {
				SkipConvey("skipping iRODS backup test since IBACKUP_TEST_COLLECTION not set", func() {})

				return
			}

			remotePath := filepath.Join(remoteDir, "db")

			handler, err := put.GetBatonHandler()
			So(err, ShouldBeNil)

			db.EnableRemoteBackups(remotePath, handler)

			err = db.Backup()
			So(err, ShouldBeNil)

			localPath := t.TempDir()
			localDB := filepath.Join(localPath, "db")

			_, err = exec.Command("iget", remotePath, localDB).CombinedOutput()
			So(err, ShouldBeNil)

			testBackupOK(localDB)

			Convey("and child baton-do processes end after upload", func() {
				p, err := process.NewProcess(int32(os.Getpid()))
				So(err, ShouldBeNil)

				children, err := p.Children()
				if errors.Is(err, process.ErrorNoChildren) {
					return
				}

				So(err, ShouldBeNil)

				count := 0

				for _, child := range children {
					exe, err := child.Exe()
					So(err, ShouldBeNil)

					if filepath.Base(exe) == "baton-do" {
						count++
					}
				}

				So(count, ShouldEqual, 0)
			})
		})
	})
}

func createFileEnts(paths []string) []*walk.Dirent {
	entries := make([]*walk.Dirent, len(paths))

	for n, path := range paths {
		entries[n] = &walk.Dirent{
			Path: path,
		}
	}

	return entries
}

func TestCountsValid(t *testing.T) {
	Convey("countsValid detects when set counts do or don't make sense", t, func() {
		s := new(Set)

		So(s.countsValid(), ShouldBeTrue)

		s.NumFiles = 1
		s.Uploaded = 1

		So(s.countsValid(), ShouldBeTrue)

		s.Hardlinks = 1

		So(s.countsValid(), ShouldBeTrue)

		s.Hardlinks = 0
		s.Symlinks = 1

		So(s.countsValid(), ShouldBeTrue)

		s.Symlinks = 0
		s.Uploaded = 0
		s.Missing = 1

		So(s.countsValid(), ShouldBeTrue)

		s.Missing = 0
		s.Failed = 1

		So(s.countsValid(), ShouldBeTrue)

		s.Failed = 2

		So(s.countsValid(), ShouldBeFalse)
	})
}
