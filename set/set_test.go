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
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/shirou/gopsutil/process"
	. "github.com/smartystreets/goconvey/convey"
	gas "github.com/wtsi-hgi/go-authserver"
	"github.com/wtsi-hgi/ibackup/baton"
	"github.com/wtsi-hgi/ibackup/internal"
	"github.com/wtsi-hgi/ibackup/internal/testutil"
	"github.com/wtsi-hgi/ibackup/slack"
	"github.com/wtsi-hgi/ibackup/transfer"
	bolt "go.etcd.io/bbolt"
)

const userPerms = 0700

func TestSet(t *testing.T) {
	Convey("With the default transformers", t, func() {
		internal.InitStatter(t)
		internal.RegisterDefaultTransformers(t)

		Convey("Set statuses convert nicely to strings", func() {
			So(PendingDiscovery.String(), ShouldEqual, "pending discovery")
			So(PendingUpload.String(), ShouldEqual, "pending upload")
			So(Uploading.String(), ShouldEqual, "uploading")
			So(Failing.String(), ShouldEqual, "failing")
			So(Complete.String(), ShouldEqual, "complete")
		})

		Convey("Entry statuses convert nicely to strings", func() {
			So(Pending.String(), ShouldEqual, "pending")
			So(Uploaded.String(), ShouldEqual, "uploaded")
			So(Failed.String(), ShouldEqual, "failed")
			So(Missing.String(), ShouldEqual, "missing")
		})

		Convey("Status methods are useful helpers", func() {
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

		Convey("Entry.ShouldUpload() gives good advice", func() {
			given := &Set{
				LastDiscovery: time.Now(),
			}

			e := &Entry{Status: Pending}
			So(e.ShouldUpload(given), ShouldBeTrue)

			e.Status = Missing
			So(e.ShouldUpload(given), ShouldBeTrue)

			e.Status = Failed
			So(e.ShouldUpload(given), ShouldBeTrue)

			e.Attempts = AttemptsToBeConsideredFailing
			So(e.ShouldUpload(given), ShouldBeTrue)

			e.Attempts = 1
			e.LastAttempt = given.LastDiscovery.Add(1 * time.Second)
			e.Status = Uploaded
			So(e.ShouldUpload(given), ShouldBeFalse)

			given.LastDiscovery = given.LastDiscovery.Add(2 * time.Second)
			So(e.ShouldUpload(given), ShouldBeTrue)

			given.Frozen = true
			So(e.ShouldUpload(given), ShouldBeFalse)
		})

		Convey("Discovered() returns friendly strings", func() {
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

		Convey("Count() and Size() return friendly strings", func() {
			s := &Set{}
			So(s.Count(), ShouldEqual, "pending")
			So(s.Size(), ShouldEqual, "pending")

			s.NumFiles = 3

			So(s.Count(), ShouldEqual, "pending")
			So(s.Size(), ShouldEqual, "pending")

			s.LastDiscovery = time.Now()

			So(s.Count(), ShouldEqual, "3")
			So(s.Size(), ShouldEqual, "0 B (and counting)")

			s.SizeTotal = 30
			So(s.Size(), ShouldEqual, "30 B (and counting)")

			s.Status = Complete
			So(s.Count(), ShouldEqual, "3")
			So(s.Size(), ShouldEqual, "30 B")

			s.LastCompletedCount = 3
			s.LastCompletedSize = 30
			s.NumFiles = 0
			s.SizeTotal = 0
			s.Status = PendingDiscovery

			So(s.Count(), ShouldEqual, "3 (as of last completion)")
			So(s.Size(), ShouldEqual, "30 B (as of last completion)")
		})

		Convey("MakeTransformer and TransformPath work", func() {
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

			dest, err = s.TransformPath("/lustre/scratch118/humgen/projects_v2/ddd/file.txt")
			So(err, ShouldBeNil)
			So(dest, ShouldEqual, "/humgen/projects/ddd/scratch118_v2/file.txt")

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

			dest, err = s.TransformPath("/lustre/scratch126/gengen/teams_v2/parts/sequencing/file.txt")
			So(err, ShouldBeNil)
			So(dest, ShouldEqual, "/humgen/gengen/teams/parts/scratch126_v2/sequencing/file.txt")

			s = &Set{Transformer: "prefix=" + dir + ":/zone"}
			trans, err = s.MakeTransformer()
			So(err, ShouldBeNil)
			remote, err = trans(filepath.Join(dir, "file.txt"))
			So(err, ShouldBeNil)
			So(remote, ShouldEqual, "/zone/file.txt")
		})

		Convey("UsageSummary returns summaries of all given sets", func() {
			nov23 := time.Unix(1700063826, 0)
			month := 730 * time.Hour
			sets := []*Set{
				{Name: "setA", Requester: "userA", LastCompleted: nov23,
					SizeTotal: 10, NumFiles: 1},
				{Name: "setB", Requester: "userA", LastCompleted: nov23.Add(month),
					SizeTotal: 20, NumFiles: 2},
				{Name: "setC", Requester: "userB", LastCompleted: nov23,
					SizeTotal: 40, NumFiles: 3},
				{Name: "setD", Requester: "userC", LastCompleted: nov23.Add(-1 * month),
					SizeTotal: 1, NumFiles: 4},
				{Name: "setE", Requester: "userD", LastCompleted: nov23.Add(-1 * month),
					SizeTotal: 1, NumFiles: 5},
				{Name: "setF", Requester: "userE", SizeTotal: 0, NumFiles: 1},
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
					SizeTotal: 10 * uint64(bytesInTiB), NumFiles: 1},
			}

			usage = UsageSummary(sets)
			So(usage.Total.SizeTiB(), ShouldEqual, 10)
		})
	})
}

func TestSetDB(t *testing.T) {
	Convey("Given a path", t, func() {
		internal.InitStatter(t)

		tDir := t.TempDir()
		dbPath := filepath.Join(tDir, "set.db")

		Convey("You can create a new database", func() {
			slackWriter := gas.NewStringLogger()
			slacker := slack.NewMock(slackWriter)

			db, err := New(dbPath, "", false)
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

				err = db.MergeFileEntries(set.ID(), []string{"/a/b.txt", "/c/d.txt", "/e/f.txt"})
				So(err, ShouldBeNil)

				err = db.MergeDirEntries(set.ID(), createFileEnts([]string{"/g/h", "/g/i"}))
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

				err = db.MergeFileEntries(set2.ID(), []string{"/a/b.txt", "/c/j.txt"})
				So(err, ShouldBeNil)

				err = db.MergeFileEntries(set2.ID(), []string{"/a/b.txt", "/c/k.txt"})
				So(err, ShouldBeNil)

				Convey("And given a mixture of complete and incomplete remove requests", func() {
					remReqs := []RemoveReq{
						{
							Path:       "a/file1.txt",
							Set:        set,
							IsDir:      false,
							IsComplete: true,
						},
						{
							Path:       "a/b",
							Set:        set,
							IsDir:      true,
							IsComplete: true,
						},
						{
							Path:       "a/b/c",
							Set:        set,
							IsDir:      true,
							IsComplete: true,
						},
						{
							Path:       "a/b/c/file2.txt",
							Set:        set,
							IsDir:      false,
							IsComplete: true,
						},
						{
							Path:       "a/b/c/file3.txt",
							Set:        set,
							IsDir:      false,
							IsComplete: true,
						},
						{
							Path:       "a/b/c/file4.txt",
							Set:        set,
							IsDir:      false,
							IsComplete: false,
						},
					}

					Convey("You can put them into the sets remove bucket and get them back", func() {
						err = db.SetRemoveRequests(set.ID(), remReqs)
						So(err, ShouldBeNil)

						rrs, errg := db.GetRemoveRequests(set.ID())
						So(errg, ShouldBeNil)
						So(len(rrs), ShouldEqual, len(remReqs))

						Convey("And you can optimise the bucket", func() {
							err = db.OptimiseRemoveBucket(set.ID())
							So(err, ShouldBeNil)

							rrs, err = db.GetRemoveRequests(set.ID())
							So(err, ShouldBeNil)

							So(len(rrs), ShouldEqual, 3)

							Convey("And you can update a remove request and optimise again", func() {
								remReqs[5].IsComplete = true

								err = db.UpdateRemoveRequest(remReqs[5])
								So(err, ShouldBeNil)

								err = db.OptimiseRemoveBucket(set.ID())
								So(err, ShouldBeNil)

								rrs, err = db.GetRemoveRequests(set.ID())
								So(err, ShouldBeNil)
								So(len(rrs), ShouldEqual, 2)
								So(rrs[1].Path, ShouldEqual, remReqs[0].Path)
								So(rrs[0].Path, ShouldEqual, remReqs[1].Path+"/")
							})
						})
					})
				})

				Convey("You can get all paths containing a prefix", func() {
					err = db.MergeFileEntries(set2.ID(), []string{"/a/a/j.txt", "/a/b/c/k.txt",
						"/a/b/c/l.txt", "/a/b/d/m.txt", "/c/n.txt"})
					So(err, ShouldBeNil)

					files, errp := db.getPathsWithPrefix(set2.ID(), fileBucket, "/a/b/c/")
					So(errp, ShouldBeNil)

					So(len(files), ShouldEqual, 2)
					So(files, ShouldContain, "/a/b/c/k.txt")
					So(files, ShouldContain, "/a/b/c/l.txt")
				})

				Convey("Then remove files and dirs from the sets", func() {
					err = db.removeEntry(set.ID(), "/a/b.txt", fileBucket)
					So(err, ShouldBeNil)

					fEntries, errg := db.GetFileEntries(set.ID(), nil)
					So(errg, ShouldBeNil)
					So(len(fEntries), ShouldEqual, 2)
					So(fEntries[0], ShouldResemble, newEntry("/c/d.txt"))
					So(fEntries[1], ShouldResemble, newEntry("/e/f.txt"))

					err = db.removeEntry(set.ID(), "/g/h", dirBucket)
					So(err, ShouldBeNil)

					dEntries, errg := db.GetDirEntries(set.ID(), nil)
					So(errg, ShouldBeNil)
					So(len(dEntries), ShouldEqual, 1)
					So(dEntries[0], ShouldResemble, newEntry("/g/i"))
				})

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

					Convey("And delete it when there's something in all the sub-buckets", func() {
						err = db.Delete("invalid")
						So(err, ShouldNotBeNil)

						errm := db.mergeEntries(set.ID(), []*Dirent{{Path: "/discovered/file"}}, discoveredBucket, Pending)
						So(errm, ShouldBeNil)

						errm = db.mergeEntries(set.ID(), []*Dirent{{Path: "/discovered/dir", Mode: fs.ModeDir}},
							discoveredFoldersBucket, Pending)
						So(errm, ShouldBeNil)

						errm = db.mergeEntries(set.ID(), []*Dirent{{Path: "/discovered/removedfile"}}, removedBucket, Pending)
						So(errm, ShouldBeNil)

						bucketList := [...]string{
							fileBucket, discoveredBucket, dirBucket, discoveredFoldersBucket, removedBucket,
						}

						for _, bucketName := range bucketList {
							entries, errg := db.getEntries(set.ID(), bucketName, nil)
							So(errg, ShouldBeNil)
							So(len(entries), ShouldBeGreaterThan, 0)
						}

						checkUserLookup := func() bool {
							found := false

							errv := db.db.View(func(tx *bolt.Tx) error {
								b := tx.Bucket([]byte(userToSetBucket))
								val := b.Get([]byte(set.Requester + separator + set.ID()))
								found = val != nil

								return nil
							})
							So(errv, ShouldBeNil)

							return found
						}

						So(checkUserLookup(), ShouldBeTrue)

						failedPath := "/path/failed"

						erru := db.db.Update(func(tx *bolt.Tx) error {
							return db.addFailedLookup(tx, set.ID(), failedPath, &Entry{Path: failedPath, Status: Failed})
						})
						So(erru, ShouldBeNil)

						checkFailedLookup := func() bool {
							found := false

							errv := db.db.View(func(tx *bolt.Tx) error {
								b, key := db.getBucketAndKeyForFailedLookup(tx, set.ID(), failedPath)
								val := b.Get(key)
								found = val != nil

								return nil
							})
							So(errv, ShouldBeNil)

							return found
						}

						So(checkFailedLookup(), ShouldBeTrue)

						err = db.Delete(set.ID())
						So(err, ShouldBeNil)

						retrieved = db.GetByID(set.ID())
						So(retrieved, ShouldBeNil)

						for _, bucketName := range bucketList {
							entries, errg := db.getEntries(set.ID(), bucketName, nil)
							So(errg, ShouldBeNil)
							So(entries, ShouldBeNil)
						}

						So(checkUserLookup(), ShouldBeFalse)
						So(checkFailedLookup(), ShouldBeFalse)
					})

					Convey("And delete it when there isn't something in all the sub-buckets", func() {
						entries, errg := db.getEntries(set.ID(), fileBucket, nil)
						So(errg, ShouldBeNil)
						So(len(entries), ShouldBeGreaterThan, 0)

						entries, errg = db.getEntries(set.ID(), dirBucket, nil)
						So(errg, ShouldBeNil)
						So(len(entries), ShouldBeGreaterThan, 0)

						err = db.Delete(set.ID())
						So(err, ShouldBeNil)

						retrieved = db.GetByID(set.ID())
						So(retrieved, ShouldBeNil)

						entries, errg = db.getEntries(set.ID(), fileBucket, nil)
						So(errg, ShouldBeNil)
						So(entries, ShouldBeNil)

						entries, errg = db.getEntries(set.ID(), discoveredBucket, nil)
						So(errg, ShouldBeNil)
						So(entries, ShouldBeNil)

						entries, errg = db.getEntries(set.ID(), dirBucket, nil)
						So(errg, ShouldBeNil)
						So(entries, ShouldBeNil)

						entries, errg = db.getEntries(set.ID(), discoveredFoldersBucket, nil)
						So(errg, ShouldBeNil)
						So(entries, ShouldBeNil)

						entries, errg = db.getEntries(set.ID(), removedBucket, nil)
						So(errg, ShouldBeNil)
						So(entries, ShouldBeNil)
					})
				})

				Convey("Then hide even read-only sets", func() {
					set.ReadOnly = true
					err = db.AddOrUpdate(set)
					So(err, ShouldBeNil)

					set.Hide = true
					err = db.AddOrUpdate(set)
					So(err, ShouldNotBeNil)
					So(err.Error(), ShouldStartWith, ErrSetIsNotWritable)

					set.Hide = false

					err = db.Hide(set)
					So(err, ShouldBeNil)

					retrieved := db.GetByID(set.ID())
					So(retrieved, ShouldNotBeNil)
					So(retrieved.ReadOnly, ShouldBeTrue)
					So(retrieved.Hide, ShouldBeTrue)

					err = db.Hide(set)
					So(err, ShouldBeNil)

					err = db.Close()
					So(err, ShouldBeNil)

					db2, errn := New(dbPath, "", false)
					So(errn, ShouldBeNil)
					So(db2, ShouldNotBeNil)

					defer db2.Close()

					retrieved = db2.GetByID(set.ID())
					So(retrieved, ShouldNotBeNil)
					So(retrieved.ReadOnly, ShouldBeTrue)
					So(retrieved.Hide, ShouldBeTrue)
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

					fEntries, errg := db.GetFileEntries(sets[1].ID(), nil)
					So(errg, ShouldBeNil)
					So(len(fEntries), ShouldEqual, 3)
					So(fEntries[0], ShouldResemble, newEntry("/a/b.txt"))
					So(fEntries[1], ShouldResemble, newEntry("/c/d.txt"))
					So(fEntries[2], ShouldResemble, newEntry("/e/f.txt"))

					dEntries, errg := db.GetDirEntries(sets[1].ID(), nil)
					So(errg, ShouldBeNil)
					So(len(dEntries), ShouldEqual, 2)
					So(dEntries[0], ShouldResemble, newEntry("/g/h"))
					So(dEntries[1], ShouldResemble, newEntry("/g/i"))

					fEntries, err = db.GetFileEntries(sets[0].ID(), nil)
					So(err, ShouldBeNil)
					So(len(fEntries), ShouldEqual, 3)
					So(fEntries[0], ShouldResemble, newEntry("/a/b.txt"))
					So(fEntries[1], ShouldResemble, newEntry("/c/j.txt"))
					So(fEntries[2], ShouldResemble, newEntry("/c/k.txt"))

					dEntries, err = db.GetDirEntries(sets[0].ID(), nil)
					So(err, ShouldBeNil)
					So(len(dEntries), ShouldEqual, 0)
				})

				Convey("Then get an particular entry from a set", func() {
					entry, errr := db.GetFileEntryForSet(set2.ID(), "/a/b.txt")
					So(errr, ShouldBeNil)
					So(entry, ShouldResemble, newEntry("/a/b.txt"))

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

						clearFileBucket(t, db, set.ID())

						err = db.MergeFileEntries(set.ID(), pureFiles)
						So(err, ShouldBeNil)

						slackWriter.Reset()

						discoverASet(db, sets[0], func() ([]*Dirent, []*Dirent, error) {
							return createFileEnts([]string{"/g/h/l.txt", "/g/i/m.txt"}), []*Dirent{}, nil
						}, func() {
							sets, err = db.GetByRequester("jim")
							So(err, ShouldBeNil)
							So(sets[0].Status, ShouldEqual, PendingDiscovery)
							So(sets[0].StartedDiscovery.IsZero(), ShouldBeFalse)
						})

						bsets, errg := db.GetByRequester("jim")
						So(errg, ShouldBeNil)
						So(bsets[0].LastDiscovery, ShouldHappenAfter, sets[0].LastDiscovery)

						fEntries, errg := db.GetFileEntries(sets[0].ID(), nil)
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
						So(sets[0].SizeTotal, ShouldEqual, 0)

						setsAll, errg := db.GetAll()
						So(errg, ShouldBeNil)
						So(setsAll, ShouldNotBeNil)
						So(len(setsAll), ShouldEqual, 2)

						r := &transfer.Request{
							Local:     pureFiles[0],
							Requester: set.Requester,
							Set:       set.Name,
							Size:      3,
							Status:    transfer.RequestStatusUploading,
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
						So(sets[0].SizeTotal, ShouldEqual, 3)
						So(sets[0].Uploaded, ShouldEqual, 0)
						So(sets[0].LastCompletedSize, ShouldEqual, 0)

						r = &transfer.Request{
							Local:     pureFiles[0],
							Requester: set.Requester,
							Set:       set.Name,
							Size:      3,
							Status:    transfer.RequestStatusUploaded,
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
						So(sets[0].SizeTotal, ShouldEqual, 3)
						So(sets[0].Uploaded, ShouldEqual, 1)
						So(sets[0].Failed, ShouldEqual, 0)
						So(sets[0].LastCompletedSize, ShouldEqual, 0)

						fEntries, err = db.GetFileEntries(sets[0].ID(), nil)
						So(err, ShouldBeNil)
						So(len(fEntries), ShouldEqual, 5)
						So(fEntries[0].Size, ShouldEqual, 3)
						So(fEntries[0].Status, ShouldEqual, Uploaded)
						So(fEntries[0].LastAttempt.IsZero(), ShouldBeFalse)

						r = &transfer.Request{
							Local:     pureFiles[1],
							Requester: set.Requester,
							Set:       set.Name,
							Size:      2,
							Status:    transfer.RequestStatusUnmodified,
							Error:     "",
						}

						_, err = db.SetEntryStatus(r)
						So(err, ShouldBeNil)

						sets, err = db.GetByRequester("jim")
						So(err, ShouldBeNil)

						So(sets[0].Status, ShouldEqual, Uploading)
						So(sets[0].SizeTotal, ShouldEqual, 5)
						So(sets[0].Uploaded, ShouldEqual, 1)
						So(sets[0].Replaced, ShouldEqual, 0)
						So(sets[0].Skipped, ShouldEqual, 1)

						fEntries, err = db.GetFileEntries(sets[0].ID(), nil)
						So(err, ShouldBeNil)
						So(len(fEntries), ShouldEqual, 5)
						So(fEntries[1].Size, ShouldEqual, 2)
						So(fEntries[1].Status, ShouldEqual, Skipped)
						So(fEntries[1].LastAttempt.IsZero(), ShouldBeFalse)

						r = &transfer.Request{
							Local:     pureFiles[2],
							Requester: set.Requester,
							Set:       set.Name,
							Size:      4,
							Status:    transfer.RequestStatusUploading,
							Error:     "",
						}

						_, err = db.SetEntryStatus(r)
						So(err, ShouldBeNil)

						r.Status = transfer.RequestStatusReplaced
						_, err = db.SetEntryStatus(r)
						So(err, ShouldBeNil)

						sets, err = db.GetByRequester("jim")
						So(err, ShouldBeNil)

						So(sets[0].NumFiles, ShouldEqual, 5)
						So(sets[0].Uploaded, ShouldEqual, 1)
						So(sets[0].Replaced, ShouldEqual, 1)
						So(sets[0].Skipped, ShouldEqual, 1)
						So(sets[0].Status, ShouldEqual, Uploading)
						So(sets[0].SizeTotal, ShouldEqual, 9)

						fEntries, err = db.GetFileEntries(sets[0].ID(), nil)
						So(err, ShouldBeNil)
						So(len(fEntries), ShouldEqual, 5)
						So(fEntries[2].Size, ShouldEqual, 4)
						So(fEntries[2].Status, ShouldEqual, Replaced)
						So(fEntries[2].LastAttempt.IsZero(), ShouldBeFalse)

						r = &transfer.Request{
							Local:     "/g/h/l.txt",
							Requester: set.Requester,
							Set:       set.Name,
							Size:      6,
							Status:    transfer.RequestStatusUploading,
							Error:     "",
						}

						_, err = db.SetEntryStatus(r)
						So(err, ShouldBeNil)

						fEntries, failSkips, errg := db.GetFailedEntries(sets[0].ID())
						So(errg, ShouldBeNil)
						So(len(fEntries), ShouldEqual, 0)
						So(failSkips, ShouldEqual, 0)

						r.Status = transfer.RequestStatusFailed
						errMsg := "upload failed"
						r.Error = errMsg
						_, err = db.SetEntryStatus(r)
						So(err, ShouldBeNil)

						sets, err = db.GetByRequester("jim")
						So(err, ShouldBeNil)

						So(sets[0].Status, ShouldEqual, Uploading)
						So(sets[0].SizeTotal, ShouldEqual, 15)
						So(sets[0].Uploaded, ShouldEqual, 1)
						So(sets[0].Replaced, ShouldEqual, 1)
						So(sets[0].Skipped, ShouldEqual, 1)
						So(sets[0].Failed, ShouldEqual, 1)

						fEntries, err = db.GetFileEntries(sets[0].ID(), nil)
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

						r.Status = transfer.RequestStatusUploading
						_, err = db.SetEntryStatus(r)
						So(err, ShouldBeNil)

						r.Status = transfer.RequestStatusFailed
						r.Error = errMsg
						_, err = db.SetEntryStatus(r)
						So(err, ShouldBeNil)

						sets, err = db.GetByRequester("jim")
						So(err, ShouldBeNil)

						So(sets[0].Status, ShouldEqual, Uploading)
						So(sets[0].SizeTotal, ShouldEqual, 15)
						So(sets[0].Uploaded, ShouldEqual, 1)
						So(sets[0].Replaced, ShouldEqual, 1)
						So(sets[0].Skipped, ShouldEqual, 1)
						So(sets[0].Failed, ShouldEqual, 1)

						fEntries, err = db.GetFileEntries(sets[0].ID(), nil)
						So(err, ShouldBeNil)
						So(len(fEntries), ShouldEqual, 5)
						So(fEntries[3].Size, ShouldEqual, 6)
						So(fEntries[3].Status, ShouldEqual, Failed)
						So(fEntries[3].Attempts, ShouldEqual, 2)
						So(fEntries[3].LastAttempt.IsZero(), ShouldBeFalse)
						So(fEntries[3].LastError, ShouldEqual, errMsg)

						r.Status = transfer.RequestStatusUploading

						_, err = db.SetEntryStatus(r)
						So(err, ShouldBeNil)

						r.Status = transfer.RequestStatusFailed
						r.Error = errMsg
						_, err = db.SetEntryStatus(r)
						So(err, ShouldBeNil)

						sets, err = db.GetByRequester("jim")
						So(err, ShouldBeNil)

						So(sets[0].Status, ShouldEqual, Failing)
						So(sets[0].SizeTotal, ShouldEqual, 15)
						So(sets[0].Uploaded, ShouldEqual, 1)
						So(sets[0].Replaced, ShouldEqual, 1)
						So(sets[0].Skipped, ShouldEqual, 1)
						So(sets[0].Failed, ShouldEqual, 1)
						So(slackWriter.String(), ShouldEqual, slack.BoxPrefixError+"`jim.set1` has failed uploads")
						slackWriter.Reset()

						fEntries, err = db.GetFileEntries(sets[0].ID(), nil)
						So(err, ShouldBeNil)
						So(len(fEntries), ShouldEqual, 5)
						So(fEntries[3].Size, ShouldEqual, 6)
						So(fEntries[3].Status, ShouldEqual, Failed)
						So(fEntries[3].Attempts, ShouldEqual, 3)
						So(fEntries[3].LastAttempt.IsZero(), ShouldBeFalse)
						So(fEntries[3].LastError, ShouldEqual, errMsg)

						r = &transfer.Request{
							Local:     "/g/i/m.txt",
							Requester: set.Requester,
							Set:       set.Name,
							Size:      0,
							Status:    transfer.RequestStatusMissing,
							Error:     "",
						}

						_, err = db.SetEntryStatus(r)
						So(err, ShouldBeNil)

						sets, err = db.GetByRequester("jim")
						So(err, ShouldBeNil)

						So(sets[0].Status, ShouldEqual, Complete)
						So(sets[0].SizeTotal, ShouldEqual, 15)
						So(sets[0].Uploaded, ShouldEqual, 1)
						So(sets[0].Replaced, ShouldEqual, 1)
						So(sets[0].Skipped, ShouldEqual, 1)
						So(sets[0].Failed, ShouldEqual, 1)
						So(sets[0].Missing, ShouldEqual, 1)
						So(slackWriter.String(), ShouldEqual,
							fmt.Sprintf("%s`jim.set1` completed backup (%d newly uploaded; %d replaced; "+
								"%d skipped; %d failed; %d missing; %d orphaned; %d abnormal; %s data uploaded)",
								slack.BoxPrefixSuccess, sets[0].Uploaded, sets[0].Replaced, sets[0].Skipped, sets[0].Failed,
								sets[0].Missing, sets[0].Orphaned, sets[0].Abnormal, sets[0].UploadedSize()))
						lastCompleted := sets[0].LastCompleted
						So(lastCompleted.IsZero(), ShouldBeFalse)
						So(sets[0].LastCompletedSize, ShouldEqual, 15)
						So(sets[0].LastCompletedCount, ShouldEqual, 4)
						So(sets[0].SizeUploaded, ShouldEqual, 13)

						fEntries, err = db.GetFileEntries(sets[0].ID(), nil)
						So(err, ShouldBeNil)
						So(len(fEntries), ShouldEqual, 5)
						So(fEntries[4].Size, ShouldEqual, 0)
						So(fEntries[4].Status, ShouldEqual, Missing)
						So(fEntries[4].LastAttempt.IsZero(), ShouldBeFalse)
						So(fEntries[4].LastError, ShouldBeBlank)

						r = &transfer.Request{
							Local:     "/g/h/l.txt",
							Requester: set.Requester,
							Set:       set.Name,
							Size:      6,
							Status:    transfer.RequestStatusUploading,
							Error:     "",
						}

						_, err = db.SetEntryStatus(r)
						So(err, ShouldBeNil)

						r.Status = transfer.RequestStatusUploaded
						_, err = db.SetEntryStatus(r)
						So(err, ShouldBeNil)

						sets, err = db.GetByRequester("jim")
						So(err, ShouldBeNil)

						So(sets[0].Status, ShouldEqual, Complete)
						So(sets[0].SizeTotal, ShouldEqual, 15)
						So(sets[0].Uploaded, ShouldEqual, 2)
						So(sets[0].Replaced, ShouldEqual, 1)
						So(sets[0].Skipped, ShouldEqual, 1)
						So(sets[0].Failed, ShouldEqual, 0)
						lastCompleted2 := sets[0].LastCompleted
						So(lastCompleted2.After(lastCompleted), ShouldBeTrue)
						So(sets[0].LastCompletedSize, ShouldEqual, 15)
						So(sets[0].LastCompletedCount, ShouldEqual, 4)

						fEntries, err = db.GetFileEntries(sets[0].ID(), nil)
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

							discoverASet(db, sets[0], func() ([]*Dirent, []*Dirent, error) {
								return createFileEnts([]string{"/g/h/l.txt", "/g/i/m.txt", "/g/i/n.txt"}), []*Dirent{}, nil
							}, func() {
								sets, err = db.GetByRequester("jim")
								So(err, ShouldBeNil)
								So(sets[0].Status, ShouldEqual, PendingDiscovery)
								So(sets[0].StartedDiscovery.After(oldStart), ShouldBeTrue)
								So(sets[0].NumFiles, ShouldEqual, 0)
								So(sets[0].SizeTotal, ShouldEqual, 0)
								So(sets[0].Uploaded, ShouldEqual, 0)
								So(sets[0].Replaced, ShouldEqual, 0)
								So(sets[0].Skipped, ShouldEqual, 0)
								So(sets[0].Failed, ShouldEqual, 0)
								So(sets[0].Missing, ShouldEqual, 0)
								So(sets[0].LastCompletedCount, ShouldEqual, 4)
								So(sets[0].LastCompletedSize, ShouldEqual, 15)
								So(sets[0].SizeUploaded, ShouldEqual, 0)
							})

							fEntries, errg := db.GetFileEntries(sets[0].ID(), nil)
							So(errg, ShouldBeNil)
							So(len(fEntries), ShouldEqual, 6)
							So(fEntries[5], ShouldResemble, &Entry{Path: "/g/i/n.txt"})

							sets, err = db.GetByRequester("jim")
							So(err, ShouldBeNil)

							So(sets[0].Status, ShouldEqual, PendingUpload)
							So(sets[0].LastDiscovery.After(oldDisc), ShouldBeTrue)
							So(sets[0].NumFiles, ShouldEqual, 6)
							So(sets[0].SizeTotal, ShouldEqual, 0)

							r = &transfer.Request{
								Local:     "/g/h/l.txt",
								Requester: set.Requester,
								Set:       set.Name,
								Size:      7,
								Status:    transfer.RequestStatusUploading,
								Error:     "",
							}

							_, err = db.SetEntryStatus(r)
							So(err, ShouldBeNil)

							r.Status = transfer.RequestStatusUploaded
							_, err = db.SetEntryStatus(r)
							So(err, ShouldBeNil)

							sets, err = db.GetByRequester("jim")
							So(err, ShouldBeNil)

							So(sets[0].Status, ShouldEqual, Uploading)
							So(sets[0].SizeTotal, ShouldEqual, 7)
							So(sets[0].Uploaded, ShouldEqual, 1)
							So(sets[0].Skipped, ShouldEqual, 0)
							So(sets[0].Replaced, ShouldEqual, 0)
							So(sets[0].Failed, ShouldEqual, 0)
							So(sets[0].SizeUploaded, ShouldEqual, 7)

							fEntries, err = db.GetFileEntries(sets[0].ID(), nil)
							So(err, ShouldBeNil)
							So(len(fEntries), ShouldEqual, 6)
							So(fEntries[3].Size, ShouldEqual, 7)
							So(fEntries[3].Status, ShouldEqual, Uploaded)
							So(fEntries[3].Attempts, ShouldEqual, 1)
							So(fEntries[3].LastError, ShouldBeBlank)

							fEntries, err = db.GetPureFileEntries(sets[0].ID())
							So(err, ShouldBeNil)
							So(len(fEntries), ShouldEqual, 3)

							r = &transfer.Request{
								Local:     "/g/i/m.txt",
								Requester: set.Requester,
								Set:       set.Name,
								Size:      6,
								Status:    transfer.RequestStatusUploading,
								Error:     "",
							}

							_, err = db.SetEntryStatus(r)
							So(err, ShouldBeNil)

							r.Status = transfer.RequestStatusReplaced
							_, err = db.SetEntryStatus(r)
							So(err, ShouldBeNil)

							sets, err = db.GetByRequester("jim")
							So(err, ShouldBeNil)

							So(sets[0].Status, ShouldEqual, Uploading)
							So(sets[0].SizeTotal, ShouldEqual, 13)
							So(sets[0].Uploaded, ShouldEqual, 1)
							So(sets[0].Skipped, ShouldEqual, 0)
							So(sets[0].Replaced, ShouldEqual, 1)
							So(sets[0].Failed, ShouldEqual, 0)
							So(sets[0].SizeUploaded, ShouldEqual, 13)

							r = &transfer.Request{
								Local:     "/g/i/n.txt",
								Requester: set.Requester,
								Set:       set.Name,
								Size:      5,
								Status:    transfer.RequestStatusUploading,
								Error:     "",
							}

							_, err = db.SetEntryStatus(r)
							So(err, ShouldBeNil)

							r.Status = transfer.RequestStatusUnmodified
							_, err = db.SetEntryStatus(r)
							So(err, ShouldBeNil)

							sets, err = db.GetByRequester("jim")
							So(err, ShouldBeNil)

							So(sets[0].Status, ShouldEqual, Uploading)
							So(sets[0].SizeTotal, ShouldEqual, 18)
							So(sets[0].Uploaded, ShouldEqual, 1)
							So(sets[0].Skipped, ShouldEqual, 1)
							So(sets[0].Replaced, ShouldEqual, 1)
							So(sets[0].Failed, ShouldEqual, 0)
							So(sets[0].SizeUploaded, ShouldEqual, 13)
						})

						Convey("Set status becomes complete on new discovery with all missing files", func() {
							clearFileBucket(t, db, sets[0].ID())
							clearDiscoveredFilesBucket(t, db, sets[0].ID())

							slackWriter.Reset()

							oldDisc := sets[0].LastDiscovery

							discoverASet(db, sets[0], func() ([]*Dirent, []*Dirent, error) {
								dirents := createFileEnts([]string{"/g/h/l.txt", "/g/i/m.txt", "/g/i/n.txt"})
								for _, dirent := range dirents {
									dirent.Inode = 0
									dirent.Mode = os.ModeIrregular
								}

								return dirents, []*Dirent{}, nil
							}, func() {})

							sets, err = db.GetByRequester("jim")
							So(err, ShouldBeNil)

							So(sets[0].Status, ShouldEqual, Complete)
							So(sets[0].LastDiscovery.After(oldDisc), ShouldBeTrue)
							So(sets[0].NumFiles, ShouldEqual, 3)
							So(sets[0].Missing, ShouldEqual, 3)
							So(sets[0].SizeTotal, ShouldEqual, 0)

							So(slackWriter.String(), ShouldEqual, slack.BoxPrefixWarn+
								"`jim.set1` completed discovery and backup due to no files")
						})

						Convey("Set status becomes complete on new discovery with no files", func() {
							clearFileBucket(t, db, sets[0].ID())
							clearDirBucket(t, db, sets[0].ID())
							clearDiscoveredFilesBucket(t, db, sets[0].ID())

							slackWriter.Reset()

							oldDisc := sets[0].LastDiscovery

							discoverASet(db, sets[0], func() ([]*Dirent, []*Dirent, error) {
								return nil, nil, nil
							}, func() {})

							sets, err = db.GetByRequester("jim")
							So(err, ShouldBeNil)

							So(sets[0].Status, ShouldEqual, Complete)
							So(sets[0].LastDiscovery.After(oldDisc), ShouldBeTrue)
							So(sets[0].NumFiles, ShouldEqual, 0)
							So(sets[0].Missing, ShouldEqual, 0)
							So(sets[0].SizeTotal, ShouldEqual, 0)

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

					err = db.SetDiscoveryStarted(set.ID())
					So(err, ShouldBeNil)

					err = db.AddOrUpdate(set)
					So(err, ShouldNotBeNil)
					So(err.Error(), ShouldStartWith, "can't add set while set is being discovered")
				})

				Convey("You can get sets with a particular file", func() {
					sets, errg := db.GetAllSetsForFile("/a/b.txt")
					So(errg, ShouldBeNil)
					So(sets, ShouldHaveLength, 2)
					So(sets, ShouldContain, set.ID())
					So(sets, ShouldContain, set2.ID())

					sets, err = db.GetAllSetsForFile("/c/k.txt")
					So(err, ShouldBeNil)
					So(sets, ShouldHaveLength, 1)
					So(sets, ShouldContain, set2.ID())
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
					err = db.MergeFileEntries(setID, []string{path1, path2, "/zmissing"})
					So(err, ShouldBeNil)

					entries, errg := db.GetPureFileEntries(setID)
					So(errg, ShouldBeNil)
					So(len(entries), ShouldEqual, 3)
					So(entries[0].Status, ShouldEqual, Registered)
					So(entries[1].Status, ShouldEqual, Registered)
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

						r := &transfer.Request{
							Local:     entry.Path,
							Status:    transfer.RequestStatusUploading,
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

						r.Status = transfer.RequestStatusUploaded

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

				Convey("then start rediscovery and remove a hard link without breaking the counts", func() {
					err = db.SetDiscoveryStarted(setl1.ID())
					So(err, ShouldBeNil)

					entries, errg := db.GetPureFileEntries(setl1.ID())
					So(errg, ShouldBeNil)
					So(entries[1].Type, ShouldEqual, Hardlink)

					removed := removeFileEntryAndCount(db, setl1.ID(), entries[1].Path)
					So(removed.Path, ShouldEqual, entries[1].Path)

					got := db.GetByID(setl1.ID())
					So(got, ShouldNotBeNil)
					So(got.Status, ShouldEqual, PendingDiscovery)
					So(got.NumFiles, ShouldEqual, 0)
					So(got.Hardlinks, ShouldEqual, 0)
					So(got.NumObjectsRemoved, ShouldEqual, 1)

					got, err = db.Discover(setl1.ID(), nil)
					So(err, ShouldBeNil)
					So(got.NumFiles, ShouldEqual, 2)
					So(got.Hardlinks, ShouldEqual, 0)
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

				err = db.MergeFileEntries(setl1.ID(), []string{path1, path2, "/zmissing"})
				So(err, ShouldBeNil)

				entries, errg := db.GetPureFileEntries(setl1.ID())
				So(errg, ShouldBeNil)
				So(len(entries), ShouldEqual, 3)
				So(entries[0].Status, ShouldEqual, Registered)
				So(entries[1].Status, ShouldEqual, Registered)
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

				Convey("then remove a file and rediscover without the file being counted out twice", func() {
					entry, errg := db.GetFileEntryForSet(setl1.ID(), path1)
					So(errg, ShouldBeNil)

					remReq := NewRemoveRequest(path1, got, false, ToTrash)
					So(db.SetRemoveRequests(setl1.ID(), []RemoveReq{remReq}), ShouldBeNil)

					removed, errr := db.RemoveFileEntry(&remReq, entry)
					So(errr, ShouldBeNil)
					So(removed.Path, ShouldEqual, path1)

					got = db.GetByID(setl1.ID())
					So(got.NumFiles, ShouldEqual, 2)
					So(got.Uploaded, ShouldEqual, 1)
					So(got.NumObjectsRemoved, ShouldEqual, 1)

					got, err = db.Discover(setl1.ID(), nil)
					So(err, ShouldBeNil)
					So(got.NumFiles, ShouldEqual, 2)

					incomplete, errg := db.GetIncompleteRemoveRequests()
					So(errg, ShouldBeNil)
					So(incomplete, ShouldBeEmpty)

					Convey("even if the removal is retried", func() {
						retry := NewRemoveRequest(path1, got, false, ToTrash)

						removed, errr = db.RemoveFileEntry(&retry, entry)
						So(errr, ShouldBeNil)
						So(removed, ShouldBeNil)

						got = db.GetByID(setl1.ID())
						So(got.NumFiles, ShouldEqual, 2)
						So(got.NumObjectsRemoved, ShouldEqual, 1)
						So(got.SizeRemoved, ShouldEqual, entry.Size)
					})
				})

				Convey("then remove 2 files, the first stopped by an older build after it deleted the "+
					"entry, and count each once", func() {
					entry1, errg := db.GetFileEntryForSet(setl1.ID(), path1)
					So(errg, ShouldBeNil)
					So(entry1.Inode, ShouldNotEqual, 0)

					mountPoint := db.GetMountPointFromPath(path1)

					inodeFiles, errg := db.GetFilesFromInode(entry1.Inode, mountPoint)
					So(errg, ShouldBeNil)
					So(inodeFiles, ShouldContain, path1)

					entry2 := entries[0]

					// an earlier removal completed, so its request stays stored
					// and the next removal resets the set's removal counts.
					missing, errg := db.GetFileEntryForSet(setl1.ID(), "/zmissing")
					So(errg, ShouldBeNil)

					earlier := NewRemoveRequest(missing.Path, got, false, ToTrash)
					So(db.SetRemoveRequests(setl1.ID(), []RemoveReq{earlier}), ShouldBeNil)
					So(db.UpdateSetTotalToRemove(setl1.ID(), 1), ShouldBeNil)

					_, errr := db.RemoveFileEntry(&earlier, missing)
					So(errr, ShouldBeNil)
					So(db.OptimiseRemoveBucket(setl1.ID()), ShouldBeNil)

					got = db.GetByID(setl1.ID())
					So(got.NumObjectsToBeRemoved, ShouldEqual, 1)
					So(got.NumObjectsRemoved, ShouldEqual, 1)

					setEntryToFailed(entry1, setl1, db)

					failed, _, errg := db.GetFailedEntries(setl1.ID())
					So(errg, ShouldBeNil)
					So(failed, ShouldHaveLength, 1)
					So(failed[0].Path, ShouldEqual, path1)

					req1 := NewRemoveRequest(path1, got, false, ToRemove)
					req2 := NewRemoveRequest(entry2.Path, got, false, ToRemove)
					So(db.SetRemoveRequests(setl1.ID(), []RemoveReq{req1, req2}), ShouldBeNil)
					So(db.UpdateSetTotalToRemove(setl1.ID(), 2), ShouldBeNil)

					req1.RemoteRemovalStatus = Removed
					So(db.UpdateRemoveRequest(req1), ShouldBeNil)

					removeBoth := func() {
						So(db.RemoveDeletedFileEntry(&req1), ShouldBeNil)

						got = db.GetByID(setl1.ID())
						So(got.NumObjectsToBeRemoved, ShouldEqual, 2)
						So(got.NumObjectsRemoved, ShouldEqual, 1)

						_, errr := db.RemoveFileEntry(&req2, entry2)
						So(errr, ShouldBeNil)

						got = db.GetByID(setl1.ID())
						So(got.NumObjectsRemoved, ShouldEqual, 2)

						incomplete, errg := db.GetIncompleteRemoveRequests()
						So(errg, ShouldBeNil)
						So(incomplete, ShouldBeEmpty)

						inodeFiles, errg = db.GetFilesFromInode(entry1.Inode, mountPoint)
						So(errg, ShouldBeNil)
						So(inodeFiles, ShouldNotContain, path1)

						failed, _, errg = db.GetFailedEntries(setl1.ID())
						So(errg, ShouldBeNil)
						So(failed, ShouldBeEmpty)
					}

					Convey("before counting it", func() {
						err = db.db.Update(func(tx *bolt.Tx) error {
							_, errd := db.deleteFileEntry(tx, setl1.ID(), path1)

							return errd
						})
						So(err, ShouldBeNil)

						removeBoth()
					})

					Convey("after counting it", func() {
						counted := req1
						_, errr := db.RemoveFileEntry(&counted, entry1)
						So(errr, ShouldBeNil)

						So(db.UpdateRemoveRequest(req1), ShouldBeNil)

						removeBoth()
					})
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

				fileDirents := []*Dirent{
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

				dirDirents := []*Dirent{
					{
						Path:  dir,
						Mode:  fs.ModeDir,
						Inode: stat.Ino,
					},
					{
						Path:  dir2,
						Mode:  fs.ModeDir,
						Inode: stat.Ino,
					},
				}

				discoverCB := func(_ []*Entry) ([]*Dirent, []*Dirent, error) { //nolint:unparam
					return fileDirents, dirDirents, nil
				}

				got, errd := db.Discover(setl1.ID(), discoverCB)
				So(errd, ShouldBeNil)

				So(got, ShouldNotBeNil)
				So(got.Hardlinks, ShouldEqual, 2)
				So(got.NumFiles, ShouldEqual, 4)

				entries, errg := db.GetFileEntries(setl1.ID(), nil)
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

				dirEntries, errd := db.GetAllDirEntries(setl1.ID())
				So(errd, ShouldBeNil)
				So(len(dirEntries), ShouldEqual, 2)

				Convey("then rediscover the set and still know about the hardlinks", func() {
					got, errd = db.Discover(setl1.ID(), func(dirEntries []*Entry) ([]*Dirent, []*Dirent, error) {
						return fileDirents, dirDirents, nil
					})
					So(errd, ShouldBeNil)
					So(got.Hardlinks, ShouldEqual, 2)

					got = db.GetByID(setl1.ID())
					So(got, ShouldNotBeNil)
					So(err, ShouldBeNil)
					So(got.Hardlinks, ShouldEqual, 2)

					dirEntries, errd = db.GetAllDirEntries(setl1.ID())
					So(errd, ShouldBeNil)
					So(len(dirEntries), ShouldEqual, 2)
				})

				Convey("then get back all known local paths for the hardlink", func() {
					paths, errh := db.HardlinkPaths(entries[1])
					So(errh, ShouldBeNil)
					So(paths, ShouldResemble, []string{local, link2})

					paths, errh = db.HardlinkPaths(entries[0])
					So(errh, ShouldBeNil)
					So(paths, ShouldResemble, []string{link1, link2})
				})

				Convey("then get a remote path for the hardlink", func() {
					path, errh := db.HardlinkRemote(entries[1])
					So(errh, ShouldBeNil)
					So(path, ShouldEqual, "/remote/sub1/file")
				})

				names := map[string]string{local: "file", link1: "link1", link2: "link2"}

				for _, order := range [][]string{
					{local, link1, link2},
					{local, link2, link1},
					{link1, local, link2},
					{link1, link2, local},
					{link2, local, link1},
					{link2, link1, local},
				} {
					desc := fmt.Sprintf("then removing the linked files in order %s, %s, %s clears our inode record",
						names[order[0]], names[order[1]], names[order[2]])

					Convey(desc, func() {
						for i, path := range order {
							entry, erre := db.GetFileEntryForSet(setl1.ID(), path)
							So(erre, ShouldBeNil)

							remReq := NewRemoveRequest(path, db.GetByID(setl1.ID()), false, ToRemove)

							_, errr := db.RemoveFileEntry(&remReq, entry)
							So(errr, ShouldBeNil)

							files, errf := db.GetFilesFromInode(stat.Ino, local)
							So(errf, ShouldBeNil)

							if i == len(order)-1 {
								So(files, ShouldBeEmpty)

								continue
							}

							So(files, ShouldNotContain, path)

							for _, remaining := range order[i+1:] {
								So(files, ShouldContain, remaining)
							}
						}

						got = db.GetByID(setl1.ID())
						So(got.NumFiles, ShouldEqual, 1)
						So(got.NumObjectsRemoved, ShouldEqual, 3)
					})
				}

				Convey("then removing the uploaded original keeps its hardlinks pointing at its inode file", func() {
					storagePath := filepath.Join(local, strconv.FormatUint(stat.Ino, 10))

					upload := func(s *Set, path string) {
						r := &transfer.Request{
							Local:     path,
							Status:    transfer.RequestStatusUploading,
							Requester: s.Requester,
							Set:       s.Name,
						}

						_, erru := db.SetEntryStatus(r)
						So(erru, ShouldBeNil)

						r.Status = transfer.RequestStatusUploaded

						_, erru = db.SetEntryStatus(r)
						So(erru, ShouldBeNil)
					}

					remove := func(s *Set, path string) {
						entry, erre := db.GetFileEntryForSet(s.ID(), path)
						So(erre, ShouldBeNil)

						remReq := NewRemoveRequest(path, db.GetByID(s.ID()), false, ToRemove)

						_, errr := db.RemoveFileEntry(&remReq, entry)
						So(errr, ShouldBeNil)
					}

					discover := func(s *Set, dirents ...*Dirent) {
						_, errd = db.Discover(s.ID(), func(_ []*Entry) ([]*Dirent, []*Dirent, error) {
							return dirents, dirDirents, nil
						})
						So(errd, ShouldBeNil)
					}

					So(db.SetDiscoveryStarted(setl1.ID()), ShouldBeNil)

					for _, path := range []string{local, link1, link2, unlinked} {
						upload(setl1, path)
					}

					remove(setl1, local)

					discover(setl1, fileDirents[1:]...)

					So(db.GetByID(setl1.ID()).Hardlinks, ShouldEqual, 2)

					for _, path := range []string{link1, link2} {
						entry, erre := db.GetFileEntryForSet(setl1.ID(), path)
						So(erre, ShouldBeNil)
						So(entry.Type, ShouldEqual, Hardlink)
						So(entry.Dest, ShouldEqual, local)
						So(entry.InodeStoragePath(), ShouldEqual, storagePath)
					}

					Convey("as does a later set adding a new hardlink to it", func() {
						link3 := filepath.Join(tdir, "link3")
						So(os.Link(local, link3), ShouldBeNil)

						setl2 := &Set{Name: "setlink2", Requester: "jim", Transformer: "prefix=" + tdir + ":/remote2"}
						So(db.AddOrUpdate(setl2), ShouldBeNil)

						discover(setl2, &Dirent{Path: link3, Inode: stat.Ino})

						entry, erre := db.GetFileEntryForSet(setl2.ID(), link3)
						So(erre, ShouldBeNil)
						So(entry.Type, ShouldEqual, Hardlink)
						So(entry.InodeStoragePath(), ShouldEqual, storagePath)
					})

					Convey("as does its last hardlink, left as the only file in the inode record", func() {
						remove(setl1, link1)

						files, errf := db.GetFilesFromInode(stat.Ino, local)
						So(errf, ShouldBeNil)
						So(files, ShouldResemble, []string{"", link2})

						discover(setl1, fileDirents[2:]...)

						entry, erre := db.GetFileEntryForSet(setl1.ID(), link2)
						So(erre, ShouldBeNil)
						So(entry.Type, ShouldEqual, Hardlink)
						So(entry.InodeStoragePath(), ShouldEqual, storagePath)

						files, errf = db.GetFilesFromInode(stat.Ino, local)
						So(errf, ShouldBeNil)
						So(files, ShouldResemble, []string{"", link2})
					})

					keptOnInodeFile := func(s *Set, path string) {
						entry, erre := db.GetFileEntryForSet(s.ID(), path)
						So(erre, ShouldBeNil)
						So(entry.Type, ShouldEqual, Hardlink)
						So(entry.Dest, ShouldEqual, local)
						So(entry.InodeStoragePath(), ShouldEqual, storagePath)
					}

					newSet := func(name string) *Set {
						s := &Set{Name: name, Requester: "jim", Transformer: setl1.Transformer}
						So(db.AddOrUpdate(s), ShouldBeNil)

						return s
					}

					link1Dirent := &Dirent{Path: link1, Inode: stat.Ino}
					link2Dirent := &Dirent{Path: link2, Inode: stat.Ino}

					Convey("as does its second hardlink once its first is deleted locally, and its first once "+
						"linked back", func() {
						So(os.Remove(link1), ShouldBeNil)

						discover(setl1, &Dirent{Path: link1}, link2Dirent)

						entry, erre := db.GetFileEntryForSet(setl1.ID(), link1)
						So(erre, ShouldBeNil)
						So(entry.Inode, ShouldEqual, 0)

						keptOnInodeFile(setl1, link2)

						setl2 := newSet("setlink2")
						discover(setl2, link2Dirent)

						keptOnInodeFile(setl2, link2)

						So(os.Link(local, link1), ShouldBeNil)

						discover(setl1, link1Dirent, link2Dirent)

						keptOnInodeFile(setl1, link1)
					})

					Convey("as does its second hardlink once its first is linked to another file, and its first "+
						"once linked back", func() {
						other := filepath.Join(tdir, "other")
						internal.CreateTestFile(t, other, "b")

						info, errs = os.Stat(other)
						So(errs, ShouldBeNil)

						statOther, oko := info.Sys().(*syscall.Stat_t)
						So(oko, ShouldBeTrue)

						So(os.Remove(link1), ShouldBeNil)
						So(os.Link(other, link1), ShouldBeNil)

						discover(setl1, &Dirent{Path: other, Inode: statOther.Ino},
							&Dirent{Path: link1, Inode: statOther.Ino}, link2Dirent)

						entry, erre := db.GetFileEntryForSet(setl1.ID(), link1)
						So(erre, ShouldBeNil)
						So(entry.Type, ShouldEqual, Hardlink)
						So(entry.Dest, ShouldEqual, other)

						keptOnInodeFile(setl1, link2)

						setl2 := newSet("setlink2")
						discover(setl2, link2Dirent)

						keptOnInodeFile(setl2, link2)

						So(os.Remove(link1), ShouldBeNil)
						So(os.Link(local, link1), ShouldBeNil)

						discover(setl1, &Dirent{Path: other, Inode: statOther.Ino}, link1Dirent, link2Dirent)

						keptOnInodeFile(setl1, link1)
					})

					Convey("as does a new hardlink when only a later set has an up-to-date entry for the "+
						"remaining one", func() {
						remove(setl1, link2)

						setl3 := newSet("setlink3")
						discover(setl3, &Dirent{Path: link1, Inode: stat.Ino})

						keptOnInodeFile(setl3, link1)

						files, errf := db.GetFilesFromInode(stat.Ino, local)
						So(errf, ShouldBeNil)
						So(files, ShouldResemble, []string{"", link1})

						setIDs, erra := db.GetAllSetsForFile(link1)
						So(erra, ShouldBeNil)
						So(len(setIDs), ShouldEqual, 2)

						So(os.Remove(link1), ShouldBeNil)
						discover(db.GetByID(setIDs[0]), &Dirent{Path: link1})
						So(os.Link(local, link1), ShouldBeNil)

						entry, erre := db.GetFileEntryForSet(setIDs[0], link1)
						So(erre, ShouldBeNil)
						So(entry.Inode, ShouldEqual, 0)

						link3 := filepath.Join(tdir, "link3")
						So(os.Link(local, link3), ShouldBeNil)

						setl2 := newSet("setlink2")
						discover(setl2, &Dirent{Path: link3, Inode: stat.Ino})

						keptOnInodeFile(setl2, link3)
					})

					Convey("as does its re-added path, made a hardlink of itself that reuses its inode file", func() {
						discover(setl1, fileDirents...)

						for _, path := range []string{local, link1, link2} {
							keptOnInodeFile(setl1, path)
						}

						files, errf := db.GetFilesFromInode(stat.Ino, local)
						So(errf, ShouldBeNil)
						So(files, ShouldResemble, []string{"", link1, link2, local})
					})

					Convey("as do its hardlinks once their own entries are for an earlier file that had the "+
						"same inode, taking the original of the inode's new group instead", func() {
						erru := db.db.Update(func(tx *bolt.Tx) error {
							return tx.Bucket([]byte(inodeBucket)).Delete(db.inodeMountPointKeyFromDirent(link2Dirent))
						})
						So(erru, ShouldBeNil)

						newOriginal := filepath.Join(tdir, "newOriginal")
						newLink := filepath.Join(tdir, "newLink")

						So(os.Link(local, newOriginal), ShouldBeNil)
						So(os.Link(local, newLink), ShouldBeNil)

						newOriginalDirent := &Dirent{Path: newOriginal, Inode: stat.Ino}
						newLinkDirent := &Dirent{Path: newLink, Inode: stat.Ino}

						setl2 := newSet("setlink2")
						discover(setl2, newOriginalDirent)
						discover(setl2, newOriginalDirent, newLinkDirent)
						remove(setl2, newOriginal)

						discover(setl1, fileDirents[1:]...)

						for _, path := range []string{link1, link2} {
							entry, erre := db.GetFileEntryForSet(setl1.ID(), path)
							So(erre, ShouldBeNil)
							So(entry.Type, ShouldEqual, Hardlink)
							So(entry.Dest, ShouldEqual, newOriginal)
							So(entry.InodeStoragePath(), ShouldEqual,
								filepath.Join(newOriginal, strconv.FormatUint(stat.Ino, 10)))
						}

						files, errf := db.GetFilesFromInode(stat.Ino, local)
						So(errf, ShouldBeNil)
						So(files, ShouldResemble, []string{"", newLink, link1, link2})
					})
				})

				Convey("then a hardlink whose inode record has a removed original that no set's entry knows "+
					"becomes the original", func() {
					stale := filepath.Join(tdir, "stale")
					internal.CreateTestFile(t, stale, "a")

					fresh := filepath.Join(tdir, "fresh")
					So(os.Link(stale, fresh), ShouldBeNil)

					info, errs = os.Stat(stale)
					So(errs, ShouldBeNil)

					statStale, oks := info.Sys().(*syscall.Stat_t)
					So(oks, ShouldBeTrue)

					staleFile := "0" + transformerInodeSeparator + stale

					erru := db.db.Update(func(tx *bolt.Tx) error {
						key := db.inodeMountPointKeyFromDirent(&Dirent{Path: stale, Inode: statStale.Ino})

						return tx.Bucket([]byte(inodeBucket)).Put(key, db.encodeToBytes([]string{"", staleFile}))
					})
					So(erru, ShouldBeNil)

					setl2 := &Set{Name: "setlink2", Requester: "jim", Transformer: setl1.Transformer}
					So(db.AddOrUpdate(setl2), ShouldBeNil)

					_, errd = db.Discover(setl2.ID(), func(_ []*Entry) ([]*Dirent, []*Dirent, error) {
						return []*Dirent{{Path: fresh, Inode: statStale.Ino}}, nil, nil
					})
					So(errd, ShouldBeNil)

					entry, erre := db.GetFileEntryForSet(setl2.ID(), fresh)
					So(erre, ShouldBeNil)
					So(entry.Type, ShouldEqual, Regular)
					So(entry.InodeStoragePath(), ShouldBeBlank)

					files, errf := db.GetFilesFromInode(statStale.Ino, stale)
					So(errf, ShouldBeNil)
					So(files, ShouldResemble, []string{fresh, stale})
				})

				Convey("then a file whose inode record has a removed original and its own path from a set "+
					"with another transformer that no set has replaces that path as the original", func() {
					ghost := filepath.Join(tdir, "ghost")
					internal.CreateTestFile(t, ghost, "a")

					ghostLink := filepath.Join(tdir, "ghostlink")
					So(os.Link(ghost, ghostLink), ShouldBeNil)

					info, errs = os.Stat(ghost)
					So(errs, ShouldBeNil)

					statGhost, okg := info.Sys().(*syscall.Stat_t)
					So(okg, ShouldBeTrue)

					setl2 := &Set{Name: "setlink2", Requester: "jim", Transformer: "prefix=" + tdir + ":/remote2"}
					So(db.AddOrUpdate(setl2), ShouldBeNil)

					discover := func(s *Set, dirents ...*Dirent) {
						_, errd = db.Discover(s.ID(), func(_ []*Entry) ([]*Dirent, []*Dirent, error) {
							return dirents, nil, nil
						})
						So(errd, ShouldBeNil)
					}

					discover(setl2)

					erru := db.db.Update(func(tx *bolt.Tx) error {
						id := tx.Bucket([]byte(transformerToIDBucket)).Get([]byte(setl2.Transformer))
						key := db.inodeMountPointKeyFromDirent(&Dirent{Path: ghost, Inode: statGhost.Ino})
						ghostFile := string(id) + transformerInodeSeparator + ghost

						return tx.Bucket([]byte(inodeBucket)).Put(key, db.encodeToBytes([]string{"", ghostFile}))
					})
					So(erru, ShouldBeNil)

					setl3 := &Set{Name: "setlink3", Requester: "jim", Transformer: setl1.Transformer}
					So(db.AddOrUpdate(setl3), ShouldBeNil)

					ghostDirent := &Dirent{Path: ghost, Inode: statGhost.Ino}
					discover(setl3, ghostDirent)

					entry, erre := db.GetFileEntryForSet(setl3.ID(), ghost)
					So(erre, ShouldBeNil)
					So(entry.Type, ShouldEqual, Regular)

					files, errf := db.GetFilesFromInode(statGhost.Ino, ghost)
					So(errf, ShouldBeNil)
					So(files, ShouldResemble, []string{ghost})

					discover(setl3, ghostDirent, &Dirent{Path: ghostLink, Inode: statGhost.Ino})

					entry, erre = db.GetFileEntryForSet(setl3.ID(), ghostLink)
					So(erre, ShouldBeNil)
					So(entry.Type, ShouldEqual, Hardlink)
					So(entry.Dest, ShouldEqual, ghost)

					files, errf = db.GetFilesFromInode(statGhost.Ino, ghost)
					So(errf, ShouldBeNil)
					So(files, ShouldResemble, []string{ghost, ghostLink})

					for _, path := range []string{ghostLink, ghost} {
						entry, erre = db.GetFileEntryForSet(setl3.ID(), path)
						So(erre, ShouldBeNil)

						remReq := NewRemoveRequest(path, db.GetByID(setl3.ID()), false, ToRemove)

						_, errr := db.RemoveFileEntry(&remReq, entry)
						So(errr, ShouldBeNil)
					}

					files, errf = db.GetFilesFromInode(statGhost.Ino, ghost)
					So(errf, ShouldBeNil)
					So(files, ShouldBeEmpty)
				})

				Convey("then removing a hardlink added by a set with another transformer clears it from our inode record", func() {
					link3 := filepath.Join(tdir, "link3")
					err = os.Link(local, link3)
					So(err, ShouldBeNil)

					setl2 := &Set{
						Name:        "setlink2",
						Requester:   "jim",
						Transformer: "prefix=" + tdir + ":/remote2",
					}

					err = db.AddOrUpdate(setl2)
					So(err, ShouldBeNil)

					_, errd = db.Discover(setl2.ID(), func(_ []*Entry) ([]*Dirent, []*Dirent, error) {
						return []*Dirent{{Path: link3, Inode: stat.Ino}}, nil, nil
					})
					So(errd, ShouldBeNil)

					for _, path := range []string{link3, local} {
						setID := setl2.ID()
						if path == local {
							setID = setl1.ID()
						}

						entry, erre := db.GetFileEntryForSet(setID, path)
						So(erre, ShouldBeNil)

						remReq := NewRemoveRequest(path, db.GetByID(setID), false, ToRemove)

						_, errr := db.RemoveFileEntry(&remReq, entry)
						So(errr, ShouldBeNil)
					}

					files, errf := db.GetFilesFromInode(stat.Ino, local)
					So(errf, ShouldBeNil)
					So(files, ShouldNotContain, link3)
					So(files, ShouldNotContain, local)
					So(files, ShouldContain, link1)
					So(files, ShouldContain, link2)
				})

				Convey("then removing a hardlink that a set with the same transformer still has keeps it in our "+
					"inode record until that set removes it too", func() {
					setl2 := &Set{
						Name:        "setlink2",
						Requester:   "jim",
						Transformer: setl1.Transformer,
					}

					err = db.AddOrUpdate(setl2)
					So(err, ShouldBeNil)

					_, errd = db.Discover(setl2.ID(), func(_ []*Entry) ([]*Dirent, []*Dirent, error) {
						return []*Dirent{{Path: link1, Inode: stat.Ino}}, nil, nil
					})
					So(errd, ShouldBeNil)

					countLink1 := func() int {
						files, errf := db.GetFilesFromInode(stat.Ino, local)
						So(errf, ShouldBeNil)

						n := 0

						for _, file := range files {
							if file == link1 {
								n++
							}
						}

						return n
					}

					remove := func(s *Set) {
						entry, erre := db.GetFileEntryForSet(s.ID(), link1)
						So(erre, ShouldBeNil)

						remReq := NewRemoveRequest(link1, db.GetByID(s.ID()), false, ToRemove)

						_, errr := db.RemoveFileEntry(&remReq, entry)
						So(errr, ShouldBeNil)
					}

					So(countLink1(), ShouldEqual, 1)

					remove(setl1)
					So(countLink1(), ShouldEqual, 1)

					remove(setl2)
					So(countLink1(), ShouldEqual, 0)
				})

				Convey("then a file whose inode was reused by another set's removed file can still be removed", func() {
					err = os.Remove(unlinked)
					So(err, ShouldBeNil)

					reused := filepath.Join(tdir, "reused")
					internal.CreateTestFile(t, reused, "b")

					setr := &Set{
						Name:        "setreuse",
						Requester:   "jim",
						Transformer: "prefix=" + tdir + ":/remote",
					}

					err = db.AddOrUpdate(setr)
					So(err, ShouldBeNil)

					_, errd = db.Discover(setr.ID(), func(_ []*Entry) ([]*Dirent, []*Dirent, error) {
						return []*Dirent{{Path: reused, Inode: statUnlinked.Ino}}, nil, nil
					})
					So(errd, ShouldBeNil)

					entry, erre := db.GetFileEntryForSet(setr.ID(), reused)
					So(erre, ShouldBeNil)

					remReq := NewRemoveRequest(reused, db.GetByID(setr.ID()), false, ToRemove)
					_, errr := db.RemoveFileEntry(&remReq, entry)
					So(errr, ShouldBeNil)

					entry, erre = db.GetFileEntryForSet(setl1.ID(), unlinked)
					So(erre, ShouldBeNil)
					So(entry.Inode, ShouldEqual, statUnlinked.Ino)

					remReq = NewRemoveRequest(unlinked, db.GetByID(setl1.ID()), false, ToRemove)
					removed, errr := db.RemoveFileEntry(&remReq, entry)
					So(errr, ShouldBeNil)
					So(removed.Path, ShouldEqual, unlinked)

					got = db.GetByID(setl1.ID())
					So(got.NumFiles, ShouldEqual, 3)
					So(got.NumObjectsRemoved, ShouldEqual, 1)
				})

				Convey("then a file can still be removed after its mount point changes", func() {
					entry, erre := db.GetFileEntryForSet(setl1.ID(), unlinked)
					So(erre, ShouldBeNil)

					db.mountList = append([]string{tdir}, db.mountList...)

					remReq := NewRemoveRequest(unlinked, db.GetByID(setl1.ID()), false, ToRemove)
					removed, errr := db.RemoveFileEntry(&remReq, entry)
					So(errr, ShouldBeNil)
					So(removed.Path, ShouldEqual, unlinked)

					got = db.GetByID(setl1.ID())
					So(got.NumFiles, ShouldEqual, 3)
					So(got.NumObjectsRemoved, ShouldEqual, 1)
				})

				Convey("then a failure to clean up a removed file's inode record leaves its removal undone", func() {
					erru := db.db.Update(func(tx *bolt.Tx) error {
						key := db.inodeMountPointKeyFromDirent(&Dirent{Path: unlinked, Inode: statUnlinked.Ino})

						return tx.Bucket([]byte(inodeBucket)).Put(key, db.encodeToBytes([]string{"corrupt"}))
					})
					So(erru, ShouldBeNil)

					entry, erre := db.GetFileEntryForSet(setl1.ID(), unlinked)
					So(erre, ShouldBeNil)

					remReq := NewRemoveRequest(unlinked, db.GetByID(setl1.ID()), false, ToRemove)
					So(db.SetRemoveRequests(setl1.ID(), []RemoveReq{remReq}), ShouldBeNil)

					for range 2 {
						_, errr := db.RemoveFileEntry(&remReq, entry)
						So(errr, ShouldNotBeNil)
						So(errr.Error(), ShouldContainSubstring, ErrInvalidTransformerPath)

						_, erre = db.GetFileEntryForSet(setl1.ID(), unlinked)
						So(erre, ShouldBeNil)

						got = db.GetByID(setl1.ID())
						So(got.NumFiles, ShouldEqual, 4)
						So(got.NumObjectsRemoved, ShouldEqual, 0)

						incomplete, erri := db.GetIncompleteRemoveRequests()
						So(erri, ShouldBeNil)
						So(incomplete, ShouldHaveLength, 1)
					}
				})

				Convey("then a set with another transformer adding the same file gets a regular entry, "+
					"and its real hardlinks are still hardlinks", func() {
					link3 := filepath.Join(tdir, "link3")
					err = os.Link(local, link3)
					So(err, ShouldBeNil)

					addSet := func(name, transformer string, dirents ...*Dirent) *Set {
						s := &Set{Name: name, Requester: "jim", Transformer: transformer}

						So(db.AddOrUpdate(s), ShouldBeNil)

						_, errd = db.Discover(s.ID(), func(_ []*Entry) ([]*Dirent, []*Dirent, error) {
							return dirents, nil, nil
						})
						So(errd, ShouldBeNil)

						return s
					}

					setl2 := addSet("setlink2", "prefix="+tdir+":/remote2",
						&Dirent{Path: unlinked, Inode: statUnlinked.Ino},
						&Dirent{Path: link3, Inode: stat.Ino})

					got = db.GetByID(setl2.ID())
					So(got.Hardlinks, ShouldEqual, 1)

					entry, erre := db.GetFileEntryForSet(setl2.ID(), unlinked)
					So(erre, ShouldBeNil)
					So(entry.Type, ShouldEqual, Regular)
					So(entry.InodeStoragePath(), ShouldBeBlank)

					entry, erre = db.GetFileEntryForSet(setl2.ID(), link3)
					So(erre, ShouldBeNil)
					So(entry.Type, ShouldEqual, Hardlink)
					So(entry.InodeStoragePath(), ShouldEqual, filepath.Join(local, strconv.FormatUint(stat.Ino, 10)))

					files, errf := db.GetFilesFromInode(statUnlinked.Ino, unlinked)
					So(errf, ShouldBeNil)
					So(files, ShouldResemble, []string{unlinked})

					Convey("and once both sets remove it, no inode record of it remains", func() {
						for _, s := range []*Set{setl1, setl2} {
							entry, erre = db.GetFileEntryForSet(s.ID(), unlinked)
							So(erre, ShouldBeNil)

							remReq := NewRemoveRequest(unlinked, db.GetByID(s.ID()), false, ToRemove)

							_, errr := db.RemoveFileEntry(&remReq, entry)
							So(errr, ShouldBeNil)
						}

						files, errf = db.GetFilesFromInode(statUnlinked.Ino, unlinked)
						So(errf, ShouldBeNil)
						So(files, ShouldBeEmpty)

						setl3 := addSet("setlink3", setl1.Transformer, &Dirent{Path: unlinked, Inode: statUnlinked.Ino})

						entry, erre = db.GetFileEntryForSet(setl3.ID(), unlinked)
						So(erre, ShouldBeNil)
						So(entry.Type, ShouldEqual, Regular)
						So(db.GetByID(setl3.ID()).Hardlinks, ShouldEqual, 0)
					})
				})

				// Test does not work, not clear how to implement
				SkipConvey("then previously seen moved files get treated as hardlinks", func() {
					moved := filepath.Join(dir, "moved")
					err = os.Rename(unlinked, moved)
					So(err, ShouldBeNil)

					fileDirents[2].Path = moved

					got, errd = db.Discover(setl1.ID(), discoverCB)
					So(errd, ShouldBeNil)

					entries, errg = db.GetFileEntries(setl1.ID(), nil)
					So(errg, ShouldBeNil)
					So(len(entries), ShouldEqual, 5)
					So(entries[0].Type, ShouldEqual, Regular)
					So(entries[1].Type, ShouldEqual, Hardlink)
					So(entries[1].Inode, ShouldEqual, stat.Ino)
					So(entries[2].Type, ShouldEqual, Hardlink)
					So(entries[3].Type, ShouldEqual, Regular)
					So(entries[3].Inode, ShouldEqual, statUnlinked.Ino)
					So(entries[4].Type, ShouldEqual, Hardlink)
					So(entries[4].Inode, ShouldEqual, stat.Ino)

					So(got, ShouldNotBeNil)
					So(got.Hardlinks, ShouldEqual, 3)
					So(got.NumFiles, ShouldEqual, 5)
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

				got, errb := db.Discover(setl1.ID(), func(_ []*Entry) ([]*Dirent, []*Dirent, error) {
					return []*Dirent{
						{
							Path:  path1,
							Inode: 1,
						},
						{
							Path:  path2,
							Mode:  os.ModeSymlink,
							Inode: 2,
						},
					}, []*Dirent{}, nil
				})
				So(errb, ShouldBeNil)
				So(got, ShouldNotBeNil)
				So(got.Symlinks, ShouldEqual, 1)
				So(got.NumFiles, ShouldEqual, 2)

				entries, errG := db.GetFileEntries(setl1.ID(), nil)
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

				err = db.MergeFileEntries(setl1.ID(), []string{missing})
				So(err, ShouldBeNil)

				entries, errg := db.GetPureFileEntries(setl1.ID())
				So(errg, ShouldBeNil)
				So(len(entries), ShouldEqual, 1)
				So(entries[0].Status, ShouldEqual, Registered)
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

				Convey("then remove the missing file and have it no longer counted", func() {
					removed := removeFileEntryAndCount(db, setl1.ID(), missing)
					So(removed.Status, ShouldEqual, Missing)

					got = db.GetByID(setl1.ID())
					So(got, ShouldNotBeNil)
					So(got.NumFiles, ShouldEqual, 0)
					So(got.Missing, ShouldEqual, 0)
					So(got.NumObjectsRemoved, ShouldEqual, 1)
				})
			})

			Convey("And add a set with a missing file and an existing file, an upload result for an entry "+
				"discovery counted replaces that count", func() {
				setl1 := &Set{
					Name:        "missingAndExisting",
					Requester:   "jim",
					Transformer: "prefix=/local:/remote",
				}

				err = db.AddOrUpdate(setl1)
				So(err, ShouldBeNil)

				dir := t.TempDir()
				missing := filepath.Join(dir, "missing")
				existing := filepath.Join(dir, "existing")
				internal.CreateTestFile(t, existing, "a")

				err = db.MergeFileEntries(setl1.ID(), []string{missing, existing})
				So(err, ShouldBeNil)

				setResult := func(path string, status transfer.RequestStatus) *Set {
					_, errs := db.SetEntryStatus(&transfer.Request{
						Local:     path,
						Requester: setl1.Requester,
						Set:       setl1.Name,
						Size:      1,
						Status:    status,
					})
					So(errs, ShouldBeNil)

					return db.GetByID(setl1.ID())
				}

				got, errd := db.Discover(setl1.ID(), nil)
				So(errd, ShouldBeNil)
				So(got.NumFiles, ShouldEqual, 2)
				So(got.Missing, ShouldEqual, 1)
				So(got.Status, ShouldEqual, PendingUpload)

				Convey("repeated failed results for the missing file count it as failed once", func() {
					for range 2 {
						got = setResult(missing, transfer.RequestStatusFailed)
						So(got.Failed, ShouldEqual, 1)
						So(got.Missing, ShouldEqual, 0)
						So(got.Status, ShouldNotEqual, Complete)
					}

					got = setResult(existing, transfer.RequestStatusUploaded)
					So(got.Uploaded, ShouldEqual, 1)
					So(got.Failed, ShouldEqual, 1)
					So(got.Status, ShouldEqual, Complete)
				})

				Convey("a missing result for the missing file and the existing file's upload complete the set", func() {
					got = setResult(missing, transfer.RequestStatusMissing)
					So(got.Missing, ShouldEqual, 1)
					So(got.Status, ShouldNotEqual, Complete)

					got = setResult(existing, transfer.RequestStatusUploaded)
					So(got.Uploaded, ShouldEqual, 1)
					So(got.Missing, ShouldEqual, 1)
					So(got.Status, ShouldEqual, Complete)

					Convey("then delete the uploaded file, add another file and rediscover; the orphaned result "+
						"leaves the still missing file's count alone", func() {
						So(os.Remove(existing), ShouldBeNil)

						other := filepath.Join(dir, "other")
						internal.CreateTestFile(t, other, "c")
						So(db.MergeFileEntries(setl1.ID(), []string{other}), ShouldBeNil)

						got, errd = db.Discover(setl1.ID(), nil)
						So(errd, ShouldBeNil)
						So(got.NumFiles, ShouldEqual, 3)
						So(got.Missing, ShouldEqual, 1)
						So(got.Orphaned, ShouldEqual, 1)
						So(got.Status, ShouldEqual, PendingUpload)

						got = setResult(existing, transfer.RequestStatusOrphaned)
						So(got.Orphaned, ShouldEqual, 1)
						So(got.Missing, ShouldEqual, 1)
						So(got.Status, ShouldNotEqual, Complete)

						got = setResult(missing, transfer.RequestStatusMissing)
						So(got.Missing, ShouldEqual, 1)
						So(got.Orphaned, ShouldEqual, 1)
						So(got.Status, ShouldNotEqual, Complete)

						got = setResult(other, transfer.RequestStatusUploaded)
						So(got.Uploaded, ShouldEqual, 1)
						So(got.Missing, ShouldEqual, 1)
						So(got.Orphaned, ShouldEqual, 1)
						So(got.Status, ShouldEqual, Complete)
					})

					Convey("then delete the uploaded file and rediscover; its orphaned result isn't counted "+
						"twice, so the set isn't complete before the other file's result", func() {
						So(os.Remove(existing), ShouldBeNil)
						internal.CreateTestFile(t, missing, "b")

						got, errd = db.Discover(setl1.ID(), nil)
						So(errd, ShouldBeNil)
						So(got.NumFiles, ShouldEqual, 2)
						So(got.Orphaned, ShouldEqual, 1)
						So(got.Status, ShouldEqual, PendingUpload)

						got = setResult(existing, transfer.RequestStatusOrphaned)
						So(got.Orphaned, ShouldEqual, 1)
						So(got.Status, ShouldNotEqual, Complete)

						got = setResult(missing, transfer.RequestStatusUploaded)
						So(got.Uploaded, ShouldEqual, 1)
						So(got.Orphaned, ShouldEqual, 1)
						So(got.Missing, ShouldEqual, 0)
						So(got.Status, ShouldEqual, Complete)

						entries, errg := db.GetFileEntries(setl1.ID(), FileEntryFilterLastState)
						So(errg, ShouldBeNil)
						So(len(entries), ShouldEqual, 1)
						So(entries[0].Path, ShouldEqual, missing)

						Convey("and removing the orphaned file afterwards leaves consistent counts", func() {
							removed := removeFileEntryAndCount(db, setl1.ID(), existing)
							So(removed.Status, ShouldEqual, Orphaned)

							got = db.GetByID(setl1.ID())
							So(got.NumFiles, ShouldEqual, 1)
							So(got.Orphaned, ShouldEqual, 0)
							So(got.Uploaded, ShouldEqual, 1)
						})
					})
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

				err = db.MergeDirEntries(setl1.ID(), []*Dirent{{
					Path: missing,
					Mode: os.ModeDir,
				}})
				So(err, ShouldBeNil)

				entries, errg := db.GetDirEntries(setl1.ID(), nil)
				So(errg, ShouldBeNil)
				So(len(entries), ShouldEqual, 1)
				So(entries[0].Status, ShouldEqual, Registered)
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

				err = db.MergeFileEntries(setl1.ID(), []string{fifoPath})
				So(err, ShouldBeNil)

				entries, errg := db.GetPureFileEntries(setl1.ID())
				So(errg, ShouldBeNil)
				So(len(entries), ShouldEqual, 1)
				So(entries[0].Status, ShouldEqual, Registered)
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

				Convey("then remove the abnormal file and have it no longer counted", func() {
					removed := removeFileEntryAndCount(db, setl1.ID(), fifoPath)
					So(removed.Status, ShouldEqual, AbnormalEntry)

					got = db.GetByID(setl1.ID())
					So(got, ShouldNotBeNil)
					So(got.NumFiles, ShouldEqual, 0)
					So(got.Abnormal, ShouldEqual, 0)
				})
			})

			Convey("And add a directory set containing an abnormal file", func() {
				setl1 := &Set{
					Name:        "abnormaldir",
					Requester:   "jim",
					Transformer: "prefix=/local:/remote",
				}

				err = db.AddOrUpdate(setl1)
				So(err, ShouldBeNil)

				dir := t.TempDir()

				fifoPath := filepath.Join(dir, "fifo")
				err = syscall.Mkfifo(fifoPath, userPerms)
				So(err, ShouldBeNil)

				regPath := filepath.Join(dir, "reg")
				internal.CreateTestFile(t, regPath, "a")

				err = db.MergeDirEntries(setl1.ID(), []*Dirent{{Path: dir, Mode: os.ModeDir}})
				So(err, ShouldBeNil)

				discover := func(paths ...string) *Set {
					got, errd := db.Discover(setl1.ID(), func([]*Entry) ([]*Dirent, []*Dirent, error) {
						dirents := make([]*Dirent, len(paths))

						for i, path := range paths {
							dirents[i] = newDirentFromPath(path)
						}

						return dirents, nil, nil
					})
					So(errd, ShouldBeNil)

					return got
				}

				got := discover(fifoPath, regPath)
				So(got.NumFiles, ShouldEqual, 2)
				So(got.Abnormal, ShouldEqual, 1)

				removeFifoAndCheckCounts := func() {
					removed := removeFileEntryAndCount(db, setl1.ID(), fifoPath)
					So(removed.Status, ShouldEqual, AbnormalEntry)

					got = db.GetByID(setl1.ID())
					So(got, ShouldNotBeNil)
					So(got.NumFiles, ShouldEqual, 1)
					So(got.Abnormal, ShouldEqual, 0)
				}

				Convey("then remove the abnormal file and have it no longer counted", func() {
					removeFifoAndCheckCounts()
				})

				Convey("then rediscover with it still present, remove it and have it no longer counted", func() {
					got = discover(fifoPath, regPath)
					So(got.Abnormal, ShouldEqual, 1)

					removeFifoAndCheckCounts()
				})

				Convey("then delete it locally, rediscover, remove it and have counts not wrap", func() {
					err = os.Remove(fifoPath)
					So(err, ShouldBeNil)

					got = discover(regPath)
					So(got.NumFiles, ShouldEqual, 2)
					So(got.Abnormal, ShouldEqual, 0)

					removeFifoAndCheckCounts()
				})
			})

			Convey("And add a directory set with a nested folder", func() {
				setl1 := &Set{
					Name:        "nesteddir",
					Requester:   "jim",
					Transformer: "prefix=/local:/remote",
				}

				So(db.AddOrUpdate(setl1), ShouldBeNil)

				dir1 := t.TempDir()
				dir2 := filepath.Join(dir1, "dir2")
				So(os.Mkdir(dir2, userPerms), ShouldBeNil)

				file := filepath.Join(dir2, "file")
				internal.CreateTestFile(t, file, "a")

				So(db.MergeDirEntries(setl1.ID(), []*Dirent{{Path: dir1, Mode: os.ModeDir}}), ShouldBeNil)

				got, errd := db.Discover(setl1.ID(), func([]*Entry) ([]*Dirent, []*Dirent, error) {
					return []*Dirent{newDirentFromPath(file)}, []*Dirent{{Path: dir2, Mode: os.ModeDir}}, nil
				})
				So(errd, ShouldBeNil)
				So(got.NumFiles, ShouldEqual, 1)

				trashSet := BuildTrashSetFromSet(setl1)

				trashSubfolder := func() {
					removeFileEntryAndCount(db, setl1.ID(), file)

					remReq := NewRemoveRequest(dir2, db.GetByID(setl1.ID()), true, ToTrash)
					So(db.RemoveDirEntry(&remReq), ShouldBeNil)

					got = db.GetByID(setl1.ID())
					So(got.Error, ShouldBeBlank)
					So(got.NumFiles, ShouldEqual, 0)
					So(got.NumObjectsRemoved, ShouldEqual, 2)

					files, errg := db.GetFileEntries(setl1.ID(), nil)
					So(errg, ShouldBeNil)
					So(files, ShouldBeEmpty)

					dirs, errg := db.GetAllDirEntries(setl1.ID())
					So(errg, ShouldBeNil)
					So(dirs, ShouldHaveLength, 1)
					So(dirs[0].Path, ShouldEqual, dir1)

					trashed, errg := db.GetFileEntries(trashSet.ID(), nil)
					So(errg, ShouldBeNil)
					So(trashed, ShouldHaveLength, 1)
					So(trashed[0].Path, ShouldEqual, file)

					trashedDirs, errg := db.GetAllDirEntries(trashSet.ID())
					So(errg, ShouldBeNil)
					So(trashedDirs, ShouldHaveLength, 1)
					So(trashedDirs[0].Path, ShouldEqual, dir2)

					_, dirPaths, errv := db.ValidateFileAndDirPaths(&trashSet, []string{dir2})
					So(errv, ShouldBeNil)
					So(dirPaths, ShouldResemble, []string{dir2})
				}

				Convey("then trash the subfolder, which moves it to the trash", func() {
					trashSubfolder()
				})

				Convey("then, as a legacy set without discovered folders, trash the subfolder", func() {
					So(db.DeleteDiscoveredFoldersBucket(setl1.ID()), ShouldBeNil)

					trashSubfolder()
				})
			})

			Convey("And add a frozen set", func() {
				setl1 := &Set{
					Name:        "freeze",
					Requester:   "jim",
					Transformer: "prefix=/tmp:/remote",
					Frozen:      true,
				}

				So(db.AddOrUpdate(setl1), ShouldBeNil)

				dir := t.TempDir()

				aFile := filepath.Join(dir, "a")
				So(os.WriteFile(aFile, []byte("a"), userPerms), ShouldBeNil)

				So(db.MergeFileEntries(setl1.ID(), []string{aFile}), ShouldBeNil)

				_, err = db.Discover(setl1.ID(), nil)
				So(err, ShouldBeNil)

				entries, err := db.GetPureFileEntries(setl1.ID())
				So(err, ShouldBeNil)
				So(len(entries), ShouldEqual, 1)
				So(entries[0].Inode, ShouldNotEqual, 0)

				setEntryToUploaded(entries[0], setl1, db)

				So(os.Remove(aFile), ShouldBeNil)

				bFile := filepath.Join(dir, "b")
				So(os.WriteFile(bFile, []byte("b"), userPerms), ShouldBeNil)
				So(os.WriteFile(aFile, []byte("aa"), userPerms), ShouldBeNil)

				So(db.MergeFileEntries(setl1.ID(), []string{aFile, bFile}), ShouldBeNil)

				_, err = db.Discover(setl1.ID(), nil)
				So(err, ShouldBeNil)

				oldInode := entries[0].Inode

				entries, err = db.GetPureFileEntries(setl1.ID())
				So(err, ShouldBeNil)
				So(len(entries), ShouldEqual, 2)
				So(entries[0].Inode, ShouldEqual, oldInode)

				Convey("which can be unfrozen to update files", func() {
					setl1.Frozen = false

					So(db.AddOrUpdate(setl1), ShouldBeNil)
					So(db.MergeFileEntries(setl1.ID(), []string{aFile, bFile}), ShouldBeNil)

					_, err = db.Discover(setl1.ID(), nil)
					So(err, ShouldBeNil)

					entries, err = db.GetPureFileEntries(setl1.ID())
					So(err, ShouldBeNil)
					So(len(entries), ShouldEqual, 2)
					So(entries[0].Inode, ShouldNotEqual, oldInode)
				})
			})

			Convey("And add a frozen set whose uploaded file is deleted locally, rediscovery stores and counts "+
				"it as orphaned", func() {
				setl1 := &Set{
					Name:        "frozenOrphan",
					Requester:   "jim",
					Transformer: "prefix=/tmp:/remote",
					Frozen:      true,
				}

				So(db.AddOrUpdate(setl1), ShouldBeNil)

				aFile := filepath.Join(t.TempDir(), "a")
				internal.CreateTestFile(t, aFile, "a")

				So(db.MergeFileEntries(setl1.ID(), []string{aFile}), ShouldBeNil)

				_, err = db.Discover(setl1.ID(), nil)
				So(err, ShouldBeNil)

				entries, errg := db.GetPureFileEntries(setl1.ID())
				So(errg, ShouldBeNil)
				So(len(entries), ShouldEqual, 1)

				setEntryToUploaded(entries[0], setl1, db)

				got := db.GetByID(setl1.ID())
				So(got.Uploaded, ShouldEqual, 1)
				So(got.Status, ShouldEqual, Complete)

				So(os.Remove(aFile), ShouldBeNil)

				got, err = db.Discover(setl1.ID(), nil)
				So(err, ShouldBeNil)
				So(got.NumFiles, ShouldEqual, 1)
				So(got.Orphaned, ShouldEqual, 1)
				So(got.Uploaded, ShouldEqual, 0)
				So(got.Status, ShouldEqual, Complete)

				entry, errg := db.GetFileEntryForSet(setl1.ID(), aFile)
				So(errg, ShouldBeNil)
				So(entry.Status, ShouldEqual, Orphaned)
				So(entry.ShouldUpload(got), ShouldBeFalse)

				Convey("then removing it leaves consistent counts", func() {
					removed := removeFileEntryAndCount(db, setl1.ID(), aFile)
					So(removed.Status, ShouldEqual, Orphaned)

					got = db.GetByID(setl1.ID())
					So(got.NumFiles, ShouldEqual, 0)
					So(got.Orphaned, ShouldEqual, 0)
					So(got.Uploaded, ShouldEqual, 0)
					So(got.NumObjectsRemoved, ShouldEqual, 1)
				})

				Convey("then rediscovering it while still deleted keeps it orphaned", func() {
					got, err = db.Discover(setl1.ID(), nil)
					So(err, ShouldBeNil)
					So(got.NumFiles, ShouldEqual, 1)
					So(got.Orphaned, ShouldEqual, 1)
					So(got.Missing, ShouldEqual, 0)

					entry, errg = db.GetFileEntryForSet(setl1.ID(), aFile)
					So(errg, ShouldBeNil)
					So(entry.Status, ShouldEqual, Orphaned)
				})

				Convey("then restoring it locally and rediscovering doesn't upload it again", func() {
					internal.CreateTestFile(t, aFile, "changed")

					got, err = db.Discover(setl1.ID(), nil)
					So(err, ShouldBeNil)
					So(got.NumFiles, ShouldEqual, 1)

					entry, errg = db.GetFileEntryForSet(setl1.ID(), aFile)
					So(errg, ShouldBeNil)
					So(entry.ShouldUpload(got), ShouldBeFalse)
				})
			})

			for _, frozen := range []bool{true, false} {
				Convey(fmt.Sprintf("And add a set (frozen: %v) whose skipped and replaced files are deleted "+
					"locally, rediscovery stores and counts them as orphaned", frozen), func() {
					setl1 := &Set{
						Name:        fmt.Sprintf("skipRepOrphan%v", frozen),
						Requester:   "jim",
						Transformer: "prefix=/tmp:/remote",
						Frozen:      frozen,
					}

					So(db.AddOrUpdate(setl1), ShouldBeNil)

					dir := t.TempDir()
					skipped := filepath.Join(dir, "skipped")
					replaced := filepath.Join(dir, "replaced")

					internal.CreateTestFile(t, skipped, "a")
					internal.CreateTestFile(t, replaced, "b")

					So(db.MergeFileEntries(setl1.ID(), []string{skipped, replaced}), ShouldBeNil)

					_, err = db.Discover(setl1.ID(), nil)
					So(err, ShouldBeNil)

					setResult := func(path string, statuses ...transfer.RequestStatus) *Set {
						for _, status := range statuses {
							_, errs := db.SetEntryStatus(&transfer.Request{
								Local:     path,
								Requester: setl1.Requester,
								Set:       setl1.Name,
								Size:      1,
								Status:    status,
							})
							So(errs, ShouldBeNil)
						}

						return db.GetByID(setl1.ID())
					}

					setResult(skipped, transfer.RequestStatusUploading, transfer.RequestStatusUnmodified)
					got := setResult(replaced, transfer.RequestStatusUploading, transfer.RequestStatusReplaced)
					So(got.Skipped, ShouldEqual, 1)
					So(got.Replaced, ShouldEqual, 1)
					So(got.Status, ShouldEqual, Complete)

					So(os.Remove(skipped), ShouldBeNil)
					So(os.Remove(replaced), ShouldBeNil)

					got, err = db.Discover(setl1.ID(), nil)
					So(err, ShouldBeNil)
					So(got.NumFiles, ShouldEqual, 2)
					So(got.Orphaned, ShouldEqual, 2)
					So(got.Missing, ShouldEqual, 0)
					So(got.Skipped, ShouldEqual, 0)
					So(got.Replaced, ShouldEqual, 0)

					for _, path := range []string{skipped, replaced} {
						entry, errg := db.GetFileEntryForSet(setl1.ID(), path)
						So(errg, ShouldBeNil)
						So(entry.Status, ShouldEqual, Orphaned)
					}

					if frozen {
						return
					}

					got = setResult(skipped, transfer.RequestStatusOrphaned)
					So(got.Orphaned, ShouldEqual, 2)
					So(got.Missing, ShouldEqual, 0)

					got = setResult(replaced, transfer.RequestStatusOrphaned)
					So(got.Orphaned, ShouldEqual, 2)
					So(got.Missing, ShouldEqual, 0)
					So(got.Status, ShouldEqual, Complete)
				})
			}
		})
	})
}

func newEntry(path string) *Entry {
	return &Entry{Path: path, Status: Registered}
}

func clearBucket(t *testing.T, db *DB, setID string,
	dbGetFun func(string, EntryFilter) ([]*Entry, error), bucketName string) {
	t.Helper()

	entries, err := dbGetFun(setID, nil)
	So(err, ShouldBeNil)

	for _, entry := range entries {
		err = db.removeEntry(setID, entry.Path, bucketName)
		So(err, ShouldBeNil)
	}
}

func clearFileBucket(t *testing.T, db *DB, setID string) {
	t.Helper()

	clearBucket(t, db, setID, db.GetFileEntries, fileBucket)
}

func wrapDBGetFun(fn func(string) ([]*Entry, error)) func(string, EntryFilter) ([]*Entry, error) {
	return func(name string, _ EntryFilter) ([]*Entry, error) {
		return fn(name)
	}
}

func clearDirBucket(t *testing.T, db *DB, setID string) {
	t.Helper()

	clearBucket(t, db, setID, db.GetDirEntries, dirBucket)
}

func clearDiscoveredFilesBucket(t *testing.T, db *DB, setID string) {
	t.Helper()

	clearBucket(t, db, setID, wrapDBGetFun(db.GetDiscoveredFileEntries), discoveredBucket)
}

func discoverASet(db *DB, set *Set, discoveryFunc func() ([]*Dirent, []*Dirent, error), pendingTestsFunc func()) {
	errCh := make(chan error, 1)
	waitCh := make(chan struct{})

	go func() {
		_, errd := db.Discover(set.ID(), func(e []*Entry) ([]*Dirent, []*Dirent, error) {
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

// removeFileEntryAndCount removes the given file from the given set and counts
// it the way the server does, returning the removed entry. It trashes rather
// than removes, so our inode records, which these tests don't make, are left
// alone.
func removeFileEntryAndCount(db *DB, setID, path string) *Entry {
	entry, err := db.GetFileEntryForSet(setID, path)
	So(err, ShouldBeNil)

	remReq := NewRemoveRequest(path, db.GetByID(setID), false, ToTrash)

	removed, err := db.RemoveFileEntry(&remReq, entry)
	So(err, ShouldBeNil)
	So(removed, ShouldNotBeNil)

	return removed
}

func setEntryToUploaded(entry *Entry, given *Set, db *DB) {
	setEntryToStatus(entry, given, db, transfer.RequestStatusUploaded)
}

func TestBackup(t *testing.T) {
	Convey("Given a example database, Backup() backs it up", t, func() {
		dir := t.TempDir()

		backupFile := filepath.Join(dir, "backup")

		db, err := New(filepath.Join(dir, "db"), backupFile, false)
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

			handler := internal.GetLocalHandler()

			db.EnableRemoteBackups(remotePath, handler)

			err = db.Backup()
			So(err, ShouldBeNil)

			_, err := os.Stat(remotePath)
			So(err, ShouldBeNil)

			testBackupOK(remotePath)
		})

		Convey("and can back it up to iRODS as well", func() {
			remoteDir := testutil.RequireIRODSTestCollection(t)
			if remoteDir == "" {
				SkipConvey("skipping iRODS backup test since IBACKUP_TEST_COLLECTION not set", func() {})

				return
			}

			remotePath := filepath.Join(remoteDir, "db")

			handler, err := baton.GetBatonHandler()
			So(err, ShouldBeNil)

			db.EnableRemoteBackups(remotePath, handler)

			err = db.Backup()
			So(err, ShouldBeNil)

			localPath := t.TempDir()
			localDB := filepath.Join(localPath, "db")
			icmd := testutil.NewIcommander(t)
			So(icmd, ShouldNotBeNil)

			_, err = icmd.IGET(remotePath, localDB)
			So(err, ShouldBeNil)

			testBackupOK(localDB)

			Convey("and child baton-do processes end after upload", func() {
				p, err := process.NewProcess(int32(os.Getpid())) //nolint:gosec
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

func createFileEnts(paths []string) []*Dirent {
	entries := make([]*Dirent, len(paths))

	for n, path := range paths {
		entries[n] = &Dirent{
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

func TestFixCountsHealsNumFiles(t *testing.T) {
	Convey("fixCounts should make countsValid true even when NumFiles was wrong", t, func() {
		s := &Set{Requester: "req", Name: "set", Transformer: "humgen"}
		s.NumFiles = 0
		s.Status = PendingUpload

		entries := []*Entry{{Path: "/a", Status: Pending}, {Path: "/b", Status: Pending}}
		calls := 0
		getFileEntries := func(setID string, _ EntryFilter) ([]*Entry, error) {
			calls++

			So(setID, ShouldEqual, s.ID())

			return entries, nil
		}

		err := s.UpdateBasedOnEntry(&Entry{Path: "/a", Status: Uploaded}, getFileEntries)
		So(err, ShouldBeNil)
		So(s.countsValid(), ShouldBeTrue)
		So(s.NumFiles, ShouldEqual, 2)
		So(calls, ShouldEqual, 1)

		err = s.UpdateBasedOnEntry(&Entry{Path: "/b", Status: Uploaded}, getFileEntries)
		So(err, ShouldBeNil)
		So(calls, ShouldEqual, 1)
	})
}

func TestSetEntryStatusRecountSeesUpdatedEntry(t *testing.T) {
	Convey("SetEntryStatus recount should see entry updates made in the same transaction", t, func() {
		tDir := t.TempDir()
		dbPath := filepath.Join(tDir, "set.db")

		db, err := New(dbPath, "", false)
		So(err, ShouldBeNil)
		So(db, ShouldNotBeNil)

		defer func() { So(db.Close(), ShouldBeNil) }()

		set := &Set{Name: "set1", Requester: "jim", Transformer: "prefix=/local:/remote"}
		err = db.AddOrUpdate(set)
		So(err, ShouldBeNil)

		err = db.MergeFileEntries(set.ID(), []string{"/a/b.txt"})
		So(err, ShouldBeNil)

		// Force a recount path during SetEntryStatus: NumFiles=0 makes
		// Uploaded++ temporarily invalid and triggers fixCounts().
		err = db.db.Update(func(tx *bolt.Tx) error {
			got, bid, b, errt := db.getSetByID(tx, set.ID())
			So(errt, ShouldBeNil)

			got.NumFiles = 0
			got.Uploaded = 0
			got.Replaced = 0
			got.Skipped = 0
			got.Failed = 0
			got.Missing = 0
			got.Orphaned = 0
			got.Abnormal = 0

			return b.Put(bid, db.encodeToBytes(got))
		})
		So(err, ShouldBeNil)

		r := &transfer.Request{
			Local:     "/a/b.txt",
			Requester: set.Requester,
			Set:       set.Name,
			Size:      1,
			Status:    transfer.RequestStatusUploaded,
			Error:     "",
		}

		_, err = db.SetEntryStatus(r)
		So(err, ShouldBeNil)

		sets, err := db.GetByRequester(set.Requester)
		So(err, ShouldBeNil)
		So(len(sets), ShouldEqual, 1)
		So(sets[0].NumFiles, ShouldEqual, 1)
		So(sets[0].Uploaded, ShouldEqual, 1)
	})
}

func TestUserMetadata(t *testing.T) {
	Convey("Given a metadata map, you can get a string of user data", t, func() {
		s := new(Set)

		s.Metadata = map[string]string{
			"ibackup:user:testKey":  "testVal",
			"ibackup:user:testKey2": "testVal2",
			"ibackup:reason":        "backup",
		}

		userMeta := s.UserMetadata()
		So(userMeta, ShouldResemble, "testKey=testVal;testKey2=testVal2")
	})
}

func setEntryToFailed(entry *Entry, given *Set, db *DB) {
	setEntryToStatus(entry, given, db, transfer.RequestStatusFailed)
}

// setEntryToStatus has the given entry start uploading, then end with the given
// status.
func setEntryToStatus(entry *Entry, given *Set, db *DB, status transfer.RequestStatus) {
	transformer, err := given.MakeTransformer()
	So(err, ShouldBeNil)

	r, err := transfer.NewRequestWithTransformedLocal(entry.Path, transformer)
	So(err, ShouldBeNil)

	r.Set = given.Name
	r.Requester = given.Requester

	if entry.Type == Hardlink {
		r.Hardlink = entry.Dest
	}

	if entry.Type == Symlink {
		r.Symlink = entry.Dest
	}

	r.Status = transfer.RequestStatusUploading
	_, err = db.SetEntryStatus(r)
	So(err, ShouldBeNil)

	r.Status = status
	_, err = db.SetEntryStatus(r)
	So(err, ShouldBeNil)
}
