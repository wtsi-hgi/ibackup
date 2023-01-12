/*******************************************************************************
 * Copyright (c) 2022 Genome Research Ltd.
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
	"os"
	"path/filepath"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/ibackup/put"
)

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

	Convey("Entry.ShouldUpload() gives good advice", t, func() {
		reuploadAfter := time.Now()

		e := &Entry{Status: Pending}
		So(e.ShouldUpload(reuploadAfter), ShouldBeTrue)

		e.Status = Missing
		So(e.ShouldUpload(reuploadAfter), ShouldBeFalse)

		e.Status = Failed
		So(e.ShouldUpload(reuploadAfter), ShouldBeTrue)

		e.Attempts = AttemptsToBeConsideredFailing
		So(e.ShouldUpload(reuploadAfter), ShouldBeFalse)

		e.Attempts = 1
		e.LastAttempt = reuploadAfter.Add(1 * time.Second)
		e.Status = Uploaded
		So(e.ShouldUpload(reuploadAfter), ShouldBeFalse)
		So(e.ShouldUpload(reuploadAfter.Add(2*time.Second)), ShouldBeTrue)
	})

	Convey("Discovered() returns friendly strings", t, func() {
		set := &Set{}
		So(set.Discovered(), ShouldEqual, "not started")

		t := time.Now()
		set.StartedDiscovery = t
		So(set.Discovered(), ShouldEqual, "started "+t.Format(dateFormat))

		t2 := t.Add(24 * time.Hour)
		set.LastDiscovery = t2
		So(set.Discovered(), ShouldEqual, "completed "+t2.Format(dateFormat))

		t3 := t2.Add(24 * time.Hour)
		set.StartedDiscovery = t3
		So(set.Discovered(), ShouldEqual, "started "+t3.Format(dateFormat))
	})

	Convey("Count() and Size() return friendly strings", t, func() {
		set := &Set{}
		So(set.Count(), ShouldEqual, "pending")
		So(set.Size(), ShouldEqual, "pending")

		set.NumFiles = 3
		So(set.Count(), ShouldEqual, "3")
		So(set.Size(), ShouldEqual, "0 B (and counting)")

		set.SizeFiles = 30
		So(set.Size(), ShouldEqual, "30 B (and counting)")

		set.Status = Complete
		So(set.Count(), ShouldEqual, "3")
		So(set.Size(), ShouldEqual, "30 B")

		set.LastCompletedCount = 3
		set.LastCompletedSize = 30
		set.NumFiles = 0
		set.SizeFiles = 0
		set.Status = PendingDiscovery

		So(set.Count(), ShouldEqual, "3 (as of last completion)")
		So(set.Size(), ShouldEqual, "30 B (as of last completion)")
	})

	Convey("MakeTransformer works", t, func() {
		set := &Set{Transformer: "humgen"}
		trans, err := set.MakeTransformer()
		So(err, ShouldBeNil)
		remote, err := trans("/lustre/scratch118/humgen/projects/ddd/file.txt")
		So(err, ShouldBeNil)
		So(remote, ShouldEqual, "/humgen/projects/ddd/scratch118/file.txt")

		dir, err := os.Getwd()
		So(err, ShouldBeNil)

		set = &Set{Transformer: "prefix=" + dir + ":/zone"}
		trans, err = set.MakeTransformer()
		So(err, ShouldBeNil)
		remote, err = trans(filepath.Join(dir, "file.txt"))
		So(err, ShouldBeNil)
		So(remote, ShouldEqual, "/zone/file.txt")
	})

	Convey("Given a path", t, func() {
		tDir := t.TempDir()
		dbPath := filepath.Join(tDir, "set.db")

		Convey("You can create a new set database", func() {
			db, err := New(dbPath)
			So(err, ShouldBeNil)
			So(db, ShouldNotBeNil)

			Convey("And add Sets to it", func() {
				set := &Set{
					Name:        "set1",
					Requester:   "jim",
					Transformer: "prefix=/local:/remote",
					Monitor:     false,
					DeleteLocal: false,
				}

				err = db.AddOrUpdate(set)
				So(err, ShouldBeNil)

				err = db.SetFileEntries(set.ID(), []string{"/a/b.txt", "/c/d.txt", "/e/f.txt"})
				So(err, ShouldBeNil)

				err = db.SetDirEntries(set.ID(), []string{"/g/h", "/g/i"})
				So(err, ShouldBeNil)

				set.Monitor = true
				err = db.AddOrUpdate(set)
				So(err, ShouldBeNil)

				set2 := &Set{
					Name:        "set2",
					Requester:   "jane",
					Transformer: "prefix=/local:/remote",
					Monitor:     false,
					DeleteLocal: true,
				}

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

					Convey("And set an Error for it, which is cleared when we start discovery again", func() {
						msg := "foo"
						err = db.SetError(set.ID(), msg)
						So(err, ShouldBeNil)

						retrieved = db.GetByID(set.ID())
						So(retrieved, ShouldNotBeNil)
						So(retrieved.Error, ShouldEqual, msg)

						err = db.SetDiscoveryStarted(set.ID())
						So(err, ShouldBeNil)

						retrieved = db.GetByID(set.ID())
						So(retrieved, ShouldNotBeNil)
						So(retrieved.Error, ShouldBeBlank)
					})

					Convey("And set bool vals back to false on update", func() {
						So(retrieved.Monitor, ShouldBeTrue)

						retrieved2 := db.GetByID(set2.ID())
						So(retrieved2, ShouldNotBeNil)
						So(retrieved2, ShouldResemble, set2)
						So(retrieved2.DeleteLocal, ShouldBeTrue)

						retrieved.Monitor = false
						retrieved2.DeleteLocal = false

						err = db.AddOrUpdate(retrieved)
						So(err, ShouldBeNil)
						err = db.AddOrUpdate(retrieved2)
						So(err, ShouldBeNil)

						retrieved = db.GetByID(set.ID())
						So(retrieved.Monitor, ShouldBeFalse)

						retrieved2 = db.GetByID(set2.ID())
						So(retrieved2.DeleteLocal, ShouldBeFalse)
					})
				})

				Convey("Then get all the Sets and their entries", func() {
					sets, err := db.GetAll()
					So(err, ShouldBeNil)
					So(sets, ShouldNotBeNil)
					So(len(sets), ShouldEqual, 2)
					So(sets, ShouldResemble, []*Set{set2, set})

					fEntries, err := db.GetFileEntries(sets[1].ID())
					So(err, ShouldBeNil)
					So(len(fEntries), ShouldEqual, 3)
					So(fEntries[0], ShouldResemble, &Entry{Path: "/a/b.txt"})
					So(fEntries[1], ShouldResemble, &Entry{Path: "/c/d.txt"})
					So(fEntries[2], ShouldResemble, &Entry{Path: "/e/f.txt"})

					dEntries, err := db.GetDirEntries(sets[1].ID())
					So(err, ShouldBeNil)
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

				Convey("Then get all the Sets for a particular Requester", func() {
					sets, err := db.GetByRequester("jim")
					So(err, ShouldBeNil)
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

						err = db.SetDiscoveryStarted(sets[0].ID())
						So(err, ShouldBeNil)

						sets, err = db.GetByRequester("jim")
						So(err, ShouldBeNil)

						So(sets[0].Status, ShouldEqual, PendingDiscovery)
						So(sets[0].StartedDiscovery.IsZero(), ShouldBeFalse)

						err = db.SetDiscoveredEntries(set.ID(), []string{"/g/h/l.txt", "/g/i/m.txt"})
						So(err, ShouldBeNil)

						fEntries, err := db.GetFileEntries(sets[0].ID())
						So(err, ShouldBeNil)
						So(len(fEntries), ShouldEqual, 5)
						So(fEntries[0], ShouldResemble, &Entry{Path: "/a/b.txt"})
						So(fEntries[1], ShouldResemble, &Entry{Path: "/c/d.txt"})
						So(fEntries[2], ShouldResemble, &Entry{Path: "/e/f.txt"})
						So(fEntries[3], ShouldResemble, &Entry{Path: "/g/h/l.txt"})
						So(fEntries[4], ShouldResemble, &Entry{Path: "/g/i/m.txt"})

						sets, err = db.GetByRequester("jim")
						So(err, ShouldBeNil)

						So(sets[0].Status, ShouldEqual, PendingUpload)
						So(sets[0].StartedDiscovery.IsZero(), ShouldBeFalse)
						So(sets[0].LastDiscovery.IsZero(), ShouldBeFalse)
						So(sets[0].NumFiles, ShouldEqual, 5)
						So(sets[0].SizeFiles, ShouldEqual, 0)

						setsAll, err := db.GetAll()
						So(err, ShouldBeNil)
						So(setsAll, ShouldNotBeNil)
						So(len(setsAll), ShouldEqual, 2)

						r := &put.Request{
							Local:     "/a/b.txt",
							Requester: set.Requester,
							Set:       set.Name,
							Size:      3,
							Status:    put.RequestStatusUploading,
							Error:     "",
						}

						e, err := db.SetEntryStatus(r)
						So(err, ShouldBeNil)
						So(e, ShouldNotBeNil)
						So(e.Path, ShouldEqual, fEntries[0].Path)
						So(e.Size, ShouldEqual, r.Size)
						So(e.Status, ShouldEqual, UploadingEntry)

						sets, err = db.GetByRequester("jim")
						So(err, ShouldBeNil)

						So(sets[0].Status, ShouldEqual, Uploading)
						So(sets[0].NumFiles, ShouldEqual, 5)
						So(sets[0].SizeFiles, ShouldEqual, 3)
						So(sets[0].Uploaded, ShouldEqual, 0)
						So(sets[0].LastCompletedSize, ShouldEqual, 0)

						r = &put.Request{
							Local:     "/a/b.txt",
							Requester: set.Requester,
							Set:       set.Name,
							Size:      3,
							Status:    put.RequestStatusUploaded,
							Error:     "",
						}

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
						So(sets[0].LastCompletedSize, ShouldEqual, 0)

						fEntries, err = db.GetFileEntries(sets[0].ID())
						So(err, ShouldBeNil)
						So(len(fEntries), ShouldEqual, 5)
						So(fEntries[0].Size, ShouldEqual, 3)
						So(fEntries[0].Status, ShouldEqual, Uploaded)
						So(fEntries[0].LastAttempt.IsZero(), ShouldBeFalse)

						r = &put.Request{
							Local:     "/c/d.txt",
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
							Local:     "/e/f.txt",
							Requester: set.Requester,
							Set:       set.Name,
							Size:      4,
							Status:    put.RequestStatusReplaced,
							Error:     "",
						}

						_, err = db.SetEntryStatus(r)
						So(err, ShouldBeNil)

						sets, err = db.GetByRequester("jim")
						So(err, ShouldBeNil)

						So(sets[0].Status, ShouldEqual, Uploading)
						So(sets[0].SizeFiles, ShouldEqual, 9)
						So(sets[0].Uploaded, ShouldEqual, 3)

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
							Status:    put.RequestStatusFailed,
							Error:     "upload failed",
						}

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
						So(fEntries[3].LastError, ShouldEqual, "upload failed")

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
						So(fEntries[3].LastError, ShouldEqual, "upload failed")

						_, err = db.SetEntryStatus(r)
						So(err, ShouldBeNil)

						sets, err = db.GetByRequester("jim")
						So(err, ShouldBeNil)

						So(sets[0].Status, ShouldEqual, Failing)
						So(sets[0].SizeFiles, ShouldEqual, 15)
						So(sets[0].Uploaded, ShouldEqual, 3)
						So(sets[0].Failed, ShouldEqual, 1)

						fEntries, err = db.GetFileEntries(sets[0].ID())
						So(err, ShouldBeNil)
						So(len(fEntries), ShouldEqual, 5)
						So(fEntries[3].Size, ShouldEqual, 6)
						So(fEntries[3].Status, ShouldEqual, Failed)
						So(fEntries[3].Attempts, ShouldEqual, 3)
						So(fEntries[3].LastAttempt.IsZero(), ShouldBeFalse)
						So(fEntries[3].LastError, ShouldEqual, "upload failed")

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
							Status:    put.RequestStatusUploaded,
							Error:     "",
						}

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
						So(fEntries[3].LastError, ShouldEqual, "upload failed")

						Convey("Finally, set status gets reset on new discovery", func() {
							oldStart := sets[0].StartedDiscovery
							oldDisc := sets[0].LastDiscovery

							err = db.SetDiscoveryStarted(sets[0].ID())
							So(err, ShouldBeNil)

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

							err = db.SetDiscoveredEntries(set.ID(), []string{"/g/h/l.txt", "/g/i/m.txt", "/g/i/n.txt"})
							So(err, ShouldBeNil)

							fEntries, err := db.GetFileEntries(sets[0].ID())
							So(err, ShouldBeNil)
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
								Status:    put.RequestStatusUploaded,
								Error:     "",
							}

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
					})
				})
			})
		})
	})
}
