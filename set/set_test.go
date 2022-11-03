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
	"path/filepath"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestSet(t *testing.T) {
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
				}

				err = db.AddOrUpdate(set2)
				So(err, ShouldBeNil)

				err = db.SetFileEntries(set2.ID(), []string{"/a/b.txt", "/c/j.txt"})
				So(err, ShouldBeNil)

				err = db.SetFileEntries(set2.ID(), []string{"/a/b.txt", "/c/k.txt"})
				So(err, ShouldBeNil)

				Convey("Then get all the Sets and their entries", func() {
					sets, err := db.GetAll()
					So(err, ShouldBeNil)
					So(sets, ShouldNotBeNil)
					So(len(sets), ShouldEqual, 2)
					So(sets, ShouldResemble, []*Set{set2, set})

					fEntries, err := sets[1].Files(db)
					So(err, ShouldBeNil)
					So(len(fEntries), ShouldEqual, 3)
					So(fEntries[0], ShouldResemble, &Entry{Path: "/a/b.txt"})
					So(fEntries[1], ShouldResemble, &Entry{Path: "/c/d.txt"})
					So(fEntries[2], ShouldResemble, &Entry{Path: "/e/f.txt"})

					dEntries, err := sets[1].Dirs(db)
					So(err, ShouldBeNil)
					So(len(dEntries), ShouldEqual, 2)
					So(dEntries[0], ShouldResemble, &Entry{Path: "/g/h"})
					So(dEntries[1], ShouldResemble, &Entry{Path: "/g/i"})

					fEntries, err = sets[0].Files(db)
					So(err, ShouldBeNil)
					So(len(fEntries), ShouldEqual, 2)
					So(fEntries[0], ShouldResemble, &Entry{Path: "/a/b.txt"})
					So(fEntries[1], ShouldResemble, &Entry{Path: "/c/k.txt"})

					dEntries, err = sets[0].Dirs(db)
					So(err, ShouldBeNil)
					So(len(dEntries), ShouldEqual, 0)
				})

				Convey("Then get all the Sets for a particular Requester", func() {
					sets, err := db.GetByRequester("jane")
					So(err, ShouldBeNil)
					So(sets, ShouldNotBeNil)
					So(len(sets), ShouldEqual, 1)
					So(sets, ShouldResemble, []*Set{set2})

					Convey("And update Set status", func() {
						So(sets[0].Status, ShouldEqual, PendingDiscovery)
						So(sets[0].StartedDiscovery.IsZero(), ShouldBeTrue)
						So(sets[0].LastDiscovery.IsZero(), ShouldBeTrue)
						So(sets[0].Description, ShouldBeBlank)

						sets[0].LastDiscovery = time.Now()
						sets[0].Description = "desc"
						err = db.AddOrUpdate(sets[0])
						So(err, ShouldBeNil)

						sets, err = db.GetByRequester("jane")
						So(err, ShouldBeNil)

						So(sets[0].Status, ShouldEqual, PendingDiscovery)
						So(sets[0].LastDiscovery.IsZero(), ShouldBeTrue)
						So(sets[0].Description, ShouldEqual, "desc")

						err = sets[0].DiscoveryStarted(db)
						So(err, ShouldBeNil)

						sets, err = db.GetByRequester("jane")
						So(err, ShouldBeNil)

						So(sets[0].Status, ShouldEqual, PendingDiscovery)
						So(sets[0].StartedDiscovery.IsZero(), ShouldBeFalse)

						err = sets[0].DiscoveryCompleted(db, 10, 100)
						So(err, ShouldBeNil)

						sets, err = db.GetByRequester("jane")
						So(err, ShouldBeNil)

						So(sets[0].Status, ShouldEqual, PendingUpload)
						So(sets[0].StartedDiscovery.IsZero(), ShouldBeFalse)
						So(sets[0].LastDiscovery.IsZero(), ShouldBeFalse)
						So(sets[0].NumFiles, ShouldEqual, 10)
						So(sets[0].SizeFiles, ShouldEqual, 100)
					})
				})
			})
		})
	})
}
