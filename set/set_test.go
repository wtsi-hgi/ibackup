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
					Requester:   "me",
					Entries:     []string{"a/b.txt", "b/c.txt"},
					Transformer: "prefix=/local:/remote",
					Monitor:     false,
				}

				err = db.AddOrUpdate(set)
				So(err, ShouldBeNil)

				set.Monitor = true
				err = db.AddOrUpdate(set)
				So(err, ShouldBeNil)

				set2 := &Set{
					Name:        "set2",
					Requester:   "me",
					Entries:     []string{"a/d.txt", "b/e.txt"},
					Transformer: "prefix=/local:/remote",
					Monitor:     false,
				}

				err = db.AddOrUpdate(set2)
				So(err, ShouldBeNil)

				Convey("Then get the Sets", func() {
					sets, err := db.GetAll()
					So(err, ShouldBeNil)
					So(sets, ShouldNotBeNil)
					So(len(sets), ShouldEqual, 2)
					So(map[string]*Set{sets[0].ID(): sets[0], sets[1].ID(): sets[1]}, ShouldResemble,
						map[string]*Set{set.ID(): set, set2.ID(): set2})
				})
			})
		})
	})
}
