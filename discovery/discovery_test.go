/*******************************************************************************
 * Copyright (c) 2025 Genome Research Ltd.
 *
 * Author: Michael Woolnough <mw31@sanger.ac.uk>
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

package discovery

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/ibackup/db"
	"github.com/wtsi-hgi/ibackup/internal/testdb"
)

func TestDiscover(t *testing.T) {
	Convey("With a test database", t, func() {
		tmp := t.TempDir()
		d := testdb.CreateTestDatabase(t)

		transformer, err := db.NewTransformer("myTransformer", "^"+tmp+"/", "/remote/")
		So(err, ShouldBeNil)

		set := &db.Set{
			Name:        "mySet",
			Requester:   "me",
			Transformer: transformer,
		}

		So(d.CreateSet(set), ShouldBeNil)

		set, err = d.GetSet(set.Name, set.Requester)
		So(err, ShouldBeNil)
		So(set.LastDiscovery, ShouldBeZeroValue)
		So(set.LastCompleted, ShouldBeZeroValue)

		var count int

		start := time.Now().Truncate(time.Second)

		So(Discover(d, set, func(f *db.File) {}), ShouldBeNil)
		So(set.Error, ShouldEqual, ErrNoFilesDiscovered.Error())

		end := time.Now().Truncate(time.Second)

		set, err = d.GetSet(set.Name, set.Requester)
		So(err, ShouldBeNil)
		So(set.StartedDiscovery, ShouldHappenOnOrAfter, start)
		So(set.StartedDiscovery, ShouldHappenOnOrBefore, end)

		dirA := filepath.Join(tmp, "dirA")
		dirB := filepath.Join(dirA, "dirB")
		fileA := filepath.Join(tmp, "fileA")
		fileB := filepath.Join(dirB, "fileB")
		fileC := filepath.Join(dirA, "fileC")

		So(os.Mkdir(dirA, 0700), ShouldBeNil)
		So(os.Mkdir(dirB, 0700), ShouldBeNil)
		So(os.WriteFile(fileA, []byte("some data"), 0600), ShouldBeNil)
		So(os.WriteFile(fileB, []byte("some more data"), 0600), ShouldBeNil)
		So(os.WriteFile(fileC, []byte("some other data"), 0600), ShouldBeNil)

		Convey("You can discover files and directories", func() {
			So(d.AddSetDiscovery(set, &db.Discover{
				Path: fileA,
				Type: db.DiscoverFile,
			}), ShouldBeNil)
			So(collectFileStatuses(t, d.GetSetFiles(set)), ShouldResemble, []fileStatus(nil))

			So(Discover(d, set, func(f *db.File) { count++ }), ShouldBeNil)
			So(count, ShouldEqual, 1)
			So(collectFileStatuses(t, d.GetSetFiles(set)), ShouldResemble, []fileStatus{{fileA, db.StatusNone}})

			count = 0

			So(d.AddSetDiscovery(set, &db.Discover{
				Path: dirA,
				Type: db.DiscoverDirectory,
			}), ShouldBeNil)

			So(Discover(d, set, func(f *db.File) { count++ }), ShouldBeNil)
			So(count, ShouldEqual, 3)
			So(collectFileStatuses(t, d.GetSetFiles(set)), ShouldResemble, []fileStatus{
				{fileB, db.StatusNone},
				{fileC, db.StatusNone},
				{fileA, db.StatusNone},
			})

			Convey("Removing a file prevents it from being re-discovered in a directory", func() {
				files := slices.Collect(d.GetSetFiles(set).Iter)
				count = 0

				So(d.RemoveSetFiles(set, slices.Values(files[1:2])), ShouldBeNil)
				So(Discover(d, set, func(f *db.File) { count++ }), ShouldBeNil)
				So(count, ShouldEqual, 2)
			})
		})

		Convey("You can discover with fofns", func() {
			fofn := filepath.Join(t.TempDir(), "fofn")

			So(os.WriteFile(fofn, []byte(fileA+"\n"+fileB+"\n"+fileC), 0600), ShouldBeNil)

			count := 0

			So(d.AddSetDiscovery(set, &db.Discover{
				Path: fofn,
				Type: db.DiscoverFOFN,
			}), ShouldBeNil)
			So(Discover(d, set, func(f *db.File) { count++ }), ShouldBeNil)
			So(count, ShouldEqual, 3)

			Convey("Removing a directory prevents it from being re-discovered in a fofn", func() {
				count := 0

				So(d.RemoveSetFilesInDir(set, dirB), ShouldBeNil)
				So(Discover(d, set, func(f *db.File) { count++ }), ShouldBeNil)
				So(count, ShouldEqual, 2)
			})
		})

		Convey("You can discover with fodns", func() {
			fodn := filepath.Join(t.TempDir(), "fodn")

			So(os.WriteFile(fodn, []byte(dirA), 0600), ShouldBeNil)

			count := 0

			So(d.AddSetDiscovery(set, &db.Discover{
				Path: fodn,
				Type: db.DiscoverFODN,
			}), ShouldBeNil)
			So(Discover(d, set, func(f *db.File) { count++ }), ShouldBeNil)
			So(count, ShouldEqual, 2)

			Convey("Removing a directory prevents it from being re-discovered in a fodn", func() {
				count := 0

				So(d.RemoveSetFilesInDir(set, dirB), ShouldBeNil)
				So(Discover(d, set, func(f *db.File) { count++ }), ShouldBeNil)
				So(count, ShouldEqual, 1)
			})
		})

		Convey("Deleting a discovered file sets it to missing when re-discovered", func() {
			So(d.AddSetDiscovery(set, &db.Discover{
				Path: dirA,
				Type: db.DiscoverDirectory,
			}), ShouldBeNil)

			count := 0

			So(Discover(d, set, func(f *db.File) { count++ }), ShouldBeNil)
			So(count, ShouldEqual, 2)
			So(collectFileStatuses(t, d.GetSetFiles(set)), ShouldResemble, []fileStatus{
				{fileB, db.StatusNone},
				{fileC, db.StatusNone},
			})
			So(os.Remove(fileC), ShouldBeNil)

			count = 0

			So(Discover(d, set, func(f *db.File) { count++ }), ShouldBeNil)
			So(count, ShouldEqual, 1)
			So(collectFileStatuses(t, d.GetSetFiles(set)), ShouldResemble, []fileStatus{
				{fileB, db.StatusNone},
				{fileC, db.StatusMissing},
			})

			Convey("Unless that set has monitorRemovals set, "+
				"in which case the deleted file is removed from the set", func() {
				set.MonitorRemovals = true

				So(d.SetSetMonitored(set), ShouldBeNil)

				count = 0

				So(Discover(d, set, func(f *db.File) { count++ }), ShouldBeNil)
				So(count, ShouldEqual, 1)

				testdb.DoTasks(t, d)

				So(collectFileStatuses(t, d.GetSetFiles(set)), ShouldResemble, []fileStatus{
					{fileB, db.StatusUploaded},
				})
			})
		})
	})
}

type fileStatus struct {
	name   string
	status db.FileStatus
}

func collectFileStatuses(t *testing.T, iter *db.IterErr[*db.File]) []fileStatus {
	t.Helper()

	var files []fileStatus

	So(iter.ForEach(func(f *db.File) error {
		files = append(files, fileStatus{f.LocalPath, f.Status})

		return nil
	}), ShouldBeNil)

	slices.SortFunc(files, func(a, b fileStatus) int { return strings.Compare(a.name, b.name) })

	return files
}
