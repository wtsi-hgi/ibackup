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

package db

import (
	"slices"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestFiles(t *testing.T) {
	Convey("With a database containing sets", t, func() {
		d := createTestDatabase(t)

		setA := &Set{
			Name:        "mySet",
			Requester:   "me",
			Transformer: complexTransformer,
			Description: "my first set",
		}

		setB := &Set{
			Name:        "my2ndSet",
			Requester:   "me",
			Transformer: complexTransformer,
		}

		So(d.CreateSet(setA), ShouldBeNil)
		So(d.CreateSet(setB), ShouldBeNil)

		files := []*File{
			{
				LocalPath:   "/some/local/file",
				RemotePath:  "/remote/file",
				Size:        100,
				Inode:       10,
				MountPount:  "/some/",
				Btime:       100,
				Mtime:       200,
				Type:        Regular,
				Owner:       "joe",
				SymlinkDest: "",
				modifiable:  true,
			},
		}

		Convey("You can add and retrieve files in that set", func() {
			var err error

			So(d.SetSetFiles(setA, slices.Values(files), noSeq[*File]), ShouldBeNil)
			So(collectIter(t, d.GetSetFiles(setA)), ShouldResemble, files)
			So(collectIter(t, d.GetSetFiles(setB)), ShouldBeNil)

			So(d.SetSetFiles(setA, slices.Values(files), noSeq[*File]), ShouldBeNil)
			So(collectIter(t, d.GetSetFiles(setA)), ShouldResemble, files)

			setA, err = d.GetSet(setA.Name, setA.Requester)
			So(err, ShouldBeNil)
			So(setA.NumFiles, ShouldEqual, 1)
			So(setA.SizeTotal, ShouldEqual, 100)

			files = append(files, &File{
				LocalPath:  "/some/other/local/file",
				RemotePath: "/remote/file2",
				Size:       110,
				Inode:      11,
				MountPount: "/some/",
				Btime:      100,
				Mtime:      200,
				Type:       Regular,
				Owner:      "bob",
				modifiable: true,
			}, &File{
				LocalPath:  "/some/local/file/hardlink",
				RemotePath: "/remote/file",
				Size:       120,
				Inode:      10,
				MountPount: "/some/",
				Btime:      100,
				Mtime:      200,
				Type:       Regular,
				Owner:      "joe",
				modifiable: true,
			})

			files[0].Size = 120

			So(d.SetSetFiles(setA, slices.Values(files[1:]), noSeq[*File]), ShouldBeNil)
			So(collectIter(t, d.GetSetFiles(setA)), ShouldResemble, files)

			setA, err = d.GetSet(setA.Name, setA.Requester)
			So(err, ShouldBeNil)
			So(setA.NumFiles, ShouldEqual, 3)
			So(setA.SizeTotal, ShouldEqual, 350)

			files = append(files, &File{
				LocalPath:   "/some/local/symlink",
				RemotePath:  "/remote/symlink",
				Size:        150,
				Inode:       10,
				MountPount:  "/some/",
				Btime:       200,
				Mtime:       200,
				Type:        Symlink,
				SymlinkDest: "/path/to/target",
				modifiable:  true,
			})

			So(d.SetSetFiles(setA, slices.Values(files[3:]), noSeq[*File]), ShouldBeNil)
			So(collectIter(t, d.GetSetFiles(setA)), ShouldResemble, files)

			setA, err = d.GetSet(setA.Name, setA.Requester)
			So(err, ShouldBeNil)
			So(setA.NumFiles, ShouldEqual, 4)
			So(setA.SizeTotal, ShouldEqual, 500)

			refs, err := d.CountRemoteFileRefs(files[0])
			So(err, ShouldBeNil)
			So(refs, ShouldEqual, 2)

			refs, err = d.CountRemoteFileRefs(files[1])
			So(err, ShouldBeNil)
			So(refs, ShouldEqual, 1)

			refs, err = d.CountRemoteFileRefs(files[2])
			So(err, ShouldBeNil)
			So(refs, ShouldEqual, 2)

			So(len(collectIter(t, d.listRemoteFiles(t))), ShouldEqual, 3)
			So(len(collectIter(t, d.listInodes(t))), ShouldEqual, 3)

			Convey("You can also remove files from a set", func() {
				now := time.Now().Truncate(time.Second)

				So(d.RemoveSetFiles(slices.Values(files[:1])), ShouldBeNil)

				setA, err = d.GetSet(setA.Name, setA.Requester)
				So(err, ShouldBeNil)
				So(setA.NumFiles, ShouldEqual, 4)
				So(setA.SizeTotal, ShouldEqual, 500)

				So(d.clearQueue(), ShouldBeNil)

				setA, err = d.GetSet(setA.Name, setA.Requester)
				So(err, ShouldBeNil)
				So(setA.NumFiles, ShouldEqual, 3)
				So(setA.SizeTotal, ShouldEqual, 380)

				refs, err = d.CountRemoteFileRefs(files[0])
				So(err, ShouldBeNil)
				So(refs, ShouldEqual, 0)

				refs, err = d.CountRemoteFileRefs(files[1])
				So(err, ShouldBeNil)
				So(refs, ShouldEqual, 1)

				refs, err = d.CountRemoteFileRefs(files[2])
				So(err, ShouldBeNil)
				So(refs, ShouldEqual, 2)

				setTrashA, err := scanSet(d.db.QueryRow(getSetByNameRequester, "\x00"+setA.Name, setA.Requester))
				So(err, ShouldBeNil)

				trashed := collectIter(t, d.GetSetFiles(setTrashA))
				So(len(trashed), ShouldEqual, 1)
				So(trashed[0].LocalPath, ShouldEqual, files[0].LocalPath)
				So(trashed[0].LastUpload, ShouldHappenOnOrAfter, now)

				So(d.RemoveSetFiles(slices.Values(trashed)), ShouldBeNil)

				refs, err = d.CountRemoteFileRefs(files[2])
				So(err, ShouldBeNil)
				So(refs, ShouldEqual, 2)

				So(d.clearQueue(), ShouldBeNil)

				refs, err = d.CountRemoteFileRefs(files[2])
				So(err, ShouldBeNil)
				So(refs, ShouldEqual, 1)

				So(len(collectIter(t, d.listRemoteFiles(t))), ShouldEqual, 3)
				So(len(collectIter(t, d.listInodes(t))), ShouldEqual, 3)

				So(d.RemoveSetFiles(slices.Values(files[2:3])), ShouldBeNil)
				So(d.clearQueue(), ShouldBeNil)

				trashFiles := d.GetSetFiles(setTrashA)
				So(d.RemoveSetFiles(trashFiles.Iter), ShouldBeNil)
				So(trashFiles.Error, ShouldBeNil)
				So(d.clearQueue(), ShouldBeNil)

				So(len(collectIter(t, d.listRemoteFiles(t))), ShouldEqual, 2)
				So(len(collectIter(t, d.listInodes(t))), ShouldEqual, 2)
			})

			Convey("Files can be removed from a set based on a path prefix", func() {
				So(d.RemoveSetFilesInDir(setA, "/some/local/"), ShouldBeNil)
				So(d.clearQueue(), ShouldBeNil)

				got := collectIter(t, d.GetSetFiles(setA))
				So(len(got), ShouldEqual, 1)

				files[1].Status = StatusUploaded
				got[0].LastUpload = time.Time{}

				So(got, ShouldResemble, files[1:2])
			})
		})

		Convey("Files that failed to upload/remove have the appropriate data set", func() {
			So(d.SetSetFiles(setA, slices.Values(files), noSeq[*File]), ShouldBeNil)

			process, err := d.RegisterProcess()
			So(err, ShouldBeNil)

			tasks := collectIter(t, d.ReserveTasks(process, 1))
			tasks[0].Error = "some error"
			now := time.Now().Truncate(time.Second)

			So(d.TaskFailed(tasks[0]), ShouldBeNil)

			files := collectIter(t, d.GetSetFiles(setA))
			So(files[0].LastFailedAttempt, ShouldHappenOnOrAfter, now)
			So(files[0].LastError, ShouldEqual, "some error")
			So(files[0].Attempts, ShouldEqual, 1)
		})
	})
}

func (d *DBRO) listRemoteFiles(t *testing.T) *IterErr[string] {
	t.Helper()

	return iterRows(d, func(s scanner) (string, error) {
		var path string

		err := s.Scan(&path)

		return path, err
	}, "SELECT `remotePath` FROM `remoteFiles`")
}

func (d *DBRO) listInodes(t *testing.T) *IterErr[int] {
	t.Helper()

	return iterRows(d, func(s scanner) (int, error) {
		var inode int

		err := s.Scan(&inode)

		return inode, err
	}, "SELECT `inode` FROM `hardlinks`")
}
