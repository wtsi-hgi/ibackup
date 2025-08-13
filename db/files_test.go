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
			Transformer: simpleTransformer,
			Description: "my first set",
		}

		setB := &Set{
			Name:        "my2ndSet",
			Requester:   "me",
			Transformer: simpleTransformer,
		}

		So(d.CreateSet(setA), ShouldBeNil)
		So(d.CreateSet(setB), ShouldBeNil)

		file1 := &File{
			LocalPath:   "/some/local/file",
			RemotePath:  "/remote/file",
			Size:        100,
			Inode:       10,
			MountPount:  "/some/",
			Btime:       100,
			Mtime:       200,
			Type:        Regular,
			Owner:       "joe",
			Group:       "my_group",
			SymlinkDest: "",
			setID:       1,
		}

		files := []*File{file1}

		Convey("You can add and retrieve files in a set", func() {
			var err error

			So(d.CompleteDiscovery(setA, slices.Values(files), noSeq[*File]), ShouldBeNil)
			So(collectIter(t, d.GetSetFiles(setA)), ShouldResemble, files)
			So(collectIter(t, d.GetSetFiles(setB)), ShouldBeNil)

			So(d.CompleteDiscovery(setA, slices.Values(files), noSeq[*File]), ShouldBeNil)
			So(collectIter(t, d.GetSetFiles(setA)), ShouldResemble, files)

			setA, err = d.GetSet(setA.Name, setA.Requester)
			So(err, ShouldBeNil)
			So(setA.NumFiles, ShouldEqual, 1)
			So(setA.SizeTotal, ShouldEqual, 100)

			file2 := &File{
				LocalPath:  "/some/other/local/file2",
				RemotePath: "/remote/file2",
				Size:       110,
				Inode:      11,
				MountPount: "/some/",
				Btime:      100,
				Mtime:      200,
				Type:       Regular,
				Owner:      "bob",
				Group:      "my_other_group",
				setID:      1,
			}

			hardlink := &File{
				LocalPath:  "/some/local/file/hardlink",
				RemotePath: "/remote/file/hardlink",
				Size:       120,
				Inode:      10,
				MountPount: "/some/",
				Btime:      100,
				Mtime:      200,
				Type:       Regular,
				Owner:      "joe",
				Group:      "my_group",
				setID:      1,
			}

			files = append(files, file2, hardlink)

			files[0].Size = 120

			So(d.CompleteDiscovery(setA, slices.Values(files[1:]), noSeq[*File]), ShouldBeNil)
			So(collectIter(t, d.GetSetFiles(setA)), ShouldResemble, files)

			setA, err = d.GetSet(setA.Name, setA.Requester)
			So(err, ShouldBeNil)
			So(setA.NumFiles, ShouldEqual, 3)
			So(setA.SizeTotal, ShouldEqual, 350)
			So(setA.Hardlinks, ShouldEqual, 1)

			symlink := &File{
				LocalPath:   "/some/local/symlink",
				RemotePath:  "/remote/symlink",
				Size:        150,
				Inode:       10,
				MountPount:  "/some/",
				Btime:       200,
				Mtime:       200,
				Type:        Symlink,
				SymlinkDest: "/path/to/target",
				setID:       1,
			}

			files = append(files, symlink)

			So(d.CompleteDiscovery(setA, slices.Values(files[3:]), noSeq[*File]), ShouldBeNil)
			So(collectIter(t, d.GetSetFiles(setA)), ShouldResemble, files)

			setA, err = d.GetSet(setA.Name, setA.Requester)
			So(err, ShouldBeNil)
			So(setA.NumFiles, ShouldEqual, 4)
			So(setA.SizeTotal, ShouldEqual, 500)

			refs, err := d.countRemoteFileRefs(files[0])
			So(err, ShouldBeNil)
			So(refs, ShouldEqual, 1)

			refs, err = d.countRemoteFileRefs(files[1])
			So(err, ShouldBeNil)
			So(refs, ShouldEqual, 1)

			refs, err = d.countRemoteFileRefs(files[2])
			So(err, ShouldBeNil)
			So(refs, ShouldEqual, 1)

			So(len(collectIter(t, d.listRemoteFiles(t))), ShouldEqual, 4)
			So(len(collectIter(t, d.listInodes(t))), ShouldEqual, 3)

			Convey("And with the same file in two sets", func() {
				file1.setID = 2

				So(d.CompleteDiscovery(setB, slices.Values(files[:1]), noSeq[*File]), ShouldBeNil)

				refs, err = d.countRemoteFileRefs(file1)
				So(err, ShouldBeNil)
				So(refs, ShouldEqual, 2)

				Convey("Remove on a file with the wrong setID returns an error", func() {
					err := d.RemoveSetFiles(setA, slices.Values([]*File{file1}))
					So(err, ShouldEqual, ErrFileNotInSet)
				})

				Convey("Trashing the file from one set does not affect the other set, inode table or remote refs", func() {
					So(d.RemoveSetFiles(setB, slices.Values([]*File{file1})), ShouldBeNil)
					So(d.clearQueue(), ShouldBeNil)

					setB, err = d.GetSet(setB.Name, setB.Requester)
					So(err, ShouldBeNil)
					So(setB.NumFiles, ShouldEqual, 0)

					setA, err = d.GetSet(setA.Name, setA.Requester)
					So(err, ShouldBeNil)
					So(setA.NumFiles, ShouldEqual, 4)

					So(len(collectIter(t, d.listInodes(t))), ShouldEqual, 3)

					refs, err = d.countRemoteFileRefs(file1)
					So(err, ShouldBeNil)
					So(refs, ShouldEqual, 2)

					Convey("And if you remove the trashed file, it doesn't change the other set but does reduce remote refs", func() {
						d.removeTrashedFile(t, setB)

						refs, err = d.countRemoteFileRefs(file1)
						So(err, ShouldBeNil)
						So(refs, ShouldEqual, 1)

						setA, err = d.GetSet(setA.Name, setA.Requester)
						So(err, ShouldBeNil)
						So(setA.NumFiles, ShouldEqual, 4)

						So(len(collectIter(t, d.listInodes(t))), ShouldEqual, 3)
					})
				})
			})

			Convey("And you can trash a file from the set", func() {
				now := time.Now().Truncate(time.Second)

				So(d.RemoveSetFiles(setA, slices.Values([]*File{file2})), ShouldBeNil)

				setA, err = d.GetSet(setA.Name, setA.Requester)
				So(err, ShouldBeNil)
				So(setA.NumFiles, ShouldEqual, 4)
				So(setA.SizeTotal, ShouldEqual, 500)

				So(d.clearQueue(), ShouldBeNil)

				setA, err = d.GetSet(setA.Name, setA.Requester)
				So(err, ShouldBeNil)
				So(setA.NumFiles, ShouldEqual, 3)
				So(setA.SizeTotal, ShouldEqual, 390)

				setTrashA, errg := d.GetTrashSet(setA.Name, setA.Requester)
				So(errg, ShouldBeNil)

				trashed := collectIter(t, d.GetSetFiles(setTrashA))
				So(len(trashed), ShouldEqual, 1)
				So(trashed[0].LocalPath, ShouldEqual, file2.LocalPath)
				So(trashed[0].LastUpload, ShouldHappenOnOrAfter, now)

				Convey("And you can remove the trashed file from the trash set", func() {
					So(d.RemoveSetFiles(setTrashA, slices.Values(trashed)), ShouldBeNil)

					So(d.clearQueue(), ShouldBeNil)

					trashed := collectIter(t, d.GetSetFiles(setTrashA))
					So(len(trashed), ShouldEqual, 0)

					Convey("Which also removes the file from the remote and inode tables", func() {
						remoteFiles := collectIter(t, d.listRemoteFiles(t))
						So(len(remoteFiles), ShouldEqual, 3)
						So(remoteFiles, ShouldNotContain, file2.RemotePath)

						refs, err = d.countRemoteFileRefs(file2)
						So(err, ShouldBeNil)
						So(refs, ShouldEqual, 0)

						So(len(collectIter(t, d.listInodes(t))), ShouldEqual, 2)
					})
				})
			})

			Convey("And you can trash a file with a hardlink", func() {
				So(d.RemoveSetFiles(setA, slices.Values([]*File{file1})), ShouldBeNil)

				So(d.clearQueue(), ShouldBeNil)

				setA, err = d.GetSet(setA.Name, setA.Requester)
				So(err, ShouldBeNil)
				So(setA.NumFiles, ShouldEqual, 3)
				So(setA.SizeTotal, ShouldEqual, 380)
				So(setA.Hardlinks, ShouldEqual, 1)

				Convey("And remove the trashed file", func() {
					So(len(collectIter(t, d.listInodes(t))), ShouldEqual, 3)

					d.removeTrashedFile(t, setA)

					Convey("Which does not remove the inode from db", func() {
						So(len(collectIter(t, d.listInodes(t))), ShouldEqual, 3)
					})

					Convey("And trash and remove the hardlink", func() {
						So(d.RemoveSetFiles(setA, slices.Values([]*File{hardlink})), ShouldBeNil)

						So(d.clearQueue(), ShouldBeNil)

						setA, err = d.GetSet(setA.Name, setA.Requester)
						So(err, ShouldBeNil)
						So(setA.NumFiles, ShouldEqual, 2)
						So(setA.Hardlinks, ShouldEqual, 0)

						d.removeTrashedFile(t, setA)

						Convey("Which removes the inode from db", func() {
							So(len(collectIter(t, d.listInodes(t))), ShouldEqual, 2)
						})
					})
				})
			})

			Convey("And you can trash a hardlink and the file", func() {
				So(d.RemoveSetFiles(setA, slices.Values([]*File{file1, hardlink})), ShouldBeNil)

				So(d.clearQueue(), ShouldBeNil)

				setA, err = d.GetSet(setA.Name, setA.Requester)
				So(err, ShouldBeNil)
				So(setA.NumFiles, ShouldEqual, 2)
				So(setA.SizeTotal, ShouldEqual, 260)
				So(setA.Hardlinks, ShouldEqual, 0)

				Convey("And remove the trashed file", func() {
					So(len(collectIter(t, d.listInodes(t))), ShouldEqual, 3)

					d.removeTrashedFile(t, setA)

					Convey("Which does not remove the inode from db", func() {
						So(len(collectIter(t, d.listInodes(t))), ShouldEqual, 2)
					})
				})
			})

			Convey("And files can be removed from a set based on a path prefix", func() {
				got := collectIter(t, d.GetSetFiles(setA))
				So(len(got), ShouldEqual, 4)

				So(d.RemoveSetFilesInDir(setA, "/some/local"), ShouldBeNil)
				So(d.clearQueue(), ShouldBeNil)

				got = collectIter(t, d.GetSetFiles(setA))
				So(len(got), ShouldEqual, 1)

				files[1].Status = StatusUploaded
				got[0].LastUpload = time.Time{}

				So(got, ShouldResemble, files[1:2])
			})

			Convey("And RemoveSetFilesInDir does nothing if the provided dir is empty", func() {
				got := collectIter(t, d.GetSetFiles(setA))
				So(len(got), ShouldEqual, 4)

				So(d.RemoveSetFilesInDir(setA, "/empty/dir/"), ShouldBeNil)
				So(d.clearQueue(), ShouldBeNil)

				got = collectIter(t, d.GetSetFiles(setA))
				So(len(got), ShouldEqual, 4)
			})

			Convey("If the set is made read only", func() {
				So(d.SetSetReadonly(setA), ShouldBeNil)

				Convey("You can still retrieve files from it", func() {
					got := collectIter(t, d.GetSetFiles(setA))
					So(len(got), ShouldEqual, 4)
				})

				Convey("You cannot add files to it", func() {
					err := d.CompleteDiscovery(setA, slices.Values(files[1:]), noSeq[*File])
					So(err, ShouldEqual, ErrReadonlySet)
				})

				Convey("You cannot remove files from it", func() {
					err := d.RemoveSetFilesInDir(setA, "/some/local/")
					So(err, ShouldEqual, ErrReadonlySet)

					err = d.RemoveSetFiles(setA, slices.Values([]*File{file1}))
					So(err, ShouldEqual, ErrReadonlySet)
				})
			})
		})

		Convey("Files that failed to upload/remove have the appropriate data set", func() {
			So(d.CompleteDiscovery(setA, slices.Values(files), noSeq[*File]), ShouldBeNil)

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

func (d *DB) removeTrashedFile(t *testing.T, set *Set) {
	t.Helper()

	setTrashB, errg := d.GetTrashSet(set.Name, set.Requester)
	So(errg, ShouldBeNil)

	trashed := collectIter(t, d.GetSetFiles(setTrashB))

	So(d.RemoveSetFiles(setTrashB, slices.Values(trashed)), ShouldBeNil)

	So(d.clearQueue(), ShouldBeNil)
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

func (d *DBRO) countRemoteFileRefs(file *File) (int64, error) {
	var count int64

	err := d.db.QueryRow("SELECT COUNT(1) FROM `localFiles` WHERE `remoteFileID` = ("+
		"SELECT `remoteFileID` FROM `localFiles` WHERE `localPathHash` = "+virtStart+
		"?"+
		virtEnd+");", file.LocalPath).Scan(&count)

	return count, err
}
