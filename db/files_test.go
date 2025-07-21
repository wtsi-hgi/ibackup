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
			Transformer: "humgen",
			Description: "my first set",
		}

		setB := &Set{
			Name:        "my2ndSet",
			Requester:   "me",
			Transformer: "humgen",
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

			So(d.AddSetFiles(setA, slices.Values(files)), ShouldBeNil)
			So(slices.Collect(d.GetSetFiles(setA).Iter), ShouldResemble, files)
			So(slices.Collect(d.GetSetFiles(setB).Iter), ShouldBeNil)

			So(d.AddSetFiles(setA, slices.Values(files)), ShouldBeNil)
			So(slices.Collect(d.GetSetFiles(setA).Iter), ShouldResemble, files)

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

			So(d.AddSetFiles(setA, slices.Values(files[1:])), ShouldBeNil)
			So(slices.Collect(d.GetSetFiles(setA).Iter), ShouldResemble, files)

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

			So(d.AddSetFiles(setA, slices.Values(files[3:])), ShouldBeNil)
			So(slices.Collect(d.GetSetFiles(setA).Iter), ShouldResemble, files)

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

			So(len(slices.Collect(d.listRemoteFiles(t).Iter)), ShouldEqual, 3)
			So(len(slices.Collect(d.listInodes(t).Iter)), ShouldEqual, 3)

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

				trashed := slices.Collect(d.GetSetFiles(setTrashA).Iter)
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

				So(len(slices.Collect(d.listRemoteFiles(t).Iter)), ShouldEqual, 3)
				So(len(slices.Collect(d.listInodes(t).Iter)), ShouldEqual, 3)

				So(d.RemoveSetFiles(slices.Values(files[2:3])), ShouldBeNil)
				So(d.clearQueue(), ShouldBeNil)
				So(d.RemoveSetFiles(d.GetSetFiles(setTrashA).Iter), ShouldBeNil)
				So(d.clearQueue(), ShouldBeNil)

				So(len(slices.Collect(d.listRemoteFiles(t).Iter)), ShouldEqual, 2)
				So(len(slices.Collect(d.listInodes(t).Iter)), ShouldEqual, 2)
			})
		})

		Convey("Files that failed to upload/remove have the appropriate data set", func() {
			So(d.AddSetFiles(setA, slices.Values(files)), ShouldBeNil)

			process, err := d.RegisterProcess()
			So(err, ShouldBeNil)

			tasks := slices.Collect(d.ReserveTasks(process, 1).Iter)
			tasks[0].Error = "some error"
			now := time.Now().Truncate(time.Second)

			So(d.TaskFailed(tasks[0]), ShouldBeNil)

			files := slices.Collect(d.GetSetFiles(setA).Iter)
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
