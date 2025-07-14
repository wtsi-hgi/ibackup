package db

import (
	"slices"
	"testing"

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

		Convey("You can add and retrieve files in that set", func() {
			var err error

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
				},
			}

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
			So(refs, ShouldEqual, 1)
		})
	})
}
