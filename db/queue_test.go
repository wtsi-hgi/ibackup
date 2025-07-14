package db

import (
	"fmt"
	"iter"
	"slices"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestQueue(t *testing.T) {
	Convey("With a database with sets and files", t, func() {
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

		pidA, err := d.RegisterProcess()
		So(err, ShouldBeNil)

		pidB, err := d.RegisterProcess()
		So(err, ShouldBeNil)

		Convey("A client can reserve queued items and set the response", func() {
			So(d.AddSetFiles(setA, genFiles(5)), ShouldBeNil)
			So(d.AddSetFiles(setB, genFiles(10)), ShouldBeNil)

			tasks := d.ReserveTasks(pidA, 3)
			So(tasks.Error, ShouldBeNil)
			So(slices.Collect(tasks.Iter), ShouldResemble, []*Task{
				{
					id:         1,
					LocalPath:  "/some/file/1_0",
					RemotePath: "/remote/file/1_0",
					UploadPath: "/remote/file/1_0",
					Type:       QueueUpload,
				},
				{
					id:         2,
					LocalPath:  "/some/file/1_1",
					RemotePath: "/remote/file/1_1",
					UploadPath: "/remote/file/1_1",
					Type:       QueueUpload,
				},
				{
					id:         3,
					LocalPath:  "/some/file/1_2",
					RemotePath: "/remote/file/1_2",
					UploadPath: "/remote/file/1_1",
					Type:       QueueUpload,
				},
			})

			tasks = d.ReserveTasks(pidB, 5)
			So(tasks.Error, ShouldBeNil)
			So(slices.Collect(tasks.Iter), ShouldResemble, []*Task{
				{
					id:         4,
					LocalPath:  "/some/file/1_3",
					RemotePath: "/remote/file/1_3",
					UploadPath: "/remote/file/1_3",
					Type:       QueueUpload,
				},
				{
					id:         5,
					LocalPath:  "/some/file/1_4",
					RemotePath: "/remote/file/1_4",
					UploadPath: "/remote/file/1_4",
					Type:       QueueUpload,
				},
				{
					id:         6,
					LocalPath:  "/some/file/2_0",
					RemotePath: "/remote/file/2_0",
					UploadPath: "/remote/file/2_0",
					Type:       QueueUpload,
				},
				{
					id:         7,
					LocalPath:  "/some/file/2_1",
					RemotePath: "/remote/file/2_1",
					UploadPath: "/remote/file/2_1",
					Type:       QueueUpload,
				},
				{
					id:         8,
					LocalPath:  "/some/file/2_2",
					RemotePath: "/remote/file/2_2",
					UploadPath: "/remote/file/2_1",
					Type:       QueueUpload,
				},
			})
		})

		Convey("A client can release their jobs", func() {
			So(d.AddSetFiles(setA, genFiles(2)), ShouldBeNil)

			tasks := slices.Collect(d.ReserveTasks(pidA, 3).Iter)
			So(len(tasks), ShouldEqual, 2)
			So(tasks[0].id, ShouldEqual, 1)
			So(tasks[1].id, ShouldEqual, 2)

			So(d.ReleaseTasks(pidA), ShouldBeNil)

			tasks = slices.Collect(d.ReserveTasks(pidA, 3).Iter)
			So(len(tasks), ShouldEqual, 2)
			So(tasks[0].id, ShouldEqual, 1)
			So(tasks[1].id, ShouldEqual, 2)
		})

		Convey("A client can complete their jobs", func() {
			So(d.AddSetFiles(setA, genFiles(2)), ShouldBeNil)

			tasks := slices.Collect(d.ReserveTasks(pidA, 2).Iter)
			So(len(tasks), ShouldEqual, 2)
			So(tasks[0].id, ShouldEqual, 1)
			So(tasks[1].id, ShouldEqual, 2)

			So(d.TaskComplete(tasks[0]), ShouldBeNil)
			So(d.ReleaseTasks(pidA), ShouldBeNil)

			tasks = slices.Collect(d.ReserveTasks(pidA, 2).Iter)
			So(len(tasks), ShouldEqual, 1)
			So(tasks[0].id, ShouldEqual, 2)
		})

		Convey("A client can fail their jobs", func() {
			So(d.AddSetFiles(setA, genFiles(2)), ShouldBeNil)

			tasks := slices.Collect(d.ReserveTasks(pidA, 2).Iter)
			So(len(tasks), ShouldEqual, 2)
			So(tasks[0].id, ShouldEqual, 1)
			So(tasks[1].id, ShouldEqual, 2)

			tasks[0].Error = "Some Error"

			So(d.TaskFailed(tasks[0]), ShouldBeNil)
			So(d.ReleaseTasks(pidA), ShouldBeNil)

			tasks = slices.Collect(d.ReserveTasks(pidA, 1).Iter)
			So(len(tasks), ShouldEqual, 1)
			So(tasks[0].id, ShouldEqual, 2)
		})
	})
}

var filePrefix int //nolint:gochecknoglobals

func genFiles(n int) iter.Seq[*File] {
	filePrefix++

	return func(yield func(*File) bool) {
		for i := range n {
			inode := int64(filePrefix*1000 + i)

			if i == 2 {
				inode = int64(filePrefix*1000 + i - 1)
			}

			if !yield(&File{
				LocalPath:   fmt.Sprintf("/some/file/%d_%d", filePrefix, i),
				RemotePath:  fmt.Sprintf("/remote/file/%d_%d", filePrefix, i),
				Size:        100,
				Inode:       inode,
				MountPount:  "/some/",
				Btime:       100,
				Mtime:       200,
				Type:        Regular,
				SymlinkDest: "",
			}) {
				break
			}
		}
	}
}

func (d *DB) clearQueue() error {
	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	if _, err := tx.Exec(deleteAllQueued); err != nil {
		return err
	}

	return tx.Commit()
}
