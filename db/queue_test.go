package db

import (
	"fmt"
	"iter"
	"slices"
	"testing"
	"time"

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
					process:    1,
					LocalPath:  "/some/file/1_0",
					RemotePath: "/remote/file/1_0",
					UploadPath: "/remote/file/1_0",
					Type:       QueueUpload,
				},
				{
					id:         2,
					process:    1,
					LocalPath:  "/some/file/1_1",
					RemotePath: "/remote/file/1_1",
					UploadPath: "/remote/file/1_1",
					Type:       QueueUpload,
				},
				{
					id:         4,
					process:    1,
					LocalPath:  "/some/file/1_3",
					RemotePath: "/remote/file/1_3",
					UploadPath: "/remote/file/1_3",
					Type:       QueueUpload,
				},
			})

			tasks = d.ReserveTasks(pidB, 5)
			So(tasks.Error, ShouldBeNil)
			So(slices.Collect(tasks.Iter), ShouldResemble, []*Task{
				{
					id:         5,
					process:    2,
					LocalPath:  "/some/file/1_4",
					RemotePath: "/remote/file/1_4",
					UploadPath: "/remote/file/1_4",
					Type:       QueueUpload,
				},
				{
					id:         6,
					process:    2,
					LocalPath:  "/some/file/2_0",
					RemotePath: "/remote/file/2_0",
					UploadPath: "/remote/file/2_0",
					Type:       QueueUpload,
				},
				{
					id:         7,
					process:    2,
					LocalPath:  "/some/file/2_1",
					RemotePath: "/remote/file/2_1",
					UploadPath: "/remote/file/2_1",
					Type:       QueueUpload,
				},
				{
					id:         9,
					process:    2,
					LocalPath:  "/some/file/2_3",
					RemotePath: "/remote/file/2_3",
					UploadPath: "/remote/file/2_3",
					Type:       QueueUpload,
				},
				{
					id:         10,
					process:    2,
					LocalPath:  "/some/file/2_4",
					RemotePath: "/remote/file/2_4",
					UploadPath: "/remote/file/2_4",
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

		Convey("A task for a remote file already being operated on cannot be reserved", func() {
			files := slices.Collect(genFiles(1))

			So(d.AddSetFiles(setA, slices.Values(files)), ShouldBeNil)
			So(d.AddSetFiles(setB, slices.Values(files)), ShouldBeNil)

			tasks := slices.Collect(d.ReserveTasks(pidA, 2).Iter)
			So(len(tasks), ShouldEqual, 1)
			So(tasks[0].id, ShouldEqual, 1)

			tasksB := slices.Collect(d.ReserveTasks(pidB, 2).Iter)
			So(len(tasksB), ShouldEqual, 0)

			So(d.TaskComplete(tasks[0]), ShouldBeNil)

			tasks = slices.Collect(d.ReserveTasks(pidB, 2).Iter)
			So(len(tasks), ShouldEqual, 1)
			So(tasks[0].id, ShouldEqual, 2)
		})

		Convey("Completing an upload tasks sets the upload date on the file", func() {
			files := slices.Collect(genFiles(1))

			So(d.AddSetFiles(setA, slices.Values(files)), ShouldBeNil)

			now := time.Now().Truncate(time.Second)
			setFiles := slices.Collect(d.GetSetFiles(setA).Iter)
			So(len(setFiles), ShouldEqual, 1)
			So(setFiles[0].LastUpload, ShouldHappenBefore, now)

			So(d.TaskComplete(slices.Collect(d.ReserveTasks(pidA, 1).Iter)[0]), ShouldBeNil)

			setFiles = slices.Collect(d.GetSetFiles(setA).Iter)
			So(len(setFiles), ShouldEqual, 1)
			So(setFiles[0].LastUpload, ShouldHappenOnOrAfter, now)
		})

		Convey("Re-inserting a file into a set causes a re-upload", func() {
			files := slices.Collect(genFiles(1))

			So(d.AddSetFiles(setA, slices.Values(files)), ShouldBeNil)

			setFiles := slices.Collect(d.GetSetFiles(setA).Iter)
			So(len(setFiles), ShouldEqual, 1)

			tasks := slices.Collect(d.ReserveTasks(pidA, 1).Iter)
			So(len(tasks), ShouldEqual, 1)
			So(d.TaskComplete(tasks[0]), ShouldBeNil)

			tasks = slices.Collect(d.ReserveTasks(pidA, 1).Iter)
			So(len(tasks), ShouldEqual, 0)

			So(d.AddSetFiles(setA, slices.Values(files)), ShouldBeNil)

			setFiles = slices.Collect(d.GetSetFiles(setA).Iter)
			So(len(setFiles), ShouldEqual, 1)

			tasks = slices.Collect(d.ReserveTasks(pidA, 1).Iter)
			So(len(tasks), ShouldEqual, 1)
		})

		Convey("Completing an removal task removes the file from the set", func() {
			files := slices.Collect(genFiles(1))

			So(d.AddSetFiles(setA, slices.Values(files)), ShouldBeNil)

			setFiles := slices.Collect(d.GetSetFiles(setA).Iter)
			So(len(setFiles), ShouldEqual, 1)

			tasks := slices.Collect(d.ReserveTasks(pidA, 1).Iter)
			So(len(tasks), ShouldEqual, 1)

			uploadTask := tasks[0]

			So(d.RemoveSetFiles(slices.Values(setFiles)), ShouldBeNil)

			tasks = slices.Collect(d.ReserveTasks(pidB, 1).Iter)
			So(len(tasks), ShouldEqual, 0)

			now := time.Now()

			So(d.TaskComplete(uploadTask), ShouldBeNil)

			setFiles = slices.Collect(d.GetSetFiles(setA).Iter)
			So(len(setFiles), ShouldEqual, 1)
			So(setFiles[0].LastUpload, ShouldHappenBefore, now)

			tasks = slices.Collect(d.ReserveTasks(pidA, 1).Iter)
			So(len(tasks), ShouldEqual, 1)

			So(d.TaskComplete(tasks[0]), ShouldBeNil)

			setFiles = slices.Collect(d.GetSetFiles(setA).Iter)
			So(len(setFiles), ShouldEqual, 0)
		})

		Convey("Skipping an upload, marks the local file as skipped", func() {
			files := slices.Collect(genFiles(1))

			So(d.AddSetFiles(setA, slices.Values(files)), ShouldBeNil)
			So(slices.Collect(d.GetSetFiles(setA).Iter), ShouldResemble, files)

			tasks := slices.Collect(d.ReserveTasks(pidA, 2).Iter)
			So(len(tasks), ShouldEqual, 1)

			So(d.TaskSkipped(tasks[0]), ShouldBeNil)

			files[0].Status = StatusSkipped

			So(slices.Collect(d.GetSetFiles(setA).Iter), ShouldResemble, files)

			tasks = slices.Collect(d.ReserveTasks(pidA, 2).Iter)
			So(len(tasks), ShouldEqual, 0)
		})

		Convey("Missing files are not uploaded", func() {
			files := slices.Collect(genFiles(1))

			files[0].Status = StatusMissing

			So(d.AddSetFiles(setA, slices.Values(files)), ShouldBeNil)
			So(slices.Collect(d.GetSetFiles(setA).Iter), ShouldResemble, files)
			So(len(slices.Collect(d.ReserveTasks(pidA, 2).Iter)), ShouldEqual, 0)

			Convey("Orphaned files count as missing when added to a set", func() {
				files[0].Status = StatusOrphaned
				So(d.AddSetFiles(setA, slices.Values(files)), ShouldBeNil)

				files[0].Status = StatusMissing

				So(slices.Collect(d.GetSetFiles(setA).Iter), ShouldResemble, files)
				So(len(slices.Collect(d.ReserveTasks(pidA, 2).Iter)), ShouldEqual, 0)
			})

			Convey("Files that were missing, but are found are uploaded", func() {
				files[0].Status = StatusNone

				So(d.AddSetFiles(setA, slices.Values(files)), ShouldBeNil)
				So(slices.Collect(d.GetSetFiles(setA).Iter), ShouldResemble, files)

				tasks := slices.Collect(d.ReserveTasks(pidA, 2).Iter)
				So(len(tasks), ShouldEqual, 1)
			})
		})

		Convey("Uploading a file marks it as uploaded", func() {
			files := slices.Collect(genFiles(1))

			So(d.AddSetFiles(setA, slices.Values(files)), ShouldBeNil)
			So(d.clearQueue(), ShouldBeNil)

			files[0].Status = StatusUploaded

			uploaded := slices.Collect(d.GetSetFiles(setA).Iter)
			So(len(uploaded), ShouldEqual, 1)
			So(uploaded[0].LastUpload, ShouldNotBeZeroValue)

			files[0].LastUpload = uploaded[0].LastUpload

			So(uploaded, ShouldResemble, files)

			Convey("Re-uploading a file marks it as replaced", func() {
				files[0].Status = StatusNone

				So(d.AddSetFiles(setA, slices.Values(files)), ShouldBeNil)
				So(d.clearQueue(), ShouldBeNil)

				files[0].Status = StatusReplaced

				uploaded = slices.Collect(d.GetSetFiles(setA).Iter)
				So(len(uploaded), ShouldEqual, 1)
				So(uploaded[0].LastUpload, ShouldNotBeZeroValue)

				files[0].LastUpload = uploaded[0].LastUpload

				So(uploaded, ShouldResemble, files)
			})
		})

		Convey("Uploading a file then deleting it marks it as orphaned", func() {
			files := slices.Collect(genFiles(1))

			So(d.AddSetFiles(setA, slices.Values(files)), ShouldBeNil)
			So(d.clearQueue(), ShouldBeNil)

			files[0].Status = StatusMissing

			So(d.AddSetFiles(setA, slices.Values(files)), ShouldBeNil)

			uploadedFiles := slices.Collect(d.GetSetFiles(setA).Iter)
			So(len(uploadedFiles), ShouldEqual, 1)
			So(uploadedFiles[0].LastUpload, ShouldNotBeZeroValue)

			files[0].Status = StatusOrphaned
			files[0].LastUpload = uploadedFiles[0].LastUpload

			So(slices.Collect(d.GetSetFiles(setA).Iter), ShouldResemble, files)
		})

		Convey("Tasks can only be failed "+string('0'+maxRetries)+" times", func() {
			files := slices.Collect(genFiles(1))

			So(d.AddSetFiles(setA, slices.Values(files)), ShouldBeNil)

			tasks := slices.Collect(d.ReserveTasks(pidA, 2).Iter)

			for range maxRetries {
				So(len(tasks), ShouldEqual, 1)

				tasks[0].Error = "some error"

				So(d.TaskFailed(tasks[0]), ShouldBeNil)

				tasks = slices.Collect(d.ReserveTasks(pidA, 2).Iter)
			}

			So(len(tasks), ShouldEqual, 0)

			Convey("Retrying a Sets tasks allows them to be reserved again", func() {
				So(d.RetrySetTasks(setA), ShouldBeNil)

				tasks := slices.Collect(d.ReserveTasks(pidA, 2).Iter)
				So(len(tasks), ShouldEqual, 1)
			})
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
				modifiable:  true,
			}) { //nolint:whitespace
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
