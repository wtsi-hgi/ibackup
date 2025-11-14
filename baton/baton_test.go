/*******************************************************************************
 * Copyright (c) 2025 Genome Research Ltd.
 *
 * Author: Rosie Kern <rk18@sanger.ac.uk>
 * Author: Iaroslav Popov <ip13@sanger.ac.uk>
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

package baton

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/ibackup/baton/meta"
	"github.com/wtsi-hgi/ibackup/internal"
)

var testStartTime time.Time //nolint:gochecknoglobals

func TestBaton(t *testing.T) {
	testStartTime = time.Now()

	h, errgbh := GetBatonHandler()
	if errgbh != nil {
		t.Logf("GetBatonHandler error: %s", errgbh)
		SkipConvey("Skipping baton tests since couldn't find baton", t, func() {})

		return
	}

	remotePath := os.Getenv("IBACKUP_TEST_COLLECTION")
	if remotePath == "" {
		SkipConvey("Skipping baton tests since IBACKUP_TEST_COLLECTION is not defined", t, func() {})

		return
	}

	resetIRODS(remotePath)

	localPath := t.TempDir()

	Convey("Given a baton handler", t, func() {
		meta := map[string]string{
			"ibackup:test:a": "1",
			"ibackup:test:b": "2",
		}

		Convey("And a local file", func() {
			file1local := filepath.Join(localPath, "file1")
			file1remote := filepath.Join(remotePath, "file1")

			internal.CreateTestFileOfLength(t, file1local, 1)

			Convey("You can stat a file not in iRODS", func() {
				exists, _, errs := h.Stat(file1remote)
				So(errs, ShouldBeNil)
				So(exists, ShouldBeFalse)
			})

			Convey("You can try to remove the file from iRODS and receive an error", func() {
				err := h.RemoveFile(file1remote)
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldContainSubstring, "does not exist")
			})

			Convey("You can put the file in iRODS", func() {
				err := h.Put(file1local, file1remote)
				So(err, ShouldBeNil)

				So(isObjectInIRODS(remotePath, "file1"), ShouldBeTrue)

				Convey("And putting a new file in the same location will overwrite existing object", func() {
					file2local := filepath.Join(localPath, "file2")
					sizeOfFile2 := 10

					internal.CreateTestFileOfLength(t, file2local, sizeOfFile2)

					err = h.Put(file2local, file1remote)
					So(err, ShouldBeNil)

					So(getSizeOfObject(file1remote), ShouldEqual, sizeOfFile2)
				})

				Convey("And then you can add metadata to it", func() {
					errm := h.AddMeta(file1remote, meta)
					So(errm, ShouldBeNil)

					So(getRemoteMeta(file1remote), ShouldContainSubstring, "ibackup:test:a")
					So(getRemoteMeta(file1remote), ShouldContainSubstring, "ibackup:test:b")
				})

				Convey("And given metadata on the file in iRODS", func() {
					for k, v := range meta {
						addRemoteMeta(file1remote, k, v)
					}

					Convey("You can get the metadata from a file in iRODS", func() {
						fileMeta, errm := h.GetMeta(file1remote)
						So(errm, ShouldBeNil)

						compareMetasWithSize(t, fileMeta, meta, 1)

						Convey("And you can close the put and meta clients", func() {
							h.Cleanup()

							So(h.AllClientsStopped(), ShouldBeTrue)
						})
					})

					Convey("You can stat a file and get its metadata", func() {
						exists, fileMeta, errm := h.Stat(file1remote)
						So(errm, ShouldBeNil)
						So(exists, ShouldBeTrue)
						compareMetasWithSize(t, fileMeta, meta, 0)
					})

					Convey("You can query if files contain specific metadata", func() {
						files, errq := h.QueryMeta(remotePath, map[string]string{"ibackup:test:a": "1"})
						So(errq, ShouldBeNil)

						So(len(files), ShouldEqual, 1)
						So(files, ShouldContain, file1remote)

						files, errq = h.QueryMeta(remotePath, map[string]string{"ibackup:test:a": "2"})
						So(errq, ShouldBeNil)

						So(len(files), ShouldEqual, 0)
					})

					Convey("You can remove specific metadata from a file in iRODS", func() {
						errm := h.RemoveMeta(file1remote, map[string]string{"ibackup:test:a": "1"})
						So(errm, ShouldBeNil)

						fileMeta, errm := h.GetMeta(file1remote)
						So(errm, ShouldBeNil)

						compareMetasWithSize(t, fileMeta, map[string]string{"ibackup:test:b": "2"}, 1)
					})
				})

				Convey("And then you can remove it from iRODS", func() {
					err = h.RemoveFile(file1remote)
					So(err, ShouldBeNil)

					So(isObjectInIRODS(remotePath, "file1"), ShouldBeFalse)

					Convey("And you can close the put and remove clients", func() {
						h.Cleanup()

						So(h.AllClientsStopped(), ShouldBeTrue)
					})
				})
			})
		})

		Convey("You can open collection clients and put an empty dir in iRODS", func() {
			dir1remote := filepath.Join(remotePath, "dir1")
			err := h.EnsureCollection(dir1remote)
			So(err, ShouldBeNil)

			So(h.collPool.IsOpen(), ShouldBeTrue)

			for _, client := range h.collClients {
				So(client.IsRunning(), ShouldBeTrue)
			}

			So(isObjectInIRODS(remotePath, "dir1"), ShouldBeTrue)

			Convey("Then you can close the collection clients", func() {
				err = h.CollectionsDone()
				So(err, ShouldBeNil)

				So(h.collPool.IsOpen(), ShouldBeFalse)
				So(len(h.collClients), ShouldEqual, 0)

				Convey("And then you can remove the dir from iRODS", func() {
					err = h.RemoveDir(dir1remote)
					So(err, ShouldBeNil)

					So(isObjectInIRODS(remotePath, "dir1"), ShouldBeFalse)

					Convey("And you can close the remove and collections clients", func() {
						h.Cleanup()

						So(h.AllClientsStopped(), ShouldBeTrue)
					})
				})
			})
		})
	})
}

func resetIRODS(remotePath string) {
	if remotePath == "" {
		return
	}

	exec.Command("irm", "-r", remotePath).Run() //nolint:errcheck,noctx

	exec.Command("imkdir", remotePath).Run() //nolint:errcheck,noctx
}

func isObjectInIRODS(remotePath, name string) bool {
	output, err := exec.Command("ils", remotePath).CombinedOutput() //nolint:noctx
	So(err, ShouldBeNil)

	return strings.Contains(string(output), name)
}

func getRemoteMeta(path string) string {
	output, err := exec.Command("imeta", "ls", "-d", path).CombinedOutput() //nolint:noctx
	So(err, ShouldBeNil)

	return string(output)
}

func addRemoteMeta(path, key, val string) {
	output, err := exec.Command("imeta", "add", "-d", path, key, val).CombinedOutput() //nolint:noctx
	if strings.Contains(string(output), "CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME") {
		return
	}

	So(err, ShouldBeNil)
}

func getSizeOfObject(path string) int {
	output, err := exec.Command("ils", "-l", path).CombinedOutput() //nolint:noctx
	So(err, ShouldBeNil)

	cols := strings.Fields(string(output))
	size, err := strconv.Atoi(cols[3])
	So(err, ShouldBeNil)

	return size
}

func compareMetasWithSize(t *testing.T, remote, expected map[string]string, size int64) {
	t.Helper()

	now := time.Now()

	var mtime, ctime time.Time

	So(ctime.UnmarshalText([]byte(remote[meta.MetaKeyRemoteCtime])), ShouldBeNil)
	So(mtime.UnmarshalText([]byte(remote[meta.MetaKeyRemoteMtime])), ShouldBeNil)
	So(mtime, ShouldHappenBetween, testStartTime, now)
	So(ctime, ShouldHappenBetween, testStartTime, now)

	delete(remote, meta.MetaKeyRemoteCtime)
	delete(remote, meta.MetaKeyRemoteMtime)

	expected[meta.MetaKeyRemoteSize] = strconv.FormatInt(size, 10)

	So(remote, ShouldResemble, expected)
}
