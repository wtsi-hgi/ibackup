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

package tasks

import (
	"bytes"
	"cmp"
	"encoding/json"
	"io/fs"
	"os"
	"os/exec"
	"os/user"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/ibackup/baton"
	"github.com/wtsi-hgi/ibackup/db"
	"github.com/wtsi-hgi/ibackup/discovery"
	"github.com/wtsi-hgi/ibackup/internal/testdb"
	"github.com/wtsi-hgi/ibackup/internal/testirods"
	"github.com/wtsi-npg/extendo/v2"
	"golang.org/x/sys/unix"
)

func TestTasks(t *testing.T) {
	Convey("You can handle upload tasks", t, func() {
		So(testirods.AddPseudoIRODsToolsToPathIfRequired(t), ShouldBeNil)

		irodsPath := os.Getenv("IBACKUP_TEST_COLLECTION")

		cleanupIRODsTesting()

		currentUser, err := user.Current()
		So(err, ShouldBeNil)

		group, err := user.LookupGroupId(currentUser.Gid)
		So(err, ShouldBeNil)

		d := testdb.CreateTestDatabase(t)

		h, err := New(d, path.Join(irodsPath, "hardlinks"))
		So(err, ShouldBeNil)

		tasks, err := h.GetTasks(1)
		So(err, ShouldBeNil)
		So(len(tasks), ShouldBeZeroValue)

		setADir := t.TempDir()

		transformer, err := db.NewTransformer("prefix", "^"+setADir, path.Join(irodsPath, "files"))
		So(err, ShouldBeNil)

		setA := &db.Set{
			Name:        "mySet",
			Requester:   "me",
			Transformer: transformer,
			Description: "my first set",
		}

		startCreate := time.Now().Truncate(time.Second)

		for i := range 5 {
			So(os.WriteFile(
				filepath.Join(setADir, string('A'+byte(i))+"File"),
				bytes.Repeat([]byte{'A' + byte(i)}, i+1),
				0600),
				ShouldBeNil,
			)
		}

		endCreate := time.Now()

		So(d.CreateSet(setA), ShouldBeNil)
		So(d.AddSetDiscovery(setA, &db.Discover{
			Path: setADir,
			Type: db.DiscoverDirectory,
		}), ShouldBeNil)
		So(discovery.Discover(d, setA, nil), ShouldBeNil)

		setA, err = d.GetSet(setA.Name, setA.Requester)
		So(err, ShouldBeNil)
		So(setA.NumFiles, ShouldEqual, 5)
		So(setA.Uploaded, ShouldEqual, 0)

		tasks, err = h.GetTasks(5)
		So(err, ShouldBeNil)
		So(len(tasks), ShouldEqual, 5)

		startUpload := time.Now().Truncate(time.Second)

		for _, task := range tasks {
			So(h.HandleTask(task), ShouldBeNil)
			So(task.Error, ShouldBeBlank)
		}

		endUpload := time.Now()

		var uploadedMTimes []time.Time

		for i := range 5 {
			var stat *baton.Stat

			stat, err = h.handler.StatWithMeta(path.Join(irodsPath, "files", string('A'+byte(i))+"File"))
			So(err, ShouldBeNil)
			So(stat.Size, ShouldBeZeroValue)

			slices.SortFunc(stat.Metadata, sortAVUs)

			So(len(stat.Metadata), ShouldEqual, 9)
			So(stat.Metadata[0].Attr, ShouldEqual, MetaKeyDate)
			So(stat.Metadata[2].Attr, ShouldEqual, MetaKeyMtime)

			mtimeStr := stat.Metadata[2].Value

			var uploadDate, mtime time.Time

			So(json.Unmarshal([]byte("\""+stat.Metadata[0].Value+"\""), &uploadDate), ShouldBeNil)
			So(json.Unmarshal([]byte("\""+mtimeStr+"\""), &mtime), ShouldBeNil)
			So(uploadDate, ShouldHappenOnOrBetween, startUpload, endUpload)
			So(mtime, ShouldHappenOnOrBetween, startCreate, endCreate)

			So(stat.Metadata, ShouldResemble, []extendo.AVU{
				{Attr: MetaKeyDate, Value: stat.Metadata[0].Value},
				{Attr: MetaKeyGroup, Value: group.Name},
				{Attr: MetaKeyMtime, Value: stat.Metadata[2].Value},
				{Attr: MetaKeyOwner, Value: currentUser.Username},
				{Attr: MetaKeyReason, Value: "", Units: "me mySet"},
				{
					Attr:  MetaKeyRemoteHardlink,
					Value: hardlinkPath(t, irodsPath, filepath.Join(setADir, string('A'+byte(i))+"File")),
				},
				{Attr: MetaKeyRemoval, Value: "0001-01-01T00:00:00Z", Units: "me mySet"},
				{Attr: MetaKeyReview, Value: "0001-01-01T00:00:00Z", Units: "me mySet"},
				{Attr: MetaKeySet, Value: "me", Units: "mySet"},
			})

			stat, err = h.handler.StatWithMeta(hardlinkPath(t, irodsPath, filepath.Join(setADir, string('A'+byte(i))+"File")))
			So(err, ShouldBeNil)
			So(stat.Size, ShouldEqual, i+1)

			slices.SortFunc(stat.Metadata, sortAVUs)

			So(stat.Metadata, ShouldResemble, []extendo.AVU{
				{Attr: MetaKeyHardlink, Value: path.Join(irodsPath, "files", string('A'+byte(i))+"File")},
				{Attr: MetaKeyMtime, Value: mtimeStr},
			})

			uploadedMTimes = append(uploadedMTimes, stat.MTime)
		}

		setA, err = d.GetSet(setA.Name, setA.Requester)
		So(err, ShouldBeNil)
		So(setA.Uploaded, ShouldEqual, 5)
		So(setA.NumFiles, ShouldEqual, 5)
		So(setA.SizeTotal, ShouldEqual, 15)

		Convey("Unchanged files shouldn't be re-uploaded", func() {
			So(discovery.Discover(d, setA, nil), ShouldBeNil)

			setA, err = d.GetSet(setA.Name, setA.Requester)
			So(err, ShouldBeNil)
			So(setA.NumFiles, ShouldEqual, 5)
			So(setA.Uploaded, ShouldEqual, 0)

			tasks, err = h.GetTasks(5)
			So(err, ShouldBeNil)
			So(len(tasks), ShouldEqual, 5)

			time.Sleep(time.Second) // To make sure that re-uploads cause a mtime change

			for _, task := range tasks {
				So(h.HandleTask(task), ShouldBeNil)
				So(task.Error, ShouldBeBlank)
			}

			for i := range 5 {
				var stat *baton.Stat

				stat, err = h.handler.StatWithMeta(hardlinkPath(t, irodsPath, filepath.Join(setADir, string('A'+byte(i))+"File")))
				So(err, ShouldBeNil)
				So(stat.MTime, ShouldEqual, uploadedMTimes[i])
				So(stat.Size, ShouldEqual, i+1)
			}
		})

		Convey("You can do remove tasks", func() {
			inodePath := func(file *db.File) string {
				return path.Join(irodsPath, "hardlinks", strconv.FormatUint(file.Inode, 10), strconv.FormatInt(file.Btime, 10))
			}

			var keepFile, removeFile *db.File

			So(d.GetSetFiles(setA).ForEach(func(f *db.File) error {
				if f.Size == 1 {
					keepFile = f
				} else if f.Size == 5 {
					removeFile = f
				}

				return nil
			}), ShouldBeNil)

			So(d.RemoveSetFiles(setA, slices.Values([]*db.File{removeFile})), ShouldBeNil)

			doTasks := func() {
				for {
					tasks, err := h.GetTasks(1)
					So(err, ShouldBeNil)

					if len(tasks) == 0 {
						break
					}

					So(h.HandleTask(tasks[0]), ShouldBeNil)
				}
			}

			doTasks()

			_, err := h.handler.StatWithMeta(keepFile.RemotePath)
			So(err, ShouldBeNil)

			stat, err := h.handler.StatWithMeta(inodePath(keepFile))
			So(err, ShouldBeNil)
			So(stat.Size, ShouldEqual, 1)

			stat, err = h.handler.StatWithMeta(removeFile.RemotePath)
			So(err, ShouldBeNil)

			So(matchAVU(stat.Metadata, extendo.AVU{
				Attr:  MetaKeySet,
				Value: setA.Requester,
				Units: setA.Name,
			}).Attr, ShouldBeBlank)

			stat, err = h.handler.StatWithMeta(inodePath(removeFile))
			So(err, ShouldBeNil)
			So(stat.Size, ShouldEqual, 5)

			trashSetA, err := d.GetTrashSet(setA.Name, setA.Requester)
			So(err, ShouldBeNil)

			So(d.RemoveSetFiles(trashSetA, d.GetSetFiles(trashSetA).Iter), ShouldBeNil)

			doTasks()

			_, err = h.handler.StatWithMeta(removeFile.RemotePath)
			So(err, ShouldEqual, fs.ErrNotExist)

			_, err = h.handler.StatWithMeta(inodePath(removeFile))
			So(err, ShouldEqual, fs.ErrNotExist)
		})
	})
}

func cleanupIRODsTesting() {
	So(exec.Command("irm", "-r", os.Getenv("IBACKUP_TEST_COLLECTION")).Run(), ShouldBeNil) //nolint:gosec
}

func hardlinkPath(t *testing.T, irodsPath, local string) string {
	t.Helper()

	var stat unix.Statx_t

	So(
		unix.Statx(0,
			local,
			unix.AT_SYMLINK_NOFOLLOW|unix.AT_STATX_SYNC_AS_STAT,
			unix.STATX_BTIME|unix.STATX_INO,
			&stat,
		),
		ShouldBeNil,
	)

	return filepath.Join(irodsPath, "hardlinks", strconv.FormatUint(stat.Ino, 10), strconv.FormatInt(stat.Btime.Sec, 10))
}

func sortAVUs(a, b extendo.AVU) int {
	return cmp.Or(strings.Compare(a.Attr, b.Attr), strings.Compare(a.Value, b.Value), strings.Compare(a.Units, b.Units))
}
