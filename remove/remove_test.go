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

package remove

import (
	"os"
	"path/filepath"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/ibackup/internal"
	"github.com/wtsi-hgi/ibackup/transfer"
)

const userPerms = 0700

func TestRemoveMock(t *testing.T) {
	Convey("Given a connected local handler", t, func() {
		lh := internal.GetLocalHandler()

		sourceDir := t.TempDir()
		destDir := t.TempDir()

		transformer := transfer.PrefixTransformer(sourceDir, destDir)

		var err error

		Convey("Given an uploaded empty dir", func() {
			dir1local := filepath.Join(sourceDir, "dir")
			dir1remote := filepath.Join(destDir, "dir")

			err = os.MkdirAll(dir1remote, userPerms)
			So(err, ShouldBeNil)

			Convey("You can remove a remote folder using the local path", func() {
				err = RemoveRemoteDir(lh, dir1local, transformer)
				So(err, ShouldBeNil)

				_, err = os.Stat(dir1remote)
				So(err.Error(), ShouldContainSubstring, "no such file or directory")
			})

			Convey("And a nested file", func() {
				file1remote := filepath.Join(dir1remote, "file1")
				internal.CreateTestFileOfLength(t, file1remote, 1)

				Convey("Removing the file will remove the dir", func() {
					err = RemoveFileAndParentFoldersIfEmpty(lh, file1remote)

					_, err = os.Stat(file1remote)
					So(err.Error(), ShouldContainSubstring, "no such file or directory")

					_, err = os.Stat(dir1remote)
					So(err.Error(), ShouldContainSubstring, "no such file or directory")
				})

				Convey("And a second nested file", func() {
					file2remote := filepath.Join(dir1remote, "file2")
					internal.CreateTestFileOfLength(t, file2remote, 1)

					Convey("Removing one file will not remove the dir", func() {
						err = RemoveFileAndParentFoldersIfEmpty(lh, file1remote)

						_, err = os.Stat(file1remote)
						So(err.Error(), ShouldContainSubstring, "no such file or directory")

						_, err = os.Stat(dir1remote)
						So(err, ShouldBeNil)
					})
				})
			})
		})

		Convey("Given a file in two sets", func() {
			file1remote := filepath.Join(destDir, "file1")

			internal.CreateTestFileOfLength(t, file1remote, 1)

			meta := map[string]string{
				transfer.MetaKeyRequester: "testUser1,testUser2",
				transfer.MetaKeySets:      "set1,set2",
			}

			err = lh.AddMeta(file1remote, meta)
			So(err, ShouldBeNil)

			Convey("You can update the metadata for sets and requesters on the remote file", func() {
				err = UpdateSetsAndRequestersOnRemoteFile(lh, file1remote, []string{"set2"}, []string{"testUser2"}, meta)
				So(err, ShouldBeNil)

				fileMeta, errg := lh.GetMeta(file1remote)
				So(errg, ShouldBeNil)

				So(fileMeta, ShouldResemble,
					map[string]string{
						transfer.MetaKeyRequester: "testUser2",
						transfer.MetaKeySets:      "set2",
					})
			})
		})
	})
}
