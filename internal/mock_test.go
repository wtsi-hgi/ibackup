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

package internal

import (
	"os"
	"path/filepath"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestRemoveMock(t *testing.T) {
	Convey("Given a mock RemoveHandler", t, func() {
		lh := GetLocalHandler()

		err := lh.InitClients()
		So(err, ShouldBeNil)

		So(lh.Connected, ShouldBeTrue)

		sourceDir := t.TempDir()
		destDir := t.TempDir()

		meta := map[string]string{
			"ibackup:test:a": "1",
			"ibackup:test:b": "2",
		}

		Convey("And a local file", func() {
			file1local := filepath.Join(sourceDir, "file1")
			file1remote := filepath.Join(destDir, "file1")

			CreateTestFileOfLength(t, file1local, 1)

			Convey("You can stat a file that's not been uploaded", func() {
				exists, _, errs := lh.Stat(file1remote)
				So(errs, ShouldBeNil)
				So(exists, ShouldBeFalse)
			})

			Convey("You can upload the file", func() {
				err = lh.Put(file1local, file1remote)
				So(err, ShouldBeNil)

				_, err = os.Stat(file1remote)
				So(err, ShouldBeNil)

				Convey("And putting a new file in the same location will overwrite existing object", func() {
					file2local := filepath.Join(sourceDir, "file2")
					sizeOfFile2 := 10

					CreateTestFileOfLength(t, file2local, sizeOfFile2)

					err = lh.Put(file2local, file1remote)
					So(err, ShouldBeNil)

					fileInfo, errs := os.Stat(file1remote)
					So(errs, ShouldBeNil)
					So(fileInfo.Size(), ShouldEqual, sizeOfFile2)
				})

				Convey("And then you can add metadata to it", func() {
					errm := lh.AddMeta(file1remote, meta)
					So(errm, ShouldBeNil)

					So(lh.Meta[file1remote], ShouldResemble, meta)
				})

				Convey("And given metadata on the uploaded file", func() {
					lh.Meta[file1remote] = meta

					Convey("You can get the metadata from the uploaded file", func() {
						fileMeta, errm := lh.GetMeta(file1remote)
						So(errm, ShouldBeNil)

						So(fileMeta, ShouldResemble, meta)
					})

					Convey("You can stat a file and get its metadata", func() {
						exists, fileMeta, errm := lh.Stat(file1remote)
						So(errm, ShouldBeNil)
						So(exists, ShouldBeTrue)
						So(fileMeta, ShouldResemble, meta)
					})

					Convey("You can query if files contain specific metadata", func() {
						files, errq := lh.QueryMeta(destDir, map[string]string{"ibackup:test:a": "1"})
						So(errq, ShouldBeNil)

						So(len(files), ShouldEqual, 1)
						So(files, ShouldContain, file1remote)

						files, errq = lh.QueryMeta(destDir, map[string]string{"ibackup:test:a": "2"})
						So(errq, ShouldBeNil)

						So(len(files), ShouldEqual, 0)
					})

					Convey("You can remove specific metadata from an uploaded file", func() {
						errm := lh.RemoveMeta(file1remote, map[string]string{"ibackup:test:a": "1"})
						So(errm, ShouldBeNil)

						fileMeta, errm := lh.GetMeta(file1remote)
						So(errm, ShouldBeNil)

						So(fileMeta, ShouldResemble, map[string]string{"ibackup:test:b": "2"})
					})
				})

				Convey("And then you can remove the uploaded file", func() {
					err = lh.RemoveFile(file1remote)
					So(err, ShouldBeNil)

					_, err = os.Stat(file1remote)
					So(err.Error(), ShouldContainSubstring, "no such file or directory")
				})
			})
		})

		Convey("You can open collection clients and upload an empty dir", func() {
			dir1remote := filepath.Join(destDir, "dir1")
			err = lh.EnsureCollection(dir1remote)
			So(err, ShouldBeNil)

			So(len(lh.Collections), ShouldEqual, 1)

			_, err = os.Stat(dir1remote)
			So(err, ShouldBeNil)

			Convey("Then you can close the collection clients", func() {
				err = lh.CollectionsDone()
				So(err, ShouldBeNil)

				So(len(lh.Collections), ShouldEqual, 0)

				Convey("And then you can remove the uploaded dir", func() {
					err = lh.RemoveDir(dir1remote)
					So(err, ShouldBeNil)

					_, err = os.Stat(dir1remote)
					So(err.Error(), ShouldContainSubstring, "no such file or directory")
				})
			})
		})

		Convey("You can close those clients", func() {
			lh.CloseClients()

			So(lh.Connected, ShouldBeFalse)
		})
	})
}
