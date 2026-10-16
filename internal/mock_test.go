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
	"strconv"
	"sync"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/ibackup/baton/meta"
	"github.com/wtsi-hgi/ibackup/errs"
)

func TestMockConcurrency(t *testing.T) {
	Convey("A local handler can be used from many goroutines at once", t, func() {
		lh := GetLocalHandler()
		dir := t.TempDir()
		removed := filepath.Join(dir, "removed")
		kept := filepath.Join(dir, "kept")

		CreateTestFileOfLength(t, kept, 1)

		const n = 50

		var wg sync.WaitGroup

		wg.Add(4)

		go func() {
			defer wg.Done()

			for range n {
				lh.RemoveFile(removed) //nolint:errcheck
			}
		}()

		go func() {
			defer wg.Done()

			for i := range n {
				lh.AddMeta(kept, map[string]string{"key" + strconv.Itoa(i): "val"}) //nolint:errcheck
			}
		}()

		go func() {
			defer wg.Done()

			for range n {
				lh.QueryMeta(dir, map[string]string{"key0": "val"}) //nolint:errcheck
				lh.Stat(kept)                                       //nolint:errcheck
				lh.Put(filepath.Join(dir, "missing"), kept)         //nolint:errcheck
				lh.Cleanup()
			}
		}()

		go func() {
			defer wg.Done()

			other := filepath.Join(dir, "other")

			for range n {
				lh.MakeMetaFail(other)
				lh.MakeStatFail(other)
				lh.MakePutFail(other)
				lh.CollectionsDone() //nolint:errcheck
			}
		}()

		wg.Wait()

		got, err := lh.GetMeta(kept)
		So(err, ShouldBeNil)
		So(got, ShouldHaveLength, n)
	})
}

func TestMock(t *testing.T) {
	Convey("Given a local handler", t, func() {
		lh := GetLocalHandler()

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

			Convey("Getting metadata of a file that's not been uploaded fails like baton", func() {
				fileMeta, err := lh.GetMeta(file1remote)
				So(fileMeta, ShouldBeNil)
				So(err, ShouldResemble, errs.PathError{Msg: ErrFileDoesNotExist, Path: file1remote})
			})

			Convey("You can try to remove the file from iRODS and receive an error", func() {
				err := lh.RemoveFile(file1remote)
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldContainSubstring, "does not exist")
			})

			Convey("You can upload the file", func() {
				err := lh.Put(file1local, file1remote)
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

				Convey("But adding metadata with an empty value fails, like iRODS, adding none of it", func() {
					errm := lh.AddMeta(file1remote, map[string]string{"ibackup:test:a": "1", "ibackup:test:b": ""})
					So(errm, ShouldNotBeNil)
					So(errm.Error(), ShouldContainSubstring, ErrEmptyMetaValue)

					fileMeta, errg := lh.GetMeta(file1remote)
					So(errg, ShouldBeNil)
					So(fileMeta, ShouldBeEmpty)
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
						compareMetasWithSize(t, fileMeta, meta, 1)
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

					Convey("But removing metadata with an empty value fails, like iRODS, removing none of it", func() {
						errm := lh.RemoveMeta(file1remote, map[string]string{"ibackup:test:a": "1", "ibackup:test:b": ""})
						So(errm, ShouldNotBeNil)
						So(errm.Error(), ShouldContainSubstring, ErrEmptyMetaValue)

						fileMeta, errg := lh.GetMeta(file1remote)
						So(errg, ShouldBeNil)
						So(fileMeta, ShouldResemble, meta)
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
			err := lh.EnsureCollection(dir1remote)
			So(err, ShouldBeNil)

			So(len(lh.Collections), ShouldEqual, 1)

			_, err = os.Stat(dir1remote)
			So(err, ShouldBeNil)

			Convey("Then you can close the collection clients", func() {
				err = lh.CollectionsDone()
				So(err, ShouldBeNil)

				Convey("And then you can remove the uploaded dir", func() {
					err = lh.RemoveDir(dir1remote)
					So(err, ShouldBeNil)

					_, err = os.Stat(dir1remote)
					So(err.Error(), ShouldContainSubstring, "no such file or directory")
				})
			})
		})

		Convey("You can close all clients", func() {
			lh.Cleanup()

			So(lh.Connected, ShouldBeFalse)
		})
	})
}

func compareMetasWithSize(t *testing.T, remote, expected map[string]string, size int64) {
	t.Helper()

	delete(remote, meta.MetaKeyRemoteCtime)
	delete(remote, meta.MetaKeyRemoteMtime)

	expected[meta.MetaKeyRemoteSize] = strconv.FormatInt(size, 10)

	So(remote, ShouldResemble, expected)
}
