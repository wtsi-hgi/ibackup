/*******************************************************************************
 * Copyright (c) 2026 Genome Research Ltd.
 *
 * Author: Sendu Bala <sb10@sanger.ac.uk>
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

package ownership

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestOwnership(t *testing.T) {
	Convey("Given a temporary directory", t, func() {
		dir := t.TempDir()
		gid := os.Getegid()

		Convey("GetDirGID returns the directory GID", func() {
			got, err := GetDirGID(dir)
			So(err, ShouldBeNil)
			So(got, ShouldEqual, gid)
		})

		Convey("GetDirGID on non-existent path returns error", func() {
			_, err := GetDirGID(filepath.Join(dir, "nonexistent"))
			So(err, ShouldNotBeNil)
		})

		Convey("CreateDirWithGID creates dir with mode 0750 and correct GID", func() {
			sub := filepath.Join(dir, "sub")

			err := CreateDirWithGID(sub, gid)
			So(err, ShouldBeNil)

			info, err := os.Stat(sub)
			So(err, ShouldBeNil)
			So(info.Mode().Perm(), ShouldEqual, os.FileMode(0750))

			stat, ok := info.Sys().(*syscall.Stat_t)
			So(ok, ShouldBeTrue)
			So(int(stat.Gid), ShouldEqual, gid)
		})

		Convey("WriteFileWithGID creates file with mode 0640 and correct GID", func() {
			fpath := filepath.Join(dir, "testfile")
			content := []byte("hello world")

			err := WriteFileWithGID(fpath, content, gid)
			So(err, ShouldBeNil)

			info, err := os.Stat(fpath)
			So(err, ShouldBeNil)
			So(info.Mode().Perm(), ShouldEqual, os.FileMode(0640))

			stat, ok := info.Sys().(*syscall.Stat_t)
			So(ok, ShouldBeTrue)
			So(int(stat.Gid), ShouldEqual, gid)

			data, err := os.ReadFile(fpath)
			So(err, ShouldBeNil)
			So(string(data), ShouldEqual, "hello world")
		})
	})
}
