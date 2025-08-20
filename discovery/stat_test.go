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

package discovery

import (
	"bytes"
	"maps"
	"os"
	"os/user"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/ibackup/db"
)

func TestStat(t *testing.T) {
	Convey("With a statter", t, func() {
		statter, err := newStatter()
		So(err, ShouldBeNil)
		So(len(statter.mountpoints), ShouldNotBeZeroValue)

		Convey("You can stat files", func() {
			username, err := user.Current()
			So(err, ShouldBeNil)

			group, err := user.LookupGroupId(username.Gid)
			So(err, ShouldBeNil)

			dir := t.TempDir()
			files := make([]string, 7)

			dirMP := ""

			mps := slices.Collect(maps.Values(statter.mountpoints))

			slices.SortFunc(mps, func(a, b string) int {
				return len(b) - len(a)
			})

			for _, mp := range mps {
				if strings.HasPrefix(dir, mp) {
					dirMP = mp

					break
				}
			}

			expectation := make([]*db.File, 7)
			files[0] = filepath.Join(dir, " ")
			expectation[0] = &db.File{
				LocalPath: files[0],
				Status:    db.StatusMissing,
				LastError: "file does not exist",
			}

			for n := range 5 {
				path := filepath.Join(dir, strconv.Itoa(n))
				files[n+1] = path

				So(os.WriteFile(path, bytes.Repeat([]byte{0}, n), 0600), ShouldBeNil)

				stat, errr := os.Stat(path)
				So(errr, ShouldBeNil)

				btime := stat.ModTime()
				mtime := btime.Add(5 * time.Second)

				So(os.Chtimes(path, mtime, mtime), ShouldBeNil)

				expectation[n+1] = &db.File{ //nolint:forcetypeassert
					LocalPath:  path,
					Size:       uint64(n), //nolint:gosec
					Btime:      btime.Unix(),
					Mtime:      mtime.Unix(),
					Inode:      stat.Sys().(*syscall.Stat_t).Ino, //nolint:errcheck
					MountPount: dirMP,
					Owner:      username.Username,
					Group:      group.Name,
				}
			}

			files[6] = files[5] + ".lnk"

			So(os.Symlink(files[5], files[6]), ShouldBeNil)
			stat, err := os.Lstat(files[6])
			So(err, ShouldBeNil)

			expectation[6] = &db.File{ //nolint:forcetypeassert
				LocalPath:  files[6],
				Size:       uint64(len(files[5])),
				Btime:      stat.ModTime().Unix(),
				Mtime:      stat.ModTime().Unix(),
				Inode:      stat.Sys().(*syscall.Stat_t).Ino, //nolint:errcheck
				MountPount: dirMP,
				Owner:      username.Username,
				Group:      group.Name,
			}

			statter.WriterAdd(1)

			go statter.StatFiles(files)

			statter.Launch(1)

			statted := slices.Collect(statter.Iter())

			slices.SortFunc(statted, func(a, b *db.File) int {
				return strings.Compare(a.LocalPath, b.LocalPath)
			})

			So(statted, ShouldResemble, expectation)
		})
	})
}
