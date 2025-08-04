/*******************************************************************************
 * Copyright (c) 2025 Genome Research Ltd.
 *
 * Author: Prasannakumar Nuniganti <pn10@sanger.ac.uk>
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
	"encoding/base64"
	"path/filepath"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/ibackup/db"
	"github.com/wtsi-hgi/ibackup/internal"
)

func TestFons(t *testing.T) {
	Convey("Fons can be read in various encodings", t, func() {
		transformer, err := db.NewTransformer("transformer", "^/some/path", "/remote")
		So(err, ShouldBeNil)

		dir := t.TempDir()

		filePath1 := filepath.Join("/some/path", "file1")
		filePath2 := filepath.Join("/some/path", "file2")
		dirPath := filepath.Join("/some/path", "dir1")

		fonPath1 := filepath.Join(dir, "fofn.txt")
		internal.CreateTestFile(t, fonPath1, filePath1+"\n"+filePath2)

		discover := &db.Discover{
			Path: fonPath1,
			Type: db.DiscoverFOFN,
		}

		lines, err := ReadFon(transformer, discover)
		So(err, ShouldBeNil)
		So(lines, ShouldResemble, []string{filePath1, filePath2})

		fonPath2 := filepath.Join(dir, "fofn2.txt")
		internal.CreateTestFile(t, fonPath2, dirPath)

		discover = &db.Discover{
			Path: fonPath2,
			Type: db.DiscoverFODN,
		}

		lines, err = ReadFon(transformer, discover)
		So(err, ShouldBeNil)
		So(lines, ShouldResemble, []string{dirPath + "/"})

		Convey("Quoted file paths are decoded correctly", func() {
			lines := []string{
				`"/some/path/file1/"`,
				`"/some/path/file2"`,
			}
			tmpFile := internal.CreateTestFileWithLineContents(t, "quoted.txt", lines)
			discover := &db.Discover{
				Path: tmpFile,
				Type: db.DiscoverFOFNQuoted,
			}

			paths, err := ReadFon(transformer, discover)
			So(err, ShouldBeNil)
			So(paths, ShouldResemble, []string{
				"/some/path/file1",
				"/some/path/file2",
			})
		})

		Convey("Quoted directory paths are decoded correctly", func() {
			lines := []string{
				`"/some/path/dir1/"`,
				`"/some/path/dir2"`,
			}
			tmpFile := internal.CreateTestFileWithLineContents(t, "quoted_dirs.txt", lines)
			discover := &db.Discover{
				Path: tmpFile,
				Type: db.DiscoverFODNQuoted,
			}

			paths, err := ReadFon(transformer, discover)
			So(err, ShouldBeNil)
			So(paths, ShouldResemble, []string{
				"/some/path/dir1/",
				"/some/path/dir2/",
			})
		})

		Convey("Base64-encoded file paths are decoded correctly", func() {
			lines := []string{
				base64.StdEncoding.EncodeToString([]byte("/some/path/file1")),
				base64.StdEncoding.EncodeToString([]byte("/some/path/file2")),
			}
			tmpFile := internal.CreateTestFileWithLineContents(t, "base64.txt", lines)

			discover := &db.Discover{
				Path: tmpFile,
				Type: db.DiscoverFOFNBase64,
			}

			paths, err := ReadFon(transformer, discover)
			So(err, ShouldBeNil)
			So(paths, ShouldResemble, []string{
				"/some/path/file1",
				"/some/path/file2",
			})
		})

		Convey("Base64-encoded directory paths are decoded correctly", func() {
			lines := []string{
				base64.StdEncoding.EncodeToString([]byte("/some/path/dir1/")),
				base64.StdEncoding.EncodeToString([]byte("/some/path/dir2")),
			}
			tmpFile := internal.CreateTestFileWithLineContents(t, "base64_dirs.txt", lines)

			discover := &db.Discover{
				Path: tmpFile,
				Type: db.DiscoverFODNBase64,
			}

			paths, err := ReadFon(transformer, discover)
			So(err, ShouldBeNil)
			So(paths, ShouldResemble, []string{
				"/some/path/dir1/",
				"/some/path/dir2/",
			})
		})
	})
}
