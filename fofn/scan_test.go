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

package fofn

import (
	"os"
	"path/filepath"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestScanForFOFNs(t *testing.T) {
	Convey("ScanForFOFNs", t, func() {
		dir := t.TempDir()

		createSubWithFOFN := func(name string) string {
			sub := filepath.Join(dir, name)
			So(os.MkdirAll(sub, 0750), ShouldBeNil)
			So(os.WriteFile(
				filepath.Join(sub, "fofn"),
				[]byte(""), 0600, //nolint:gosec
			), ShouldBeNil)

			return sub
		}

		createSubWithoutFOFN := func(name string) {
			sub := filepath.Join(dir, name)
			So(os.MkdirAll(sub, 0750), ShouldBeNil)
		}

		Convey("3 subdirs, 2 with fofn returns 2 SubDirs", func() {
			pathA := createSubWithFOFN("a")

			createSubWithoutFOFN("b")

			pathC := createSubWithFOFN("c")

			result, err := ScanForFOFNs(dir)
			So(err, ShouldBeNil)
			So(result, ShouldHaveLength, 2)

			paths := []string{result[0].Path, result[1].Path}
			So(paths, ShouldContain, pathA)
			So(paths, ShouldContain, pathC)

			for _, sd := range result {
				So(sd.FofnMtime, ShouldBeGreaterThan, 0)
			}
		})

		Convey("no subdirs returns empty slice, no error", func() {
			emptyDir := filepath.Join(dir, "empty")
			So(os.MkdirAll(emptyDir, 0750), ShouldBeNil)

			result, err := ScanForFOFNs(emptyDir)
			So(err, ShouldBeNil)
			So(result, ShouldBeEmpty)
		})

		Convey("1 subdir with fofn, 1 without returns 1 SubDir", func() {
			pathX := createSubWithFOFN("x")

			createSubWithoutFOFN("y")

			result, err := ScanForFOFNs(dir)
			So(err, ShouldBeNil)
			So(result, ShouldHaveLength, 1)
			So(result[0].Path, ShouldEqual, pathX)
		})

		Convey("non-existent watchDir returns error", func() {
			_, err := ScanForFOFNs(filepath.Join(dir, "nope"))
			So(err, ShouldNotBeNil)
		})
	})
}
