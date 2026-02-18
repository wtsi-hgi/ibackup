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
	"time"

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

func TestNeedsProcessing(t *testing.T) {
	Convey("NeedsProcessing", t, func() {
		dir := t.TempDir()

		createSubWithFOFN := func(name string, mtime int64) SubDir {
			sub := filepath.Join(dir, name)
			So(os.MkdirAll(sub, 0750), ShouldBeNil)

			fofnPath := filepath.Join(sub, "fofn")
			So(os.WriteFile(fofnPath, []byte(""), 0600), ShouldBeNil) //nolint:gosec

			tm := time.Unix(mtime, 0)
			So(os.Chtimes(fofnPath, tm, tm), ShouldBeNil)

			return SubDir{Path: sub}
		}

		Convey("fofn with no run dirs needs processing", func() {
			sd := createSubWithFOFN("proj1", 1000)

			needs, mtime, err := NeedsProcessing(sd)
			So(err, ShouldBeNil)
			So(needs, ShouldBeTrue)
			So(mtime, ShouldEqual, int64(1000))
		})

		Convey("fofn with matching run dir does not need processing", func() {
			sd := createSubWithFOFN("proj2", 1000)
			So(os.MkdirAll(filepath.Join(sd.Path, "1000"), 0750), ShouldBeNil)

			needs, mtime, err := NeedsProcessing(sd)
			So(err, ShouldBeNil)
			So(needs, ShouldBeFalse)
			So(mtime, ShouldEqual, int64(0))
		})

		Convey("fofn with stale run dir needs processing", func() {
			sd := createSubWithFOFN("proj3", 2000)
			So(os.MkdirAll(filepath.Join(sd.Path, "1000"), 0750), ShouldBeNil)

			needs, mtime, err := NeedsProcessing(sd)
			So(err, ShouldBeNil)
			So(needs, ShouldBeTrue)
			So(mtime, ShouldEqual, int64(2000))
		})
	})
}
