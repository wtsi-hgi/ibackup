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

package scanner

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestScanNullTerminated(t *testing.T) {
	Convey("Given a scanner package", t, func() {
		dir := t.TempDir()

		Convey("ScanNullTerminated with three null-terminated paths", func() {
			path := filepath.Join(dir, "three.fofn")
			err := os.WriteFile(path, []byte("/a/b\x00/c/d\x00/e/f\x00"), 0600)
			So(err, ShouldBeNil)

			var got []string

			err = ScanNullTerminated(path, func(entry string) error {
				got = append(got, entry)

				return nil
			})

			So(err, ShouldBeNil)
			So(got, ShouldResemble, []string{"/a/b", "/c/d", "/e/f"})
		})

		Convey("ScanNullTerminated with an empty file", func() {
			path := filepath.Join(dir, "empty.fofn")
			err := os.WriteFile(path, []byte{}, 0600)
			So(err, ShouldBeNil)

			called := false

			err = ScanNullTerminated(path, func(entry string) error {
				called = true

				return nil
			})

			So(err, ShouldBeNil)
			So(called, ShouldBeFalse)
		})

		Convey("ScanNullTerminated with embedded newline in path", func() {
			path := filepath.Join(dir, "newline.fofn")
			err := os.WriteFile(path, []byte("/a/b\n/c\x00"), 0600)
			So(err, ShouldBeNil)

			var got []string

			err = ScanNullTerminated(path, func(entry string) error {
				got = append(got, entry)

				return nil
			})

			So(err, ShouldBeNil)
			So(got, ShouldResemble, []string{"/a/b\n/c"})
		})

		Convey("ScanNullTerminated with non-existent file", func() {
			err := ScanNullTerminated("/no/such/file", func(entry string) error {
				return nil
			})

			So(err, ShouldNotBeNil)
		})

		Convey("ScanNullTerminated with no trailing null", func() {
			path := filepath.Join(dir, "notrail.fofn")
			err := os.WriteFile(path, []byte("/a/b\x00/c/d"), 0600)
			So(err, ShouldBeNil)

			var got []string

			err = ScanNullTerminated(path, func(entry string) error {
				got = append(got, entry)

				return nil
			})

			So(err, ShouldBeNil)
			So(got, ShouldResemble, []string{"/a/b", "/c/d"})
		})

		Convey("ScanNullTerminated stops early on callback error", func() {
			path := filepath.Join(dir, "stop.fofn")
			err := os.WriteFile(path, []byte("/a/b\x00/c/d\x00/e/f\x00"), 0600)
			So(err, ShouldBeNil)

			cbErr := errors.New("stop here")
			count := 0

			err = ScanNullTerminated(path, func(entry string) error {
				count++
				if count == 2 {
					return cbErr
				}

				return nil
			})

			So(err, ShouldEqual, cbErr)
			So(count, ShouldEqual, 2)
		})

		Convey("ScanNullTerminated streams without excessive memory", func() {
			const (
				numEntries = 1_000_000
				entryLen   = 100
			)

			path := filepath.Join(dir, "large.fofn")

			f, err := os.Create(path)
			So(err, ShouldBeNil)

			entry := "/" + strings.Repeat("x", entryLen-1)

			writeErr := 0

			for range numEntries {
				if _, wErr := f.WriteString(entry + "\x00"); wErr != nil {
					writeErr++
				}
			}

			err = f.Close()
			So(err, ShouldBeNil)
			So(writeErr, ShouldEqual, 0)

			runtime.GC()

			var before runtime.MemStats
			runtime.ReadMemStats(&before)

			count := 0

			err = ScanNullTerminated(path, func(_ string) error {
				count++

				return nil
			})

			runtime.GC()

			var after runtime.MemStats
			runtime.ReadMemStats(&after)

			So(err, ShouldBeNil)
			So(count, ShouldEqual, numEntries)

			var growth uint64
			if after.HeapInuse > before.HeapInuse {
				growth = after.HeapInuse - before.HeapInuse
			}

			So(growth, ShouldBeLessThan, 10*1024*1024)
		})
	})
}
