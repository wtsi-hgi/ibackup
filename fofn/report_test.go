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

func TestReportLine(t *testing.T) {
	Convey("FormatReportLine and ParseReportLine", t, func() {
		Convey("format a simple entry", func() {
			entry := ReportEntry{
				Local:  "/a/b",
				Remote: "/c/d",
				Status: "uploaded",
				Error:  "",
			}

			result := FormatReportLine(entry)
			So(result, ShouldEqual,
				"\"/a/b\"\t\"/c/d\"\tuploaded\t\"\"")
		})

		Convey("format an entry with tabs in paths and error", func() {
			entry := ReportEntry{
				Local:  "/a\tb",
				Remote: "/c\td",
				Status: "failed",
				Error:  "conn\treset",
			}

			result := FormatReportLine(entry)
			So(result, ShouldEqual,
				"\"/a\\tb\"\t\"/c\\td\"\tfailed\t\"conn\\treset\"")
		})

		Convey("parse a simple line", func() {
			line := "\"/a/b\"\t\"/c/d\"\tuploaded\t\"\""

			entry, err := ParseReportLine(line)
			So(err, ShouldBeNil)
			So(entry.Local, ShouldEqual, "/a/b")
			So(entry.Remote, ShouldEqual, "/c/d")
			So(entry.Status, ShouldEqual, "uploaded")
			So(entry.Error, ShouldEqual, "")
		})

		Convey("parse a line with tabs in fields", func() {
			line := "\"/a\\tb\"\t\"/c\\td\"\tfailed\t\"conn\\treset\""

			entry, err := ParseReportLine(line)
			So(err, ShouldBeNil)
			So(entry.Local, ShouldEqual, "/a\tb")
			So(entry.Remote, ShouldEqual, "/c\td")
			So(entry.Status, ShouldEqual, "failed")
			So(entry.Error, ShouldEqual, "conn\treset")
		})

		Convey("parse a malformed line returns error", func() {
			line := "only\ttwo"

			_, err := ParseReportLine(line)
			So(err, ShouldNotBeNil)
		})
	})
}

func TestReportWriteAndRead(t *testing.T) {
	Convey("WriteReportEntry and ParseReportCallback", t, func() {
		Convey("round-trip entries with special characters", func() {
			dir := t.TempDir()
			path := filepath.Join(dir, "report.tsv")

			entries := []ReportEntry{
				{Local: "/a/b", Remote: "/c/d",
					Status: "uploaded", Error: ""},
				{Local: "/path\twith\ttabs", Remote: "/remote\ttab",
					Status: "failed", Error: "conn\treset"},
				{Local: "/normal/path", Remote: "/remote/path",
					Status: "unmodified", Error: ""},
			}

			f, err := os.Create(path)
			So(err, ShouldBeNil)

			for _, e := range entries {
				err = WriteReportEntry(f, e)
				So(err, ShouldBeNil)
			}

			err = f.Close()
			So(err, ShouldBeNil)

			var got []ReportEntry

			err = ParseReportCallback(path,
				func(entry ReportEntry) error {
					got = append(got, entry)

					return nil
				})
			So(err, ShouldBeNil)
			So(len(got), ShouldEqual, 3)

			for i, e := range entries {
				So(got[i].Local, ShouldEqual, e.Local)
				So(got[i].Remote, ShouldEqual, e.Remote)
				So(got[i].Status, ShouldEqual, e.Status)
				So(got[i].Error, ShouldEqual, e.Error)
			}
		})

		Convey("non-existent file returns error", func() {
			err := ParseReportCallback("/no/such/file",
				func(ReportEntry) error {
					return nil
				})
			So(err, ShouldNotBeNil)
		})

		Convey("empty file invokes no callbacks", func() {
			dir := t.TempDir()
			path := filepath.Join(dir, "empty.tsv")

			f, err := os.Create(path)
			So(err, ShouldBeNil)

			err = f.Close()
			So(err, ShouldBeNil)

			count := 0

			err = ParseReportCallback(path,
				func(ReportEntry) error {
					count++

					return nil
				})
			So(err, ShouldBeNil)
			So(count, ShouldEqual, 0)
		})

		Convey("read 10 entries from a report file", func() {
			dir := t.TempDir()
			path := filepath.Join(dir, "ten.tsv")

			f, err := os.Create(path)
			So(err, ShouldBeNil)

			for i := range 10 {
				e := ReportEntry{
					Local:  "/local/" + string(rune('a'+i)),
					Remote: "/remote/" + string(rune('a'+i)),
					Status: "uploaded",
					Error:  "",
				}

				err = WriteReportEntry(f, e)
				So(err, ShouldBeNil)
			}

			err = f.Close()
			So(err, ShouldBeNil)

			count := 0

			err = ParseReportCallback(path,
				func(ReportEntry) error {
					count++

					return nil
				})
			So(err, ShouldBeNil)
			So(count, ShouldEqual, 10)
		})
	})
}
