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
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestWriteStatusFromRun(t *testing.T) {
	Convey("WriteStatusFromRun", t, func() {
		dir := t.TempDir()

		Convey("3 chunks with reports, no buried", func() {
			runDir := filepath.Join(dir, "run1")
			So(os.MkdirAll(runDir, 0750), ShouldBeNil)

			createChunkAndReport(runDir, 0, 10,
				[]string{"uploaded", "replaced"})
			createChunkAndReport(runDir, 1, 10,
				[]string{"unmodified", "missing"})
			createChunkAndReport(runDir, 2, 5,
				[]string{"failed", "frozen", "warning"})

			statusPath := filepath.Join(dir, "status1")

			err := WriteStatusFromRun(
				runDir, statusPath, nil,
			)
			So(err, ShouldBeNil)

			entries, counts, err := ParseStatus(statusPath)
			So(err, ShouldBeNil)
			So(len(entries), ShouldEqual, 25)

			total := counts.Uploaded + counts.Replaced +
				counts.Unmodified + counts.Missing +
				counts.Failed + counts.Frozen +
				counts.Warning + counts.Hardlink +
				counts.Orphaned + counts.NotProcessed
			So(total, ShouldEqual, 25)
			So(counts.NotProcessed, ShouldEqual, 0)
		})

		Convey("3 chunks, 1 buried with no report", func() {
			runDir := filepath.Join(dir, "run2")
			So(os.MkdirAll(runDir, 0750), ShouldBeNil)

			createChunkAndReport(runDir, 0, 10,
				[]string{"uploaded"})
			createChunkAndReport(runDir, 1, 10,
				[]string{"uploaded"})
			createChunkOnly(runDir, 2, 5)

			statusPath := filepath.Join(dir, "status2")

			err := WriteStatusFromRun(
				runDir, statusPath,
				[]string{"chunk.000002"},
			)
			So(err, ShouldBeNil)

			entries, counts, err := ParseStatus(statusPath)
			So(err, ShouldBeNil)
			So(len(entries), ShouldEqual, 25)
			So(counts.Uploaded, ShouldEqual, 20)
			So(counts.NotProcessed, ShouldEqual, 5)
		})

		Convey("1 buried chunk with partial report", func() {
			runDir := filepath.Join(dir, "run3")
			So(os.MkdirAll(runDir, 0750), ShouldBeNil)

			createChunkWithPartialReport(
				runDir, 0, 10, 5,
			)

			statusPath := filepath.Join(dir, "status3")

			err := WriteStatusFromRun(
				runDir, statusPath,
				[]string{"chunk.000000"},
			)
			So(err, ShouldBeNil)

			entries, counts, err := ParseStatus(statusPath)
			So(err, ShouldBeNil)
			So(len(entries), ShouldEqual, 10)
			So(counts.Uploaded, ShouldEqual, 5)
			So(counts.NotProcessed, ShouldEqual, 5)
		})

		Convey("0 chunk files produces SUMMARY only", func() {
			runDir := filepath.Join(dir, "run4")
			So(os.MkdirAll(runDir, 0750), ShouldBeNil)

			statusPath := filepath.Join(dir, "status4")

			err := WriteStatusFromRun(
				runDir, statusPath, nil,
			)
			So(err, ShouldBeNil)

			entries, counts, err := ParseStatus(statusPath)
			So(err, ShouldBeNil)
			So(len(entries), ShouldEqual, 0)
			So(counts.Uploaded, ShouldEqual, 0)
			So(counts.NotProcessed, ShouldEqual, 0)
		})

		Convey("ParseStatus returns correct entries "+
			"and counts", func() {
			runDir := filepath.Join(dir, "run5")
			So(os.MkdirAll(runDir, 0750), ShouldBeNil)

			createChunkAndReport(runDir, 0, 10,
				[]string{"uploaded", "replaced"})
			createChunkAndReport(runDir, 1, 10,
				[]string{"unmodified", "missing"})
			createChunkAndReport(runDir, 2, 5,
				[]string{"failed"})

			statusPath := filepath.Join(dir, "status5")

			err := WriteStatusFromRun(
				runDir, statusPath, nil,
			)
			So(err, ShouldBeNil)

			entries, counts, err := ParseStatus(statusPath)
			So(err, ShouldBeNil)
			So(len(entries), ShouldEqual, 25)

			entryCount := 0

			for _, e := range entries {
				if e.Local != "" && e.Remote != "" {
					entryCount++
				}
			}

			So(entryCount, ShouldEqual, 25)

			sumFromCounts := counts.Uploaded +
				counts.Replaced + counts.Unmodified +
				counts.Missing + counts.Failed +
				counts.Frozen + counts.Orphaned +
				counts.Warning + counts.Hardlink +
				counts.NotProcessed
			So(sumFromCounts, ShouldEqual, 25)
		})
	})
}

// createChunkAndReport creates a chunk file and its
// corresponding complete report. Statuses are cycled.
func createChunkAndReport(
	runDir string, index, count int, statuses []string,
) {
	chunkPath := filepath.Join(
		runDir, fmt.Sprintf(chunkNameFormat, index),
	)
	reportPath := chunkPath + ".report"

	cf, err := os.Create(chunkPath)
	So(err, ShouldBeNil)

	rf, err := os.Create(reportPath)
	So(err, ShouldBeNil)

	for i := range count {
		local := fmt.Sprintf("/local/%d/%d", index, i)
		remote := fmt.Sprintf("/remote/%d/%d", index, i)

		writeChunkEntry(cf, local, remote)

		status := statuses[i%len(statuses)]

		err = WriteReportEntry(rf, ReportEntry{
			Local:  local,
			Remote: remote,
			Status: status,
		})
		So(err, ShouldBeNil)
	}

	So(cf.Close(), ShouldBeNil)
	So(rf.Close(), ShouldBeNil)
}

// createChunkOnly creates a chunk file without a report.
func createChunkOnly(runDir string, index, count int) {
	chunkPath := filepath.Join(
		runDir, fmt.Sprintf(chunkNameFormat, index),
	)

	cf, err := os.Create(chunkPath)
	So(err, ShouldBeNil)

	for i := range count {
		local := fmt.Sprintf("/local/%d/%d", index, i)
		remote := fmt.Sprintf("/remote/%d/%d", index, i)

		writeChunkEntry(cf, local, remote)
	}

	So(cf.Close(), ShouldBeNil)
}

// createChunkWithPartialReport creates a chunk file with
// totalEntries entries but only reportedEntries in the
// report.
func createChunkWithPartialReport(
	runDir string, index, totalEntries, reportedEntries int,
) {
	chunkPath := filepath.Join(
		runDir, fmt.Sprintf(chunkNameFormat, index),
	)
	reportPath := chunkPath + ".report"

	cf, err := os.Create(chunkPath)
	So(err, ShouldBeNil)

	rf, err := os.Create(reportPath)
	So(err, ShouldBeNil)

	for i := range totalEntries {
		local := fmt.Sprintf("/local/%d/%d", index, i)
		remote := fmt.Sprintf("/remote/%d/%d", index, i)

		writeChunkEntry(cf, local, remote)

		if i < reportedEntries {
			err = WriteReportEntry(rf, ReportEntry{
				Local:  local,
				Remote: remote,
				Status: "uploaded",
			})
			So(err, ShouldBeNil)
		}
	}

	So(cf.Close(), ShouldBeNil)
	So(rf.Close(), ShouldBeNil)
}

// writeChunkEntry writes a base64-encoded chunk line.
func writeChunkEntry(f *os.File, local, remote string) {
	local64 := base64.StdEncoding.EncodeToString(
		[]byte(local),
	)
	remote64 := base64.StdEncoding.EncodeToString(
		[]byte(remote),
	)

	_, err := fmt.Fprintf(f, "%s\t%s\n", local64, remote64)
	So(err, ShouldBeNil)
}

func TestStatusMemory(t *testing.T) {
	Convey("WriteStatusFromRun streams without "+
		"excessive memory", t, func() {
		dir := t.TempDir()
		runDir := filepath.Join(dir, "memrun")
		So(os.MkdirAll(runDir, 0750), ShouldBeNil)

		const (
			numChunks       = 100
			entriesPerChunk = 10_000
		)

		statuses := []string{
			"uploaded", "replaced", "unmodified",
			"missing", "failed",
		}

		createErrors := 0

		for i := range numChunks {
			err := createLargeChunkAndReport(
				runDir, i, entriesPerChunk, statuses,
			)
			if err != nil {
				createErrors++
			}
		}

		So(createErrors, ShouldEqual, 0)

		statusPath := filepath.Join(dir, "memstatus")

		runtime.GC()

		var before runtime.MemStats
		runtime.ReadMemStats(&before)

		err := WriteStatusFromRun(
			runDir, statusPath, nil,
		)
		So(err, ShouldBeNil)

		runtime.GC()

		var after runtime.MemStats
		runtime.ReadMemStats(&after)

		var growth uint64
		if after.HeapInuse > before.HeapInuse {
			growth = after.HeapInuse - before.HeapInuse
		}

		So(growth, ShouldBeLessThan, 20*1024*1024)
	})
}

// createLargeChunkAndReport creates a chunk and report
// with many entries for memory testing. Does not use
// So() assertions inside the loop.
func createLargeChunkAndReport(
	runDir string, index, count int, statuses []string,
) error {
	chunkPath := filepath.Join(
		runDir, fmt.Sprintf(chunkNameFormat, index),
	)
	reportPath := chunkPath + ".report"

	cf, err := os.Create(chunkPath)
	if err != nil {
		return err
	}
	defer cf.Close()

	rf, err := os.Create(reportPath)
	if err != nil {
		return err
	}
	defer rf.Close()

	for i := range count {
		local := fmt.Sprintf("/local/%d/%06d", index, i)
		remote := fmt.Sprintf("/remote/%d/%06d", index, i)

		local64 := base64.StdEncoding.EncodeToString(
			[]byte(local),
		)
		remote64 := base64.StdEncoding.EncodeToString(
			[]byte(remote),
		)

		if _, werr := fmt.Fprintf(
			cf, "%s\t%s\n", local64, remote64,
		); werr != nil {
			return werr
		}

		status := statuses[i%len(statuses)]

		if werr := WriteReportEntry(rf, ReportEntry{
			Local:  local,
			Remote: remote,
			Status: status,
		}); werr != nil {
			return werr
		}
	}

	return nil
}
