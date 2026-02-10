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
	"bufio"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"unsafe"
)

const (
	statusNotProcessed  = "not_processed"
	summaryPrefix       = "SUMMARY"
	chunkGlob           = "chunk.*"
	chunkFieldCount     = 2
	summaryKVFieldCount = 2
)

// ErrMalformedSummary is returned when a summary field
// does not have a key=value format.
var ErrMalformedSummary = errors.New(
	"malformed summary field",
)

// ErrMalformedChunkLine is returned when a chunk line
// does not have exactly the expected number of fields.
var ErrMalformedChunkLine = errors.New(
	"malformed chunk line",
)

// statusOffsets maps status strings to their byte offset
// within StatusCounts. Populated once at init time.
var statusOffsets = buildStatusOffsets() //nolint:gochecknoglobals

// StatusCounts holds counts per upload status for a
// completed run.
type StatusCounts struct {
	Uploaded     int
	Replaced     int
	Unmodified   int
	Missing      int
	Failed       int
	Frozen       int
	Orphaned     int
	Warning      int
	Hardlink     int
	NotProcessed int
}

// ParseStatus reads a status file produced by
// WriteStatusFromRun and returns all entries plus the
// summary counts from the SUMMARY line.
func ParseStatus(
	path string,
) ([]ReportEntry, StatusCounts, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, StatusCounts{}, err
	}
	defer f.Close()

	return scanStatusFile(f)
}

func scanStatusFile(
	r *os.File,
) ([]ReportEntry, StatusCounts, error) {
	var (
		entries = make([]ReportEntry, 0)
		counts  StatusCounts
	)

	s := bufio.NewScanner(r)

	for s.Scan() {
		line := s.Text()
		if line == "" {
			continue
		}

		var err error

		entries, counts, err = handleStatusLine(
			line, entries, counts,
		)
		if err != nil {
			return nil, StatusCounts{}, err
		}
	}

	if err := s.Err(); err != nil {
		return nil, StatusCounts{}, err
	}

	return entries, counts, nil
}

func handleStatusLine(
	line string,
	entries []ReportEntry,
	counts StatusCounts,
) ([]ReportEntry, StatusCounts, error) {
	if strings.HasPrefix(line, summaryPrefix+"\t") {
		parsed, err := parseSummaryLine(line)

		return entries, parsed, err
	}

	entry, err := ParseReportLine(line)
	if err != nil {
		return nil, StatusCounts{},
			fmt.Errorf("parse status line: %w", err)
	}

	return append(entries, entry), counts, nil
}

func parseSummaryLine(
	line string,
) (StatusCounts, error) {
	var counts StatusCounts

	fields := strings.Split(line, "\t")

	for _, field := range fields[1:] {
		if err := parseSummaryField(
			&counts, field,
		); err != nil {
			return StatusCounts{}, err
		}
	}

	return counts, nil
}

func processAllChunks(
	w *bufio.Writer,
	chunks []string,
	buried map[string]bool,
) (StatusCounts, error) {
	var counts StatusCounts

	for _, chunk := range chunks {
		if err := processChunk(
			w, chunk, buried, &counts,
		); err != nil {
			return StatusCounts{}, err
		}
	}

	return counts, nil
}

func parseSummaryField(
	counts *StatusCounts, field string,
) error {
	parts := strings.SplitN(
		field, "=", summaryKVFieldCount,
	)
	if len(parts) != summaryKVFieldCount {
		return fmt.Errorf("%w: %s",
			ErrMalformedSummary, field,
		)
	}

	val, err := strconv.Atoi(parts[1])
	if err != nil {
		return fmt.Errorf(
			"parse summary count %s: %w", parts[0], err,
		)
	}

	assignSummaryCount(counts, parts[0], val)

	return nil
}

func assignSummaryCount(
	counts *StatusCounts, key string, val int,
) {
	ptr := statusFieldPtr(counts, key)
	if ptr != nil {
		*ptr = val
	}
}

func writeStatusFile(
	statusPath string,
	chunks []string,
	buried map[string]bool,
) error {
	f, err := os.Create(statusPath)
	if err != nil {
		return fmt.Errorf("create status file: %w", err)
	}
	defer f.Close()

	w := bufio.NewWriter(f)

	counts, err := processAllChunks(w, chunks, buried)
	if err != nil {
		return err
	}

	if err := writeSummaryLine(w, counts); err != nil {
		return err
	}

	return w.Flush()
}

func writeSummaryLine(
	w *bufio.Writer, counts StatusCounts,
) error {
	line := fmt.Sprintf(
		"%s\tuploaded=%d\treplaced=%d\tunmodified=%d\t"+
			"missing=%d\tfailed=%d\tfrozen=%d\torphaned=%d\t"+
			"warning=%d\thardlink=%d\tnot_processed=%d",
		summaryPrefix,
		counts.Uploaded,
		counts.Replaced,
		counts.Unmodified,
		counts.Missing,
		counts.Failed,
		counts.Frozen,
		counts.Orphaned,
		counts.Warning,
		counts.Hardlink,
		counts.NotProcessed,
	)

	_, err := fmt.Fprintln(w, line)

	return err
}

func processChunk(
	w *bufio.Writer,
	chunkPath string,
	buried map[string]bool,
	counts *StatusCounts,
) error {
	reportPath := chunkPath + ".report"
	isBuried := buried[filepath.Base(chunkPath)]

	if !isBuried {
		return streamReport(w, reportPath, counts)
	}

	return processBuriedChunk(
		w, chunkPath, reportPath, counts,
	)
}

func streamReport(
	w *bufio.Writer, reportPath string, counts *StatusCounts,
) error {
	return ParseReportCallback(
		reportPath, func(entry ReportEntry) error {
			tallyStatus(counts, entry.Status)

			return WriteReportEntry(w, entry)
		},
	)
}

func processBuriedChunk(
	w *bufio.Writer,
	chunkPath, reportPath string,
	counts *StatusCounts,
) error {
	reportedLocals, err := streamExistingReport(
		w, reportPath, counts,
	)
	if err != nil {
		return err
	}

	return emitUnprocessedEntries(
		w, chunkPath, reportedLocals, counts,
	)
}

func streamExistingReport(
	w *bufio.Writer, reportPath string, counts *StatusCounts,
) (map[string]bool, error) {
	reported := make(map[string]bool)

	err := ParseReportCallback(
		reportPath, func(entry ReportEntry) error {
			reported[entry.Local] = true
			tallyStatus(counts, entry.Status)

			return WriteReportEntry(w, entry)
		},
	)

	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	return reported, nil
}

func emitUnprocessedEntries(
	w *bufio.Writer,
	chunkPath string,
	reported map[string]bool,
	counts *StatusCounts,
) error {
	f, err := os.Open(chunkPath)
	if err != nil {
		return fmt.Errorf("open chunk file: %w", err)
	}
	defer f.Close()

	return scanChunkForUnprocessed(
		w, f, reported, counts,
	)
}

func scanChunkForUnprocessed(
	w *bufio.Writer,
	r *os.File,
	reported map[string]bool,
	counts *StatusCounts,
) error {
	s := bufio.NewScanner(r)

	for s.Scan() {
		line := s.Text()
		if line == "" {
			continue
		}

		if err := emitIfUnprocessed(
			w, line, reported, counts,
		); err != nil {
			return err
		}
	}

	return s.Err()
}

func emitIfUnprocessed(
	w *bufio.Writer,
	line string,
	reported map[string]bool,
	counts *StatusCounts,
) error {
	local, remote, err := decodeChunkLine(line)
	if err != nil {
		return err
	}

	if reported[local] {
		return nil
	}

	entry := ReportEntry{
		Local:  local,
		Remote: remote,
		Status: statusNotProcessed,
	}

	tallyStatus(counts, statusNotProcessed)

	return WriteReportEntry(w, entry)
}

func decodeChunkLine(line string) (string, string, error) {
	parts := strings.SplitN(line, "\t", chunkFieldCount+1)
	if len(parts) != chunkFieldCount {
		return "", "", ErrMalformedChunkLine
	}

	localBytes, err := base64.StdEncoding.DecodeString(
		parts[0],
	)
	if err != nil {
		return "", "", fmt.Errorf("decode local: %w", err)
	}

	remoteBytes, err := base64.StdEncoding.DecodeString(
		parts[1],
	)
	if err != nil {
		return "", "", fmt.Errorf("decode remote: %w", err)
	}

	return string(localBytes), string(remoteBytes), nil
}

func tallyStatus(counts *StatusCounts, status string) {
	ptr := statusFieldPtr(counts, status)
	if ptr != nil {
		*ptr++
	}
}

func statusFieldPtr(
	counts *StatusCounts, status string,
) *int {
	offset, ok := statusOffsets[status]
	if !ok {
		return nil
	}

	return (*int)(unsafe.Add(
		unsafe.Pointer(counts), offset,
	))
}

func buildStatusOffsets() map[string]uintptr {
	var sc StatusCounts

	base := uintptr(unsafe.Pointer(&sc))

	return map[string]uintptr{
		"uploaded":         uintptr(unsafe.Pointer(&sc.Uploaded)) - base,
		"replaced":         uintptr(unsafe.Pointer(&sc.Replaced)) - base,
		"unmodified":       uintptr(unsafe.Pointer(&sc.Unmodified)) - base,
		"missing":          uintptr(unsafe.Pointer(&sc.Missing)) - base,
		"failed":           uintptr(unsafe.Pointer(&sc.Failed)) - base,
		"frozen":           uintptr(unsafe.Pointer(&sc.Frozen)) - base,
		"orphaned":         uintptr(unsafe.Pointer(&sc.Orphaned)) - base,
		"warning":          uintptr(unsafe.Pointer(&sc.Warning)) - base,
		"hardlink":         uintptr(unsafe.Pointer(&sc.Hardlink)) - base,
		statusNotProcessed: uintptr(unsafe.Pointer(&sc.NotProcessed)) - base,
	}
}

// WriteStatusFromRun reads all chunk report files in
// runDir, writes a combined status file at statusPath,
// and appends a SUMMARY line with tallied counts.
//
// Chunks listed in buriedChunks are treated specially:
// if their report is incomplete or missing, remaining
// entries are emitted as not_processed.
func WriteStatusFromRun(
	runDir, statusPath string, buriedChunks []string,
) error {
	chunks, err := findChunkFiles(runDir)
	if err != nil {
		return err
	}

	buried := makeBuriedSet(buriedChunks)

	return writeStatusFile(
		statusPath, chunks, buried,
	)
}

func findChunkFiles(runDir string) ([]string, error) {
	matches, err := filepath.Glob(
		filepath.Join(runDir, chunkGlob),
	)
	if err != nil {
		return nil, fmt.Errorf("glob chunks: %w", err)
	}

	chunks := filterChunkFiles(matches)
	sort.Strings(chunks)

	return chunks, nil
}

func filterChunkFiles(matches []string) []string {
	chunks := make([]string, 0, len(matches))

	for _, m := range matches {
		base := filepath.Base(m)

		if isChunkAuxFile(base) {
			continue
		}

		chunks = append(chunks, m)
	}

	return chunks
}

func isChunkAuxFile(base string) bool {
	return strings.HasSuffix(base, ".log") ||
		strings.HasSuffix(base, ".out") ||
		strings.HasSuffix(base, ".report")
}

func makeBuriedSet(buriedChunks []string) map[string]bool {
	buried := make(map[string]bool, len(buriedChunks))

	for _, b := range buriedChunks {
		buried[b] = true
	}

	return buried
}
