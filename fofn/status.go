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
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/wtsi-hgi/ibackup/transfer"
)

const (
	statusNotProcessed = "not_processed"
	summaryPrefix      = "SUMMARY"
	chunkGlob          = "chunk.[0-9]*"
	chunkFieldCount    = 2
	issueLineUnknown   = -1
	issuesSuffix       = ".issues"
)

// ErrMalformedSummary is returned when a summary field
// does not have a key=value format.
var ErrMalformedSummary = errors.New("malformed summary field")

// ErrMalformedChunkLine is returned when a chunk line
// does not have exactly the expected number of fields.
var ErrMalformedChunkLine = errors.New("malformed chunk line")

// statusCounts holds counts per upload status for a completed run.
type statusCounts struct {
	Uploaded     int
	Replaced     int
	Skipped      int
	Missing      int
	Failed       int
	Frozen       int
	Orphaned     int
	Warning      int
	Hardlinks    int
	NotProcessed int
}

// parseStatus reads a status file produced by WriteStatusFromRun and returns
// all entries plus the summary counts from the SUMMARY line.
func parseStatus(path string) (map[string]ReportEntry, statusCounts, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, statusCounts{}, err
	}
	defer f.Close()

	return scanStatusFile(f)
}

// parseStatusCounts reads a status file produced by WriteStatusFromRun and
// returns the summary counts from the SUMMARY line.
func parseStatusCounts(path string) (statusCounts, error) {
	f, err := os.Open(path)
	if err != nil {
		return statusCounts{}, err
	}
	defer f.Close()

	return scanStatusFileCounts(f)
}

func scanStatusFile(r io.Reader) (map[string]ReportEntry, statusCounts, error) {
	var (
		entries = make(map[string]ReportEntry)
		counts  statusCounts
	)

	s := bufio.NewScanner(r)

	for s.Scan() {
		line := s.Text()
		if line == "" {
			continue
		}

		var err error

		counts, err = handleStatusLine(line, entries, counts)
		if err != nil {
			return nil, statusCounts{}, err
		}
	}

	if err := s.Err(); err != nil {
		return nil, statusCounts{}, err
	}

	return entries, counts, nil
}

func handleStatusLine(line string, entries map[string]ReportEntry, counts statusCounts) (statusCounts, error) {
	if strings.HasPrefix(line, summaryPrefix+"\t") {
		return parseSummaryLine(line)
	}

	entry, err := parseReportLine(line)
	if err != nil {
		return statusCounts{}, fmt.Errorf("parse status line: %w", err)
	}

	entries[entry.Local] = entry

	return counts, nil
}

func scanStatusFileCounts(r io.Reader) (statusCounts, error) {
	s := bufio.NewScanner(r)

	for s.Scan() {
		line := s.Text()
		if line == "" {
			continue
		}

		if strings.HasPrefix(line, summaryPrefix+"\t") {
			return parseSummaryLine(line)
		}
	}

	return statusCounts{}, s.Err()
}

func parseSummaryLine(line string) (statusCounts, error) {
	var counts statusCounts

	fields := strings.Split(line, "\t")

	for _, field := range fields[1:] {
		if err := parseSummaryField(&counts, field); err != nil {
			return statusCounts{}, err
		}
	}

	return counts, nil
}

func processAllChunks(w *bufWriter, chunks []string, buried map[string]bool) (statusCounts, []string, error) {
	var counts statusCounts

	issues := make([]string, 0)

	for _, chunk := range chunks {
		if err := processChunk(w, chunk, buried, &counts, &issues); err != nil {
			return statusCounts{}, nil, err
		}
	}

	return counts, issues, nil
}

func parseSummaryField(
	counts *statusCounts, field string,
) error {
	key, valStr, ok := strings.Cut(field, "=")
	if !ok {
		return fmt.Errorf("%w: %s", ErrMalformedSummary, field)
	}

	val, err := strconv.Atoi(valStr)
	if err != nil {
		return fmt.Errorf("parse summary count %s: %w", key, err)
	}

	assignSummaryCount(counts, key, val)

	return nil
}

func assignSummaryCount( //nolint:gocyclo,cyclop,funlen
	counts *statusCounts, key string, val int,
) {
	switch key {
	case "uploaded":
		counts.Uploaded = val
	case "replaced":
		counts.Replaced = val
	case "unmodified":
		counts.Skipped = val
	case "missing":
		counts.Missing = val
	case "failed":
		counts.Failed = val
	case "frozen":
		counts.Frozen = val
	case "orphaned":
		counts.Orphaned = val
	case "warning":
		counts.Warning = val
	case "hardlink":
		counts.Hardlinks = val
	case statusNotProcessed:
		counts.NotProcessed = val
	}
}

func writeStatus(w *bufWriter, chunks []string, buried map[string]bool) ([]string, error) {
	counts, issues, err := processAllChunks(w, chunks, buried)
	if err != nil {
		return nil, err
	}

	if err := writeSummaryLine(w, counts); err != nil {
		return nil, err
	}

	return issues, nil
}

func writeSummaryLine(w *bufWriter, counts statusCounts) error {
	line := fmt.Sprintf(
		"%s\tuploaded=%d\treplaced=%d\tunmodified=%d\t"+
			"missing=%d\tfailed=%d\tfrozen=%d\torphaned=%d\t"+
			"warning=%d\thardlink=%d\tnot_processed=%d",
		summaryPrefix,
		counts.Uploaded,
		counts.Replaced,
		counts.Skipped,
		counts.Missing,
		counts.Failed,
		counts.Frozen,
		counts.Orphaned,
		counts.Warning,
		counts.Hardlinks,
		counts.NotProcessed,
	)

	_, err := fmt.Fprintln(w, line)

	return err
}

func processChunk(
	w *bufWriter,
	chunkPath string,
	buried map[string]bool,
	counts *statusCounts,
	issues *[]string,
) error {
	reportPath := chunkPath + ".report"
	isBuried := buried[filepath.Base(chunkPath)]

	reportedLocals, hasIssues, err := streamReportBestEffort(
		w, chunkPath, reportPath, counts, issues,
	)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			addIssue(issues, chunkPath, reportPath, issueLineUnknown, fmt.Errorf("read report: %w", err))
		}

		hasIssues = true
	}

	if !isBuried && !hasIssues {
		return nil
	}

	return emitUnprocessedEntries(w, chunkPath, reportedLocals, counts)
}

func addIssue(issues *[]string, chunkPath, reportPath string, lineNo int, err error) {
	*issues = append(*issues, formatIssue(chunkPath, reportPath, lineNo, err))
}

func formatIssue(chunkPath, reportPath string, lineNo int, err error) string {
	if lineNo == issueLineUnknown {
		return fmt.Sprintf("chunk=%s report=%s error=%s", filepath.Base(chunkPath), reportPath, err)
	}

	return fmt.Sprintf("chunk=%s report=%s line=%d error=%s", filepath.Base(chunkPath), reportPath, lineNo, err)
}

func streamReportBestEffort(
	w *bufWriter,
	chunkPath, reportPath string,
	counts *statusCounts,
	issues *[]string,
) (map[string]bool, bool, error) {
	reported := make(map[string]bool)

	f, err := os.Open(reportPath)
	if err != nil {
		return reported, true, err
	}
	defer f.Close()

	return scanReportBestEffort(w, f, chunkPath, reportPath, counts, issues, reported)
}

func scanReportBestEffort(
	w *bufWriter,
	f *os.File,
	chunkPath, reportPath string,
	counts *statusCounts,
	issues *[]string,
	reported map[string]bool,
) (map[string]bool, bool, error) {
	s := bufio.NewScanner(f)

	hasIssues, lineNo, err := scanReportLines(s, w, chunkPath, reportPath, counts, issues, reported)
	if err != nil {
		return reported, hasIssues, err
	}

	handleReportScanError(s.Err(), issues, chunkPath, reportPath, lineNo, &hasIssues)

	return reported, hasIssues, nil
}

func handleReportScanError(
	scanErr error,
	issues *[]string,
	chunkPath, reportPath string,
	lineNo int,
	hasIssues *bool,
) {
	if scanErr == nil {
		return
	}

	*hasIssues = true

	addIssue(issues, chunkPath, reportPath, lineNo, fmt.Errorf("scan report: %w", scanErr))
}

func scanReportLines(
	s *bufio.Scanner,
	w *bufWriter,
	chunkPath, reportPath string,
	counts *statusCounts,
	issues *[]string,
	reported map[string]bool,
) (bool, int, error) {
	hasIssues := false
	lineNo := 0

	for s.Scan() {
		lineNo++

		issued, err := processReportTextLine(
			w, s.Text(), lineNo,
			chunkPath, reportPath,
			counts, issues, reported,
		)
		if err != nil {
			return hasIssues, lineNo, err
		}

		if issued {
			hasIssues = true
		}
	}

	return hasIssues, lineNo, nil
}

func processReportTextLine(
	w *bufWriter,
	line string,
	lineNo int,
	chunkPath, reportPath string,
	counts *statusCounts,
	issues *[]string,
	reported map[string]bool,
) (bool, error) {
	if line == "" {
		return false, nil
	}

	return processReportLine(
		w, line, lineNo,
		chunkPath, reportPath,
		counts, issues, reported,
	)
}

func processReportLine(
	w *bufWriter,
	line string,
	lineNo int,
	chunkPath, reportPath string,
	counts *statusCounts,
	issues *[]string,
	reported map[string]bool,
) (bool, error) {
	entry, parseErr := parseReportLine(line)
	if parseErr != nil {
		addIssue(issues, chunkPath, reportPath, lineNo,
			fmt.Errorf("parse report line: %w", parseErr))

		return true, nil
	}

	reported[entry.Local] = true
	tallyStatus(counts, entry.Status)

	if writeErr := WriteReportEntry(w, entry); writeErr != nil {
		return false, writeErr
	}

	return false, nil
}

func emitUnprocessedEntries(
	w *bufWriter,
	chunkPath string,
	reported map[string]bool,
	counts *statusCounts,
) error {
	f, err := os.Open(chunkPath)
	if err != nil {
		return fmt.Errorf("open chunk file: %w", err)
	}
	defer f.Close()

	return scanChunkForUnprocessed(w, f, reported, counts)
}

func scanChunkForUnprocessed(
	w *bufWriter,
	r *os.File,
	reported map[string]bool,
	counts *statusCounts,
) error {
	s := bufio.NewScanner(r)

	for s.Scan() {
		line := s.Text()
		if line == "" {
			continue
		}

		if err := emitIfUnprocessed(w, line, reported, counts); err != nil {
			return err
		}
	}

	return s.Err()
}

func emitIfUnprocessed(
	w *bufWriter,
	line string,
	reported map[string]bool,
	counts *statusCounts,
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

	localBytes, err := base64.StdEncoding.DecodeString(parts[0])
	if err != nil {
		return "", "", fmt.Errorf("decode local: %w", err)
	}

	remoteBytes, err := base64.StdEncoding.DecodeString(parts[1])
	if err != nil {
		return "", "", fmt.Errorf("decode remote: %w", err)
	}

	return string(localBytes), string(remoteBytes), nil
}

func tallyStatus(counts *statusCounts, status transfer.RequestStatus) { //nolint:gocyclo,cyclop,funlen
	switch status {
	case transfer.RequestStatusUploaded:
		counts.Uploaded++
	case transfer.RequestStatusReplaced:
		counts.Replaced++
	case transfer.RequestStatusUnmodified:
		counts.Skipped++
	case transfer.RequestStatusMissing:
		counts.Missing++
	case transfer.RequestStatusFailed:
		counts.Failed++
	case transfer.RequestStatusFrozen:
		counts.Frozen++
	case transfer.RequestStatusOrphaned:
		counts.Orphaned++
	case transfer.RequestStatusWarning:
		counts.Warning++
	case transfer.RequestStatusHardlinkSkipped:
		counts.Hardlinks++
	case statusNotProcessed:
		counts.NotProcessed++
	}
}

// writeStatusFromRun reads all chunk report files in runDir, writes a combined
// status file at statusPath, and appends a SUMMARY line with tallied counts.
//
// Chunks listed in buriedChunks are treated specially: if their report is
// incomplete or missing, remaining entries are emitted as not_processed.
func writeStatusFromRun(runDir, statusPath string, buriedChunks []string) error {
	chunks, err := findChunkFiles(runDir)
	if err != nil {
		return err
	}

	buried := makeBuriedSet(buriedChunks)

	return writeStatusFile(statusPath, chunks, buried)
}

func findChunkFiles(runDir string) ([]string, error) {
	matches, err := filepath.Glob(filepath.Join(runDir, chunkGlob))
	if err != nil {
		return nil, fmt.Errorf("glob chunks: %w", err)
	}

	chunks := filterChunkFiles(matches)
	slices.Sort(chunks)

	if _, err := os.Lstat(filepath.Join(runDir, unmodifiedReport)); err == nil {
		chunks = append(chunks, filepath.Join(runDir, unmodified))
	}

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

func writeStatusFile(statusPath string, chunks []string, buried map[string]bool) error {
	tmpPath := statusPath + ".tmp"

	issues, err := writeStatusToFile(tmpPath, chunks, buried)
	if err != nil {
		os.Remove(tmpPath)

		return err
	}

	if err := os.Rename(tmpPath, statusPath); err != nil {
		os.Remove(tmpPath)

		return fmt.Errorf("rename status file: %w", err)
	}

	if err := writeIssuesFile(statusPath+issuesSuffix, issues); err != nil {
		return err
	}

	return nil
}

func writeStatusToFile(path string, chunks []string, buried map[string]bool) (issues []string, err error) {
	w, err := newBufWriter(path)
	if err != nil {
		return nil, fmt.Errorf("create status file: %w", err)
	}

	defer func() {
		if errr := w.Close(); err == nil {
			err = errr
		}
	}()

	issues, err = writeStatus(w, chunks, buried)
	if err != nil {
		return nil, err
	}

	return issues, nil
}

func writeIssuesFile(path string, issues []string) (err error) {
	if len(issues) == 0 {
		return removeIssuesFile(path)
	}

	tmpPath := path + ".tmp"

	bw, err := newBufWriter(tmpPath)
	if err != nil {
		return fmt.Errorf("create issues file: %w", err)
	}

	if err := writeAllIssues(bw, issues); err != nil {
		bw.Close()

		return err
	}

	if err := bw.Close(); err != nil {
		return fmt.Errorf("close issues file: %w", err)
	}

	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("rename issues file: %w", err)
	}

	return nil
}

func removeIssuesFile(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove issues file: %w", err)
	}

	return nil
}

func writeAllIssues(w io.Writer, issues []string) error {
	for _, issue := range issues {
		if _, err := fmt.Fprintln(w, issue); err != nil {
			return fmt.Errorf("write issues file: %w", err)
		}
	}

	return nil
}
