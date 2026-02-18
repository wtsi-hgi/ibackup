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
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

const reportFieldCount = 4

// ErrMalformedLine is returned when a report line does not
// have exactly the expected number of tab-separated fields.
var ErrMalformedLine = errors.New("malformed report line")

// ReportEntry holds the details of a single report line, describing the local
// and remote paths, upload status, and any error message.
type ReportEntry struct {
	Local  string
	Remote string
	Status string
	Error  string
}

// ParseReportLine parses a tab-separated report line into a
// ReportEntry. It expects exactly 4 fields separated by
// literal tabs. Returns an error if the line is malformed.
func ParseReportLine(line string) (ReportEntry, error) {
	fields := strings.Split(line, "\t")
	if len(fields) != reportFieldCount {
		return ReportEntry{}, ErrMalformedLine
	}

	local, err := unquoteField(fields[0], "local")
	if err != nil {
		return ReportEntry{}, err
	}

	remote, err := unquoteField(fields[1], "remote")
	if err != nil {
		return ReportEntry{}, err
	}

	errMsg, err := unquoteField(fields[3], "error")
	if err != nil {
		return ReportEntry{}, err
	}

	return ReportEntry{
		Local:  local,
		Remote: remote,
		Status: fields[2],
		Error:  errMsg,
	}, nil
}

// WriteReportEntry writes a single ReportEntry line to the
// given writer. Each entry is followed by a newline.
func WriteReportEntry(w io.Writer, entry ReportEntry) error {
	_, err := fmt.Fprintln(w, FormatReportLine(entry))

	return err
}

// FormatReportLine formats a ReportEntry as a tab-separated line. Local, Remote
// and Error fields are quoted using strconv.Quote; Status is plain text. No
// trailing newline is appended.
func FormatReportLine(entry ReportEntry) string {
	return strconv.Quote(entry.Local) + "\t" +
		strconv.Quote(entry.Remote) + "\t" +
		entry.Status + "\t" +
		strconv.Quote(entry.Error)
}

// CollectReport reads all entries from a report file into a slice. This is a
// convenience wrapper around ParseReportCallback for tests and small files.
func CollectReport(path string) ([]ReportEntry, error) {
	var entries []ReportEntry

	err := ParseReportCallback(path, func(entry ReportEntry) error {
		entries = append(entries, entry)

		return nil
	})

	return entries, err
}

// ParseReportCallback reads a report file line by line, parsing each line into
// a ReportEntry and passing it to the callback. It returns an error if the file
// cannot be opened or if any line fails to parse.
func ParseReportCallback(
	path string, cb func(ReportEntry) error,
) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	s := bufio.NewScanner(f)

	for s.Scan() {
		line := s.Text()
		if line == "" {
			continue
		}

		entry, parseErr := ParseReportLine(line)
		if parseErr != nil {
			return fmt.Errorf("parse report line: %w", parseErr)
		}

		if cbErr := cb(entry); cbErr != nil {
			return cbErr
		}
	}

	return s.Err()
}

func unquoteField(field, name string) (string, error) {
	val, err := strconv.Unquote(field)
	if err != nil {
		return "", fmt.Errorf("unquote %s: %w", name, err)
	}

	return val, nil
}
