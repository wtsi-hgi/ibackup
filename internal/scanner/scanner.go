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

// Package scanner provides functions for scanning files containing
// null-terminated or newline-terminated paths.
package scanner

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
)

// ScanNulls is a bufio.SplitFunc like bufio.ScanLines, but it
// splits on null characters instead of newlines.
func ScanNulls(
	data []byte, atEOF bool,
) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}

	if i := bytes.IndexByte(data, '\000'); i >= 0 {
		return i + 1, data[0:i], nil
	}

	if atEOF {
		return len(data), data, nil
	}

	return 0, nil, nil
}

// FofnLineSplitter returns a bufio.SplitFunc that splits on
// newlines by default, or on null characters if onNull is true.
func FofnLineSplitter(onNull bool) bufio.SplitFunc {
	if onNull {
		return ScanNulls
	}

	return bufio.ScanLines
}

// ScanNullTerminated opens the file at path and calls cb for
// each null-terminated entry. It streams the file without loading
// it all into memory. If cb returns a non-nil error, scanning
// stops and that error is returned.
func ScanNullTerminated(
	path string, cb func(entry string) error,
) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open file: %w", err)
	}
	defer f.Close()

	s := bufio.NewScanner(f)
	s.Split(ScanNulls)

	for s.Scan() {
		if err := cb(s.Text()); err != nil {
			return err
		}
	}

	return s.Err()
}

// CollectNullTerminated reads all null-terminated entries from
// the file at path and returns them as a slice.
func CollectNullTerminated(path string) ([]string, error) {
	var entries []string

	err := ScanNullTerminated(path, func(entry string) error {
		entries = append(entries, entry)

		return nil
	})

	return entries, err
}
