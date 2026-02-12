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
	"io"
	"os"
)

const countBufSize = 32 * 1024

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

// CountNullTerminated counts the number of null-terminated
// entries in the file at path without per-entry string
// allocation. It reads the file in large buffer chunks and
// counts null bytes. If the file is non-empty and does not end
// with a null byte, the trailing content counts as one
// additional entry (matching ScanNullTerminated semantics).
// An empty file returns 0.
func CountNullTerminated(path string) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, fmt.Errorf("open file: %w", err)
	}
	defer f.Close()

	count, lastByte, err := countNullBytes(f)
	if err != nil {
		return 0, err
	}

	if count == 0 && lastByte == 0 {
		return 0, nil
	}

	if lastByte != 0 {
		count++
	}

	return count, nil
}

func countNullBytes(r io.Reader) (int, byte, error) {
	buf := make([]byte, countBufSize)
	count := 0
	lastByte := byte(0)

	for {
		n, readErr := r.Read(buf)
		if n > 0 {
			count += bytes.Count(buf[:n], []byte{0})
			lastByte = buf[n-1]
		}

		if readErr == io.EOF {
			return count, lastByte, nil
		}

		if readErr != nil {
			return 0, 0, fmt.Errorf("read file: %w", readErr)
		}
	}
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
