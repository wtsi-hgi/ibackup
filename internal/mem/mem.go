/*******************************************************************************
 * Copyright (c) 2026 Genome Research Ltd.
 *
 * Authors: Michael Woolnough <mw31@sanger.ac.uk>
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

package mem

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
)

func getMemLine(prefix []byte) (uint64, error) {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0, fmt.Errorf("error opening meminfo: %w", err)
	}

	defer f.Close()

	br := bufio.NewReader(f)

	for {
		line, err := br.ReadBytes('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				return 0, fmt.Errorf("error parsing meminfo: %w", io.ErrUnexpectedEOF)
			}

			return 0, err
		}

		if !bytes.HasPrefix(line, prefix) {
			continue
		}

		return parseNumberLine(bytes.TrimPrefix(line, prefix))
	}
}

func parseNumberLine(line []byte) (uint64, error) {
	maxMem, err := strconv.ParseUint(string(
		bytes.TrimSuffix(bytes.TrimSpace(line), []byte(" kB")),
	), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("error parsing meminfo: %w", err)
	}

	return maxMem, nil
}

// GetAvailableMemory returns the amount of currently available system memory in
// KiB.
func GetAvailableMemory() (uint64, error) {
	return getMemLine([]byte("MemFree:"))
}

// GetAvailableMemory returns the total amount of system memory in KiB.
func GetTotalMemory() (uint64, error) {
	return getMemLine([]byte("MemTotal:"))
}
