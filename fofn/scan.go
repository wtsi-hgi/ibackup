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
	"fmt"
	"os"
	"path/filepath"
)

const fofnFilename = "fofn"

// SubDir represents a subdirectory that contains a fofn file. FofnMtime holds
// the fofn's modification time as a Unix timestamp, populated during scan so
// that callers need not stat each fofn separately.
type SubDir struct {
	Path      string // absolute path to subdirectory
	FofnMtime int64  // Unix mtime of the fofn file
}

// ScanForFOFNs returns subdirectories of watchDir that contain a file named
// "fofn". Each returned SubDir includes the fofn's mtime, avoiding a separate
// stat call per directory during the poll cycle.
func ScanForFOFNs(watchDir string) ([]SubDir, error) {
	if _, err := os.Stat(watchDir); err != nil {
		return nil, err
	}

	matches, err := filepath.Glob(filepath.Join(watchDir, "*", fofnFilename))
	if err != nil {
		return nil, fmt.Errorf("glob fofns: %w", err)
	}

	result := make([]SubDir, 0, len(matches))

	for _, m := range matches {
		info, statErr := os.Stat(m)
		if statErr != nil {
			return nil, fmt.Errorf("stat fofn: %w", statErr)
		}

		result = append(result, SubDir{
			Path:      filepath.Dir(m),
			FofnMtime: info.ModTime().Unix(),
		})
	}

	return result, nil
}
