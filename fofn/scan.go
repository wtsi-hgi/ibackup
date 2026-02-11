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
	"strconv"
)

const fofnFilename = "fofn"

// SubDir represents a subdirectory that contains a fofn file.
type SubDir struct {
	Path string // absolute path to subdirectory
}

// ScanForFOFNs returns subdirectories of watchDir that
// contain a file named "fofn".
func ScanForFOFNs(watchDir string) ([]SubDir, error) {
	entries, err := os.ReadDir(watchDir)
	if err != nil {
		return nil, fmt.Errorf("read watch dir: %w", err)
	}

	var result []SubDir

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		subPath := filepath.Join(watchDir, entry.Name())
		fofnPath := filepath.Join(subPath, fofnFilename)

		if _, err := os.Stat(fofnPath); err == nil {
			result = append(result, SubDir{Path: subPath})
		}
	}

	return result, nil
}

// NeedsProcessing checks whether the fofn in the given SubDir needs to be
// processed. It compares the fofn file's mtime against the newest numeric run
// directory name. Returns true and the mtime if processing is needed, or false
// and 0 if the newest run directory already matches the fofn mtime.
func NeedsProcessing(subDir SubDir) (bool, int64, error) {
	fofnPath := filepath.Join(subDir.Path, fofnFilename)

	info, err := os.Stat(fofnPath)
	if err != nil {
		return false, 0, fmt.Errorf("stat fofn: %w", err)
	}

	mtime := info.ModTime().Unix()

	newest, found, err := newestRunDir(subDir.Path)
	if err != nil {
		return false, 0, err
	}

	if !found || newest != mtime {
		return true, mtime, nil
	}

	return false, 0, nil
}

// newestRunDir finds the largest numeric directory name in dir. Returns the
// value, whether one was found, and any error.
func newestRunDir(dir string) (int64, bool, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, false, fmt.Errorf("read dir for run dirs: %w", err)
	}

	var (
		best  int64
		found bool
	)

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		n, err := strconv.ParseInt(entry.Name(), 10, 64)
		if err != nil {
			continue
		}

		if !found || n > best {
			best = n
			found = true
		}
	}

	return best, found, nil
}
