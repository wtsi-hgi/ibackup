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

package ownership

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

const (
	dirMode  = 0750
	fileMode = 0640
)

// ErrUnsupportedPlatform is returned when the platform
// does not support syscall.Stat_t.
var ErrUnsupportedPlatform = errors.New("unsupported platform")

// GetDirGID returns the GID of the given directory.
func GetDirGID(dir string) (int, error) {
	info, err := os.Stat(dir)
	if err != nil {
		return 0, fmt.Errorf("stat dir: %w", err)
	}

	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, ErrUnsupportedPlatform
	}

	return int(stat.Gid), nil
}

// CreateDirWithGID creates a directory with mode 0750
// and sets its group to the given GID.
func CreateDirWithGID(path string, gid int) error {
	if err := os.Mkdir(path, dirMode); err != nil {
		return fmt.Errorf("mkdir: %w", err)
	}

	if err := os.Chown(path, -1, gid); err != nil {
		return fmt.Errorf("chown dir: %w", err)
	}

	return nil
}

// WriteFileWithGID writes content to a file with mode
// 0640 and sets its group to the given GID.
func WriteFileWithGID(path string, content []byte, gid int) error {
	if err := os.WriteFile(path, content, fileMode); err != nil {
		return fmt.Errorf("write file: %w", err)
	}

	if err := os.Chown(path, -1, gid); err != nil {
		return fmt.Errorf("chown file: %w", err)
	}

	return nil
}
