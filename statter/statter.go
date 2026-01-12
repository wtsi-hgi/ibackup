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

package statter

import (
	"errors"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"syscall"

	"github.com/wtsi-hgi/statter/client"
)

var (
	statterExe string

	mu      sync.Mutex
	statter client.Statter
)

func Init(exe string) error {
	mu.Lock()
	defer mu.Unlock()

	var err error

	if exe == "" {
		exe, err = determineStatterEXE()
	}

	statterExe = exe

	return err
}

func determineStatterEXE() (string, error) {
	statter := os.Getenv("IBACKUP_STATTER")
	if statter == "" {
		exe, err := os.Executable()
		if err != nil {
			return "", nil
		}

		statter = filepath.Join(filepath.Dir(exe), "statter")

		if fi, err := os.Stat(statter); err != nil || fi.IsDir() || isExecutable(fi) {
			return exec.LookPath("statter")
		}
	}

	return statter, nil
}

func isExecutable(fi fs.FileInfo) bool {
	u, err := user.Current()
	if err != nil {
		return false
	}

	st := fi.Sys().(*syscall.Stat_t)

	if u.Uid == strconv.FormatUint(uint64(st.Uid), 10) {
		return fi.Mode()&0100 > 0
	}

	groups, err := u.GroupIds()
	if err != nil {
		return false
	}

	if slices.Contains(groups, strconv.FormatUint(uint64(st.Gid), 10)) {
		return fi.Mode()&0010 > 0
	}

	return fi.Mode()&001 > 0
}

func Stat(path string) (fs.FileInfo, error) {
	mu.Lock()
	defer mu.Unlock()

	if statter == nil {
		var err error

		statter, err = client.CreateStatter(statterExe)

		if err != nil {
			return nil, err
		}
	}

	fi, err := statter(path)
	if errors.Is(err, io.EOF) {
		statter = nil
		err = os.ErrDeadlineExceeded
	}

	return fi, err
}

type DirEnt = client.Dirent
type PathCallback = client.PathCallback
type ErrCallback = client.ErrCallback

func Walk(path string, cb PathCallback, errCB ErrCallback) error {
	return client.WalkPath(statterExe, path, cb, errCB)
}
