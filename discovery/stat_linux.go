/*******************************************************************************
 * Copyright (c) 2025 Genome Research Ltd.
 *
 * Author: Michael Woolnough <mw31@sanger.ac.uk>
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

package discovery

import (
	"errors"
	"io/fs"
	"os"
	"syscall"

	"github.com/wtsi-hgi/ibackup/db"
	"golang.org/x/sys/unix"
)

const (
	statFlags = unix.AT_SYMLINK_NOFOLLOW | unix.AT_STATX_SYNC_AS_STAT
	statMask  = unix.STATX_BTIME | unix.STATX_INO | unix.STATX_MTIME |
		unix.STATX_SIZE | unix.STATX_TYPE | unix.STATX_MNT_ID
)

func (s *Statter) stat(path string) (*db.File, error) { //nolint:funlen,gocyclo
	var resp unix.Statx_t

	err := unix.Statx(0, path, statFlags, statMask, &resp)
	if errors.Is(err, syscall.ENOENT) {
		return nil, fs.ErrNotExist
	} else if err != nil {
		return nil, err
	}

	mode := db.Unknown

	var link string

	switch resp.Mode {
	case unix.S_IFBLK, unix.S_IFCHR, unix.S_IFIFO, unix.S_IFSOCK:
		mode = db.Abnormal
	case unix.S_IFDIR:
		mode = db.Directory
	case unix.S_IFLNK:
		mode = db.Symlink

		link, err = os.Readlink(path)
		if err != nil {
			return nil, err
		}
	case unix.S_IFREG:
		mode = db.Regular
	}

	return &db.File{
		LocalPath:   path,
		Btime:       resp.Btime.Sec,
		Mtime:       resp.Mtime.Sec,
		Size:        resp.Size,
		Inode:       resp.Ino,
		Type:        mode,
		SymlinkDest: link,
		MountPount:  s.mountpoints[resp.Mnt_id],
	}, nil
}
