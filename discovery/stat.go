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
	"iter"
	"sync/atomic"

	"github.com/moby/sys/mountinfo"
	"github.com/wtsi-hgi/ibackup/db"
)

type Statter struct {
	mountpoints      map[uint64]string
	requests         chan string
	results          chan *db.File
	writers, readers atomic.Int64
	*ownerCache
}

func newStatter() (*Statter, error) {
	mps, err := getMountPoints()
	if err != nil {
		return nil, err
	}

	return &Statter{
		mountpoints: mps,
		requests:    make(chan string),
		results:     make(chan *db.File),
		ownerCache:  newOwnerCache(),
	}, nil
}

func (s *Statter) WriterAdd(n int64) {
	s.writers.Add(n)
}

func (s *Statter) WriterDone() {
	if s.writers.Add(-1) == 0 {
		close(s.requests)
	}
}

func (s *Statter) readerDone() {
	if s.readers.Add(-1) == 0 {
		close(s.results)
	}
}

func (s *Statter) Launch(n uint8) {
	s.readers.Add(int64(n))

	for range n {
		go s.statter()
	}
}

func (s *Statter) statter() {
	defer s.readerDone()

	for {
		req, ok := <-s.requests
		if !ok {
			break
		}

		s.statFile(req)
	}
}

func (s *Statter) statFile(req string) {
	if file, err := s.stat(req); errors.Is(err, fs.ErrNotExist) { //nolint:nestif
		s.PassFile(&db.File{LocalPath: req, Status: db.StatusMissing, LastError: err.Error()})
	} else if err != nil { //nolint:gocritic
		s.PassFile(&db.File{LocalPath: req, Status: db.StatusSkipped, LastError: err.Error()})
	} else if file.Type == db.Directory {
		s.PassFile(&db.File{LocalPath: req, Status: db.StatusSkipped, LastError: "is directory"})
	} else {
		s.PassFile(file)
	}
}

func (s *Statter) StatFiles(fons ...[]string) {
	defer s.WriterDone()

	for _, files := range fons {
		for _, file := range files {
			s.StatFile(file)
		}
	}
}

func (s *Statter) StatFile(file string) {
	s.requests <- file
}

func (s *Statter) PassFile(file *db.File) {
	s.results <- file
}

func (s *Statter) Iter() iter.Seq[*db.File] {
	return func(yield func(*db.File) bool) {
		for file := range s.results {
			if !yield(file) {
				return
			}
		}
	}
}

func getMountPoints() (map[uint64]string, error) {
	mountList := make(map[uint64]string)

	if _, err := mountinfo.GetMounts(func(info *mountinfo.Info) (bool, bool) {
		switch info.FSType {
		case "devpts", "devtmpfs", "cgroup", "rpc_pipefs", "fusectl",
			"binfmt_misc", "sysfs", "debugfs", "tracefs", "proc", "securityfs",
			"pstore", "mqueue", "hugetlbfs", "configfs":
		default:
			mountList[uint64(info.ID)] = info.Mountpoint //nolint:gosec
		}

		return true, false
	}); err != nil {
		return nil, err
	}

	return mountList, nil
}
