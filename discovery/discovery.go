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
	"slices"
	"strings"

	"github.com/wtsi-hgi/ibackup/db"
)

const numStatters = 16

var ErrNoFilesDiscovered = errors.New("no files discovered during discovery")

func noCB(*db.File) {}

func Discover(d *db.DB, set *db.Set, cb func(*db.File)) error {
	files, dirs, removedFiles, removedDirs, fofns, fodns, err := readDiscoveryFromDB(d, set)
	if err != nil {
		return err
	}

	if err := d.SetSetDicoveryStarted(set); err != nil {
		return err
	}

	if cb == nil {
		cb = noCB
	}

	if err := doDiscover(d, set, cb, files, dirs, removedFiles, removedDirs, fofns, fodns); err != nil {
		set.Error = err.Error()

		d.SetSetDicoveryCompleted(set) //nolint:errcheck
		d.SetSetError(set)             //nolint:errcheck

		return err
	}

	return d.SetSetDicoveryCompleted(set)
}

func readDiscoveryFromDB(d *db.DB, set *db.Set) ( //nolint:gocyclo
	[]string, []string, []string, []string, []*db.Discover, []*db.Discover, error,
) {
	var (
		files, dirs, removedFiles, removedDirs []string
		fofns, fodns                           []*db.Discover
	)

	if err := d.GetSetDiscovery(set).ForEach(func(d *db.Discover) error {
		switch d.Type {
		case db.DiscoverFile:
			files = append(files, d.Path)
		case db.DiscoverDirectory:
			dirs = append(dirs, d.Path)
		case db.DiscoverFOFN, db.DiscoverFODNBase64, db.DiscoverFOFNQuoted:
			fofns = append(fofns, d)
		case db.DiscoverFODN, db.DiscoverFOFNBase64, db.DiscoverFODNQuoted:
			fodns = append(fodns, d)
		case db.DiscoverRemovedFile:
			removedFiles = append(removedFiles, d.Path)
		case db.DiscoverRemovedDirectory:
			removedDirs = append(removedDirs, d.Path)
		}

		return nil
	}); err != nil {
		return nil, nil, nil, nil, nil, nil, err
	}

	return files, dirs, removedFiles, removedDirs, fofns, fodns, nil
}

func doDiscover(d *db.DB, set *db.Set, cb func(*db.File),
	files, dirs, removedFiles, removedDirs []string, fofns, fodns []*db.Discover) error {
	fodnDirs, err := readFons(set.Transformer, fodns, nil)
	if err != nil {
		return err
	}

	dirs = append(dirs, fodnDirs...)

	filter, err := makeFileFilter(files, dirs, removedFiles, removedDirs)
	if err != nil {
		return err
	}

	fofnFiles, err := readFons(set.Transformer, fofns, filter)
	if err != nil {
		return err
	}

	stattedFiles, err := statFiles(files, dirs, fofnFiles, cb, filter)
	if err != nil {
		return err
	}

	return addFilesToSet(d, set, stattedFiles)
}

func statFiles(files, dirs, fofnFiles []string, cb func(*db.File),
	filter StateMachine[bool]) ([]*db.File, error) {
	errCh := make(chan error)

	statter, err := newStatter()
	if err != nil {
		return nil, err
	}

	statter.WriterAdd(1)
	walkDirs(dirs, statter, filter, errCh)

	go statter.StatFiles(files, fofnFiles)

	statter.Launch(numStatters)

	return collectFiles(statter, cb, errCh)
}

func makeFileFilter(files, dirs, removedFiles, removedDirs []string) (StateMachine[bool], error) {
	t := new(bool)
	f := new(bool)

	*t = true

	lines := make([]PathGroup[bool], 0, len(files)+len(dirs)+len(removedFiles)+len(removedDirs))
	lines = addLines(lines, files, t)
	lines = addLines(lines, dirs, t)
	lines = addLines(lines, removedFiles, f)
	lines = addLines(lines, removedDirs, f)

	return NewStatemachine(lines)
}

func addLines(lines []PathGroup[bool], entries []string, value *bool) []PathGroup[bool] {
	for _, entry := range entries {
		path := toBytes(entry)

		if strings.HasSuffix(entry, "/") {
			path = append(path, '*')
		}

		lines = append(lines, PathGroup[bool]{Path: path, Group: value})
	}

	return lines
}

func walkDirs(dirs []string, statter *Statter, filter StateMachine[bool], errCh chan error) {
	if len(dirs) > 0 {
		statter.WriterAdd(1)

		go func() {
			err := walkDirectories(dirs, filter, statter)

			statter.WriterDone()

			errCh <- err
		}()
	} else {
		close(errCh)
	}
}

func collectFiles(statter *Statter,
	cb func(*db.File), errChan chan error) ([]*db.File, error) {
	var stattedFiles []*db.File //nolint:prealloc

	for file := range statter.Iter() {
		stattedFiles = append(stattedFiles, file)

		cb(file)
	}

	return stattedFiles, <-errChan
}

func addFilesToSet(d *db.DB, set *db.Set, files []*db.File) error {
	if len(files) == 0 {
		return ErrNoFilesDiscovered
	}

	sm, err := buildFileStateMachine(files)
	if err != nil {
		return err
	}

	var removedFiles []*db.File

	if err := d.GetSetFiles(set).ForEach(func(f *db.File) error {
		if m := sm.Match(toBytes(f.LocalPath)); m == nil {
			if set.MonitorRemovals {
				removedFiles = append(removedFiles, f)
			} else {
				f.Status = db.StatusMissing
				files = append(files, f)
			}
		}

		return nil
	}); err != nil {
		return err
	}

	return d.SetSetFiles(set, slices.Values(files), slices.Values(removedFiles))
}

func buildFileStateMachine(files []*db.File) (StateMachine[struct{}], error) {
	exists := new(struct{})

	list := make([]PathGroup[struct{}], 0, len(files))

	for _, f := range files {
		list = append(list, PathGroup[struct{}]{Path: toBytes(f.LocalPath), Group: exists})
	}

	return NewStatemachine(list)
}
