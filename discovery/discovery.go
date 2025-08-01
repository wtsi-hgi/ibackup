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
	"bufio"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
	"strings"
	"unsafe"

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

func readAllFons(transformer *db.Transformer, fodns, fofns []*db.Discover) ([]string, []string, error) {
	fodnDirs, err := readFons(transformer, fodns, true)
	if err != nil {
		return nil, nil, err
	}

	fofnFiles, err := readFons(transformer, fofns, false)
	if err != nil {
		return nil, nil, err
	}

	return fodnDirs, fofnFiles, nil
}

func readFons(transformer *db.Transformer, fons []*db.Discover, dirs bool) ([]string, error) {
	var list []string

	for _, fon := range fons {
		contents, err := ReadFon(transformer, fon, dirs)
		if err != nil {
			return nil, err
		}

		list = append(list, contents...)
	}

	return list, nil
}

func ReadFon(transformer *db.Transformer, d *db.Discover, dirs bool) ([]string, error) {
	decoder := nullDecoder

	switch d.Type { //nolint:exhaustive
	case db.DiscoverFODNBase64, db.DiscoverFOFNBase64:
		decoder = base64Decoder
	case db.DiscoverFODNQuoted, db.DiscoverFOFNQuoted:
		decoder = strconv.Unquote
	}

	f, err := os.Open(d.Path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	return readLines(transformer, f, decoder, dirs)
}

func nullDecoder(str string) (string, error) {
	return str, nil
}

func base64Decoder(str string) (string, error) {
	fn, err := base64.StdEncoding.DecodeString(str)
	if err != nil {
		return "", err
	}

	return toString(fn), nil
}

func toString(b []byte) string {
	return unsafe.String(unsafe.SliceData(b), len(b))
}

func toBytes(b string) []byte {
	return unsafe.Slice(unsafe.StringData(b), len(b))
}

//nolint:gocognit,gocyclo
func readLines(trns *db.Transformer, r io.Reader, decoder func(string) (string, error), dirs bool) ([]string, error) {
	var lines []string //nolint:prealloc

	buf := bufio.NewReader(r)

	for {
		line, err := buf.ReadBytes('\n')
		if errors.Is(err, io.EOF) {
			return lines, nil
		} else if err != nil {
			return nil, err
		}

		decoded, err := decoder(toString(line))
		if err != nil {
			return nil, err
		}

		if dirs && !strings.HasSuffix(decoded, "/") {
			decoded += "/"
		}

		if !trns.Match(decoded) {
			return nil, fmt.Errorf("path: %s: %w", decoded, db.ErrInvalidTransformPath)
		}

		lines = append(lines, decoded)
	}
}

func doDiscover(d *db.DB, set *db.Set, cb func(*db.File),
	files, dirs, removedFiles, removedDirs []string, fofns, fodns []*db.Discover) error {
	fodnDirs, fofnFiles, err := readAllFons(set.Transformer, fodns, fofns)
	if err != nil {
		return err
	}

	dirs = append(dirs, fodnDirs...)
	errCh := make(chan error, 1)

	statter, err := newStatter()
	if err != nil {
		return err
	}

	statter.WriterAdd(2) //nolint:mnd

	if err = walkDirs(files, dirs, removedFiles, removedDirs, set.MonitorRemovals, statter, errCh); err != nil {
		return err
	}

	go statter.StatFiles(files, fofnFiles)

	statter.Launch(numStatters)

	stattedFiles, excludedFiles, err := collectFiles(set.Transformer, statter, cb, errCh)
	if err != nil {
		return err
	}

	return addFilesToSet(d, set, stattedFiles, excludedFiles)
}

func walkDirs(files, dirs, removedFiles, removedDirs []string,
	monitorRemovals bool, statter *Statter, errCh chan error) error {
	defer close(errCh)
	defer statter.WriterDone()

	if len(dirs) > 0 {
		filter, err := makeFileFilter(files, dirs, removedFiles, removedDirs)
		if err != nil {
			return err
		}

		go func() {
			errCh <- walkDirectories(dirs, filter, monitorRemovals, statter)
		}()
	}

	return nil
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

func collectFiles(transformer *db.Transformer, statter *Statter,
	cb func(*db.File), errChan chan error) ([]*db.File, []string, error) {
	var (
		stattedFiles  []*db.File
		excludedFiles []string
	)

	for file := range statter.Iter() {
		switch file.Status {
		default:
			stattedFiles = append(stattedFiles, file)
		case db.StatusSkipped:
			file.RemotePath, _ = transformer.Transform(file.LocalPath) //nolint:errcheck
			excludedFiles = append(excludedFiles, file.LocalPath)
		}

		cb(file)
	}

	return stattedFiles, excludedFiles, <-errChan
}

func addFilesToSet(d *db.DB, set *db.Set, files []*db.File, excluded []string) error {
	if len(files) == 0 {
		return ErrNoFilesDiscovered
	}

	sm, err := buildFileStateMachine(files, excluded)
	if err != nil {
		return err
	}

	var removedFiles []*db.File

	if err := d.GetSetFiles(set).ForEach(func(f *db.File) error {
		if m := sm.Match(toBytes(f.LocalPath)); m == nil {
			f.Status = db.StatusMissing
			files = append(files, f)
		} else if *m {
			removedFiles = append(removedFiles, f)
		}

		return nil
	}); err != nil {
		return err
	}

	return d.SetSetFiles(set, slices.Values(files), slices.Values(removedFiles))
}

func buildFileStateMachine(files []*db.File, excluded []string) (StateMachine[bool], error) {
	exists := new(bool)
	removed := new(bool)
	*removed = true

	list := make([]PathGroup[bool], 0, len(files)+len(excluded))

	for _, f := range files {
		list = append(list, PathGroup[bool]{Path: toBytes(f.LocalPath), Group: exists})
	}

	list = addLines(list, excluded, removed)

	return NewStatemachine(list)
}
