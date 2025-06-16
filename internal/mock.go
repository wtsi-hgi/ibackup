/*******************************************************************************
 * Copyright (c) 2025 Genome Research Ltd.
 *
 * Author: Rosie Kern <rk18@sanger.ac.uk>
 * Author: Iaroslav Popov <ip13@sanger.ac.uk>
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

package internal

import (
	"io"
	"maps"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/wtsi-hgi/ibackup/baton/meta"
	"github.com/wtsi-hgi/ibackup/errs"
)

const ErrMockStatFail = "stat fail"
const ErrMockPutFail = "put fail"
const ErrMockMetaFail = "meta fail"
const ErrFileDoesNotExist = "file does not exist"

// LocalHandler satisfies the Handler interface, treating "Remote" as local
// paths and moving from Local to Remote for the Put().
type LocalHandler struct {
	Connected   bool
	Cleaned     bool
	Collections []string
	Meta        map[string]map[string]string
	statFail    string
	putFail     string
	putSlow     string
	putDur      time.Duration
	metaFail    string
	removeSlow  bool
	mu          sync.RWMutex
}

// GetLocalHandler returns a Handler that doesn't actually interact with iRODS,
// but instead simply treats "Remote" as local paths and copies from Local to
// Remote for any Put()s. For use during tests.
func GetLocalHandler() *LocalHandler {
	return &LocalHandler{
		Meta: make(map[string]map[string]string),
	}
}

// Cleanup just records this was called.
func (l *LocalHandler) Cleanup() {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.Cleaned = true
	l.Connected = false
}

// EnsureCollection creates the given dir locally and records that we did this.
func (l *LocalHandler) EnsureCollection(dir string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.Collections = append(l.Collections, dir)

	return os.MkdirAll(dir, UserPerms)
}

// CollectionsDone just records this was called.
func (l *LocalHandler) CollectionsDone() error { //nolint:unparam
	l.Connected = true

	return nil
}

// MakeStatFail will result in any subsequent Stat()s for a Request with the
// given remote path failing.
func (l *LocalHandler) MakeStatFail(remote string) {
	l.statFail = remote
}

// Stat returns info about the Remote file, which is a local file on disk.
// Returns an error if statFail == Remote.
func (l *LocalHandler) Stat(remote string) (bool, map[string]string, error) {
	if l.statFail == remote {
		return false, nil, errs.PathError{Msg: ErrMockStatFail, Path: ""}
	}

	fi, err := os.Stat(remote)
	if os.IsNotExist(err) {
		return false, nil, nil
	}

	if err != nil {
		return false, nil, err
	}

	l.mu.RLock()
	defer l.mu.RUnlock()

	m, exists := l.Meta[remote]
	if !exists {
		m = make(map[string]string)
	} else {
		m = maps.Clone(m)
	}

	m[meta.MetaKeyRemoteCtime] = fi.ModTime().String()
	m[meta.MetaKeyRemoteMtime] = fi.ModTime().String()
	m[meta.MetaKeyRemoteSize] = strconv.FormatInt(fi.Size(), 10)

	return true, m, nil
}

// MakePutFail will result in any subsequent Put()s for a Request with the
// given remote path failing.
func (l *LocalHandler) MakePutFail(remote string) {
	l.putFail = remote
}

// MakePutSlow will result in any subsequent Put()s for a Request with the
// given local path taking the given amount of time.
func (l *LocalHandler) MakePutSlow(remote string, dur time.Duration) {
	l.putSlow = remote
	l.putDur = dur
}

// MakeRemoveSlow will result in any subsequent RemoveFile() or RemoveDir()
// calls taking 1s to complete.
func (l *LocalHandler) MakeRemoveSlow() {
	l.removeSlow = true
}

// Put just copies from Local to Remote. Returns an error if putFail == Remote.
func (l *LocalHandler) Put(local, remote string) error {
	if l.putFail == remote {
		return errs.PathError{Msg: ErrMockPutFail, Path: ""}
	}

	if l.putSlow == remote {
		<-time.After(l.putDur)
	}

	return copyFile(local, remote)
}

func (l *LocalHandler) Get(local, remote string) error {
	if l.putFail == remote {
		return errs.PathError{Msg: ErrMockPutFail, Path: ""}
	}

	if l.putSlow == remote {
		<-time.After(l.putDur)
	}

	return copyFile(remote, local)
}

// copyFile copies source to dest.
func copyFile(source, dest string) error {
	in, err := os.Open(source)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dest)
	if err != nil {
		return err
	}

	defer func() {
		cerr := out.Close()
		if err == nil {
			err = cerr
		}
	}()

	if _, err = io.Copy(out, in); err != nil {
		return err
	}

	return out.Sync()
}

// MakeMetaFail will result in any subsequent *Meta()s for a Request with the
// given remote path failing.
func (l *LocalHandler) MakeMetaFail(remote string) {
	l.metaFail = remote
}

// RemoveMeta deletes the given keys from a map stored under path. Returns an
// error if metaFail == path.
func (l *LocalHandler) RemoveMeta(path string, meta map[string]string) error {
	if l.metaFail == path {
		return errs.PathError{Msg: ErrMockMetaFail, Path: ""}
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	pathMeta, exists := l.Meta[path]
	if !exists {
		return nil
	}

	for key := range meta {
		delete(pathMeta, key)
	}

	return nil
}

// AddMeta adds the given meta to a map stored under path. Returns an error
// if metafail == path, or if keys were already defined in the map.
func (l *LocalHandler) AddMeta(path string, meta map[string]string) error {
	if l.metaFail == path {
		return errs.PathError{Msg: ErrMockMetaFail, Path: ""}
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	pathMeta, exists := l.Meta[path]
	if !exists {
		pathMeta = make(map[string]string)
		l.Meta[path] = pathMeta
	}

	for key, val := range meta {
		if _, exists := pathMeta[key]; exists {
			return errs.PathError{Msg: ErrMockMetaFail, Path: key}
		}

		pathMeta[key] = val
	}

	return nil
}

// GetMeta gets the metadata stored for the given path (returns an empty map if
// path is not known about or has no metadata).
func (l *LocalHandler) GetMeta(path string) (map[string]string, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.Meta == nil {
		return make(map[string]string), nil
	}

	currentMeta, exists := l.Meta[path]
	if !exists {
		currentMeta = make(map[string]string)
	}

	meta := make(map[string]string, len(currentMeta))

	for k, v := range currentMeta {
		meta[k] = v
	}

	return meta, nil
}

// QueryMeta return paths to all objects with given metadata inside the provided
// scope.
func (l *LocalHandler) QueryMeta(dirToSearch string, meta map[string]string) ([]string, error) { //nolint:unparam
	var objects []string

	for path, pathMeta := range l.Meta {
		if !strings.HasPrefix(path, dirToSearch) {
			continue
		}

		if doesMetaContainMeta(pathMeta, meta) {
			objects = append(objects, path)
		}
	}

	return objects, nil
}

func doesMetaContainMeta(sourceMeta, targetMeta map[string]string) bool {
	valid := true

	for k, v := range targetMeta {
		if sourceMeta[k] != v {
			valid = false

			break
		}
	}

	return valid
}

// RemoveDir removes the empty dir.
func (l *LocalHandler) RemoveDir(path string) error {
	if l.removeSlow {
		time.Sleep(1 * time.Second)
	}

	err := os.Remove(path)
	if err != nil && strings.Contains(err.Error(), "directory not empty") {
		return errs.NewDirNotEmptyError(path)
	}

	return err
}

// RemoveFile removes the file and its metadata.
func (l *LocalHandler) RemoveFile(path string) error {
	if l.removeSlow {
		time.Sleep(1 * time.Second)
	}

	delete(l.Meta, path)

	err := os.Remove(path)
	if err != nil && strings.Contains(err.Error(), "no such file or directory") {
		return errs.PathError{Msg: ErrFileDoesNotExist, Path: path}
	}

	return err
}

// TimeToMeta converts a time to a string suitable for storing as metadata, in
// a way that ObjectInfo.ModTime() will understand and be able to convert back
// again.
func TimeToMeta(t time.Time) (string, error) {
	b, err := t.UTC().Truncate(time.Second).MarshalText()
	if err != nil {
		return "", err
	}

	return string(b), nil
}
