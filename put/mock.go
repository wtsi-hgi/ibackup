/*******************************************************************************
 * Copyright (c) 2022, 2023 Genome Research Ltd.
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

package put

import (
	"io"
	"maps"
	"os"
	"sync"
	"time"
)

const userPerms = 0700
const ErrMockStatFail = "stat fail"
const ErrMockPutFail = "put fail"
const ErrMockMetaFail = "meta fail"

// LocalHandler satisfies the Handler interface, treating "Remote" as local
// paths and moving from Local to Remote for the Put().
type LocalHandler struct {
	connected   bool
	cleaned     bool
	collections []string
	Meta        map[string]map[string]string
	statFail    string
	putFail     string
	putSlow     string
	putDur      time.Duration
	metaFail    string
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
func (l *LocalHandler) Cleanup() error {
	l.cleaned = true

	return nil
}

// EnsureCollection creates the given dir locally and records that we did this.
func (l *LocalHandler) EnsureCollection(dir string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.collections = append(l.collections, dir)

	return os.MkdirAll(dir, userPerms)
}

// CollectionsDone says we connected and prepares us for metadata handling.
func (l *LocalHandler) CollectionsDone() error {
	l.connected = true

	return nil
}

// MakeStatFail will result in any subsequent Stat()s for a Request with the
// given remote path failing.
func (l *LocalHandler) MakeStatFail(remote string) {
	l.statFail = remote
}

// Stat returns info about the Remote file, which is a local file on disk.
// Returns an error if statFail == Remote.
func (l *LocalHandler) Stat(request *Request) (*ObjectInfo, error) {
	if l.statFail == request.Remote {
		return nil, Error{ErrMockStatFail, ""}
	}

	_, err := os.Stat(request.Remote)
	if os.IsNotExist(err) {
		return &ObjectInfo{Exists: false}, nil
	}

	if err != nil {
		return nil, err
	}

	l.mu.RLock()
	defer l.mu.RUnlock()

	meta, exists := l.Meta[request.Remote]
	if !exists {
		meta = make(map[string]string)
	} else {
		meta = maps.Clone(meta)
	}

	return &ObjectInfo{Exists: true, Meta: meta}, nil
}

// MakePutFail will result in any subsequent Put()s for a Request with the
// given remote path failing.
func (l *LocalHandler) MakePutFail(remote string) {
	l.putFail = remote
}

// MakePutSlow will result in any subsequent Put()s for a Request with the
// given local path taking the given amount of time.
func (l *LocalHandler) MakePutSlow(local string, dur time.Duration) {
	l.putSlow = local
	l.putDur = dur
}

// Put just copies from Local to Remote. Returns an error if putFail == Remote.
func (l *LocalHandler) Put(request *Request) error {
	if l.putFail == request.Remote {
		return Error{ErrMockPutFail, ""}
	}

	if l.putSlow == request.Local {
		<-time.After(l.putDur)
	}

	return copyFile(request.LocalDataPath(), request.Remote)
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
		return Error{ErrMockMetaFail, ""}
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
		return Error{ErrMockMetaFail, ""}
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
			return Error{ErrMockMetaFail, key}
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
