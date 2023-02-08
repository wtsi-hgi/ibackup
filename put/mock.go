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
	"os"
	"sync"
)

const userPerms = 0700
const errMockStatFail = "stat fail"
const errMockPutFail = "put fail"
const errMockMetaFail = "meta fail"

// mockHandler satisfies the Handler interface, treating "Remote" as local
// paths and moving from Local to Remote for the Put().
type mockHandler struct {
	connected   bool
	cleaned     bool
	collections []string
	meta        map[string]map[string]string
	statfail    string
	putfail     string
	metafail    string
	mu          sync.RWMutex
}

// GetLocalHandler returns a Handler that doesn't actually interact with iRODS,
// but instead simply treats "Remote" as local paths and copies from Local to
// Remote for any Put()s. Only for use during tests.
func GetLocalHandler() (Handler, error) {
	return &mockHandler{}, nil
}

// Connect does no actual connection, just records this was called and prepares
// a private variable.
func (m *mockHandler) Connect() error {
	m.connected = true
	m.meta = make(map[string]map[string]string)

	return nil
}

// Cleanup just records this was called.
func (m *mockHandler) Cleanup() error {
	m.cleaned = true

	return nil
}

// EnsureCollection creates the given dir locally and records that we did this.
func (m *mockHandler) EnsureCollection(dir string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.collections = append(m.collections, dir)

	return os.MkdirAll(dir, userPerms)
}

// Stat returns info about the Remote file, which is a local file on disk.
// Returns an error if statfail == Remote.
func (m *mockHandler) Stat(request *Request) (*ObjectInfo, error) {
	if m.statfail == request.Remote {
		return nil, Error{errMockStatFail, ""}
	}

	_, err := os.Stat(request.Remote)
	if os.IsNotExist(err) {
		return &ObjectInfo{Exists: false}, nil
	}

	if err != nil {
		return nil, err
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	meta, exists := m.meta[request.Remote]
	if !exists {
		meta = make(map[string]string)
	}

	return &ObjectInfo{Exists: true, Meta: meta}, nil
}

// Put just copies from Local to Remote. Returns an error if putfail == Remote.
func (m *mockHandler) Put(request *Request) error {
	if m.putfail == request.Remote {
		return Error{errMockPutFail, ""}
	}

	in, err := os.Open(request.Local)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(request.Remote)
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

	err = out.Sync()

	return err
}

// RemoveMeta deletes the given keys from a map stored under path. Returns an
// error if metafail == path.
func (m *mockHandler) RemoveMeta(path string, meta map[string]string) error {
	if m.metafail == path {
		return Error{errMockMetaFail, ""}
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	pathMeta, exists := m.meta[path]
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
func (m *mockHandler) AddMeta(path string, meta map[string]string) error {
	if m.metafail == path {
		return Error{errMockMetaFail, ""}
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	pathMeta, exists := m.meta[path]
	if !exists {
		pathMeta = make(map[string]string)
		m.meta[path] = pathMeta
	}

	for key, val := range meta {
		if _, exists = pathMeta[key]; exists {
			return Error{errMockMetaFail, ""}
		}

		pathMeta[key] = val
	}

	return nil
}
