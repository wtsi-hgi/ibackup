/*******************************************************************************
 * Copyright (c) 2022 Genome Research Ltd.
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
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
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

func TestPutMock(t *testing.T) {
	Convey("Given Requests and a mock Handler, you can make a new Putter", t, func() {
		requests, expectedCollections := makeMockRequests(t)

		mh := &mockHandler{}

		p, err := New(mh, requests)
		So(err, ShouldBeNil)
		So(p, ShouldNotBeNil)
		So(mh.connected, ShouldBeTrue)

		Convey("CreateCollections() creates the minimal number of collections", func() {
			err = p.CreateCollections()
			So(err, ShouldBeNil)

			for _, request := range requests {
				_, err = os.Stat(filepath.Dir(request.Remote))
				So(err, ShouldBeNil)
			}

			So(mh.collections, ShouldResemble, expectedCollections)

			Convey("Put() then puts the files, and adds the metadata", func() {
				requests[0].Requester = "John"
				requests[0].Set = "setA"

				rCh := p.Put()

				for request := range rCh {
					So(request.Status, ShouldEqual, RequestStatusUploaded)
					So(request.Size, ShouldEqual, 2)

					_, err = os.Stat(request.Remote)
					So(err, ShouldBeNil)

					mh.mu.RLock()
					So(mh.meta[request.Remote], ShouldResemble, request.Meta)
					checkAddedMeta(mh.meta[request.Remote])
					mh.mu.RUnlock()
				}

				Convey("A second put of the same files skips them all, except for modified ones", func() {
					requests[0].Requester = "Sam"
					requests[0].Set = "setB"
					requests[0].Meta = map[string]string{"a": "1", "b": "3", "c": "4"}
					touchFile(requests[0].Local, 1*time.Hour)

					rCh = p.Put()

					replaced, unmod, other := 0, 0, 0

					var date string

					for request := range rCh {
						switch request.Status {
						case RequestStatusReplaced:
							replaced++
							mh.mu.RLock()
							So(mh.meta[request.Remote], ShouldResemble, requests[0].Meta)
							So(mh.meta[request.Remote][metaKeyRequester], ShouldEqual, "John,Sam")
							So(mh.meta[request.Remote][metaKeySets], ShouldEqual, "setA,setB")
							date = mh.meta[request.Remote][metaKeyDate]
							mh.mu.RUnlock()
						case RequestStatusUnmodified:
							unmod++
						default:
							other++
						}
					}

					So(replaced, ShouldEqual, 1)
					So(unmod, ShouldEqual, len(requests)-1)
					So(other, ShouldEqual, 0)

					Convey("A third request of the same unchagned file updates Requester and Set", func() {
						requests[0].Requester = "Sam"
						requests[0].Set = "setC"

						p.requests = []*Request{requests[0]}

						<-time.After(1 * time.Second)
						rCh = p.Put()

						request := <-rCh

						So(request.Status, ShouldEqual, RequestStatusUnmodified)
						mh.mu.RLock()
						So(mh.meta[request.Remote][metaKeyRequester], ShouldEqual, "John,Sam")
						So(mh.meta[request.Remote][metaKeySets], ShouldEqual, "setA,setB,setC")
						So(mh.meta[request.Remote][metaKeyDate], ShouldEqual, date)
						mh.mu.RUnlock()
					})
				})

				Convey("Finally, Cleanup() defers to the handler", func() {
					err = p.Cleanup()
					So(err, ShouldBeNil)
					So(mh.cleaned, ShouldBeTrue)
				})
			})

			Convey("Put() fails if the local files don't exist", func() {
				for _, r := range requests {
					err = os.Remove(r.Local)
					So(err, ShouldBeNil)
				}

				rCh := p.Put()

				for request := range rCh {
					So(request.Status, ShouldEqual, RequestStatusMissing)
				}
			})

			Convey("Underlying put and metadata operation failures result in failed requests", func() {
				mh.statfail = requests[0].Remote
				mh.putfail = requests[1].Remote
				mh.metafail = requests[2].Remote

				rCh := p.Put()

				fails, uploaded, other := 0, 0, 0

				for r := range rCh {
					switch r.Status {
					case RequestStatusFailed:
						fails++

						switch r.Remote {
						case requests[0].Remote:
							So(r.Error.Error(), ShouldContainSubstring, errMockStatFail)
						case requests[1].Remote:
							So(r.Error.Error(), ShouldContainSubstring, errMockPutFail)
						case requests[2].Remote:
							So(r.Error.Error(), ShouldContainSubstring, errMockMetaFail)
						}
					case RequestStatusUploaded:
						uploaded++
					default:
						other++
					}
				}

				So(fails, ShouldEqual, 3)
				So(uploaded, ShouldEqual, len(requests)-3)
				So(other, ShouldEqual, 0)

			})
		})

		Convey("Put() fails if CreateCollections wasn't run", func() {
			rCh := p.Put()

			for request := range rCh {
				So(request.Status, ShouldEqual, RequestStatusFailed)
				_, err = os.Stat(request.Remote)
				So(err, ShouldNotBeNil)
			}
		})
	})

	Convey("CreateCollections() also works with 0 or 1 requests", t, func() {
		mh := &mockHandler{}

		var requests []*Request

		p, err := New(mh, requests)
		So(err, ShouldBeNil)
		So(p, ShouldNotBeNil)

		err = p.CreateCollections()
		So(err, ShouldBeNil)

		So(mh.collections, ShouldBeNil)

		ddir := t.TempDir()
		col := filepath.Join(ddir, "bar")
		remote := filepath.Join(col, "file.txt")

		requests = append(requests, &Request{Local: "foo", Remote: remote})
		p, err = New(mh, requests)
		So(err, ShouldBeNil)
		So(p, ShouldNotBeNil)

		err = p.CreateCollections()
		So(err, ShouldBeNil)

		So(mh.collections, ShouldResemble, []string{col})
	})

	Convey("Relative local paths are made absolute", t, func() {
		mh := &mockHandler{}
		p, err := New(mh, []*Request{{Local: "foo", Remote: "/bar"}})
		So(err, ShouldBeNil)

		wd, err := os.Getwd()
		So(err, ShouldBeNil)

		So(p.requests[0].Local, ShouldEqual, filepath.Join(wd, "foo"))

		Convey("unless that's not possible", func() {
			dir := t.TempDir()
			err = os.Chdir(dir)
			So(err, ShouldBeNil)
			err = os.RemoveAll(dir)
			So(err, ShouldBeNil)

			defer func() {
				err = os.Chdir(wd)
				if err != nil {
					t.Logf("%s", err)
				}
			}()

			_, err = New(mh, []*Request{{Local: "foo", Remote: "/bar"}})
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, ErrLocalNotAbs)
			So(err.Error(), ShouldContainSubstring, "foo")
		})
	})

	Convey("You can't make a Putter with relative remote paths", t, func() {
		mh := &mockHandler{}
		_, err := New(mh, []*Request{{Local: "/foo", Remote: "bar"}})
		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, ErrRemoteNotAbs)
		So(err.Error(), ShouldContainSubstring, "bar")
	})
}

// makeMockRequests creates some local directories and files, and returns
// requests that all share the same metadata, with remotes pointing to another
// local temp directory (but the remote sub-directories are not created).
// Also returns the execpted remote directories that would have to be created.
func makeMockRequests(t *testing.T) ([]*Request, []string) {
	t.Helper()

	sourceDir := t.TempDir()
	destDir := t.TempDir()

	requests := makeTestRequests(t, sourceDir, destDir)

	return requests, []string{
		filepath.Join(destDir, "a", "b", "c"),
		filepath.Join(destDir, "a", "b", "d", "e"),
	}
}

func makeTestRequests(t *testing.T, sourceDir, destDir string) []*Request {
	t.Helper()

	sourcePaths := []string{
		filepath.Join(sourceDir, "a", "b", "c", "file.1"),
		filepath.Join(sourceDir, "a", "b", "file.2"),
		filepath.Join(sourceDir, "a", "b", "d", "file.3"),
		filepath.Join(sourceDir, "a", "b", "d", "e", "file.4"),
		filepath.Join(sourceDir, "a", "b", "d", "e", "file.5"),
	}

	requests := make([]*Request, len(sourcePaths))
	meta := map[string]string{"a": "1", "b": "2"}

	for i, path := range sourcePaths {
		dir := filepath.Dir(path)

		err := os.MkdirAll(dir, userPerms)
		if err != nil {
			t.Fatal(err)
		}

		f, err := os.Create(path)
		if err != nil {
			t.Fatal(err)
		}

		_, err = f.WriteString("1\n")
		if err != nil {
			t.Fatal(err)
		}

		f.Close()

		requests[i] = &Request{
			Local:  path,
			Remote: strings.Replace(path, sourceDir, destDir, 1),
			Meta:   meta,
		}
	}

	return requests
}

// checkAddedMeta checks that the given map contains all the extra metadata keys
// that we add to requests.
func checkAddedMeta(meta map[string]string) {
	So(meta[metaKeyMtime], ShouldNotBeBlank)
	So(meta[metaKeyOwner], ShouldNotBeBlank)
	So(meta[metaKeyGroup], ShouldNotBeBlank)
	So(meta[metaKeyDate], ShouldNotBeBlank)
}

// touchFile alters the mtime of the given file by the given duration. Returns
// the time set.
func touchFile(path string, d time.Duration) time.Time {
	newT := time.Now().Local().Add(d)
	err := os.Chtimes(path, newT, newT)
	So(err, ShouldBeNil)

	return newT
}
