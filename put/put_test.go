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
	"os"
	"path/filepath"
	"strings"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

const userPerms = 0700
const errMockPutFail = "put fail"
const errMockMetaFail = "meta fail"

// mockHandler satisfies the Handler interface, treating "Remote" as local
// paths and moving from Local to Remote for the Put().
type mockHandler struct {
	connected   bool
	cleaned     bool
	collections []string
	meta        map[string]map[string]string
	putfail     string
	metafail    string
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

// Put just does a rename from Local to Remote. Returns an error if putfail ==
// Remote.
func (m *mockHandler) Put(request *Request) error {
	if m.putfail == request.Remote {
		return Error{errMockPutFail, ""}
	}

	return os.Rename(request.Local, request.Remote)
}

// ReplaceMetadata stores the requests metadata in a map, with Remote as the
// key. Returns an error if metafail == Remote.
func (m *mockHandler) ReplaceMetadata(request *Request) error {
	if m.metafail == request.Remote {
		return Error{errMockMetaFail, ""}
	}

	m.meta[request.Remote] = request.Meta

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
				failed := p.Put()
				So(failed, ShouldBeNil)

				for _, request := range requests {
					_, err = os.Stat(request.Remote)
					So(err, ShouldBeNil)

					So(mh.meta[request.Remote], ShouldResemble, request.Meta)
				}

				Convey("Finally, Cleanup() defers to the handler", func() {
					err = p.Cleanup()
					So(err, ShouldBeNil)
					So(mh.cleaned, ShouldBeTrue)
				})
			})

			Convey("Underlying put and metadata operation failures result in returned requests", func() {
				mh.putfail = requests[0].Remote
				mh.metafail = requests[1].Remote

				failed := p.Put()
				So(failed, ShouldNotBeNil)
				So(len(failed), ShouldEqual, 2)
				So(failed[0].Error.Error(), ShouldContainSubstring, errMockPutFail)
				So(failed[1].Error.Error(), ShouldContainSubstring, errMockMetaFail)
			})
		})

		Convey("Put() fails if CreateCollections wasn't run", func() {
			failed := p.Put()
			So(failed, ShouldNotBeNil)
			So(len(failed), ShouldEqual, len(requests))

			for _, request := range requests {
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

		f.Close()

		requests[i] = &Request{
			Local:  path,
			Remote: strings.Replace(path, sourceDir, destDir, 1),
			Meta:   meta,
		}
	}

	return requests
}
