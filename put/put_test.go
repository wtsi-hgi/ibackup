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
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestPutMock(t *testing.T) { //nolint:cyclop
	Convey("Given Requests and a mock Handler, you can make a new Putter", t, func() {
		requests, expectedCollections := makeMockRequests(t)

		mh := &mockHandler{}

		p, err := New(mh)
		So(err, ShouldBeNil)
		So(p, ShouldNotBeNil)
		So(mh.connected, ShouldBeTrue)

		Convey("CreateCollections() creates the minimal number of collections", func() {
			err = p.CreateCollections(requests)
			So(err, ShouldBeNil)

			for _, request := range requests {
				_, err = os.Stat(filepath.Dir(request.Remote))
				So(err, ShouldBeNil)
			}

			So(mh.collections, ShouldResemble, expectedCollections)

			Convey("Put() then puts the files, and adds the metadata", func() {
				requests[0].Requester = "John"
				requests[0].Set = "setA"

				for _, r := range requests {
					shouldPut, metadataChangeOnly := p.Validate(r)
					So(shouldPut, ShouldBeTrue)
					So(metadataChangeOnly, ShouldBeFalse)
					So(r.Status, ShouldEqual, RequestStatusUploaded)
					So(r.Size, ShouldEqual, 2)
					_, err = os.Stat(r.Remote)
					So(err, ShouldNotBeNil)

					p.Put(r)
					_, err = os.Stat(r.Remote)
					So(err, ShouldBeNil)

					mh.mu.RLock()
					So(mh.meta[r.Remote], ShouldResemble, r.Meta)
					checkAddedMeta(mh.meta[r.Remote])
					mh.mu.RUnlock()
				}

				Convey("A second put of the same files skips them all, except for modified ones", func() {
					requests[0].Requester = "Sam"
					requests[0].Set = "setB"
					requests[0].Meta = map[string]string{"a": "1", "b": "3", "c": "4"}
					touchFile(requests[0].Local, 1*time.Hour)

					replaced, unmod := 0, 0

					var date string

					for _, r := range requests {
						shouldPut, metadataChangeOnly := p.Validate(r)
						So(metadataChangeOnly, ShouldBeFalse)

						if shouldPut {
							replaced++
							So(r.Status, ShouldEqual, RequestStatusReplaced)

							p.Put(r)
							mh.mu.RLock()
							So(mh.meta[r.Remote], ShouldResemble, requests[0].Meta)
							So(mh.meta[r.Remote][metaKeyRequester], ShouldEqual, "John,Sam")
							So(mh.meta[r.Remote][metaKeySets], ShouldEqual, "setA,setB")
							date = mh.meta[r.Remote][metaKeyDate]
							mh.mu.RUnlock()
						} else {
							unmod++
							So(r.Status, ShouldEqual, RequestStatusUnmodified)
						}
					}

					So(replaced, ShouldEqual, 1)
					So(unmod, ShouldEqual, len(requests)-1)

					Convey("A third request of the same unchanged file updates Requester and Set", func() {
						requests[0].Requester = "Sam"
						requests[0].Set = "setC"

						<-time.After(1 * time.Second)

						shouldPut, metadataChangeOnly := p.Validate(requests[0])
						So(shouldPut, ShouldBeTrue)
						So(metadataChangeOnly, ShouldBeTrue)
						So(requests[0].Status, ShouldEqual, RequestStatusUnmodified)

						p.Put(requests[0])

						mh.mu.RLock()
						So(mh.meta[requests[0].Remote][metaKeyRequester], ShouldEqual, "John,Sam")
						So(mh.meta[requests[0].Remote][metaKeySets], ShouldEqual, "setA,setB,setC")
						So(mh.meta[requests[0].Remote][metaKeyDate], ShouldEqual, date)
						mh.mu.RUnlock()
					})
				})

				Convey("Finally, Cleanup() defers to the handler", func() {
					err = p.Cleanup()
					So(err, ShouldBeNil)
					So(mh.cleaned, ShouldBeTrue)
				})
			})

			Convey("Validate() fails if the local files don't exist", func() {
				for _, r := range requests {
					err = os.Remove(r.Local)
					So(err, ShouldBeNil)
				}

				shouldPut, metadataChangeOnly := p.Validate(requests[0])
				So(shouldPut, ShouldBeFalse)
				So(metadataChangeOnly, ShouldBeFalse)
				So(requests[0].Status, ShouldEqual, RequestStatusMissing)
			})

			Convey("Underlying put and metadata operation failures result in failed requests", func() {
				mh.statfail = requests[0].Remote
				mh.putfail = requests[1].Remote
				mh.metafail = requests[2].Remote

				uploading, fails, uploaded, other, cases := 0, 0, 0, 0, 0

				for _, r := range requests {
					shouldPut, metadataChangeOnly := p.Validate(r)

					if shouldPut {
						uploading++
					}

					So(metadataChangeOnly, ShouldBeFalse)

					p.Put(r)

					switch r.Status {
					case RequestStatusFailed:
						fails++

						switch r.Remote {
						case requests[0].Remote:
							So(r.Error, ShouldContainSubstring, errMockStatFail)
							cases++
						case requests[1].Remote:
							So(r.Error, ShouldContainSubstring, errMockPutFail)
							cases++
						case requests[2].Remote:
							So(r.Error, ShouldContainSubstring, errMockMetaFail)
							cases++
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
				So(uploading, ShouldEqual, fails+uploaded-1)
				So(cases, ShouldEqual, 3)
			})
		})

		Convey("Put() fails if CreateCollections wasn't run", func() {
			shouldPut, metadataChangeOnly := p.Validate(requests[0])
			So(shouldPut, ShouldBeTrue)
			So(metadataChangeOnly, ShouldBeFalse)

			p.Put(requests[0])

			So(requests[0].Status, ShouldEqual, RequestStatusFailed)
			_, err = os.Stat(requests[0].Remote)
			So(err, ShouldNotBeNil)
		})
	})

	Convey("CreateCollections() also works with 0 or 1 requests", t, func() {
		mh := &mockHandler{}

		var requests []*Request

		p, err := New(mh)
		So(err, ShouldBeNil)
		So(p, ShouldNotBeNil)

		err = p.CreateCollections(requests)
		So(err, ShouldBeNil)

		So(mh.collections, ShouldBeNil)

		ddir := t.TempDir()
		col := filepath.Join(ddir, "bar")
		remote := filepath.Join(col, "file.txt")

		requests = append(requests, &Request{Local: "foo", Remote: remote})
		p, err = New(mh)
		So(err, ShouldBeNil)
		So(p, ShouldNotBeNil)

		err = p.CreateCollections(requests)
		So(err, ShouldBeNil)

		So(mh.collections, ShouldResemble, []string{col})
	})

	Convey("Relative local paths are made absolute", t, func() {
		mh := &mockHandler{}
		p, err := New(mh)
		So(err, ShouldBeNil)

		wd, err := os.Getwd()
		So(err, ShouldBeNil)

		requests, _ := makeMockRequests(t)
		dir := filepath.Dir(requests[0].Local)

		err = os.Chdir(dir)
		So(err, ShouldBeNil)

		defer func() {
			err = os.Chdir(wd)
			if err != nil {
				t.Logf("%s", err)
			}
		}()

		request := &Request{Local: filepath.Base(requests[0].Local), Remote: "/bar"}
		shouldPut, metadataChangeOnly := p.Validate(request)
		So(request.Error, ShouldBeBlank)
		So(shouldPut, ShouldBeTrue)
		So(metadataChangeOnly, ShouldBeFalse)

		p.Put(request)
		So(request.Local, ShouldEqual, requests[0].Local)

		Convey("unless that's not possible", func() {
			dir := t.TempDir()
			err = os.Chdir(dir)
			So(err, ShouldBeNil)
			err = os.RemoveAll(dir)
			So(err, ShouldBeNil)

			request = &Request{Local: "foo", Remote: "/bar"}
			shouldPut, metadataChangeOnly = p.Validate(request)
			So(shouldPut, ShouldBeFalse)
			So(metadataChangeOnly, ShouldBeFalse)
			So(request.Error, ShouldNotBeBlank)
			So(request.Error, ShouldContainSubstring, ErrLocalNotAbs)
			So(request.Error, ShouldContainSubstring, "foo")
		})
	})

	Convey("You can't Validate with relative remote paths", t, func() {
		mh := &mockHandler{}
		p, err := New(mh)
		So(err, ShouldBeNil)

		request := &Request{Local: "/foo", Remote: "bar"}
		shouldPut, metadataChangeOnly := p.Validate(request)
		So(shouldPut, ShouldBeFalse)
		So(metadataChangeOnly, ShouldBeFalse)
		So(request.Error, ShouldNotBeBlank)
		So(request.Error, ShouldContainSubstring, ErrRemoteNotAbs)
		So(request.Error, ShouldContainSubstring, "bar")
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
