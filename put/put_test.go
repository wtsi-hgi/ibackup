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
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/ibackup/internal"
)

func TestPutMock(t *testing.T) {
	Convey("Given Requests and a mock Handler, you can make a new Putter", t, func() {
		requests, expectedCollections := makeMockRequests(t)

		lh := GetLocalHandler()

		p, err := New(lh, requests)
		So(err, ShouldBeNil)
		So(p, ShouldNotBeNil)

		Convey("CreateCollections() creates the minimal number of collections", func() {
			err = p.CreateCollections()
			So(err, ShouldBeNil)
			So(lh.connected, ShouldBeTrue)

			for _, request := range requests {
				_, err = os.Stat(filepath.Dir(request.Remote))
				So(err, ShouldBeNil)
			}

			sort.Strings(lh.collections)

			So(lh.collections, ShouldResemble, expectedCollections)

			Convey("Put() then puts the files, and adds the metadata", func() {
				requests[0].Requester = "John"
				requests[0].Set = "setA"

				uCh, urCh, srCh := p.Put()

				for request := range uCh {
					So(request.Status, ShouldEqual, RequestStatusUploading)
				}

				for request := range urCh {
					So(request.Status, ShouldEqual, RequestStatusUploaded)
					So(request.Size, ShouldEqual, 2)

					_, err = os.Stat(request.Remote)
					So(err, ShouldBeNil)

					lh.mu.RLock()
					So(lh.meta[request.Remote], ShouldResemble, request.Meta.LocalMeta)
					checkAddedMeta(lh.meta[request.Remote])
					lh.mu.RUnlock()
				}

				skipped := 0
				for range srCh {
					skipped++
				}
				So(skipped, ShouldEqual, 0)

				Convey("A second put of the same files skips them all, except for modified ones", func() {
					requests[0].Requester = "Sam"
					requests[0].Set = "setB"
					requests[0].Meta = &Meta{LocalMeta: map[string]string{"a": "1", "b": "3", "c": "4"}}
					touchFile(requests[0].Local, 1*time.Hour)

					uCh, urCh, srCh = p.Put()

					uploading, replaced, unmod, other := 0, 0, 0, 0

					var date string

					for range uCh {
						uploading++
					}

					for request := range urCh {
						switch request.Status {
						case RequestStatusReplaced:
							replaced++
							lh.mu.RLock()
							So(lh.meta[request.Remote], ShouldResemble, requests[0].Meta.LocalMeta)
							So(lh.meta[request.Remote][MetaKeyRequester], ShouldEqual, "John,Sam")
							So(lh.meta[request.Remote][MetaKeySets], ShouldEqual, "setA,setB")
							date = lh.meta[request.Remote][MetaKeyDate]
							lh.mu.RUnlock()
						default:
							other++
						}
					}

					metadata, errm := lh.GetMeta(requests[0].Remote)
					So(errm, ShouldBeNil)
					So(metadata, ShouldResemble, requests[0].Meta.LocalMeta)

					for request := range srCh {
						switch request.Status {
						case RequestStatusUnmodified:
							unmod++
						default:
							other++
						}
					}

					So(replaced, ShouldEqual, 1)
					So(uploading, ShouldEqual, replaced)
					So(unmod, ShouldEqual, len(requests)-1)
					So(other, ShouldEqual, 0)

					Convey("A third request of the same unchanged file updates Requester and Set", func() {
						requests[0].Requester = "Sam"
						requests[0].Set = "setC"

						p.requests = []*Request{requests[0]}

						<-time.After(1 * time.Second)
						uCh, urCh, srCh = p.Put()

						request := <-uCh
						So(request, ShouldBeNil)

						request = <-urCh
						So(request, ShouldBeNil)

						request = <-srCh
						So(request.Status, ShouldEqual, RequestStatusUnmodified)
						lh.mu.RLock()
						So(lh.meta[request.Remote][MetaKeyRequester], ShouldEqual, "John,Sam")
						So(lh.meta[request.Remote][MetaKeySets], ShouldEqual, "setA,setB,setC")
						So(lh.meta[request.Remote][MetaKeyDate], ShouldEqual, date)
						lh.mu.RUnlock()
					})
				})

				Convey("Finally, Cleanup() defers to the handler", func() {
					err = p.Cleanup()
					So(err, ShouldBeNil)
					So(lh.cleaned, ShouldBeTrue)
				})

				Convey("RemoveFile removes a file", func() {
					filePath := requests[0].Remote

					err = lh.RemoveFile(filePath)
					So(err, ShouldBeNil)

					_, err = os.Stat(filePath)
					So(err, ShouldNotBeNil)

					So(lh.meta[filePath], ShouldBeNil)

					Convey("RemoveDir removes an empty directory", func() {
						dirPath := filepath.Dir(filePath)

						err = lh.RemoveDir(dirPath)
						So(err, ShouldBeNil)

						_, err = os.Stat(dirPath)
						So(err, ShouldNotBeNil)
					})
				})

				Convey("queryMeta returns all paths with matching metadata", func() {
					paths, errq := lh.QueryMeta("", map[string]string{"a": "1"})
					So(errq, ShouldBeNil)

					So(len(paths), ShouldEqual, 5)

					paths, err = lh.QueryMeta("", map[string]string{MetaKeyRequester: requests[0].Requester})
					So(err, ShouldBeNil)

					So(len(paths), ShouldEqual, 1)

					Convey("queryMeta only returns paths in the provided scope", func() {
						paths, errq := lh.QueryMeta(expectedCollections[1], map[string]string{"a": "1"})
						So(errq, ShouldBeNil)

						So(len(paths), ShouldEqual, 2)
					})
				})
			})

			Convey("Put() fails if the local files don't exist", func() {
				for _, r := range requests {
					err = os.Remove(r.Local)
					So(err, ShouldBeNil)
				}

				uCh, urCh, srCh := p.Put()

				request := <-uCh
				So(request, ShouldBeNil)

				request = <-urCh
				So(request, ShouldBeNil)

				for request := range srCh {
					So(request.Status, ShouldEqual, RequestStatusMissing)
				}
			})

			Convey("Put() uploads an empty file in place of links, with hardlink data going to the Hardlink location", func() {
				err = os.Remove(requests[0].Local)
				So(err, ShouldBeNil)

				err = os.Symlink(requests[2].Local, requests[0].Local)
				So(err, ShouldBeNil)

				requests[0].Symlink = requests[2].Local

				err = os.Remove(requests[2].Local)
				So(err, ShouldBeNil)

				err = os.Link(requests[3].Local, requests[2].Local)
				So(err, ShouldBeNil)

				err = os.Remove(requests[4].Local)
				So(err, ShouldBeNil)

				err = os.Link(requests[3].Local, requests[4].Local)
				So(err, ShouldBeNil)

				inodeBaseDir := t.TempDir()
				inodeDir := filepath.Join(inodeBaseDir, "mountpoints")
				inodeRemote := filepath.Join(inodeDir, "inode.file")
				requests[2].Hardlink = inodeRemote
				setMetaKey := "set"
				requests[2].Meta = &Meta{LocalMeta: map[string]string{setMetaKey: "a"}}
				requests[4].Hardlink = inodeRemote
				requests[4].Meta = &Meta{LocalMeta: map[string]string{setMetaKey: "b"}}

				clonedRequest0 := requests[0].Clone()
				clonedRequest2 := requests[2].Clone()

				uploading, skipped, statusCounts := uploadRequests(t, lh, requests)

				So(uploading, ShouldEqual, len(requests))
				So(statusCounts[RequestStatusUploaded], ShouldEqual, len(requests))
				So(skipped, ShouldEqual, 0)

				info, errs := os.Stat(requests[0].Remote)
				So(errs, ShouldBeNil)
				So(info.Size(), ShouldEqual, 0)
				symlinkMtime := info.ModTime()

				info, errs = os.Stat(requests[2].Remote)
				So(errs, ShouldBeNil)
				So(info.Size(), ShouldEqual, 0)

				info, errs = os.Stat(requests[4].Remote)
				So(errs, ShouldBeNil)
				So(info.Size(), ShouldEqual, 0)

				info, errs = os.Stat(inodeRemote)
				So(errs, ShouldBeNil)
				So(info.Size(), ShouldEqual, 2)

				So(lh.meta[requests[2].Remote][setMetaKey], ShouldEqual, "a")
				So(lh.meta[requests[2].Hardlink][setMetaKey], ShouldNotBeBlank)
				So(lh.meta[requests[2].Remote][MetaKeyRemoteHardlink], ShouldEqual, requests[2].Hardlink)
				So(lh.meta[requests[2].Remote][MetaKeyHardlink], ShouldEqual, requests[2].Local)

				So(lh.meta[requests[4].Remote][setMetaKey], ShouldEqual, "b")
				So(lh.meta[requests[4].Hardlink][setMetaKey], ShouldNotBeBlank)
				So(lh.meta[requests[4].Remote][MetaKeyRemoteHardlink], ShouldEqual, requests[2].Hardlink)
				So(lh.meta[requests[4].Remote][MetaKeyHardlink], ShouldEqual, requests[4].Local)

				Convey("re-uploading an unmodified hardlink does not replace remote files", func() {
					hardlinkMTime := info.ModTime()

					reclonedRequest2 := clonedRequest2.Clone()

					uploading, skipped, statusCounts = uploadRequests(t, lh, []*Request{clonedRequest2})

					So(uploading, ShouldEqual, 0)

					So(statusCounts[RequestStatusUploaded], ShouldEqual, 0)
					So(statusCounts[RequestStatusReplaced], ShouldEqual, 0)
					So(skipped, ShouldEqual, 1)

					info, errs = os.Stat(requests[2].Hardlink)
					So(errs, ShouldBeNil)
					So(info.ModTime(), ShouldResemble, hardlinkMTime)

					Convey("re-uploading a modified hardlink does replace remote files, but doesn't reupload symlink", func() {
						touchFile(requests[2].Local, time.Hour)

						info, errs = os.Stat(requests[2].Local)
						So(errs, ShouldBeNil)
						So(info.ModTime().After(hardlinkMTime), ShouldBeTrue)

						uploading, skipped, statusCounts = uploadRequests(t, lh, []*Request{reclonedRequest2, clonedRequest0})
						So(uploading, ShouldEqual, 1)
						So(statusCounts[RequestStatusReplaced], ShouldEqual, 1)
						So(skipped, ShouldEqual, 1)

						info, errs = os.Stat(requests[2].Hardlink)
						So(errs, ShouldBeNil)
						So(info.ModTime().After(hardlinkMTime), ShouldBeTrue)

						info, errs = os.Stat(requests[0].Remote)
						So(errs, ShouldBeNil)
						So(info.Size(), ShouldEqual, 0)
						So(info.ModTime(), ShouldEqual, symlinkMtime)
					})
				})
			})

			Convey("Put() uploads broken symlinks without issue", func() {
				err = os.Remove(requests[0].Local)
				So(err, ShouldBeNil)

				err = os.Symlink(requests[2].Local, requests[0].Local)
				So(err, ShouldBeNil)

				requests[0].Symlink = requests[2].Local

				err = os.Remove(requests[2].Local)
				So(err, ShouldBeNil)

				uploading, skipped, statusCounts := uploadRequests(t, lh, requests[:1])

				So(uploading, ShouldEqual, 1)
				So(statusCounts[RequestStatusUploaded], ShouldEqual, 1)
				So(skipped, ShouldEqual, 0)

				info, errs := os.Stat(requests[0].Remote)
				So(errs, ShouldBeNil)
				So(info.Size(), ShouldEqual, 0)
			})

			Convey("Underlying put and metadata operation failures result in failed requests", func() {
				lh.MakeStatFail(requests[0].Remote)
				lh.MakePutFail(requests[1].Remote)
				lh.MakeMetaFail(requests[2].Remote)

				uCh, urCh, srCh := p.Put()

				uploading, fails, uploaded, other, cases := 0, 0, 0, 0, 0

				for range uCh {
					uploading++

					r := <-urCh
					switch r.Status {
					case RequestStatusFailed:
						fails++

						switch r.Remote {
						case requests[1].Remote:
							So(r.Error, ShouldContainSubstring, ErrMockPutFail)
							cases++
						case requests[2].Remote:
							So(r.Error, ShouldContainSubstring, ErrMockMetaFail)
							cases++
						}
					case RequestStatusUploaded:
						uploaded++
					default:
						other++
					}
				}

				for r := range srCh {
					switch r.Status {
					case RequestStatusFailed:
						fails++

						if r.Remote == requests[0].Remote {
							So(r.Error, ShouldContainSubstring, ErrMockStatFail)
							cases++
						}
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
			_, urCh, _ := p.Put()

			for request := range urCh {
				So(request.Status, ShouldEqual, RequestStatusFailed)
				_, err = os.Stat(request.Remote)
				So(err, ShouldNotBeNil)
			}
		})

		Convey("Putting multiple requests with the same local & remote paths should succeed", func() {
			localDir := t.TempDir()
			localPath := filepath.Join(localDir, "localFile")
			remoteDir := t.TempDir()
			remotePath := filepath.Join(remoteDir, "remoteFile")

			internal.CreateTestFile(t, localPath, "abc123")

			request1 := &Request{
				Local:     localPath,
				Remote:    remotePath,
				Requester: "aRequester",
				Set:       "aSet",
				Status:    RequestStatusPending,
				Meta: &Meta{
					LocalMeta: map[string]string{
						"aKey": "aValue",
					},
				},
			}

			request2 := request1.Clone()
			request2.Meta.LocalMeta["aKey"] = "bValue"
			request2.Meta.LocalMeta["bKey"] = "anotherValue"
			request2.Set = "bSet"

			request3 := request1.Clone()
			request3.Meta.LocalMeta["aKey"] = "cValue"
			request2.Meta.LocalMeta["bKey"] = "yetAnotherValue"
			request3.Set = "cSet"

			uploading, skipped, statusCounts := uploadRequests(t, lh, []*Request{request1, request2, request3})

			So(uploading, ShouldEqual, 1)
			So(skipped, ShouldEqual, 2)
			So(statusCounts[RequestStatusUploaded], ShouldEqual, 1)
			So(statusCounts[RequestStatusUnmodified], ShouldEqual, 0)

			So(lh.meta[remotePath][MetaKeySets], ShouldEqual, "aSet,bSet,cSet")
			So(lh.meta[remotePath]["aKey"], ShouldEqual, "cValue")
			So(lh.meta[remotePath]["bKey"], ShouldEqual, "yetAnotherValue")
		})
	})

	Convey("CreateCollections() also works with 0 or 1 requests", t, func() {
		lh := &LocalHandler{}

		var requests []*Request

		p, err := New(lh, requests)
		So(err, ShouldBeNil)
		So(p, ShouldNotBeNil)

		err = p.CreateCollections()
		So(err, ShouldBeNil)

		So(lh.collections, ShouldBeNil)

		ddir := t.TempDir()
		col := filepath.Join(ddir, "bar")
		remote := filepath.Join(col, "file.txt")

		requests = append(requests, &Request{Local: "foo", Remote: remote})
		p, err = New(lh, requests)
		So(err, ShouldBeNil)
		So(p, ShouldNotBeNil)

		err = p.CreateCollections()
		So(err, ShouldBeNil)

		So(lh.collections, ShouldResemble, []string{col})
	})

	Convey("Relative local paths are made absolute", t, func() {
		lh := &LocalHandler{}
		p, err := New(lh, []*Request{{Local: "foo", Remote: "/bar"}})
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

			_, err = New(lh, []*Request{{Local: "foo", Remote: "/bar"}})
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, ErrLocalNotAbs)
			So(err.Error(), ShouldContainSubstring, "foo")
		})
	})

	Convey("You can't make a Putter with relative remote paths", t, func() {
		lh := &LocalHandler{}
		_, err := New(lh, []*Request{{Local: "/foo", Remote: "bar"}})
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

	return createTestRequests(t, sourceDir, destDir, sourcePaths)
}

func createTestRequests(t *testing.T, sourceDir, destDir string, sourcePaths []string) []*Request {
	t.Helper()

	requests := make([]*Request, len(sourcePaths))
	localMeta := map[string]string{"a": "1", "b": "2"}

	for i, path := range sourcePaths {
		dir := filepath.Dir(path)

		err := os.MkdirAll(dir, userPerms)
		if err != nil {
			t.Fatal(err)
		}

		internal.CreateTestFile(t, path, "1\n")

		requests[i] = &Request{
			Local:  path,
			Remote: strings.Replace(path, sourceDir, destDir, 1),
			Meta:   &Meta{LocalMeta: localMeta},
		}
	}

	return requests
}

// checkAddedMeta checks that the given map contains all the extra metadata keys
// that we add to requests.
func checkAddedMeta(meta map[string]string) {
	So(meta[MetaKeyMtime], ShouldNotBeBlank)
	So(meta[MetaKeyOwner], ShouldNotBeBlank)
	So(meta[MetaKeyGroup], ShouldNotBeBlank)
	So(meta[MetaKeyDate], ShouldNotBeBlank)
}

// touchFile alters the mtime of the given file by the given duration. Returns
// the time set.
func touchFile(path string, d time.Duration) time.Time {
	newT := time.Now().Local().Add(d)
	err := os.Chtimes(path, newT, newT)
	So(err, ShouldBeNil)

	return newT
}

// uploadRequests uploads the requests with the given handler and returns the
// count of uploading files, skipped files and a map of RequestStatus count for
// uploaded ones.
func uploadRequests(t *testing.T, h Handler, requests []*Request) (int, int, map[RequestStatus]int) {
	t.Helper()

	p, err := New(h, requests)
	So(err, ShouldBeNil)
	So(p, ShouldNotBeNil)

	err = p.CreateCollections()
	So(err, ShouldBeNil)

	uCh, urCh, srCh := p.Put()

	uploading := 0
	skipped := 0
	requestStatusCounts := make(map[RequestStatus]int)

	for range uCh {
		uploading++
	}

	for request := range urCh {
		currentCount := requestStatusCounts[request.Status]
		requestStatusCounts[request.Status] = currentCount + 1

		if request.Error != "" {
			t.Logf("%s failed: %s\n", request.Local, request.Error)
		}
	}

	for range srCh {
		skipped++
	}

	return uploading, skipped, requestStatusCounts
}
