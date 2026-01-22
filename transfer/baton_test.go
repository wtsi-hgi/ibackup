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

package transfer

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/ibackup/baton"
	"github.com/wtsi-hgi/ibackup/baton/meta"
	"github.com/wtsi-hgi/ibackup/internal"
	"github.com/wtsi-hgi/ibackup/internal/testutil"
	ex "github.com/wtsi-npg/extendo/v2"
)

func TestPutBaton(t *testing.T) {
	h, errgbh := baton.GetBatonHandler()
	if errgbh != nil {
		t.Logf("GetBatonHandler error: %s", errgbh)
		SkipConvey("Skipping baton tests since couldn't find baton", t, func() {})

		return
	}

	rootCollection := testutil.RequireIRODSTestCollection(t, "imkdir", "irm", "ils", "imeta")
	if rootCollection == "" {
		SkipConvey("Skipping baton tests since IBACKUP_TEST_COLLECTION is not defined", t, func() {})

		return
	}

	Convey("Given Requests and a baton Handler, you can make a new Putter", t, func() {
		internal.InitStatter(t)

		requests, expectedCollections := makeRequests(t, rootCollection)

		p, err := New(h, requests)
		So(err, ShouldBeNil)
		So(p, ShouldNotBeNil)

		Convey("CreateCollections() creates the needed collections", func() {
			testClient, clearup := clearTestCollection(t, h, rootCollection)
			defer clearup()

			for _, col := range expectedCollections {
				So(checkPathExistsWithBaton(testClient, col), ShouldBeFalse)
			}

			err = p.CreateCollections()
			So(err, ShouldBeNil)

			for _, col := range expectedCollections {
				So(checkPathExistsWithBaton(testClient, col), ShouldBeTrue)
			}

			Convey("Put() then puts the files, and adds the metadata", func() {
				requester := "John"
				requests[0].Requester = requester
				requests[0].Set = "setA"
				expectedMTime := touchFile(requests[0].Local, -1*time.Hour)

				uCh, urCh, srCh := p.Put()

				for request := range uCh {
					So(request.Status, ShouldEqual, RequestStatusUploading)
				}

				for request := range urCh {
					So(request.Error, ShouldBeBlank)
					So(request.Status, ShouldEqual, RequestStatusUploaded)
					So(request.Size, ShouldEqual, 2)

					meta := getObjectMetadataWithBaton(testClient, request.Remote)
					compareMetasWithSize(t, meta, request.Meta.LocalMeta, 2)

					checkAddedMeta(meta)

					it, errg := getItemWithBaton(testClient, request.Remote)
					So(errg, ShouldBeNil)
					So(it.ISize, ShouldEqual, 2)

					if request.Local == requests[0].Local {
						mtime := time.Time{}
						err = mtime.UnmarshalText([]byte(meta[MetaKeyMtime]))
						So(err, ShouldBeNil)

						So(mtime.UTC().Truncate(time.Second), ShouldEqual, expectedMTime.UTC().Truncate(time.Second))
					}
				}

				So(<-srCh, ShouldBeNil)

				Convey("You can put the same file again if it changed, with different metadata", func() {
					request := requests[0]
					request.Requester = requester
					request.Set = "setB"
					request.Meta = &Meta{LocalMeta: map[string]string{"a": "1", "b": "3", "c": "4"}}
					touchFile(request.Local, 1*time.Hour)

					p, err = New(h, []*Request{request})
					So(err, ShouldBeNil)

					err = p.CreateCollections()
					So(err, ShouldBeNil)

					uCh, urCh, srCh = p.Put()

					got := <-uCh
					So(got.Status, ShouldEqual, RequestStatusUploading)

					got = <-srCh
					So(got, ShouldBeNil)

					got = <-urCh
					So(got.Error, ShouldBeBlank)
					So(got.Status, ShouldEqual, RequestStatusReplaced)

					meta := getObjectMetadataWithBaton(testClient, request.Remote)
					compareMetasWithSize(t, meta, request.Meta.LocalMeta, 2)

					Convey("Finally, Cleanup() stops the clients", func() {
						p.Cleanup()

						So(h.AllClientsStopped(), ShouldBeTrue)
					})
				})

				Convey("Unchanged files aren't replaced", func() {
					request := requests[0]

					p, err = New(h, []*Request{request})
					So(err, ShouldBeNil)

					uCh, urCh, srCh = p.Put()

					got := <-uCh
					So(got, ShouldBeNil)

					got = <-urCh
					So(got, ShouldBeNil)

					got = <-srCh
					So(got.Error, ShouldBeBlank)
					So(got.Status, ShouldEqual, RequestStatusUnmodified)
				})
			})

			Convey("Put() uploads an empty file in place of links", func() {
				err = os.Remove(requests[0].Local)
				So(err, ShouldBeNil)

				err = os.Symlink(requests[1].Local, requests[0].Local)
				So(err, ShouldBeNil)

				requests[0].Symlink = requests[1].Local

				err = os.Remove(requests[2].Local)
				So(err, ShouldBeNil)

				err = os.Link(requests[3].Local, requests[2].Local)
				So(err, ShouldBeNil)

				inodesDir := filepath.Join(rootCollection, "mountpoints")
				requests[2].Hardlink = filepath.Join(inodesDir, "inode.file")

				uploading, skipped, statusCounts := uploadRequests(t, h, requests)

				So(uploading, ShouldEqual, len(requests))
				So(statusCounts[RequestStatusUploaded], ShouldEqual, len(requests))
				So(skipped, ShouldEqual, 0)

				it, err := getItemWithBaton(testClient, requests[0].Remote)
				So(err, ShouldBeNil)
				So(it.ISize, ShouldEqual, 0)

				it, err = getItemWithBaton(testClient, requests[2].Remote)
				So(err, ShouldBeNil)
				So(it.ISize, ShouldEqual, 0)
				meta := baton.RodsItemToMeta(it)
				So(meta[MetaKeyRemoteHardlink], ShouldEqual, requests[2].Hardlink)
				So(meta[MetaKeyHardlink], ShouldEqual, requests[2].Local)

				it, err = getItemWithBaton(testClient, requests[2].Hardlink)
				So(err, ShouldBeNil)
				So(it.ISize, ShouldEqual, 2)
			})
		})
	})

	Convey("Putting multiple files, in parallel, gets and sets the correct metadata", t, func() {
		_, clearup := clearTestCollection(t, h, rootCollection)
		defer clearup()

		requests := makeXTestRequests(t, rootCollection, 100)

		for n, r := range requests {
			r.Meta.LocalMeta = map[string]string{}
			tm := time.Unix(int64(n), 0)

			err := os.Chtimes(r.Local, tm, tm)
			So(err, ShouldBeNil)
		}

		p, err := New(h, requests)
		So(err, ShouldBeNil)

		err = p.CreateCollections()
		So(err, ShouldBeNil)

		_, returnCh, skipCh := p.Put()

		for x := range returnCh {
			So(x.Error, ShouldBeEmpty)
		}

		for x := range skipCh {
			So(x.Error, ShouldBeEmpty)
		}

		p, err = New(h, requests)
		So(err, ShouldBeNil)

		err = p.CreateCollections()
		So(err, ShouldBeNil)

		_, returnCh, skipCh = p.Put()

		for x := range returnCh {
			So(x.Error, ShouldBeEmpty)
		}

		for x := range skipCh {
			So(x.Error, ShouldBeEmpty)
		}
	})

	Convey("Uploading a strange path works", t, func() {
		strangePath, p := testPreparePutFile(t, h, "%s.txt", rootCollection)
		urCh := testPutFile(p)

		for request := range urCh {
			So(request.Error, ShouldBeBlank)
			So(request.Status, ShouldEqual, RequestStatusUploaded)
			So(request.Size, ShouldEqual, 1)
			So(request.Local, ShouldEqual, strangePath)
		}
	})

	Convey("Uploading a file with no read permission gives a useful error", t, func() {
		permsPath, p := testPreparePutFile(t, h, "my.txt", rootCollection)
		err := os.Chmod(permsPath, 0o200)
		So(err, ShouldBeNil)

		urCh := testPutFile(p)

		for request := range urCh {
			So(request.Status, ShouldEqual, RequestStatusFailed)
			So(request.Error, ShouldContainSubstring, "Permission denied")
		}
	})

	Convey("Uploading files to the same remote path simultaneously results in 1"+
		" good upload and the others error", t, func() {
		numFiles := 5
		putters := make([]*Putter, numFiles)

		remotePath := filepath.Join(rootCollection, "multi")
		testDeleteCollection(t, h, remotePath)

		fourkContents := make([]byte, 4096)

		for i := range putters {
			path, _ := testCreateLocalFile(t, "file")

			for j := range fourkContents {
				fourkContents[j] = byte(i)
			}

			f, err := os.Create(path)
			So(err, ShouldBeNil)

			for j := 0; j < 25000; j++ {
				_, err = f.Write(fourkContents)
				if err != nil {
					So(err, ShouldBeNil)
				}
			}

			err = f.Close()
			So(err, ShouldBeNil)

			req := &Request{
				Local:  path,
				Remote: remotePath,
				Meta:   NewMeta(),
			}

			p, err := New(h, []*Request{req})
			So(err, ShouldBeNil)
			So(p, ShouldNotBeNil)

			err = p.CreateCollections()
			So(err, ShouldBeNil)

			putters[i] = p
		}

		type ri struct {
			i int
			*Request
		}

		allUploadsCh := make(chan *ri, numFiles)

		var wg sync.WaitGroup

		for i, putter := range putters {
			wg.Add(1)

			go func(p *Putter, i int) {
				defer wg.Done()

				uCh, urCh, srCh := p.Put()

				<-uCh
				<-srCh

				for request := range urCh {
					allUploadsCh <- &ri{i, request}
				}
			}(putter, i)
		}

		wg.Wait()
		close(allUploadsCh)

		numWorked := 0
		expectedByte := -1

		for ri := range allUploadsCh {
			if ri.Status == RequestStatusUploaded {
				numWorked++
				expectedByte = ri.i
			}
		}

		So(numWorked, ShouldEqual, 1)

		localDir := t.TempDir()
		gotPath := filepath.Join(localDir, "got")
		cmd := testutil.NewIRODSCmd(t, "iget")
		So(cmd, ShouldNotBeNil)

		outB, err := cmd.IGET(remotePath, gotPath)
		if err != nil {
			t.Logf("iget failed with output: %s", string(outB))
		}

		So(err, ShouldBeNil)

		f, err := os.Open(gotPath)
		So(err, ShouldBeNil)

		b := make([]byte, 1)

		n, err := f.Read(b)
		So(err, ShouldBeNil)
		So(n, ShouldEqual, 1)
		first := b[0]

		if first != byte(expectedByte) {
			t.Logf("(iRODS does not stop you uploading the same file at the same time;" +
				" errors only generated when we try to set the same metadata)")
		}

		for {
			n, err = f.Read(fourkContents)
			if errors.Is(err, io.EOF) {
				break
			}

			if err != nil {
				So(err, ShouldBeNil)
			}

			for _, b := range fourkContents[:n] {
				if b != first {
					So(b, ShouldEqual, first)
				}
			}
		}
	})
}

func clearTestCollection(t *testing.T, h *baton.Baton, rootCollection string) (*ex.Client, func()) {
	t.Helper()

	testPool := ex.NewClientPool(ex.DefaultClientPoolParams, "")
	testClientCh, err := h.GetClientsFromPoolConcurrently(testPool, 1)
	So(err, ShouldBeNil)

	testClient := <-testClientCh

	_, err = testClient.RemDir(ex.Args{Force: true, Recurse: true}, ex.RodsItem{
		IPath: rootCollection,
	})
	if err != nil {
		errStr := err.Error()

		ignore := slices.ContainsFunc(
			[]string{"-816000", "-310000", "-817000"},
			func(code string) bool { return strings.Contains(errStr, code) },
		)
		if !ignore {
			So(err, ShouldBeNil)
		}
	}

	return testClient, func() {
		testPool.Close()
		testClient.StopIgnoreError()
	}
}

// makeRequests creates some local directories and files, and returns requests
// that all share the same metadata, with remotes pointing to corresponding
// paths within remoteCollection. Also returns the expected remote directories
// that would have to be created.
func makeRequests(t *testing.T, remoteCollection string) ([]*Request, []string) {
	t.Helper()

	sourceDir := t.TempDir()

	requests := makeTestRequests(t, sourceDir, remoteCollection)

	return requests, []string{
		filepath.Join(remoteCollection, "a", "b", "c"),
		filepath.Join(remoteCollection, "a", "b", "d", "e"),
	}
}

func makeXTestRequests(t *testing.T, remoteCollection string, x int) []*Request {
	t.Helper()

	sourceDir := t.TempDir()
	sourcePaths := make([]string, x)

	for n := range sourcePaths {
		sourcePaths[n] = filepath.Join(sourceDir, "x", "file."+strconv.Itoa(n))
	}

	return createTestRequests(t, sourceDir, remoteCollection, sourcePaths)
}

func checkPathExistsWithBaton(client *ex.Client, path string) bool {
	_, err := getItemWithBaton(client, path)

	return err == nil
}

func getItemWithBaton(client *ex.Client, path string) (ex.RodsItem, error) {
	return client.ListItem(ex.Args{AVU: true, Timestamp: true, Size: true}, ex.RodsItem{
		IPath: filepath.Dir(path),
		IName: filepath.Base(path),
	})
}

func getObjectMetadataWithBaton(client *ex.Client, path string) map[string]string {
	it, err := getItemWithBaton(client, path)
	So(err, ShouldBeNil)

	return baton.RodsItemToMeta(it)
}

func testPreparePutFile(t *testing.T, h *baton.Baton, basename, rootCollection string) (string, *Putter) {
	t.Helper()

	path, sourceDir := testCreateLocalFile(t, basename)

	req := &Request{
		Local:  path,
		Remote: strings.Replace(path, sourceDir, rootCollection, 1),
		Meta:   NewMeta(),
	}

	return path, testPreparePutter(t, h, req, rootCollection)
}

func testCreateLocalFile(t *testing.T, basename string) (string, string) {
	t.Helper()

	sourceDir := t.TempDir()

	path := filepath.Join(sourceDir, "testput", basename)
	internal.CreateTestFile(t, path, "1")

	return path, sourceDir
}

func testPreparePutter(t *testing.T, h *baton.Baton, req *Request, rootCollection string) *Putter {
	t.Helper()

	p, err := New(h, []*Request{req})
	So(err, ShouldBeNil)
	So(p, ShouldNotBeNil)

	testDeleteCollection(t, h, rootCollection)

	err = p.CreateCollections()
	So(err, ShouldBeNil)

	return p
}

func testDeleteCollection(t *testing.T, h *baton.Baton, collection string) {
	t.Helper()

	testPool := ex.NewClientPool(ex.DefaultClientPoolParams, "")
	testClientCh, err := h.GetClientsFromPoolConcurrently(testPool, 1)
	So(err, ShouldBeNil)

	testClient := <-testClientCh

	defer testClient.StopIgnoreError()
	defer testPool.Close()

	_, err = testClient.RemDir(ex.Args{Force: true, Recurse: true}, ex.RodsItem{
		IPath: collection,
	})
	if err != nil && !strings.Contains(err.Error(), "-816000") && !strings.Contains(err.Error(), "-310000") {
		So(err, ShouldBeNil)
	}
}

func testPutFile(p *Putter) chan *Request {
	uCh, urCh, srCh := p.Put()

	for request := range uCh {
		So(request.Status, ShouldEqual, RequestStatusUploading)
	}

	So(<-srCh, ShouldBeNil)

	return urCh
}

func compareMetasWithSize(t *testing.T, remote, expected map[string]string, size int64) {
	t.Helper()

	delete(remote, meta.MetaKeyRemoteCtime)
	delete(remote, meta.MetaKeyRemoteMtime)

	expected[meta.MetaKeyRemoteSize] = strconv.FormatInt(size, 10)

	So(remote, ShouldResemble, expected)
}
