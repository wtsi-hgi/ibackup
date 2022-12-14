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
	ex "github.com/wtsi-npg/extendo/v2"
)

func TestPutBaton(t *testing.T) {
	h, err := GetBatonHandler()
	if err != nil {
		t.Logf("GetBatonHandler error: %s", err)
		SkipConvey("Skipping baton tests since couldn't find baton", t, func() {})

		return
	}

	rootCollection := os.Getenv("IBACKUP_TEST_COLLECTION")
	if rootCollection == "" {
		SkipConvey("Skipping baton tests since IBACKUP_TEST_COLLECTION is not defined", t, func() {})

		return
	}

	Convey("Given Requests and a baton Handler, you can make a new Putter", t, func() {
		requests, expectedCollections := makeRequests(t, rootCollection)

		p, err := New(h, requests)
		So(err, ShouldBeNil)
		So(p, ShouldNotBeNil)

		So(h.pool.IsOpen(), ShouldBeTrue)
		So(h.putClient.IsRunning(), ShouldBeTrue)
		So(h.metaClient.IsRunning(), ShouldBeTrue)

		Convey("CreateCollections() creates the needed collections", func() {
			_, err = h.putClient.RemDir(ex.Args{Force: true, Recurse: true}, ex.RodsItem{
				IPath: rootCollection,
			})
			if err != nil && !strings.Contains(err.Error(), "-816000") && !strings.Contains(err.Error(), "-310000") {
				So(err, ShouldBeNil)
			}

			for _, col := range expectedCollections {
				So(checkPathExistsWithBaton(h.putClient, col), ShouldBeFalse)
			}

			err = p.CreateCollections()
			So(err, ShouldBeNil)

			for _, col := range expectedCollections {
				So(checkPathExistsWithBaton(h.putClient, col), ShouldBeTrue)
			}

			Convey("Put() then puts the files, and adds the metadata", func() {
				requester := "John"
				requests[0].Requester = requester
				requests[0].Set = "setA"
				expectedMTime := touchFile(requests[0].Local, -1*time.Hour)
				rCh := p.Put()

				for request := range rCh {
					So(request.Error, ShouldBeNil)
					So(request.Status, ShouldEqual, RequestStatusUploaded)
					meta := getObjectMetadataWithBaton(h.putClient, request.Remote)
					So(meta, ShouldResemble, request.Meta)
					checkAddedMeta(meta)

					if request.Local == requests[0].Local {
						mtime := time.Time{}
						err = mtime.UnmarshalText([]byte(meta[metaKeyMtime]))
						So(err, ShouldBeNil)

						So(mtime.UTC().Truncate(time.Second), ShouldEqual, expectedMTime.UTC().Truncate(time.Second))
					}
				}

				Convey("You can put the same file again if it changed, with different metadata", func() {
					request := requests[0]
					request.Requester = requester
					request.Set = "setB"
					request.Meta = map[string]string{"a": "1", "b": "3", "c": "4"}
					touchFile(request.Local, 1*time.Hour)

					p, err = New(h, []*Request{request})
					So(err, ShouldBeNil)

					err = p.CreateCollections()
					So(err, ShouldBeNil)

					rCh = p.Put()

					got := <-rCh
					So(got.Error, ShouldBeNil)
					So(got.Status, ShouldEqual, RequestStatusReplaced)
					meta := getObjectMetadataWithBaton(h.putClient, request.Remote)
					So(meta, ShouldResemble, request.Meta)
					So(meta[metaKeyRequester], ShouldEqual, requester)
					So(meta[metaKeySets], ShouldEqual, "setA,setB")

					Convey("Finally, Cleanup() stops the clients", func() {
						err = p.Cleanup()
						So(err, ShouldBeNil)

						So(h.pool.IsOpen(), ShouldBeFalse)
						So(h.putClient.IsRunning(), ShouldBeFalse)
						So(h.metaClient.IsRunning(), ShouldBeFalse)
					})
				})

				Convey("Unchanged files aren't replaced", func() {
					request := requests[0]

					p, err = New(h, []*Request{request})
					So(err, ShouldBeNil)

					rCh = p.Put()

					got := <-rCh
					So(got.Error, ShouldBeNil)
					So(got.Status, ShouldEqual, RequestStatusUnmodified)
				})
			})
		})
	})
}

// makeRequests creates some local directories and files, and returns requests
// that all share the same metadata, with remotes pointing to corresponding
// paths within remoteCollection. Also returns the execpted remote directories
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

func checkPathExistsWithBaton(client *ex.Client, path string) bool {
	_, err := getItemWithBaton(client, path)

	return err == nil
}

func getItemWithBaton(client *ex.Client, path string) (ex.RodsItem, error) {
	return client.ListItem(ex.Args{AVU: true, Timestamp: true}, ex.RodsItem{
		IPath: filepath.Dir(path),
		IName: filepath.Base(path),
	})
}

func getObjectMetadataWithBaton(client *ex.Client, path string) map[string]string {
	it, err := getItemWithBaton(client, path)
	So(err, ShouldBeNil)

	return rodsItemToMeta(it)
}
