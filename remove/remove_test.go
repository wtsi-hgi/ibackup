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

package remove

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/ibackup/internal"
	"github.com/wtsi-hgi/ibackup/put"
)

const userPerms = 0700

func TestRemoveMock(t *testing.T) {
	Convey("Given a mock RemoveHandler", t, func() {
		requests, expectedCollections := makeMockRequests(t)

		lh := internal.GetLocalHandler()

		requests[0].Requester = "John"

		for _, request := range requests {
			uploadRequest(t, lh, request)
		}

		Convey("RemoveFile removes a file", func() {
			filePath := requests[0].Remote

			err := lh.RemoveFile(filePath)
			So(err, ShouldBeNil)

			_, err = os.Stat(filePath)
			So(err, ShouldNotBeNil)

			So(lh.Meta[filePath], ShouldBeNil)

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

			paths, err := lh.QueryMeta("", map[string]string{put.MetaKeyRequester: requests[0].Requester})
			So(err, ShouldBeNil)

			So(len(paths), ShouldEqual, 1)

			Convey("queryMeta only returns paths in the provided scope", func() {
				paths, errq := lh.QueryMeta(expectedCollections[1], map[string]string{"a": "1"})
				So(errq, ShouldBeNil)

				So(len(paths), ShouldEqual, 2)
			})
		})
	})
}

func uploadRequest(t *testing.T, lh *internal.LocalHandler, request *put.Request) {
	err := os.MkdirAll(filepath.Dir(request.Remote), userPerms)
	So(err, ShouldBeNil)

	err = lh.Put(request.LocalDataPath(), request.Remote, request.Meta.Metadata())
	So(err, ShouldBeNil)

	request.Meta.LocalMeta[put.MetaKeyRequester] = request.Requester
	err = lh.AddMeta(request.Remote, request.Meta.LocalMeta)
	So(err, ShouldBeNil)
}

// makeMockRequests creates some local directories and files, and returns
// requests that all share the same metadata, with remotes pointing to another
// local temp directory (but the remote sub-directories are not created).
// Also returns the execpted remote directories that would have to be created.
func makeMockRequests(t *testing.T) ([]*put.Request, []string) {
	t.Helper()

	sourceDir := t.TempDir()
	destDir := t.TempDir()

	requests := makeTestRequests(t, sourceDir, destDir)

	return requests, []string{
		filepath.Join(destDir, "a", "b", "c"),
		filepath.Join(destDir, "a", "b", "d", "e"),
	}
}

func makeTestRequests(t *testing.T, sourceDir, destDir string) []*put.Request {
	t.Helper()

	sourcePaths := []string{
		filepath.Join(sourceDir, "a", "b", "c", "file.1"),
		filepath.Join(sourceDir, "a", "b", "file.2"),
		filepath.Join(sourceDir, "a", "b", "d", "file.3"),
		filepath.Join(sourceDir, "a", "b", "d", "e", "file.4"),
		filepath.Join(sourceDir, "a", "b", "d", "e", "file.5"),
	}

	requests := make([]*put.Request, len(sourcePaths))
	localMeta := map[string]string{"a": "1", "b": "2"}

	for i, path := range sourcePaths {
		dir := filepath.Dir(path)

		err := os.MkdirAll(dir, userPerms)
		if err != nil {
			t.Fatal(err)
		}

		internal.CreateTestFile(t, path, "1\n")

		requests[i] = &put.Request{
			Local:  path,
			Remote: strings.Replace(path, sourceDir, destDir, 1),
			Meta:   &put.Meta{LocalMeta: localMeta},
		}
	}

	return requests
}
