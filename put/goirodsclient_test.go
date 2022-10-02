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
	"testing"

	"github.com/cyverse/go-irodsclient/fs"
	. "github.com/smartystreets/goconvey/convey"
)

func TestPutGIC(t *testing.T) {
	h, err := GetGICHandler()
	if err != nil {
		t.Logf("GetGICHandler error: %s", err)
		SkipConvey("Skipping gic tests since couldn't load config yml", t, func() {})

		return
	}

	rootCollection := os.Getenv("IBACKUP_TEST_COLLECTION")
	if rootCollection == "" {
		SkipConvey("Skipping gic tests since IBACKUP_TEST_COLLECTION is not defined", t, func() {})

		return
	}

	Convey("Given Requests and a gic Handler, you can make a new Putter", t, func() {
		requests, expectedCollections := makeRequests(t, rootCollection)

		p, err := New(h, requests)
		So(err, ShouldBeNil)
		So(p, ShouldNotBeNil)

		Convey("CreateCollections() creates the needed collections", func() {
			err = h.fsMkdir.RemoveDir(rootCollection, true, true)
			if err != nil && err.Error() != "USER_FILE_DOES_NOT_EXIST" {
				So(err, ShouldBeNil)
			}

			for _, col := range expectedCollections {
				So(checkPathExistsWithGIC(h.fsMkdir, col), ShouldBeFalse)
			}

			err = p.CreateCollections()
			So(err, ShouldBeNil)

			for _, col := range expectedCollections {
				So(checkPathExistsWithGIC(h.fsMkdir, col), ShouldBeTrue)
			}

			Convey("Put() then puts the files, and adds the metadata", func() {
				failed := p.Put()
				So(failed, ShouldBeNil)

				for _, request := range requests {
					So(checkPathExistsWithGIC(h.fsUpload, request.Remote), ShouldBeTrue)

					meta := getObjectMetadataWithGIC(h.fsUpload, request.Remote)
					So(meta, ShouldResemble, request.Meta)
				}

				Convey("You can put the same file again, with different metadata", func() {
					request := requests[0]
					request.Meta = map[string]string{"a": "1", "b": "3", "c": "4"}

					p, err = New(h, []*Request{request})
					So(err, ShouldBeNil)

					err = p.CreateCollections()
					So(err, ShouldBeNil)

					failed = p.Put()
					So(failed, ShouldBeNil)

					meta := getObjectMetadataWithGIC(h.fsUpload, request.Remote)
					So(meta, ShouldResemble, request.Meta)
				})
			})
		})
	})
}

func checkPathExistsWithGIC(fs *fs.FileSystem, path string) bool {
	return fs.Exists(path)
}

func getObjectMetadataWithGIC(fs *fs.FileSystem, path string) map[string]string {
	ims, err := fs.ListMetadata(path)
	So(err, ShouldBeNil)

	meta := make(map[string]string, len(ims))

	for _, im := range ims {
		meta[im.Name] = im.Value
	}

	return meta
}
