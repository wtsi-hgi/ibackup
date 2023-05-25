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
	"reflect"
	"strconv"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestRequest(t *testing.T) {
	Convey("You can get request IDs", t, func() {
		r := &Request{Local: "/a", Remote: "/b"}
		id := r.ID()
		So(id, ShouldNotBeBlank)

		r.Set = "c"
		id2 := r.ID()
		So(id2, ShouldNotBeBlank)
		So(id2, ShouldNotEqual, id)

		r.Requester = "d"
		id3 := r.ID()
		So(id3, ShouldNotBeBlank)
		So(id3, ShouldNotEqual, id2)

		r.Local = "/e"
		id4 := r.ID()
		So(id4, ShouldNotBeBlank)
		So(id4, ShouldNotEqual, id3)

		r.Remote = "/f"
		id5 := r.ID()
		So(id5, ShouldNotBeBlank)
		So(id5, ShouldNotEqual, id4)

		r2 := &Request{Local: "/e", Remote: "/f", Set: "c", Requester: "d"}

		So(r2.ID(), ShouldEqual, id5)
	})

	Convey("You can validate request paths", t, func() {
		r := &Request{Local: "/root/../foo", Remote: "/bar"}
		err := r.ValidatePaths()
		So(err, ShouldBeNil)
		So(r.Local, ShouldEqual, "/foo")
		So(r.Remote, ShouldEqual, "/bar")

		r = &Request{Local: "foo", Remote: "/bar"}
		err = r.ValidatePaths()
		So(err, ShouldBeNil)
		wd, err := os.Getwd()
		So(err, ShouldBeNil)
		So(r.Local, ShouldEqual, filepath.Join(wd, "foo"))

		r = &Request{Local: "/foo", Remote: "bar"}
		err = r.ValidatePaths()
		So(err, ShouldNotBeNil)
	})

	Convey("You can make new requests using the prefix transform", t, func() {
		transform := PrefixTransformer("/mnt/diska", "/zone")
		r, err := NewRequestWithTransformedLocal("/mnt/diska/project1/file.txt", transform)
		So(err, ShouldBeNil)
		So(r.Local, ShouldEqual, "/mnt/diska/project1/file.txt")
		So(r.Remote, ShouldEqual, "/zone/project1/file.txt")

		r, err = NewRequestWithTransformedLocal("project2/file.zip", transform)
		So(err, ShouldBeNil)
		So(r.Local, ShouldEqual, "project2/file.zip")
		So(r.Remote, ShouldEqual, "/zone/project2/file.zip")
	})

	Convey("You can make new requests using the humgen transform", t, func() {
		r, err := NewRequestWithTransformedLocal("/lustre/scratch117/casm/team78/so11/file.txt", HumgenTransformer)
		So(err, ShouldNotBeNil)
		So(r, ShouldBeNil)

		r, err = NewRequestWithTransformedLocal("file.txt", HumgenTransformer)
		So(err, ShouldNotBeNil)
		So(r, ShouldBeNil)

		locals := []string{
			"/lustre/scratch118/humgen/projects/ddd/file.txt",
			"/lustre/scratch118/humgen/hgi/projects/ibdx10/file.txt",
			"/lustre/scratch118/humgen/hgi/users/hp3/file.txt",
			"/lustre/scratch119/realdata/mdt3/projects/interval_rna/file.txt",
			"/lustre/scratch119/realdata/mdt3/teams/parts/ap32/file.txt",
			"/lustre/scratch123/hgi/mdt2/projects/chromo_ndd/file.txt",
			"/lustre/scratch123/hgi/mdt1/teams/martin/dm22/file.txt",
			"/lustre/scratch123/hgi/mdt1/teams/martin/dm22/sub/folder/file.txt",
		}

		expected := []string{
			"/humgen/projects/ddd/scratch118/file.txt",
			"/humgen/projects/ibdx10/scratch118/file.txt",
			"/humgen/users/hp3/scratch118/file.txt",
			"/humgen/projects/interval_rna/scratch119/file.txt",
			"/humgen/teams/parts/scratch119/ap32/file.txt",
			"/humgen/projects/chromo_ndd/scratch123/file.txt",
			"/humgen/teams/martin/scratch123/dm22/file.txt",
			"/humgen/teams/martin/scratch123/dm22/sub/folder/file.txt",
		}

		for i, local := range locals {
			r, err = NewRequestWithTransformedLocal(local, HumgenTransformer)
			So(err, ShouldBeNil)
			So(r.Remote, ShouldEqual, expected[i])
		}
	})

	Convey("You can create and stringify Stucks", t, func() {
		n := time.Now()
		s := NewStuck(n)
		So(s, ShouldNotBeNil)
		So(s.Host, ShouldNotBeBlank)
		So(s.PID, ShouldEqual, os.Getpid())
		So(s.String(), ShouldContainSubstring, "upload stuck? started ")
		So(s.String(), ShouldContainSubstring, " on host "+s.Host)
		So(s.String(), ShouldContainSubstring, ", PID "+strconv.Itoa(s.PID))
	})

	Convey("You can specify symlink files and get back a path to an empty file for uploading", t, func() {
		local := "/a/file"
		size := uint64(123)

		r := &Request{Local: local, Symlink: "/another/file", Size: size}
		So(r.LocalDataPath(), ShouldEqual, os.DevNull)
		So(r.UploadedSize(), ShouldEqual, 0)

		r = &Request{Local: local, Size: size}
		So(r.LocalDataPath(), ShouldEqual, local)
		So(r.UploadedSize(), ShouldEqual, size)
	})

	Convey("Cloning a request creates an exact copy with cloned maps", t, func() {
		r := &Request{
			Local:     "/some/path",
			Remote:    "/some/remote/path",
			Requester: "someRequester",
			Set:       "testSet",
			Meta: map[string]string{
				"metaKey": "metaValue",
			},
			Status:   RequestStatusFailed,
			Symlink:  "/path/to/dest",
			Hardlink: "/path/to/original",
			Size:     123,
			Error:    "oh no",
			Stuck:    new(Stuck),
			remoteMeta: map[string]string{
				"remoteMetaKey": "remoteMetaValue",
			},
			skipPut: true,
		}

		v := reflect.ValueOf(r).Elem()
		t := v.Type()

		fields := t.NumField()

		for i := 0; i < fields; i++ {
			if t.Field(i).Name == "LocalForJSON" || t.Field(i).Name == "RemoteForJSON" ||
				t.Field(i).Name == "origRemote" || t.Field(i).Name == "originalRemoteMeta" {

				continue
			}

			So(v.Field(i).IsZero(), ShouldBeFalse)
		}

		clone := r.Clone()
		So(r, ShouldNotEqual, clone)
		So(*r, ShouldResemble, *clone)

		r.Requester = "someOtherRequester"
		So(r.Requester, ShouldNotEqual, clone.Requester)

		r.Meta["metaKey"] = "anotherMetaValue"
		So(r.Meta, ShouldNotResemble, clone.Meta)
	})
}
