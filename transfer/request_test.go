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

		r, err = NewRequestWithTransformedLocal("project2/mnt/diska/file.zip", transform)
		So(err, ShouldBeNil)
		So(r.Local, ShouldEqual, "project2/mnt/diska/file.zip")
		So(r.Remote, ShouldEqual, "/zone/project2/mnt/diska/file.zip")
	})

	Convey("You can make new requests using regex transformers", t, func() {
		registry := createTestRegistry()
		info, ok := registry.Get("humgen")
		So(ok, ShouldBeTrue)

		humgenTransformer := info.Transformer

		r, err := NewRequestWithTransformedLocal(
			"/lustre/scratch117/casm/team78/so11/file.txt",
			humgenTransformer,
		)
		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, "not a valid humgen path")
		So(r, ShouldBeNil)

		r, err = NewRequestWithTransformedLocal("file.txt", humgenTransformer)
		So(err, ShouldNotBeNil)
		So(r, ShouldBeNil)

		r, err = NewRequestWithTransformedLocal(
			"/lustre/scratch127/hgi/mdt1/teams_v2/martin/dm22/file.txt",
			humgenTransformer,
		)
		So(err, ShouldBeNil)
		So(r.Remote, ShouldEqual, "/humgen/teams/martin/scratch127_v2/dm22/file.txt")

		info, ok = registry.Get("gengen")
		So(ok, ShouldBeTrue)

		gengenTransformer := info.Transformer

		r, err = NewRequestWithTransformedLocal(
			"/lustre/scratch117/casm/team78/so11/file.txt",
			gengenTransformer,
		)
		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, "not a valid gengen path")
		So(r, ShouldBeNil)

		r, err = NewRequestWithTransformedLocal("file.txt", gengenTransformer)
		So(err, ShouldNotBeNil)
		So(r, ShouldBeNil)

		r, err = NewRequestWithTransformedLocal(
			"/lustre/scratch127/gengen/teams_v2/parts/sequencing/file.txt",
			gengenTransformer,
		)
		So(err, ShouldBeNil)
		So(r.Remote, ShouldEqual, "/humgen/gengen/teams/parts/scratch127_v2/sequencing/file.txt")
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
			Meta: &Meta{
				LocalMeta: map[string]string{
					"metaKey": "metaValue",
				},
				remoteMeta: map[string]string{
					"remoteMetaKey": "remoteMetaValue",
				},
			},
			Status:   RequestStatusFailed,
			Symlink:  "/path/to/dest",
			Hardlink: "/path/to/original",
			Size:     123,
			Error:    "oh no",
			Stuck:    new(Stuck),
			skipPut:  true,
		}

		v := reflect.ValueOf(r).Elem()
		t := v.Type()

		fields := t.NumField()

		for i := range fields {
			if t.Field(i).Name == "LocalForJSON" || t.Field(i).Name == "RemoteForJSON" ||
				t.Field(i).Name == "emptyFileRequest" || t.Field(i).Name == "inodeRequest" ||
				t.Field(i).Name == "onlyUploadEmptyFile" {

				continue
			}

			So(v.Field(i).IsZero(), ShouldBeFalse)
		}

		clone := r.Clone()
		So(r, ShouldNotEqual, clone)
		So(*r, ShouldResemble, *clone)

		r.Requester = "someOtherRequester"
		So(r.Requester, ShouldNotEqual, clone.Requester)

		r.Meta.LocalMeta["metaKey"] = "anotherMetaValue"
		So(r.Meta, ShouldNotResemble, clone.Meta)
	})
}

func createTestRegistry() *TransformerRegistry {
	registry := NewTransformerRegistry()

	registry.Register( //nolint:errcheck
		"humgen",
		"Human Genetics path transformer",
		humgenMatchRegex,
		humgenReplaceRegex,
	)

	registry.Register( //nolint:errcheck
		"gengen",
		"Genetics and Genomics path transformer",
		humgenMatchRegex,
		gengenReplaceRegex,
	)

	return registry
}
