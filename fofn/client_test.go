/*******************************************************************************
 * Copyright (c) 2026 Genome Research Ltd.
 *
 * Authors: Michael Woolnough <mw31@sanger.ac.uk>
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

package fofn

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/ibackup/server"
	"github.com/wtsi-hgi/ibackup/set"
	"github.com/wtsi-hgi/ibackup/transfer"
)

func TestClient(t *testing.T) {
	Convey("With a client", t, func() {
		tmp := t.TempDir()

		client := NewClient(tmp)

		Convey("You can create a new set and get it by name", func() {
			got := &set.Set{
				Name:        "MySet",
				Requester:   "Me",
				Transformer: "prefix=/lacol:/remote",
				Metadata: map[string]string{
					"MyMeta":                "value",
					transfer.MetaKeyReason:  "backup",
					transfer.MetaKeyReview:  "2000-01-01T00:00:00Z",
					transfer.MetaKeyRemoval: "2001-01-01T00:00:00Z",
				},
			}

			So(client.AddOrUpdateSet(got), ShouldBeNil)

			got2, err := client.GetSetByName("Me", "MySet")
			So(err, ShouldBeNil)
			So(got2.Transformer, ShouldEqual, got.Transformer)
			So(got2.Metadata, ShouldResemble, got.Metadata)

			got.Transformer = "prefix=/local:/remote"

			So(client.AddOrUpdateSet(got), ShouldBeNil)

			got2, err = client.GetSetByName("Me", "MySet")
			So(err, ShouldBeNil)
			So(got2.Transformer, ShouldResemble, got.Transformer)

			Convey("and you can set a fofn", func() {
				So(client.MergeFilesWithMTimes(got.ID(), []server.PathMTime{
					{Path: "abc", MTime: 1234},
					{Path: "def", MTime: 5678},
					{Path: "ghi", MTime: 633805},
				}), ShouldBeNil)

				fofnPath := filepath.Join(tmp, got.ID(), fofnFilename)

				_, err := os.Lstat(fofnPath)
				So(os.IsNotExist(err), ShouldBeTrue)

				So(client.TriggerDiscovery(got.ID(), false), ShouldBeNil)

				fofn, err := os.ReadFile(fofnPath)
				So(err, ShouldBeNil)

				fofnData := append(binary.LittleEndian.AppendUint64(nil, 1234), "abc\x00"...)
				fofnData = append(binary.LittleEndian.AppendUint64(fofnData, 5678), "def\x00"...)
				fofnData = append(binary.LittleEndian.AppendUint64(fofnData, 633805), "ghi\x00"...)

				So(fofn, ShouldResemble, fofnData)
			})
		})
	})
}
