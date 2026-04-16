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
