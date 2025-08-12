package testirods

import (
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/ibackup/baton"
	"github.com/wtsi-npg/extendo/v2"
)

func TestPseudo(t *testing.T) {
	Convey("With the pseudo-irods exes in place", t, func() {
		So(addToolsToPath(t), ShouldBeNil)

		Convey("You can use the baton package to perform operations", func() {
			b, err := baton.GetBatonHandler()
			So(err, ShouldBeNil)

			dir := t.TempDir()

			localA := filepath.Join(dir, "localA")
			localB := filepath.Join(dir, "localB")
			remoteA := "/irods/test/path/remoteA"

			So(os.WriteFile(localA, []byte("some data"), 0600), ShouldBeNil)

			So(b.Mkdir(path.Dir(remoteA)), ShouldBeNil)

			timeA := time.Now().Truncate(time.Second)

			So(b.Put(localA, remoteA), ShouldBeNil)

			timeB := time.Now()

			So(b.Get(localB, remoteA), ShouldBeNil)

			got, err := os.ReadFile(localB)
			So(err, ShouldBeNil)
			So(got, ShouldResemble, []byte("some data"))

			stat, err := b.StatWithMeta(remoteA)
			So(err, ShouldBeNil)
			So(stat.MTime, ShouldHappenOnOrBetween, timeA, timeB)
			So(stat.Size, ShouldEqual, 9)
			So(len(stat.Metadata), ShouldBeZeroValue)

			meta := []extendo.AVU{
				{Attr: "keyA", Value: "valueA"},
				{Attr: "keyB", Value: "valueB"},
				{Attr: "keyB", Value: "valueC"},
				{Attr: "keyB", Value: "valueC", Units: "unitA"},
			}

			So(b.AddAVUs(remoteA, meta), ShouldBeNil)

			stat, err = b.StatWithMeta(remoteA)
			So(err, ShouldBeNil)
			So(stat.MTime, ShouldHappenOnOrBetween, timeA, timeB)
			So(stat.Size, ShouldEqual, 9)
			So(stat.Metadata, ShouldResemble, meta)

			So(b.RemoveFile(remoteA), ShouldBeNil)

			_, err = b.StatWithMeta(remoteA)
			So(err, ShouldEqual, fs.ErrNotExist)
		})
	})
}
