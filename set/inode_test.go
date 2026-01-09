package set

import (
	"os"
	"path/filepath"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/ibackup/internal"
)

func TestInodes(t *testing.T) {
	Convey("With a new database", t, func() {
		internal.InitStatter(t)

		tmp := t.TempDir()
		fileA := filepath.Join(tmp, "testA")
		fileB := filepath.Join(tmp, "testB")

		So(os.WriteFile(fileA, []byte("Some Data"), 0600), ShouldBeNil)

		dbPath := filepath.Join(tmp, "set.db")

		db, err := New(dbPath, "", false)
		So(err, ShouldBeNil)
		So(db, ShouldNotBeNil)

		Convey("Hardlinks get de-duped", func() {
			got := &Set{
				Name:        "TestSet1",
				Requester:   "me",
				Transformer: "prefix=" + tmp + ":/remote/",
			}

			So(db.AddOrUpdate(got), ShouldBeNil)
			So(db.MergeFileEntries(got.ID(), []string{fileA}), ShouldBeNil)

			got, err = db.Discover(got.ID(), nil)
			So(err, ShouldBeNil)
			So(got.Hardlinks, ShouldBeZeroValue)

			So(os.Rename(fileA, fileB), ShouldBeNil)
			So(os.WriteFile(fileA, []byte("Some Other Data"), 0600), ShouldBeNil)

			got = &Set{
				Name:        "TestSet2",
				Requester:   "me",
				Transformer: "prefix=" + tmp + ":/remote/",
			}

			So(db.AddOrUpdate(got), ShouldBeNil)
			So(db.MergeFileEntries(got.ID(), []string{fileB}), ShouldBeNil)

			got, err = db.Discover(got.ID(), nil)
			So(err, ShouldBeNil)
			So(got.Hardlinks, ShouldBeZeroValue)
		})
	})
}
