package sql

import (
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3" //
	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/ibackup/set/db"
)

func TestSQL(t *testing.T) {
	SQLDriver = "sqlite3"

	connStr := filepath.Join(t.TempDir(), "db")

	Convey("", t, func() {
		var (
			setBucket = []byte("set")
			key1      = []byte("key1")
			key2      = []byte("key2")
			valueA    = []byte("valueA")
			valueB    = []byte("valueB")
		)

		d, err := New(connStr)
		So(err, ShouldBeNil)

		err = d.Update(func(tx db.Tx) error {
			b, errr := tx.CreateBucketIfNotExists(setBucket)
			So(errr, ShouldBeNil)

			So(b.Put(key1, valueA), ShouldBeNil)
			So(b.Put(key2, valueB), ShouldBeNil)

			return nil
		})
		So(err, ShouldBeNil)

		err = d.View(func(tx db.Tx) error {
			b := tx.Bucket(setBucket)
			So(b, ShouldNotBeNil)

			So(b.Get(key1), ShouldResemble, valueA)
			So(b.Get(key2), ShouldResemble, valueB)

			values := [][2][]byte{
				{key1, valueA},
				{key2, valueB},
			}

			return b.ForEach(func(key, value []byte) error {
				So(values, ShouldNotBeEmpty)
				So(key, ShouldResemble, values[0][0])
				So(value, ShouldResemble, values[0][1])

				values = values[1:]

				return nil
			})
		})
		So(err, ShouldBeNil)

		err = d.Update(func(tx db.Tx) error {
			b := tx.Bucket(setBucket)
			So(b, ShouldNotBeNil)

			So(b.Put(key1, valueB), ShouldBeNil)
			So(b.Get(key1), ShouldResemble, valueB)

			return nil
		})
		So(err, ShouldBeNil)
	})
}
