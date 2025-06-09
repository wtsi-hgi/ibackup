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
			subBucket = []byte("sub-bucket")
			key1      = []byte("key1")
			key2      = []byte("key2")
			valueA    = []byte("valueA")
			valueB    = []byte("valueB")
			valueC    = []byte("valueC")
			valueD    = []byte("valueD")
		)

		d, err := New(connStr)
		So(err, ShouldBeNil)

		err = d.Update(func(tx db.Tx) error {
			b, errr := tx.CreateBucketIfNotExists(setBucket)
			So(errr, ShouldBeNil)

			So(b.Put(key1, valueA), ShouldBeNil)
			So(b.Put(key2, valueB), ShouldBeNil)

			s, errr := b.CreateBucketIfNotExists(subBucket)
			So(errr, ShouldBeNil)

			So(s.Put(key1, valueC), ShouldBeNil)
			So(s.Put(key2, valueD), ShouldBeNil)

			return nil
		})
		So(err, ShouldBeNil)

		err = d.View(func(tx db.Tx) error {
			b := tx.Bucket(setBucket)
			So(b, ShouldNotBeNil)

			So(b.Get(key1), ShouldResemble, valueA)
			So(b.Get(key2), ShouldResemble, valueB)

			s := b.Bucket(subBucket)
			So(s, ShouldNotBeNil)

			So(s.Get(key1), ShouldResemble, valueC)
			So(s.Get(key2), ShouldResemble, valueD)

			values := [][2][]byte{
				{key1, valueA},
				{key2, valueB},
			}

			So(b.ForEach(func(key, value []byte) error {
				So(values, ShouldNotBeEmpty)
				So(key, ShouldResemble, values[0][0])
				So(value, ShouldResemble, values[0][1])

				values = values[1:]

				return nil
			}), ShouldBeNil)

			values = [][2][]byte{
				{key1, valueC},
				{key2, valueD},
			}

			return s.ForEach(func(key, value []byte) error {
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

			sb := b.Bucket(key1)

			So(sb.Put(key1, valueC), ShouldBeNil)
			So(sb.Put(key2, valueD), ShouldBeNil)

			return nil
		})
		So(err, ShouldBeNil)
	})
}
