package sql

import (
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3" //
	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/ibackup/set/db"
)

func TestSQL(t *testing.T) {
	Convey("With an SQL-based DB", t, func() {
		connStr := filepath.Join(t.TempDir(), "db")

		var (
			setBucket   = []byte("set")
			subBucket   = []byte("sub-bucket")
			countBucket = []byte("count")
			key1        = []byte("key1")
			key2        = []byte("key2")
			valueA      = []byte("valueA")
			valueB      = []byte("valueB")
			valueC      = []byte("valueC")
			valueD      = []byte("valueD")
		)

		d, err := New("sqlite3", connStr, false)
		So(err, ShouldBeNil)

		Convey("You can set and get values", func() {
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

				So(b.Put(key1, valueD), ShouldEqual, ErrTxNotWritable)

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

			Convey("Readonly prevents Updates", func() {
				dr, errr := New("sqlite3", connStr, true)
				So(errr, ShouldBeNil)

				So(dr.Update(func(_ db.Tx) error { return nil }), ShouldEqual, ErrReadOnly)
				err = dr.View(func(tx db.Tx) error {
					b := tx.Bucket(setBucket)

					So(b.Get(key1), ShouldResemble, valueB)

					return nil
				})
			})
		})

		Convey("You can get the next ID", func() {
			err = d.Update(func(tx db.Tx) error {
				b, errr := tx.CreateBucketIfNotExists(countBucket)
				So(errr, ShouldBeNil)

				id, errr := b.NextSequence()
				So(errr, ShouldBeNil)
				So(id, ShouldEqual, 0)

				id, errr = b.NextSequence()
				So(errr, ShouldBeNil)
				So(id, ShouldEqual, 0)

				So(b.Put([]byte{'0'}, nil), ShouldBeNil)

				id, errr = b.NextSequence()
				So(errr, ShouldBeNil)
				So(id, ShouldEqual, 1)

				So(b.Put([]byte{'9'}, nil), ShouldBeNil)

				id, errr = b.NextSequence()
				So(errr, ShouldBeNil)
				So(id, ShouldEqual, 10)

				So(b.Put([]byte{'1', '0'}, nil), ShouldBeNil)

				id, errr = b.NextSequence()
				So(errr, ShouldBeNil)
				So(id, ShouldEqual, 11)

				So(b.Put([]byte{'9', '9', '9'}, nil), ShouldBeNil)

				id, errr = b.NextSequence()
				So(errr, ShouldBeNil)
				So(id, ShouldEqual, 1000)

				So(b.Put([]byte{'1', '0', '0', '9'}, nil), ShouldBeNil)

				id, errr = b.NextSequence()
				So(errr, ShouldBeNil)
				So(id, ShouldEqual, 1010)

				return nil
			})
			So(err, ShouldBeNil)
		})

		Convey("You can use a Cursor to navigate", func() {
			err = d.Update(func(tx db.Tx) error {
				b, errr := tx.CreateBucketIfNotExists([]byte("something"))
				So(errr, ShouldBeNil)

				table := [...][2]string{
					{"ABC", "1"},
					{"ABE", "2"},
					{"BCD", "3"},
					{"CDE", "4"},
				}

				for _, row := range table {
					So(b.Put([]byte(row[0]), []byte(row[1])), ShouldBeNil)
				}

				testCursor := func(table [][2]string, first func(db.Cursor) ([]byte, []byte)) {
					c := b.Cursor()

					for k, v := first(c); k != nil; k, v = c.Next() {
						So(len(table), ShouldBeGreaterThan, 0)
						So(string(k), ShouldEqual, table[0][0])
						So(string(v), ShouldEqual, table[0][1])

						table = table[1:]
					}

					So(len(table), ShouldEqual, 0)
				}

				testCursor(table[:], func(c db.Cursor) ([]byte, []byte) { return c.First() })
				testCursor(table[1:], func(c db.Cursor) ([]byte, []byte) { return c.Seek([]byte("ABD")) })
				testCursor(table[1:], func(c db.Cursor) ([]byte, []byte) { return c.Seek([]byte("ABE")) })
				testCursor(table[2:], func(c db.Cursor) ([]byte, []byte) { return c.Seek([]byte("B")) })
				testCursor(table[4:], func(c db.Cursor) ([]byte, []byte) { return c.Seek([]byte("Z")) })

				return nil
			})
			So(err, ShouldBeNil)
		})
	})
}
