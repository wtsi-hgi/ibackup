package db

import (
	"path/filepath"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	_ "modernc.org/sqlite" //
)

func TestDB(t *testing.T) {
	Convey("With a new database", t, func() {
		dbPath := filepath.Join(t.TempDir(), "db")

		d, err := Init("sqlite", dbPath)
		So(err, ShouldBeNil)

		setA := &Set{
			Name:        "mySet",
			Requester:   "me",
			Transformer: "humgen",
			Description: "my first set",
		}

		setB := &Set{
			Name:        "my2ndSet",
			Requester:   "me",
			Transformer: "humgen",
			Description: "my second set",
		}

		setC := &Set{
			Name:        "mySet",
			Requester:   "you",
			Transformer: "humgen",
			Description: "my first set",
		}

		So(d.CreateSet(setA), ShouldBeNil)
		So(d.CreateSet(setA), ShouldNotBeNil)
		So(d.CreateSet(setB), ShouldBeNil)
		So(d.CreateSet(setC), ShouldBeNil)

		got, err := d.GetSet("mySet", "me")
		So(err, ShouldBeNil)
		So(got, ShouldResemble, setA)

		got, err = d.GetSet("my2ndSet", "me")
		So(err, ShouldBeNil)
		So(got, ShouldResemble, setB)

		got, err = d.GetSet("mySet", "you")
		So(err, ShouldBeNil)
		So(got, ShouldResemble, setC)
	})
}
