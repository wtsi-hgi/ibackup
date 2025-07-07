package db

import (
	"slices"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestSet(t *testing.T) {
	Convey("With a new database", t, func() {
		d := createTestDatabase(t)

		Convey("You can add and retrieve Sets", func() {
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

			So(slices.Collect(d.GetSetsByRequester("me").Iter), ShouldResemble, []*Set{setA, setB})
			So(slices.Collect(d.GetSetsByRequester("you").Iter), ShouldResemble, []*Set{setC})
			So(slices.Collect(d.GetAllSets().Iter), ShouldResemble, []*Set{setA, setB, setC})

			So(d.DeleteSet(setA), ShouldBeNil)

			So(slices.Collect(d.GetAllSets().Iter), ShouldResemble, []*Set{setB, setC})

			So(d.DeleteSet(setC), ShouldBeNil)
			So(slices.Collect(d.GetAllSets().Iter), ShouldResemble, []*Set{setB})
		})
	})
}
