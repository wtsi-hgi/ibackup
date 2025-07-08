package db

import (
	"slices"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestDiscovery(t *testing.T) {
	Convey("With a new database", t, func() {
		d := createTestDatabase(t)

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

		So(d.CreateSet(setA), ShouldBeNil)
		So(d.CreateSet(setB), ShouldBeNil)

		Convey("You can add and remove discovery locations for a set", func() {
			discoveries := []*Discover{
				{
					Path: "/path/to/directory/",
					Type: DiscoverDirectory,
				},
				{
					Path: "/path/to/fofn",
					Type: DiscoverFODN,
				},
				{
					Path: "/path/to/fodn",
					Type: DiscoverFODN,
				},
			}

			So(d.AddSetDiscovery(setA, discoveries[0]), ShouldBeNil)
			So(slices.Collect(d.GetSetDiscovery(setA).Iter), ShouldResemble, discoveries[:1])

			So(d.AddSetDiscovery(setA, discoveries[1]), ShouldBeNil)
			So(slices.Collect(d.GetSetDiscovery(setA).Iter), ShouldResemble, discoveries[:2])

			discoveries[1].Type = DiscoverFOFN

			So(d.AddSetDiscovery(setA, discoveries[1]), ShouldBeNil)
			So(slices.Collect(d.GetSetDiscovery(setA).Iter), ShouldResemble, discoveries[:2])

			So(d.AddSetDiscovery(setB, discoveries[2]), ShouldBeNil)
			So(slices.Collect(d.GetSetDiscovery(setB).Iter), ShouldResemble, discoveries[2:])

			So(d.DeleteSetDiscovery(setA, discoveries[0].Path), ShouldBeNil)
			So(slices.Collect(d.GetSetDiscovery(setA).Iter), ShouldResemble, discoveries[1:2])
		})
	})
}
