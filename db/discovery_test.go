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
			So(collectIter(t, d.GetSetDiscovery(setA)), ShouldResemble, discoveries[:1])

			So(d.AddSetDiscovery(setA, discoveries[1]), ShouldBeNil)
			So(collectIter(t, d.GetSetDiscovery(setA)), ShouldResemble, discoveries[:2])

			discoveries[1].Type = DiscoverFOFN

			So(d.AddSetDiscovery(setA, discoveries[1]), ShouldBeNil)
			So(collectIter(t, d.GetSetDiscovery(setA)), ShouldResemble, discoveries[:2])

			So(d.AddSetDiscovery(setB, discoveries[2]), ShouldBeNil)
			So(collectIter(t, d.GetSetDiscovery(setB)), ShouldResemble, discoveries[2:])

			So(d.DeleteSetDiscovery(setA, discoveries[0].Path), ShouldBeNil)
			So(collectIter(t, d.GetSetDiscovery(setA)), ShouldResemble, discoveries[1:2])

			Convey("Adding a DiscoverDirectory will not overwrite a DiscoverFODN", func() {
				discoveries = append(discoveries, &Discover{
					Path: "/path/to/fodn",
					Type: DiscoverDirectory,
				})

				So(d.AddSetDiscovery(setB, discoveries[3]), ShouldBeNil)
				So(collectIter(t, d.GetSetDiscovery(setB)), ShouldResemble, discoveries[2:])
			})
		})

		Convey("Removing files in a set adds a DiscoverRemovedFile discovery entry for each file", func() {
			files := slices.Collect(genFiles(5))

			So(d.AddSetFiles(setA, slices.Values(files)), ShouldBeNil)
			So(collectIter(t, d.GetSetDiscovery(setA)), ShouldBeNil)

			So(d.RemoveSetFiles(slices.Values(files[:2])), ShouldBeNil)
			So(collectIter(t, d.GetSetDiscovery(setA)), ShouldResemble, []*Discover{
				{
					Path: files[0].LocalPath,
					Type: DiscoverRemovedFile,
				},
				{
					Path: files[1].LocalPath,
					Type: DiscoverRemovedFile,
				},
			})

			So(d.AddSetDiscovery(setA, &Discover{
				Path: files[0].LocalPath,
				Type: DiscoverFile,
			}), ShouldBeNil)

			So(collectIter(t, d.GetSetDiscovery(setA)), ShouldResemble, []*Discover{
				{
					Path: files[0].LocalPath,
					Type: DiscoverFile,
				},
				{
					Path: files[1].LocalPath,
					Type: DiscoverRemovedFile,
				},
			})
		})

		Convey("Removing files in a set by prefix adds a DiscoverRemovedDirectory "+
			"discovery entry and remove now invalid discover entries", func() {
			discoveries := []*Discover{
				{
					Path: "/path/to/directory/file",
					Type: DiscoverFile,
				},
				{
					Path: "/path/to/directory/file",
					Type: DiscoverFODN,
				},
				{
					Path: "/path/to/directory/",
					Type: DiscoverRemovedDirectory,
				},
			}

			So(d.AddSetDiscovery(setA, discoveries[0]), ShouldBeNil)
			So(d.AddSetDiscovery(setA, discoveries[1]), ShouldBeNil)
			So(collectIter(t, d.GetSetDiscovery(setA)), ShouldResemble, discoveries[:2])
			So(d.RemoveSetFilesInDir(setA, "/path/to/directory/"), ShouldBeNil)
			So(collectIter(t, d.GetSetDiscovery(setA)), ShouldResemble, discoveries[1:3])
		})
	})
}
