package db

import (
	"fmt"
	"slices"
	"strings"
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

			So(collectIter(t, d.GetSetsByRequester("me")), ShouldResemble, []*Set{setA, setB})
			So(collectIter(t, d.GetSetsByRequester("you")), ShouldResemble, []*Set{setC})
			So(collectIter(t, d.GetAllSets()), ShouldResemble, []*Set{setA, setB, setC})

			So(d.DeleteSet(setA), ShouldBeNil)

			So(collectIter(t, d.GetAllSets()), ShouldResemble, []*Set{setB, setC})

			So(d.DeleteSet(setC), ShouldBeNil)
			So(collectIter(t, d.GetAllSets()), ShouldResemble, []*Set{setB})

			Convey("A set can be hidden and unhidden", func() {
				So(setB.Hidden, ShouldBeFalse)
				So(d.SetSetHidden(setB), ShouldBeNil)
				So(setB.Hidden, ShouldBeTrue)

				got, err = d.GetSet("my2ndSet", "me")
				So(err, ShouldBeNil)
				So(got.Hidden, ShouldBeTrue)

				So(d.SetSetVisible(setB), ShouldBeNil)
				So(setB.Hidden, ShouldBeFalse)

				got, err = d.GetSet("my2ndSet", "me")
				So(err, ShouldBeNil)
				So(got.Hidden, ShouldBeFalse)
			})

			Convey("A set can be made readonly", func() {
				So(setB.IsReadonly(), ShouldBeFalse)
				So(d.SetSetReadonly(setB), ShouldBeNil)
				So(setB.IsReadonly(), ShouldBeTrue)

				got, err = d.GetSet("my2ndSet", "me")
				So(err, ShouldBeNil)
				So(got.IsReadonly(), ShouldBeTrue)

				Convey("A readonly set cannot be changed", func() {
					So(d.SetSetHidden(setB), ShouldEqual, ErrReadonlySet)
					So(d.SetSetWarning(setB), ShouldEqual, ErrReadonlySet)
					So(d.SetSetError(setB), ShouldEqual, ErrReadonlySet)
					So(d.SetSetDicoveryStarted(setB), ShouldEqual, ErrReadonlySet)
					So(d.SetSetDicoveryCompleted(setB), ShouldEqual, ErrReadonlySet)
					So(d.DeleteSet(setB), ShouldEqual, ErrReadonlySet)
					So(d.AddSetFiles(setB, slices.Values([]*File{})), ShouldEqual, ErrReadonlySet)
				})

				So(d.SetSetModifiable(setB), ShouldBeNil)
				So(setB.IsReadonly(), ShouldBeFalse)

				got, err = d.GetSet("my2ndSet", "me")
				So(err, ShouldBeNil)
				So(got.IsReadonly(), ShouldBeFalse)
			})

			Convey("You can change the transformer on a set", func() {
				files := slices.Collect(genFiles(5))

				discovery := &Discover{Path: "/some/path", Type: DiscoverFODN}

				So(d.AddSetDiscovery(setB, discovery), ShouldBeNil)
				So(d.AddSetFiles(setB, slices.Values(files)), ShouldBeNil)
				So(d.clearQueue(), ShouldBeNil)

				setA.Transformer = "prefix=/some/:/other/remote/"
				oldID := setB.id

				So(d.SetSetTransformer(setB, func(s string) (string, error) {
					return "/other/remote/" + strings.TrimPrefix(s, "/some/"), nil
				}), ShouldBeNil)

				for _, file := range files {
					file.id += 5
					file.RemotePath = "/other" + file.RemotePath
				}

				So(collectIter(t, d.GetSetFiles(setB)), ShouldResemble, files)
				So(collectIter(t, d.GetSetDiscovery(setB)), ShouldResemble, []*Discover{discovery})

				Convey("The old set is removed once the queued items are dealt with", func() {
					requester := fmt.Sprintf("\x00%d\x00%s", oldID, setB.Requester)

					_, err := scanSet(d.db.QueryRow(getSetByNameRequester, setB.Name, requester))
					So(err, ShouldBeNil)
					So(d.clearQueue(), ShouldBeNil)

					_, err = scanSet(d.db.QueryRow(getSetByNameRequester, setB.Name, requester))
					So(err, ShouldNotBeNil)
				})
			})

			Convey("Adding files to a set updates the set file numbers", func() {
				setB, err := d.GetSet("my2ndSet", "me")
				So(err, ShouldBeNil)
				So(setB.NumFiles, ShouldEqual, 0)
				So(d.AddSetFiles(setB, genFiles(5)), ShouldBeNil)

				setB, err = d.GetSet("my2ndSet", "me")
				So(err, ShouldBeNil)
				So(setB.NumFiles, ShouldEqual, 5)

				files := d.GetSetFiles(setB)
				So(d.RemoveSetFiles(files.Iter), ShouldBeNil)
				So(files.Error, ShouldBeNil)

				setB, err = d.GetSet("my2ndSet", "me")
				So(err, ShouldBeNil)
				So(setB.NumFiles, ShouldEqual, 5)
				So(d.clearQueue(), ShouldBeNil)

				setB, err = d.GetSet("my2ndSet", "me")
				So(err, ShouldBeNil)

				So(setB.NumFiles, ShouldEqual, 0)
			})
		})
	})
}

func collectIter[T any](t *testing.T, i *IterErr[T]) []T {
	t.Helper()

	vs := slices.Collect(i.Iter)
	So(i.Error, ShouldBeNil)

	return vs
}
