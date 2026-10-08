/*******************************************************************************
 * Copyright (c) 2026 Genome Research Ltd.
 *
 * Author: Sendu Bala <sb10@sanger.ac.uk>
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
 * CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 ******************************************************************************/

package testutil

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

var errTestList = errors.New("list failed")

func TestParseIquestCollections(t *testing.T) {
	Convey("iquest output is parsed into collections and their creation times", t, func() {
		for _, tc := range []struct {
			name string
			out  string
			want []testCollection
		}{
			{
				name: "zero-padded epochs and a name containing spaces",
				out:  "01791402001 /base/ibackup_test_a\n01791402002 /base/ibackup_test_a b  c\n",
				want: []testCollection{
					{path: "/base/ibackup_test_a", created: time.Unix(1791402001, 0)},
					{path: "/base/ibackup_test_a b  c", created: time.Unix(1791402002, 0)},
				},
			},
			{
				name: "no rows found",
				out:  "CAT_NO_ROWS_FOUND: Nothing was found matching your query\n",
			},
		} {
			Convey(tc.name, func() {
				collections, err := parseIquestCollections(tc.out)
				So(err, ShouldBeNil)
				So(collections, ShouldResemble, tc.want)
			})
		}

		for _, out := range []string{"/base/ibackup_test_a\n", "notanumber /base/ibackup_test_a\n", "01791402001\n"} {
			Convey("a malformed line is an error: "+out, func() {
				_, err := parseIquestCollections(out)
				So(err, ShouldWrap, errBadIquestLine)
			})
		}
	})
}

func TestListIRODSSubcollections(t *testing.T) {
	Convey("A base containing a quote is rejected before any query is made", t, func() {
		collections, err := listIRODSSubcollections("/base/it's")
		So(err, ShouldWrap, errQuoteInIRODSPath)
		So(collections, ShouldBeNil)
	})
}

// fakeLister lists fixed collections, or fails with err.
type fakeLister struct {
	collections []testCollection
	err         error
	listed      string
}

func (f *fakeLister) list(base string) ([]testCollection, error) {
	f.listed = base

	return f.collections, f.err
}

func TestSweepStaleCollections(t *testing.T) {
	Convey("Given test collections of various ages in a base", t, func() {
		now := time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)

		collection := func(name string, age time.Duration) testCollection {
			return testCollection{path: testPoolBase + "/" + name, created: now.Add(-age)}
		}

		old := collection(collectionPrefix+"old", staleCollectionAge+time.Second)

		lister := &fakeLister{collections: []testCollection{
			old,
			collection(collectionPrefix+"young", time.Hour),
			collection(collectionPrefix+"cutoff", staleCollectionAge),
			collection("other_old", 10*staleCollectionAge),
			collection("x"+collectionPrefix+"old", 10*staleCollectionAge),
		}}

		fake := &fakeCollections{}

		Convey("only the ibackup_test_ collections older than the cutoff are removed", func() {
			So(sweepStaleCollections(testPoolBase, lister.list, fake.remove, now), ShouldBeNil)
			So(lister.listed, ShouldEqual, testPoolBase)
			So(fake.removedCopy(), ShouldResemble, []string{old.path})
		})

		Convey("a failed listing removes nothing and returns the error", func() {
			lister.err = errTestList

			So(sweepStaleCollections(testPoolBase, lister.list, fake.remove, now), ShouldWrap, errTestList)
			So(fake.removedCopy(), ShouldBeEmpty)
		})
	})
}

func TestSweepStaleIRODSCollections(t *testing.T) {
	if _, err := exec.LookPath("iquest"); err != nil {
		t.Skip("skipping iRODS sweep test since iquest is not available")
	}

	Convey("Given a base with a test collection, another collection and a test-named data object", t, func() {
		base := RequireIRODSTestCollection(t)
		icmd := NewIcommander(t)

		staleable := filepath.Join(base, collectionPrefix+"coll")
		other := filepath.Join(base, "other_coll")
		object := filepath.Join(base, collectionPrefix+"obj")

		local := filepath.Join(t.TempDir(), "file")
		So(os.WriteFile(local, []byte("data"), 0o600), ShouldBeNil)

		_, err := icmd.IMKDIR("-p", staleable, other)
		So(err, ShouldBeNil)

		_, err = icmd.IPUT(local, object)
		So(err, ShouldBeNil)

		Convey("a listing has both collections with recent creation times, and not the data object", func() {
			collections, errl := listIRODSSubcollections(base)
			So(errl, ShouldBeNil)
			So(collections, ShouldHaveLength, 2)

			for _, c := range collections {
				So([]string{staleable, other}, ShouldContain, c.path)
				So(time.Since(c.created), ShouldBeBetween, -time.Hour, time.Hour)
			}

			collections, errl = listIRODSSubcollections(other)
			So(errl, ShouldBeNil)
			So(collections, ShouldBeEmpty)

			Convey("so a sweep now removes nothing", func() {
				So(sweepStaleCollections(base, listIRODSSubcollections, removeIRODSCollection, time.Now()), ShouldBeNil)
				So(ilsNames(t, icmd, base), ShouldResemble,
					[]string{collectionPrefix + "coll", collectionPrefix + "obj", "other_coll"})
			})
		})

		Convey("a sweep after the cutoff removes only the test collection", func() {
			later := time.Now().Add(2 * staleCollectionAge)

			So(sweepStaleCollections(base, listIRODSSubcollections, removeIRODSCollection, later), ShouldBeNil)
			So(ilsNames(t, icmd, base), ShouldResemble, []string{collectionPrefix + "obj", "other_coll"})
		})
	})
}

// ilsNames returns the sorted base names of the entries in collection.
func ilsNames(t *testing.T, icmd *ICommander, collection string) []string {
	t.Helper()

	out, err := icmd.ILS(collection)
	So(err, ShouldBeNil)

	lines := strings.Split(strings.TrimSpace(string(out)), "\n")[1:]
	names := make([]string, 0, len(lines))

	for _, line := range lines {
		line = strings.TrimPrefix(strings.TrimSpace(line), "C- ")
		names = append(names, filepath.Base(line))
	}

	return sorted(names)
}
