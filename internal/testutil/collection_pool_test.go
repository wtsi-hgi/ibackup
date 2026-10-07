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
	"slices"
	"sync"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

const testPoolBase = "/zone/home/user/pool"

var errTestCreate = errors.New("create failed")

// fakeCollections records the collections a pool creates and removes.
type fakeCollections struct {
	mu        sync.Mutex
	createErr error
	created   []string
	removed   []string
}

func (f *fakeCollections) create(collection string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.created = append(f.created, collection)

	return f.createErr
}

func (f *fakeCollections) remove(collection string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.removed = append(f.removed, collection)
}

func (f *fakeCollections) createdCopy() []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return slices.Clone(f.created)
}

func (f *fakeCollections) removedCopy() []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return slices.Clone(f.removed)
}

func TestCollectionPool(t *testing.T) {
	Convey("Given a collection pool with fake iRODS commands", t, func() {
		const depth = 2

		fake := &fakeCollections{}
		pool := newCollectionPool(testPoolBase, depth, fake.create, fake.remove)

		Reset(pool.Close)

		Convey("nothing is created before the first Require", func() {
			pool.wg.Wait()

			So(fake.createdCopy(), ShouldBeEmpty)
		})

		Convey("Require hands out distinct collections, keeping depth more created ahead", func() {
			const takes = 5

			var (
				taken      []string
				createdNow []int
			)

			for range takes {
				t.Run("take", func(st *testing.T) {
					taken = append(taken, pool.Require(st))

					pool.wg.Wait()

					createdNow = append(createdNow, len(fake.createdCopy()))
				})
			}

			So(taken, ShouldHaveLength, takes)
			So(createdNow, ShouldResemble, []int{depth + 1, depth + 2, depth + 3, depth + 4, depth + 5})

			distinct := make(map[string]bool)

			for _, collection := range taken {
				distinct[collection] = true
			}

			So(distinct, ShouldHaveLength, takes)

			created := fake.createdCopy()

			for _, collection := range taken {
				So(collection, ShouldStartWith, testPoolBase+"/"+collectionPrefix)
				So(created, ShouldContain, collection)
			}

			Convey("each test's collections are removed when it ends", func() {
				So(fake.removedCopy(), ShouldResemble, taken)
			})

			Convey("Close removes the collections not handed out", func() {
				pool.Close()

				unused := slices.DeleteFunc(fake.createdCopy(), func(c string) bool {
					return slices.Contains(taken, c)
				})

				So(unused, ShouldHaveLength, depth)
				So(sorted(fake.removedCopy()[takes:]), ShouldResemble, sorted(unused))
			})
		})

		Convey("a test's collections are all removed together when it ends", func() {
			var collections []string

			var removedDuring []string

			t.Run("take twice", func(st *testing.T) {
				collections = append(collections, pool.Require(st), pool.Require(st))
				removedDuring = fake.removedCopy()
			})

			So(removedDuring, ShouldBeEmpty)
			So(collections[0], ShouldNotEqual, collections[1])
			So(sorted(fake.removedCopy()), ShouldResemble, sorted(collections))
		})

		Convey("Require sets IBACKUP_TEST_COLLECTION to the collection for the test", func() {
			var collection, env string

			t.Run("take", func(st *testing.T) {
				collection = pool.Require(st)
				env = os.Getenv("IBACKUP_TEST_COLLECTION")
			})

			So(env, ShouldEqual, collection)
		})

		Convey("a failed create makes Require fall back to RequireIRODSTestCollection", func() {
			fake.createErr = errTestCreate

			skipped := requireSkipsWithoutIRODS(t, pool)

			So(skipped, ShouldBeTrue)
			So(fake.createdCopy(), ShouldNotBeEmpty)

			Convey("and Close does not remove failed collections", func() {
				pool.Close()

				So(fake.removedCopy(), ShouldBeEmpty)
			})
		})

		Convey("after Close, Require falls back and nothing more is created", func() {
			pool.Close()

			skipped := requireSkipsWithoutIRODS(t, pool)

			So(skipped, ShouldBeTrue)

			pool.refill()
			pool.wg.Wait()

			So(fake.createdCopy(), ShouldBeEmpty)
			So(fake.removedCopy(), ShouldBeEmpty)
		})

		Convey("a take that was handed a collection before Close creates no replacement", func() {
			t.Run("take", func(st *testing.T) {
				pool.Require(st)
			})

			pool.Close()

			created := len(fake.createdCopy())

			pool.refill()
			pool.wg.Wait()

			So(fake.createdCopy(), ShouldHaveLength, created)
		})
	})

	Convey("A nil pool behaves like RequireIRODSTestCollection, and closes without error", t, func() {
		var pool *CollectionPool

		So(requireSkipsWithoutIRODS(t, pool), ShouldBeTrue)
		So(pool.Close, ShouldNotPanic)
	})
}

func sorted(collections []string) []string {
	collections = slices.Clone(collections)
	slices.Sort(collections)

	return collections
}

// requireSkipsWithoutIRODS runs pool.Require in a subtest with no iRODS test
// collection configured, and returns whether it skipped the subtest the way
// RequireIRODSTestCollection does.
func requireSkipsWithoutIRODS(t *testing.T, pool *CollectionPool) bool {
	t.Helper()

	var skipped bool

	t.Run("require without iRODS", func(st *testing.T) {
		st.Setenv("IBACKUP_TEST_COLLECTION", "")
		st.Setenv("IBACKUP_TEST_COLLECTION_BASE", "")

		st.Cleanup(func() { skipped = st.Skipped() })

		pool.Require(st)
	})

	return skipped
}
