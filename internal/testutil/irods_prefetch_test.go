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
	"os/exec"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestPrefetchAfterStop(t *testing.T) {
	Convey("A take that loaded the prefetcher before it stopped creates no collections", t, func() {
		const (
			base  = "/zone/home/user/prefetch"
			depth = 2
		)

		var created atomic.Int32

		p := &collectionPrefetcher{
			base:  base,
			depth: depth,
			create: func(string) error {
				created.Add(1)

				return nil
			},
			results: make(chan prefetchResult, depth),
		}

		prefetchMu.Lock()
		prefetcher = p
		prefetchMu.Unlock()

		StopPrefetchingIRODSTestCollections()

		prefetchMu.Lock()
		prefetcher = p
		prefetchMu.Unlock()

		So(takePrefetchedCollection(base), ShouldEqual, "")

		p.start()
		p.wg.Wait()

		So(created.Load(), ShouldEqual, 0)
		So(p.results, ShouldBeEmpty)

		Reset(func() {
			prefetchMu.Lock()
			prefetcher = nil
			prefetchMu.Unlock()
		})
	})
}

func TestPrefetchIRODSTestCollections(t *testing.T) {
	Convey("With prefetching enabled, tests get unique, empty, existing collections", t, func() {
		base := irodsBaseFromEnv()
		if base == "" || !iCommandsAvailable() {
			SkipConvey("skipping iRODS test since IBACKUP_TEST_COLLECTION or iCommands are not available", func() {})

			return
		}

		const depth = 2

		PrefetchIRODSTestCollections(depth)

		So(prefetcher, ShouldNotBeNil)
		So(prefetcher.started, ShouldBeFalse)

		var (
			taken   []string
			listing []string
		)

		for range depth + 1 {
			t.Run("take", func(st *testing.T) {
				collection := RequireIRODSTestCollection(st)
				taken = append(taken, collection)
				listing = append(listing, ilsOutput(st, collection))
			})
		}

		So(len(taken), ShouldEqual, depth+1)

		for i, collection := range taken {
			So(collection, ShouldStartWith, base+"/"+collectionPrefix)
			So(slices.Index(taken, collection), ShouldEqual, i)
			So(listing[i], ShouldEqual, collection+":")
		}

		Convey("each test's collection is removed when it ends", func() {
			for _, collection := range taken {
				So(ilsOutput(t, collection), ShouldBeEmpty)
			}
		})

		Convey("stopping removes the collections not handed out", func() {
			p := prefetcher
			So(p, ShouldNotBeNil)

			p.wg.Wait()

			unused := make([]string, 0, depth)

			for range depth {
				r := <-p.results
				So(r.err, ShouldBeNil)
				So(ilsOutput(t, r.collection), ShouldEqual, r.collection+":")

				unused = append(unused, r.collection)
				p.results <- r
			}

			StopPrefetchingIRODSTestCollections()
			So(prefetcher, ShouldBeNil)

			for _, collection := range unused {
				So(ilsOutput(t, collection), ShouldBeEmpty)
			}
		})

		Reset(StopPrefetchingIRODSTestCollections)
	})
}

func iCommandsAvailable() bool {
	for _, command := range []string{"imkdir", "irm", "ils"} {
		if _, err := exec.LookPath(command); err != nil {
			return false
		}
	}

	return true
}

// ilsOutput returns the trimmed ils listing of collection, or an empty string
// if it does not exist.
func ilsOutput(t *testing.T, collection string) string {
	t.Helper()

	out, err := exec.CommandContext(t.Context(), "ils", collection).CombinedOutput()
	if err != nil {
		return ""
	}

	return strings.TrimSpace(string(out))
}
