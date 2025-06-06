/*******************************************************************************
 * Copyright (c) 2023 Genome Research Ltd.
 *
 * Authors: Michael Woolnough <mw31@sanger.ac.uk>
 *          Sendu Bala <sb10@sanger.ac.uk>
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

package server

import (
	"sync"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/ibackup/set"
)

func TestMonitorHeap(t *testing.T) {
	Convey("Given a new Monitor Heap", t, func() {
		var mu sync.Mutex
		names := ""

		mh := NewMonitor(func(given *set.Set) {
			mu.Lock()
			defer mu.Unlock()

			names += given.Name
		})

		Convey("You can add sets to it", func() {
			ld := time.Now()

			set1 := set.Set{
				Name:          "first",
				MonitorTime:   1 * time.Second,
				LastDiscovery: ld,
			}

			mh.Add(&set.Set{
				Name:          "third",
				MonitorTime:   3 * time.Second,
				LastDiscovery: ld,
			})

			mh.Add(&set1)

			mh.Add(&set.Set{
				Name:          "second",
				MonitorTime:   2 * time.Second,
				LastDiscovery: ld,
			})

			Convey("And then get the next time to discover, and the next sets", func() {
				So(mh.NextDiscovery(), ShouldEqual, ld.Add(1*time.Second))
				next := mh.NextSet()
				So(next.Name, ShouldEqual, "first")

				So(mh.NextDiscovery(), ShouldEqual, ld.Add(2*time.Second))
				next = mh.NextSet()
				So(next.Name, ShouldEqual, "second")

				So(mh.NextDiscovery(), ShouldEqual, ld.Add(3*time.Second))
				next = mh.NextSet()
				So(next.Name, ShouldEqual, "third")

				So(mh.NextDiscovery().IsZero(), ShouldBeTrue)
				next = mh.NextSet()
				So(next, ShouldBeNil)
			})

			Convey("And then you can remove sets from it", func() {
				err := mh.Remove(set1.ID())
				So(err, ShouldBeNil)

				next := mh.NextSet()
				So(next.Name, ShouldEqual, "second")
			})
		})

		Convey("You can add another sets to it", func() {
			ld := time.Now()

			set1 := set.Set{
				Name:          "first",
				MonitorTime:   10 * time.Millisecond,
				LastDiscovery: ld,
			}

			mh.Add(&set.Set{
				Name:          "third",
				MonitorTime:   30 * time.Millisecond,
				LastDiscovery: ld,
			})

			mh.Add(&set1)

			mh.Add(&set.Set{
				Name:          "second",
				MonitorTime:   20 * time.Millisecond,
				LastDiscovery: ld,
			})

			Convey("Which get automatically sent to the callback at the right time", func() {
				<-time.After(100 * time.Millisecond)

				mu.Lock()
				defer mu.Unlock()
				So(names, ShouldEqual, "firstsecondthird")

				So(mh.NextDiscovery().IsZero(), ShouldBeTrue)
				next := mh.NextSet()
				So(next, ShouldBeNil)
			})

			Convey("And you can remove sets from it", func() {
				err := mh.Remove(set1.ID())
				So(err, ShouldBeNil)

				<-time.After(100 * time.Millisecond)

				mu.Lock()
				defer mu.Unlock()
				So(names, ShouldEqual, "secondthird")
			})
		})
	})
}
