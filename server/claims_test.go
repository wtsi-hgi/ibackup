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

package server

import (
	"context"
	"testing"

	"github.com/VertebrateResequencing/wr/queue"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/ibackup/transfer"
)

func TestRemoteClaimsReleasedCount(t *testing.T) {
	Convey("Given claims on a running upload's remote object", t, func() {
		ctx := context.Background()
		q := queue.New(ctx, "put")
		removeQueue := queue.New(ctx, "remove")

		Reset(func() {
			So(q.Destroy(), ShouldBeNil)
			So(removeQueue.Destroy(), ShouldBeNil)
		})

		rc := newRemoteClaims(q, removeQueue)

		r := &transfer.Request{Local: "/claimed/file", Remote: "/remote/file", Requester: "claimer", Set: "claims"}
		_, _, err := q.AddMany(ctx, []*queue.ItemDef{{Key: r.ID(), Data: r, TTR: ttr}})
		So(err, ShouldBeNil)

		_, err = q.Reserve("", 0)
		So(err, ShouldBeNil)

		batch := make(map[string]bool)
		removalKey := "removal key"

		claimed, err := rc.claim(r, batch)
		So(err, ShouldBeNil)
		So(claimed, ShouldBeTrue)

		before := rc.releasedCount()

		Convey("releasedCount() goes up by one when the upload releases its claim", func() {
			So(rc.release(r), ShouldBeNil)
			So(rc.releasedCount(), ShouldEqual, before+1)
		})

		Convey("releasedCount() goes up by one for each remote path when a hardlink upload releases its claims", func() {
			So(rc.release(r), ShouldBeNil)

			hardlink := *r
			hardlink.Hardlink = "/inode"
			before = rc.releasedCount()

			claimed, err = rc.claim(&hardlink, make(map[string]bool))
			So(err, ShouldBeNil)
			So(claimed, ShouldBeTrue)

			So(rc.release(&hardlink), ShouldBeNil)
			So(rc.releasedCount(), ShouldEqual, before+2)
		})

		Convey("releasedCount() goes up by one when the claim is unclaimed", func() {
			So(rc.unclaim(r, batch), ShouldBeNil)
			So(rc.releasedCount(), ShouldEqual, before+1)
		})

		Convey("releasedCount() goes up by one when a re-reservation onto a different path gives up the stale claim", func() {
			moved := *r
			moved.Hardlink = "/inode"

			claimed, err = rc.claim(&moved, make(map[string]bool))
			So(err, ShouldBeNil)
			So(claimed, ShouldBeTrue)
			So(rc.releasedCount(), ShouldEqual, before+1)
		})

		Convey("releasedCount() goes up by one when a removal gives up the claim of an upload no longer running", func() {
			So(q.Release(ctx, r.ID()), ShouldBeNil)

			claimed, err = rc.claimForRemoval(r.Set, removalKey, []string{r.RemoteDataPath()})
			So(err, ShouldBeNil)
			So(claimed, ShouldBeTrue)
			So(rc.releasedCount(), ShouldEqual, before+1)
		})

		Convey("releasedCount() doesn't change when a removal waits on the running upload", func() {
			_, _, err = removeQueue.AddMany(ctx, []*queue.ItemDef{{Key: removalKey, Data: removalKey, TTR: ttr}})
			So(err, ShouldBeNil)

			_, err = removeQueue.Reserve("", 0)
			So(err, ShouldBeNil)

			claimed, err = rc.claimForRemoval(r.Set, removalKey, []string{r.RemoteDataPath()})
			So(err, ShouldBeNil)
			So(claimed, ShouldBeFalse)
			So(rc.releasedCount(), ShouldEqual, before)
		})
	})
}

func TestRemoteClaimsWakeWaiters(t *testing.T) {
	Convey("Given a running hardlink upload's claims, and a hardlink waiting on its inode path", t, func() {
		ctx := context.Background()
		q := queue.New(ctx, "put")
		removeQueue := queue.New(ctx, "remove")

		Reset(func() {
			So(q.Destroy(), ShouldBeNil)
			So(removeQueue.Destroy(), ShouldBeNil)
		})

		rc := newRemoteClaims(q, removeQueue)

		hardlink := &transfer.Request{
			Local: "/claimed/link", Remote: "/remote/file", Hardlink: "/remote/inode",
			Requester: "claimer", Set: "claims",
		}
		_, _, err := q.AddMany(ctx, []*queue.ItemDef{{Key: hardlink.ID(), Data: hardlink, TTR: ttr}})
		So(err, ShouldBeNil)

		_, err = q.Reserve("", 0)
		So(err, ShouldBeNil)

		batch := make(map[string]bool)

		claimed, err := rc.claim(hardlink, batch)
		So(err, ShouldBeNil)
		So(claimed, ShouldBeTrue)

		waiter := &transfer.Request{
			Local: "/waiting/link", Remote: "/remote/other", Hardlink: hardlink.Hardlink,
			Requester: "waiter", Set: "waits",
		}
		_, _, err = q.AddMany(ctx, []*queue.ItemDef{{
			Key: waiter.ID(), Data: waiter, TTR: ttr,
			Dependencies: []string{remoteClaimDependencyPrefix + hardlink.Hardlink},
		}})
		So(err, ShouldBeNil)

		waiterState := func() queue.ItemState {
			item, errg := q.Get(waiter.ID())
			So(errg, ShouldBeNil)

			return item.State()
		}

		So(waiterState(), ShouldEqual, queue.ItemStateDependent)

		Convey("a re-reservation that no longer uploads to the inode path makes the waiter ready", func() {
			moved := *hardlink
			moved.Hardlink = ""

			claimed, err = rc.claim(&moved, make(map[string]bool))
			So(err, ShouldBeNil)
			So(claimed, ShouldBeTrue)
			So(waiterState(), ShouldEqual, queue.ItemStateReady)
		})

		Convey("a re-reservation blocked by a regular file of its earlier batch makes the waiter ready", func() {
			regular := &transfer.Request{
				Local: "/claimed/file", Remote: hardlink.Remote, Requester: hardlink.Requester, Set: hardlink.Set,
			}
			_, _, err = q.AddMany(ctx, []*queue.ItemDef{{Key: regular.ID(), Data: regular, TTR: ttr}})
			So(err, ShouldBeNil)

			_, err = q.Reserve("", 0)
			So(err, ShouldBeNil)

			claimed, err = rc.claim(regular, batch)
			So(err, ShouldBeNil)
			So(claimed, ShouldBeTrue)

			claimed, err = rc.claim(hardlink, make(map[string]bool))
			So(err, ShouldBeNil)
			So(claimed, ShouldBeFalse)
			So(waiterState(), ShouldEqual, queue.ItemStateReady)
		})
	})
}
