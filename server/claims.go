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
	"sync"

	"github.com/VertebrateResequencing/wr/queue"
	"github.com/wtsi-hgi/ibackup/transfer"
)

const remoteClaimDependencyPrefix = "remote:"

// claimQueue is the part of our queue that remoteClaims uses.
type claimQueue interface {
	Requeue(ctx context.Context, key string, deps []string) error
	SatisfyDependency(ctx context.Context, key string) error
}

// remoteClaims makes sure that hardlink requests which share a remote inode file
// are only given to one put client at a time. Concurrent uploads of the same
// iRODS object from different clients fail with
// CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME, whereas a single client handles such
// requests sequentially.
type remoteClaims struct {
	sync.Mutex
	queue   claimQueue
	holders map[string]map[string]struct{}
	paths   map[string]string
}

func newRemoteClaims(q *queue.Queue) *remoteClaims {
	return &remoteClaims{
		queue:   q,
		holders: make(map[string]map[string]struct{}),
		paths:   make(map[string]string),
	}
}

// claim claims the remote inode file of the given hardlink request, which must
// be running in our queue, for the batch of requests being given to one client.
// The batch map records the paths claimed for that batch. Requests that aren't
// hardlinks are always claimable.
//
// A claim the request still holds from an earlier reservation (because it was
// abandoned by a dead client, or released for retry) is stale and is given up
// first.
//
// If a different client already holds the claim, the request is instead
// requeued to wait until the claim is released, and false is returned.
func (rc *remoteClaims) claim(r *transfer.Request, batch map[string]bool) (bool, error) {
	if r.Hardlink == "" {
		return true, nil
	}

	path := r.Hardlink
	rid := r.ID()

	rc.Lock()
	defer rc.Unlock()

	if err := rc.forgetStale(rid, path); err != nil {
		return false, err
	}

	if _, held := rc.holders[path]; held && !batch[path] {
		return false, rc.queue.Requeue(context.Background(), rid,
			[]string{remoteClaimDependencyPrefix + path})
	}

	rc.hold(rid, path)
	batch[path] = true

	return true, nil
}

// hold records that the given request holds a claim on the given path. You
// must hold the lock.
func (rc *remoteClaims) hold(rid, path string) {
	holders, held := rc.holders[path]
	if !held {
		holders = make(map[string]struct{})
		rc.holders[path] = holders
	}

	holders[rid] = struct{}{}
	rc.paths[rid] = path
}

// release gives up the given request's claim on its remote inode file. Once no
// request holds the claim, requests waiting on it become ready to be reserved.
func (rc *remoteClaims) release(r *transfer.Request) error {
	rid := r.ID()

	rc.Lock()

	path, ok := rc.paths[rid]
	if !ok {
		rc.Unlock()

		return nil
	}

	remaining := rc.forget(rid, path)
	rc.Unlock()

	if remaining {
		return nil
	}

	return rc.queue.SatisfyDependency(context.Background(), remoteClaimDependencyPrefix+path)
}

// forgetStale gives up any claim the given request has from an earlier
// reservation. If that claim was on a path other than the given one, and no
// other request holds it, requests waiting on it become ready. You must hold
// the lock.
func (rc *remoteClaims) forgetStale(rid, path string) error {
	old, ok := rc.paths[rid]
	if !ok || rc.forget(rid, old) || old == path {
		return nil
	}

	return rc.queue.SatisfyDependency(context.Background(), remoteClaimDependencyPrefix+old)
}

// forget removes the given request's claim on the given path, if it has one,
// returning true if other requests still hold a claim on the path. You must
// hold the lock.
func (rc *remoteClaims) forget(rid, path string) bool {
	holders, held := rc.holders[path]
	if !held {
		return false
	}

	if _, holding := holders[rid]; holding {
		delete(holders, rid)
		delete(rc.paths, rid)
	}

	if len(holders) > 0 {
		return true
	}

	delete(rc.holders, path)

	return false
}
