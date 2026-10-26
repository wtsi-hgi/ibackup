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
	"errors"
	"slices"
	"sync"

	"github.com/VertebrateResequencing/wr/queue"
	"github.com/wtsi-hgi/ibackup/set"
	"github.com/wtsi-hgi/ibackup/transfer"
)

const remoteClaimDependencyPrefix = "remote:"

// claimQueue is the part of our queues that remoteClaims uses.
type claimQueue interface {
	Get(key string) (*queue.Item, error)
	Requeue(ctx context.Context, key string, deps []string) error
	SatisfyDependency(ctx context.Context, key string) error
}

// remoteClaims makes sure that requests which upload to the same remote data
// object (such as hardlinks sharing a remote inode file, or different sets'
// requests for the same file) are only given to one put client at a time.
// Concurrent uploads of the same iRODS object from different clients fail with
// CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME, whereas a single client handles such
// requests sequentially.
//
// It also makes a file's removal wait until no upload of its remote objects is
// in progress, and stops such uploads being given out while the removal runs,
// so that an upload can't write a removed set's metadata back on an object the
// removal already changed. Removals wait in the remove queue, and uploads in
// the put queue, on the same dependency keys.
type remoteClaims struct {
	sync.Mutex
	queue       claimQueue
	removeQueue claimQueue
	holders     map[string]map[string]struct{}
	paths       map[string]string
	waiting     map[string]map[string]struct{}
}

func newRemoteClaims(q, removeQueue *queue.Queue) *remoteClaims {
	return &remoteClaims{
		queue:       q,
		removeQueue: removeQueue,
		holders:     make(map[string]map[string]struct{}),
		paths:       make(map[string]string),
		waiting:     make(map[string]map[string]struct{}),
	}
}

// claim claims the remote data object (RemoteDataPath()) of the given request,
// which must be running in our queue, for the batch of requests being given to
// one client. The batch map records the paths claimed for that batch.
//
// A claim the request still holds from an earlier reservation (because it was
// abandoned by a dead client, or released for retry) is stale and is given up
// first.
//
// If a different client or a removal already holds the claim, the request is
// instead requeued to wait until the claim is released, and false is returned.
func (rc *remoteClaims) claim(r *transfer.Request, batch map[string]bool) (bool, error) {
	path := r.RemoteDataPath()
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
	rc.addHolder(rid, path)
	rc.paths[rid] = path
}

// addHolder adds the given holder to those of the given path. You must hold the
// lock.
func (rc *remoteClaims) addHolder(holder, path string) {
	holders, held := rc.holders[path]
	if !held {
		holders = make(map[string]struct{})
		rc.holders[path] = holders
	}

	holders[holder] = struct{}{}
}

// unclaim gives up the claim that claim() just made for the given request for
// the given batch, for a request that won't be given to the client after all.
func (rc *remoteClaims) unclaim(r *transfer.Request, batch map[string]bool) error {
	path := r.RemoteDataPath()

	rc.Lock()

	if rc.forget(r.ID(), path) {
		rc.Unlock()

		return nil
	}

	delete(batch, path)
	rc.Unlock()

	return rc.satisfy(path)
}

// release gives up the given request's claim on its remote data object. Once no
// request holds the claim, requests and removals waiting on it become ready to
// be reserved.
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

	return rc.satisfy(path)
}

// satisfy makes the requests and removals waiting on a claim of the given path
// ready to be reserved. A queue closed by the server stopping has nothing left
// to wait, so isn't an error.
func (rc *remoteClaims) satisfy(path string) error {
	key := remoteClaimDependencyPrefix + path

	var errs error

	for _, q := range []claimQueue{rc.queue, rc.removeQueue} {
		if err := q.SatisfyDependency(context.Background(), key); !errors.Is(err, queue.ErrQueueClosed) {
			errs = errors.Join(errs, err)
		}
	}

	return errs
}

// claimForRemoval claims the given remote paths for the given file removal,
// which must be running in the remove queue, so that no upload of them is
// given to a client until releaseRemoval(). It claims all of them or none.
//
// If an upload, or another removal, holds a claim on one of them, the removal
// is instead requeued to wait until that claim is released, and false is
// returned. Until the removal is reserved again (see removalReserved()),
// removalsWaiting() is true for its set. The stale claim of an upload request
// no longer running, such as one whose reservation expired, doesn't hold up the
// removal, and is given up, as claim() gives it up when the request is
// reserved again.
func (rc *remoteClaims) claimForRemoval(removeReq *set.RemoveReq, paths []string) (bool, error) {
	key := removeReq.Key()

	rc.Lock()
	defer rc.Unlock()

	for _, path := range paths {
		if !rc.heldByActive(path) {
			continue
		}

		if err := rc.removeQueue.Requeue(context.Background(), key,
			[]string{remoteClaimDependencyPrefix + path}); err != nil {
			return false, err
		}

		rc.addWaiting(removeReq.Set.ID(), key)

		return false, nil
	}

	for _, path := range paths {
		for holder := range rc.holders[path] {
			rc.forget(holder, path)
		}

		rc.addHolder(key, path)
	}

	return true, nil
}

// heldByActive returns true if a removal, or an upload request running in our
// queue, holds a claim on the given path. You must hold the lock.
func (rc *remoteClaims) heldByActive(path string) bool {
	for holder := range rc.holders[path] {
		if _, upload := rc.paths[holder]; !upload {
			return true
		}

		if item, err := rc.queue.Get(holder); err == nil && item.State() == queue.ItemStateRun {
			return true
		}
	}

	return false
}

// addWaiting records that the removal with the given key, of the set with the
// given ID, waits on a claim. You must hold the lock.
func (rc *remoteClaims) addWaiting(sid, key string) {
	keys, ok := rc.waiting[sid]
	if !ok {
		keys = make(map[string]struct{})
		rc.waiting[sid] = keys
	}

	keys[key] = struct{}{}
}

// removalReserved records that the given removal, just reserved from the remove
// queue, no longer waits on a claim, if it did.
func (rc *remoteClaims) removalReserved(removeReq *set.RemoveReq) {
	sid := removeReq.Set.ID()

	rc.Lock()
	defer rc.Unlock()

	delete(rc.waiting[sid], removeReq.Key())

	if len(rc.waiting[sid]) == 0 {
		delete(rc.waiting, sid)
	}
}

// removalsWaiting returns true if any removal of the set with the given ID is
// waiting on a claim, so will be ready to be reserved once the claim is
// released.
func (rc *remoteClaims) removalsWaiting(sid string) bool {
	rc.Lock()
	defer rc.Unlock()

	return len(rc.waiting[sid]) > 0
}

// releaseRemoval gives up the given removal's claims on the given paths, made
// by claimForRemoval(). Requests and removals waiting on a path no longer
// claimed become ready to be reserved.
func (rc *remoteClaims) releaseRemoval(removeReq *set.RemoveReq, paths []string) error {
	key := removeReq.Key()

	rc.Lock()

	freed := slices.DeleteFunc(slices.Clone(paths), func(path string) bool {
		return rc.forget(key, path)
	})

	rc.Unlock()

	var err error

	for _, path := range freed {
		err = errors.Join(err, rc.satisfy(path))
	}

	return err
}

// forgetStale gives up any claim the given request has from an earlier
// reservation. If that claim was on a path other than the given one, and no
// other request holds it, requests and removals waiting on it become ready. You
// must hold the lock.
func (rc *remoteClaims) forgetStale(rid, path string) error {
	old, ok := rc.paths[rid]
	if !ok || rc.forget(rid, old) || old == path {
		return nil
	}

	return rc.satisfy(old)
}

// forget removes the given holder's claim on the given path, if it has one,
// returning true if others still hold a claim on the path. You must hold the
// lock.
func (rc *remoteClaims) forget(holder, path string) bool {
	holders, held := rc.holders[path]
	if !held {
		return false
	}

	if _, holding := holders[holder]; holding {
		delete(holders, holder)
		delete(rc.paths, holder)
	}

	if len(holders) > 0 {
		return true
	}

	delete(rc.holders, path)

	return false
}
