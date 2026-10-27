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

// remoteClaims makes sure that requests which upload to the same remote path
// (such as hardlinks sharing a remote inode file, different sets' requests for
// the same file, or a hardlink's empty file at the path another set uploads a
// regular file to) are only given to one put client at a time. Concurrent
// uploads of the same iRODS object from different clients fail with
// CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME, and their concurrent merges of its
// metadata can lose a set's name, whereas a single client handles such requests
// sequentially.
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
	paths       map[string][]string
	waiting     map[string]map[string]struct{}

	// released counts the claims given up so far, one for each path of a
	// holder that gives up its claim on it. removeFileFromIRODSandDB()
	// relies on it to skip reading a file's entry again when no claim was given
	// up while it claimed the file's objects.
	released uint64
}

func newRemoteClaims(q, removeQueue *queue.Queue) *remoteClaims {
	return &remoteClaims{
		queue:       q,
		removeQueue: removeQueue,
		holders:     make(map[string]map[string]struct{}),
		paths:       make(map[string][]string),
		waiting:     make(map[string]map[string]struct{}),
	}
}

// claim claims every remote path the given request uploads to (Remotes()),
// which must be running in our queue, for the batch of requests being given to
// one client. The batch map records the paths claimed for that batch. It claims
// all of them or none: a hardlink also writes its empty file and metadata at
// Remote, which another set may upload to as a regular file.
//
// A claim the request still holds from an earlier reservation (because it was
// abandoned by a dead client, or released for retry) is stale and is given up
// first.
//
// If a different client or a removal already holds a claim on one of the paths,
// the request is instead requeued to wait until that claim is released, and
// false is returned.
func (rc *remoteClaims) claim(r *transfer.Request, batch map[string]bool) (bool, error) {
	paths := r.Remotes()
	rid := r.ID()

	rc.Lock()
	defer rc.Unlock()

	freed, err := rc.forgetStale(rid, paths)
	if err != nil {
		return false, err
	}

	for _, path := range paths {
		if _, held := rc.holders[path]; held && !batch[path] {
			return false, errors.Join(rc.satisfyAll(freed), rc.queue.Requeue(context.Background(), rid,
				[]string{remoteClaimDependencyPrefix + path}))
		}
	}

	for _, path := range paths {
		rc.addHolder(rid, path)
		batch[path] = true
	}

	rc.paths[rid] = paths

	return true, nil
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

// unclaim gives up the claims that claim() just made for the given request for
// the given batch, for a request that won't be given to the client after all.
func (rc *remoteClaims) unclaim(r *transfer.Request, batch map[string]bool) error {
	rc.Lock()

	freed := rc.forgetAll(r.ID())

	for _, path := range freed {
		delete(batch, path)
	}

	rc.Unlock()

	return rc.satisfyAll(freed)
}

// release gives up the given request's claims on its remote paths. Once no
// request holds a claim, requests and removals waiting on it become ready to
// be reserved. A finished upload must record its result in its entry before
// its request leaves our queue's run state, since from then claimForRemoval()
// may give up the claims on its behalf; either way a removal seeing a claim
// given up then reads that result.
func (rc *remoteClaims) release(r *transfer.Request) error {
	rc.Lock()
	freed := rc.forgetAll(r.ID())
	rc.Unlock()

	return rc.satisfyAll(freed)
}

// forgetAll gives up all the claims of the given upload request, returning the
// paths no longer claimed by anyone. You must hold the lock.
func (rc *remoteClaims) forgetAll(rid string) []string {
	return slices.DeleteFunc(slices.Clone(rc.paths[rid]), func(path string) bool {
		return rc.forget(rid, path)
	})
}

// satisfyAll calls satisfy() on each of the given paths.
func (rc *remoteClaims) satisfyAll(paths []string) error {
	var err error

	for _, path := range paths {
		err = errors.Join(err, rc.satisfy(path))
	}

	return err
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

// claimForRemoval claims the given remote paths for the file removal with the
// given key (see set.RemoveReq.Key()), of the set with the given ID, which must
// be running in the remove queue, so that no upload of them is given to a
// client until releaseRemoval(). It claims all of them or none.
//
// If an upload, or another removal, holds a claim on one of them, the removal
// is instead requeued to wait until that claim is released, and false is
// returned. Until the removal is reserved again (see removalReserved()),
// removalsWaiting() is true for its set. The stale claim of an upload request
// no longer running, such as one whose reservation expired, doesn't hold up the
// removal, and is given up, as claim() gives it up when the request is
// reserved again.
func (rc *remoteClaims) claimForRemoval(sid, key string, paths []string) (bool, error) {
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

		rc.addWaiting(sid, key)

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
	rc.Lock()
	defer rc.Unlock()

	if len(rc.waiting) == 0 {
		return
	}

	sid := removeReq.Set.ID()

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

// releasedCount returns the number of claims given up so far, so that a change
// between two calls shows that one was given up in between.
func (rc *remoteClaims) releasedCount() uint64 {
	rc.Lock()
	defer rc.Unlock()

	return rc.released
}

// releaseRemoval gives up the claims on the given paths of the removal with the
// given key, made by claimForRemoval(). Requests and removals waiting on a path
// no longer claimed become ready to be reserved.
func (rc *remoteClaims) releaseRemoval(key string, paths []string) error {
	rc.Lock()

	freed := slices.DeleteFunc(slices.Clone(paths), func(path string) bool {
		return rc.forget(key, path)
	})

	rc.Unlock()

	return rc.satisfyAll(freed)
}

// forgetStale gives up any claims the given request has from an earlier
// reservation. Requests and removals waiting on a path no longer claimed by
// anyone, other than the given paths the request is about to claim again,
// become ready. Those given paths that are no longer claimed are returned, so
// that the caller can make their waiters ready if it doesn't claim them after
// all. You must hold the lock.
func (rc *remoteClaims) forgetStale(rid string, paths []string) ([]string, error) {
	freed := rc.forgetAll(rid)
	reclaimed := slices.DeleteFunc(slices.Clone(freed), func(path string) bool {
		return !slices.Contains(paths, path)
	})

	return reclaimed, rc.satisfyAll(slices.DeleteFunc(freed, func(path string) bool {
		return slices.Contains(paths, path)
	}))
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
		rc.forgetPath(holder, path)
		rc.released++
	}

	if len(holders) > 0 {
		return true
	}

	delete(rc.holders, path)

	return false
}

// forgetPath removes the given path from those the given upload request holds,
// if it's an upload request. You must hold the lock.
func (rc *remoteClaims) forgetPath(rid, path string) {
	paths, upload := rc.paths[rid]
	if !upload {
		return
	}

	paths = slices.DeleteFunc(paths, func(p string) bool { return p == path })
	if len(paths) == 0 {
		delete(rc.paths, rid)

		return
	}

	rc.paths[rid] = paths
}
