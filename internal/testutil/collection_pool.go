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
	"path/filepath"
	"sync"
	"testing"
)

// maxConcurrentRemovals is how many of a test's pool collections are removed
// at once when it ends.
const maxConcurrentRemovals = 8

// Creating a collection takes several seconds on the test zone, but many can
// be created at once, so a test process that uses a fresh collection per test
// (or per Convey leaf) can have them created in the background ahead of use.

type poolResult struct {
	collection string
	err        error
}

// CollectionPool hands out unique, empty iRODS test collections that it
// creates in the background ahead of use.
type CollectionPool struct {
	base     string
	depth    int
	create   func(collection string) error
	remove   func(collection string)
	ready    chan poolResult
	mu       sync.Mutex
	started  bool
	closed   bool
	wg       sync.WaitGroup
	cleanups map[testing.TB][]string
}

// NewCollectionPool returns a pool that, from the first Require, keeps depth
// collections ready or being created under the iRODS test collection, so a
// test process that never requires one creates none. Close it when the tests
// finish, to remove those not handed out. It returns nil if the iRODS test
// collection or iCommands are not available, or depth is less than 1.
func NewCollectionPool(depth int) *CollectionPool {
	base := irodsBaseFromEnv()
	if base == "" || depth < 1 {
		return nil
	}

	if _, err := exec.LookPath("imkdir"); err != nil {
		return nil
	}

	return newCollectionPool(base, depth, createIRODSCollection, removeIRODSCollection)
}

func newCollectionPool(
	base string,
	depth int,
	create func(string) error,
	remove func(string),
) *CollectionPool {
	return &CollectionPool{
		base:     base,
		depth:    depth,
		create:   create,
		remove:   remove,
		ready:    make(chan poolResult, depth),
		cleanups: make(map[testing.TB][]string),
	}
}

// Require returns a unique, empty collection from the pool, set as
// IBACKUP_TEST_COLLECTION for tb and removed when tb ends. On a nil pool, or if
// the pool is closed or its background creation failed, it behaves like
// RequireIRODSTestCollection.
func (p *CollectionPool) Require(tb testing.TB) string {
	tb.Helper()

	if p == nil {
		return RequireIRODSTestCollection(tb)
	}

	collection := p.take()
	if collection == "" {
		return RequireIRODSTestCollection(tb)
	}

	setTestCollectionEnv(tb, collection)
	p.cleanupWhenDone(tb, collection)

	return collection
}

// take returns a collection created in the background, or an empty string if
// the pool is closed or the background creation failed.
func (p *CollectionPool) take() string {
	if !p.fill() {
		return ""
	}

	// After Close closes ready, this receives the zero value, so we return "".
	r := <-p.ready

	p.refill()

	if r.err != nil {
		return ""
	}

	return r.collection
}

// fill begins creating depth collections on its first call, and returns false
// if closed.
func (p *CollectionPool) fill() bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return false
	}

	if !p.started {
		p.started = true

		for range p.depth {
			p.createInBackground()
		}
	}

	return true
}

// refill begins creating one collection in the background to replace a
// handed-out one, unless closed. So no more than depth are ever ready or being
// created, and sends to ready never block.
func (p *CollectionPool) refill() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.closed {
		p.createInBackground()
	}
}

func (p *CollectionPool) createInBackground() {
	p.wg.Go(func() {
		collection, err := randomCollectionName()
		if err == nil {
			collection = filepath.Join(p.base, collection)
			err = p.create(collection)
		}

		p.ready <- poolResult{collection: collection, err: err}
	})
}

// cleanupWhenDone removes collection when tb ends. A test can take many
// collections (one per Convey leaf), and each removal takes a second or more,
// so all of a test's collections are removed together, a few at a time.
func (p *CollectionPool) cleanupWhenDone(tb testing.TB, collection string) {
	tb.Helper()

	p.mu.Lock()
	defer p.mu.Unlock()

	_, registered := p.cleanups[tb]
	p.cleanups[tb] = append(p.cleanups[tb], collection)

	if registered {
		return
	}

	tb.Cleanup(func() {
		p.mu.Lock()
		collections := p.cleanups[tb]

		delete(p.cleanups, tb)
		p.mu.Unlock()

		p.removeAll(collections)
	})
}

func (p *CollectionPool) removeAll(collections []string) {
	var wg sync.WaitGroup

	limit := make(chan struct{}, maxConcurrentRemovals)

	for _, collection := range collections {
		limit <- struct{}{}

		wg.Go(func() {
			p.remove(collection)
			<-limit
		})
	}

	wg.Wait()
}

// Close stops the pool creating collections, waits for those being created,
// then removes those not handed out. Later Requires behave like
// RequireIRODSTestCollection. Close does nothing on a nil or closed pool.
func (p *CollectionPool) Close() {
	if p == nil {
		return
	}

	p.mu.Lock()
	wasClosed := p.closed
	p.closed = true
	p.mu.Unlock()

	if wasClosed {
		return
	}

	p.wg.Wait()
	close(p.ready)

	var unused []string

	for r := range p.ready {
		if r.err == nil {
			unused = append(unused, r.collection)
		}
	}

	p.removeAll(unused)
}
