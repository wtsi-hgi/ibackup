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
)

// Creating a collection takes several seconds on the test zone, but many can
// be created at once, so a test process that uses a fresh collection per test
// (or per Convey leaf) can have them created in the background ahead of use.

type prefetchResult struct {
	collection string
	err        error
}

type collectionPrefetcher struct {
	base    string
	depth   int
	create  func(collection string) error
	results chan prefetchResult
	wg      sync.WaitGroup
	mu      sync.Mutex
	started bool
	stopped bool
}

var (
	prefetchMu sync.Mutex            //nolint:gochecknoglobals
	prefetcher *collectionPrefetcher //nolint:gochecknoglobals
)

// PrefetchIRODSTestCollections makes RequireIRODSTestCollection hand out
// unique, empty collections created in the background, keeping depth of them
// ready or being created from the first time one is required, so a test
// process that never requires one creates none. Call
// StopPrefetchingIRODSTestCollections when the tests finish, to remove those
// not handed out. It does nothing if the iRODS test collection or iCommands
// are not available.
func PrefetchIRODSTestCollections(depth int) {
	base := irodsBaseFromEnv()
	if base == "" || depth < 1 {
		return
	}

	if _, err := exec.LookPath("imkdir"); err != nil {
		return
	}

	p := &collectionPrefetcher{
		base:    base,
		depth:   depth,
		create:  createIRODSCollection,
		results: make(chan prefetchResult, depth),
	}

	prefetchMu.Lock()
	prefetcher = p
	prefetchMu.Unlock()
}

// StopPrefetchingIRODSTestCollections waits for background collection creation
// to finish, then removes the collections that were not handed out.
func StopPrefetchingIRODSTestCollections() {
	prefetchMu.Lock()
	p := prefetcher
	prefetcher = nil
	prefetchMu.Unlock()

	if p == nil {
		return
	}

	p.mu.Lock()
	p.stopped = true
	p.mu.Unlock()

	p.wg.Wait()
	close(p.results)

	var unused []string

	for r := range p.results {
		if r.err == nil {
			unused = append(unused, r.collection)
		}
	}

	removeIRODSCollections(unused)
}

// start begins creating one collection in the background to replace a
// handed-out one, unless stopped. So no more than depth are ever ready or
// being created, and sends to results never block.
func (p *collectionPrefetcher) start() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.stopped {
		p.createInBackground()
	}
}

func (p *collectionPrefetcher) createInBackground() {
	p.wg.Go(func() {
		collection, err := randomCollectionName()
		if err == nil {
			collection = filepath.Join(p.base, collection)
			err = p.create(collection)
		}

		p.results <- prefetchResult{collection: collection, err: err}
	})
}

// fill begins creating depth collections on its first call, and returns false
// if stopped.
func (p *collectionPrefetcher) fill() bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.stopped {
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

// takePrefetchedCollection returns a collection created in the background
// under base, or an empty string if prefetching is not enabled for base, is
// stopped, or the background creation failed (so the caller creates one
// itself and reports any persistent error).
func takePrefetchedCollection(base string) string {
	prefetchMu.Lock()
	p := prefetcher
	prefetchMu.Unlock()

	if p == nil || p.base != base {
		return ""
	}

	if !p.fill() {
		return ""
	}

	// After Stop closes results, this receives the zero value, so we return
	// "" and the caller creates its own collection.
	r := <-p.results

	p.start()

	if r.err != nil {
		return ""
	}

	return r.collection
}
