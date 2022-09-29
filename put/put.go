/*******************************************************************************
 * Copyright (c) 2022 Genome Research Ltd.
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

// package put is used to put files in iRODS.

package put

import (
	"path/filepath"
	"sort"
	"strings"
)

const minDirsForUnique = 2

// Handler is something that knows how to communicate with iRODS and carry out
// certain operations.
type Handler interface {
	// Connect uses environmental details to make 1 or more connections to
	// iRODS, ready for subsequent use.
	Connect() error

	// Cleanup stops any connections from Connect() and does any other cleanup
	// needed.
	Cleanup() error

	// EnsureCollection checks if the given collection exists in iRODS, creates
	// it if not, then double-checks it now exists.
	EnsureCollection(collection string) error

	// Put uploads the Request's Local file to the Remote location, overwriting
	// any existing object, and ensuring that a locally calculated and remotely
	// calculated md5 checksum match.
	Put(request *Request) error

	// ReplaceMetadata replaces the Requests's Remote object metadata with the
	// Request's metadata.
	ReplaceMetadata(request *Request) error
}

// Request represents a local file you would like transferred to a remote iRODS
// path, and the metadata you'd like to associate with it.
type Request struct {
	Local  string
	Remote string
	Meta   map[string]string
	Error  error
}

// Putter is used to Put() files in iRODS.
type Putter struct {
	handler  Handler
	requests []*Request
}

// New returns a *Putter that will use the given Handler to Put() all the
// requests in iRODS. You should defer Cleanup() on the return value.
func New(handler Handler, requests []*Request) (*Putter, error) {
	err := handler.Connect()
	if err != nil {
		return nil, err
	}

	return &Putter{handler: handler, requests: requests}, nil
}

// Cleanup should be deferred after making a New Putter. It handles things like
// disconnecting.
func (p *Putter) Cleanup() error {
	return p.handler.Cleanup()
}

// CreateCollections will determine the minimal set of collections that need to
// be created to support a future Put() request for our requests, checks if
// those collections exist in iRODS, and creates them if not.
//
// You should call this before Put(), unless you're sure all collections already
// exist.
//
// Gives up on the first failed operation, and returns that error.
func (p *Putter) CreateCollections() error {
	dirs := p.getUniqueRequestLeafCollections()

	for _, dir := range dirs {
		if err := p.handler.EnsureCollection(dir); err != nil {
			return err
		}
	}

	return nil
}

func (p *Putter) getUniqueRequestLeafCollections() []string {
	dirs := p.getSortedRequestCollections()
	if len(dirs) < minDirsForUnique {
		return dirs
	}

	var uniqueLeafs []string //nolint:prealloc

	previous, dirs := dirs[0], dirs[1:]

	for _, dir := range dirs {
		if dir == previous || strings.HasPrefix(dir, previous+"/") {
			previous = dir

			continue
		}

		uniqueLeafs = append(uniqueLeafs, previous)
		previous = dir
	}

	if noLeavesOrNewLeaf(uniqueLeafs, previous) {
		uniqueLeafs = append(uniqueLeafs, previous)
	}

	return uniqueLeafs
}

func (p *Putter) getSortedRequestCollections() []string {
	dirs := make([]string, len(p.requests))

	for i, request := range p.requests {
		dirs[i] = filepath.Dir(request.Remote)
	}

	sort.Strings(dirs)

	return dirs
}

func noLeavesOrNewLeaf(uniqueLeafs []string, last string) bool {
	return len(uniqueLeafs) == 0 || last != uniqueLeafs[len(uniqueLeafs)-1]
}

// Put will put all our request Local files in iRODS at the Remote locations.
// You ought to call CreateCollections() before calling this.
//
// Existing files in iRODS will be overwritten. MD5 checksums will be calculated
// locally and remotely and compared to ensure a perfect upload. The request
// metadata will then replace any existing metadata for the Remote object.
//
// If any requests fail during any of these operations, those requests will be
// returned, with Error set. The return value wil be nil on complete success.
func (p *Putter) Put() []*Request {
	failedCh := p.putFilesInIRODS()

	var failed []*Request //nolint:prealloc

	for request := range failedCh {
		failed = append(failed, request)
	}

	return failed
}

// putFilesInIRODS uses our handler to Put() all our requests in to iRODS
// sequentially, and concurrently ReplaceMetadata() on those objects once
// they're in.
func (p *Putter) putFilesInIRODS() chan *Request {
	failedCh := make(chan *Request, len(p.requests))
	metaCh := make(chan *Request, len(p.requests))

	metaDoneCh := p.applyMetadataConcurrently(metaCh, failedCh)

	for _, request := range p.requests {
		if err := p.handler.Put(request); err != nil {
			request.Error = err
			failedCh <- request

			continue
		}

		metaCh <- request
	}

	close(metaCh)
	<-metaDoneCh
	close(failedCh)

	return failedCh
}

func (p *Putter) applyMetadataConcurrently(metaCh chan *Request, failedCh chan *Request) chan struct{} {
	doneCh := make(chan struct{})

	go func() {
		for request := range metaCh {
			if err := p.handler.ReplaceMetadata(request); err != nil {
				request.Error = err
				failedCh <- request
			}
		}

		close(doneCh)
	}()

	return doneCh
}
