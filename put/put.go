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
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

type Error struct {
	msg  string
	path string
}

func (e Error) Error() string {
	if e.path != "" {
		return fmt.Sprintf("%s [%s]", e.msg, e.path)
	}

	return e.msg
}

const (
	ErrLocalNotAbs   = "local path could not be made absolute"
	ErrRemoteNotAbs  = "remote path not absolute"
	minDirsForUnique = 2
)

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

	// Stat checks if the Request's Remote object exists. If it does, records
	// its metadata in the returned ObjectInfo. Returns an error if there was a
	// problem finding out information (but not if the object does not exist).
	Stat(request *Request) (*ObjectInfo, error)

	// Put uploads the Request's Local file to the Remote location, overwriting
	// any existing object, and ensuring that a locally calculated and remotely
	// calculated md5 checksum match.
	Put(request *Request) error

	// RemoveMeta deletes the given metadata from the given object.
	RemoveMeta(path string, meta map[string]string) error

	// AddMeta adds the given metadata to the given object. Given metadata keys
	// should already have been removed with RemoveMeta() from the remote
	// object.
	AddMeta(path string, meta map[string]string) error
}

// Putter is used to Put() files in iRODS.
type Putter struct {
	handler  Handler
	requests []*Request
}

// New returns a *Putter that will use the given Handler to Put() all the
// requests in iRODS. You should defer Cleanup() on the return value. All the
// incoming requests will have their paths validated (they must be absolute).
func New(handler Handler, requests []*Request) (*Putter, error) {
	err := handler.Connect()
	if err != nil {
		return nil, err
	}

	for _, request := range requests {
		if err := request.ValidatePaths(); err != nil {
			return nil, err
		}
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

// Put will upload all our request Local files to iRODS at the Remote locations.
// You ought to call CreateCollections() before calling this.
//
// Existing files in iRODS will be overwritten if the local file's mtime is
// different to the remote's. If the same, the put will be skipped.
//
// MD5 checksums will be calculated locally and remotely and compared to ensure
// a perfect upload. The request metadata will then replace any existing
// metadata with the same keys on the Remote object.
//
// All our requests are eventually sent on the first returned channel (in
// potentially a different order to our input Request slice) with Status set on
// them:
//
//	"uploaded":   a new object was uploaded to iRODS
//	"replaced":   an existing object was replaced in iRODS, because Local was
//	              newer than Remote
//	"unmodified": Local and Remote had the same modification time, so nothing
//	              was done
//	"missing":    Local path could not be accessed, upload skipped; see Error
//	"failed":     An upload attempt failed; see Error
//
// The second returned channel receives an input request with Status uploading
// as soon as we start to upload that request. All input requests are not
// guaranteed to be send on this channel (eg. not missing or unmodified ones).
func (p *Putter) Put() (chan *Request, chan *Request) {
	returnCh := make(chan *Request, len(p.requests))
	uploadStartCh := make(chan *Request, len(p.requests))
	putCh := make(chan *Request, len(p.requests))

	go p.pickFilesToPut(putCh, returnCh)
	go p.putFilesInIRODS(putCh, returnCh, uploadStartCh)

	return returnCh, uploadStartCh
}

// pickFilesToPut goes through all our Requests, immediately returns bad ones
// (eg. local file doesn't exist) and ones we don't need to do (hasn't been
// modified since last uploaded) via the returnCh, and sends the remainder to
// the putCh.
func (p *Putter) pickFilesToPut(putCh chan *Request, returnCh chan *Request) {
	var wg sync.WaitGroup

	for _, request := range p.requests {
		wg.Add(1)

		go func(request *Request) {
			defer wg.Done()

			p.statPathsAndReturnOrPut(request, putCh, returnCh)
		}(request)
	}

	wg.Wait()
	close(putCh)
}

// statPathsAndReturnOrPut stats the Local and Remote paths. On error, sends the
// request to the returnCh straight away. If mtime unchanged, doesn't do the
// put, sending to returnCh if metdata unchanged, otherwise to putCh but with a
// note to skip the actual put and just do metadata. Otherwise, sends them to
// the putCh normally.
func (p *Putter) statPathsAndReturnOrPut(request *Request, putCh chan *Request, returnCh chan *Request) {
	lInfo, err := Stat(request.Local)
	if err != nil {
		sendRequest(request, RequestStatusMissing, err, returnCh)

		return
	}

	request.Size = lInfo.Size

	rInfo, err := p.handler.Stat(request)
	if err != nil {
		sendRequest(request, RequestStatusFailed, err, returnCh)

		return
	}

	request.addStandardMeta(lInfo.Meta, rInfo.Meta)

	if !rInfo.Exists {
		sendRequest(request, RequestStatusUploaded, nil, putCh)

		return
	}

	if lInfo.HasSameModTime(rInfo) {
		ch := returnCh

		if request.needsMetadataUpdate() {
			ch = putCh
		}

		sendRequest(request, RequestStatusUnmodified, nil, ch)

		return
	}

	sendRequest(request, RequestStatusReplaced, nil, putCh)
}

// sendRequest sets the given status and err on the given request, then sends it
// down the given channel.
func sendRequest(request *Request, status RequestStatus, err error, ch chan *Request) {
	request.Status = status

	if err != nil {
		request.Error = err.Error()
	} else {
		request.Error = ""
	}

	ch <- request
}

// putFilesInIRODS uses our handler to Put() requests sent on the given putCh in
// to iRODS sequentially, and concurrently ReplaceMetadata() on those objects
// once they're in. When each request is about to start uploading, it is sent
// on uploadStartCh.
func (p *Putter) putFilesInIRODS(putCh chan *Request, returnCh, uploadStartCh chan *Request) {
	metaCh := make(chan *Request, len(p.requests))
	metaDoneCh := p.applyMetadataConcurrently(metaCh, returnCh)

	for request := range putCh {
		if request.skipPut {
			metaCh <- request

			continue
		}

		uploading := request.Clone()
		uploading.Status = RequestStatusUploading
		uploadStartCh <- uploading

		if err := p.handler.Put(request); err != nil {
			request.Status = RequestStatusFailed
			request.Error = err.Error()
			returnCh <- request

			continue
		}

		metaCh <- request
	}

	close(uploadStartCh)
	close(metaCh)
	<-metaDoneCh
	close(returnCh)
}

func (p *Putter) applyMetadataConcurrently(metaCh chan *Request, returnCh chan *Request) chan struct{} {
	doneCh := make(chan struct{})

	go func() {
		for request := range metaCh {
			toRemove, toAdd := request.determineMetadataToRemoveAndAdd()

			if err := p.removeMeta(request.Remote, toRemove); err != nil {
				request.Status = RequestStatusFailed
				request.Error = err.Error()
				returnCh <- request

				continue
			}

			if err := p.addMeta(request.Remote, toAdd); err != nil {
				request.Status = RequestStatusFailed
				request.Error = err.Error()
			}

			returnCh <- request
		}

		close(doneCh)
	}()

	return doneCh
}

func (p *Putter) removeMeta(path string, toRemove map[string]string) error {
	if len(toRemove) == 0 {
		return nil
	}

	return p.handler.RemoveMeta(path, toRemove)
}

func (p *Putter) addMeta(path string, toAdd map[string]string) error {
	if len(toAdd) == 0 {
		return nil
	}

	return p.handler.AddMeta(path, toAdd)
}
