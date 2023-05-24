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
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gammazero/workerpool"
	"github.com/hashicorp/go-multierror"
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

func (e Error) Is(err error) bool {
	var putErr *Error
	if errors.As(err, &putErr) {
		return putErr.msg == e.msg
	}

	return false
}

const (
	ErrLocalNotAbs         = "local path could not be made absolute"
	ErrRemoteNotAbs        = "remote path not absolute"
	ErrReadTimeout         = "local file read timed out"
	ErrStuckTimeout        = "upload killed because it was stuck"
	minDirsForUnique       = 2
	numPutGoroutines       = 2
	defaultFileReadTimeout = 10 * time.Second

	// workerPoolSizeStats is the max number of concurrent file stats we'll do
	// during Put().
	workerPoolSizeStats = 16

	// workerPoolSizeCollections is the max number of concurrent collection
	// creations we'll do during CreateCollections().
	workerPoolSizeCollections = 2
)

// Handler is something that knows how to communicate with iRODS and carry out
// certain operations.
type Handler interface {
	// EnsureCollection checks if the given collection exists in iRODS, creates
	// it if not, then double-checks it now exists. Must support being called
	// concurrently.
	EnsureCollection(collection string) error

	// CollectionsDone is called after all collections have been created. This
	// method can do things like cleaning up connections created for collection
	// creation. It can also create new connections for subsequent Put() and
	// *Meta calls that are likely to occur.
	CollectionsDone() error

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

	// Cleanup stops any connections created earlier and does any other cleanup
	// needed.
	Cleanup() error
}

// FileReadTester is a function that attempts to open and read the given path,
// returning any error doing so. Should stop and clean up if the given ctx
// becomes done before the open and read succeeds.
type FileReadTester func(ctx context.Context, path string) error

// headRead is a FileReadTester that uses the "head" command and kills it if the
// ctx becomes done. It's the default FileReadTester used for Putters.
func headRead(ctx context.Context, path string) error {
	out, err := exec.CommandContext(ctx, "head", "-c", "1", path).CombinedOutput()
	if err != nil && len(out) > 0 {
		err = Error{msg: string(out)}
	}

	return err
}

// FileStatusCallback returns RequestStatusPending if the file is to be
// uploaded, and returns any other RequestStatus, such as RequestStatusHardlink
// and RequestStatusSymlink, to not be uploaded.
type FileStatusCallback func(absPath string, fi os.FileInfo) RequestStatus

// Putter is used to Put() files in iRODS.
type Putter struct {
	handler           Handler
	fileReadTimeout   time.Duration
	fileReadTester    FileReadTester
	requests          []*Request
	duplicateRequests []*Request
}

// New returns a *Putter that will use the given Handler to Put() all the
// requests in iRODS. You should defer Cleanup() on the return value. All the
// incoming requests will have their paths validated (they must be absolute).
//
// Extra requests are created for requests representing a hardlink that will put
// them in hardlink location, along with an empty file for the original.
func New(handler Handler, requests []*Request) (*Putter, error) {
	rs, dups := modifyHardlinkRequests(requests)

	for _, request := range append(rs, dups...) {
		if err := request.ValidatePaths(); err != nil {
			return nil, err
		}
	}

	return &Putter{
		handler:           handler,
		fileReadTimeout:   defaultFileReadTimeout,
		fileReadTester:    headRead,
		requests:          rs,
		duplicateRequests: dups,
	}, nil
}

// modifyHardlinkRequests sets Remote to Hardlink to hardlink requests. Stores
// the original Remote so it can be recovered later.
func modifyHardlinkRequests(requests []*Request) ([]*Request, []*Request) {
	rs := make([]*Request, 0, len(requests))
	seen := make(map[string]bool)

	var dups []*Request

	for _, r := range requests {
		var rr *Request

		if r.Hardlink != "" {
			rr = r.Clone()
			rr.origRemote = r.Remote
			rr.Remote = r.Hardlink
		} else {
			rr = r
		}

		if seen[rr.Remote] {
			if r.Hardlink != "" {
				rr = r.Clone()
				rr.Local = os.DevNull
				rr.Meta[MetaKeyHardlink] = r.Local
				rr.Meta[MetaKeyRemoteHardlink] = r.Hardlink
				rr.Hardlink = ""
			}
			dups = append(dups, rr)
		} else {
			rs = append(rs, rr)
		}

		seen[rr.Remote] = true
	}

	return rs, dups
}

// SetFileReadTimeout sets how long to wait on a test open and read of each
// local file before considering it not possible to upload. The default is
// 10seconds.
func (p *Putter) SetFileReadTimeout(timeout time.Duration) {
	p.fileReadTimeout = timeout
}

// SetFileReadTester sets the function used to see if a file can be opened and
// read. If this attempt hangs, the function should stop and clean up in
// response to its context becoming done (when the FileReadTimeout elapses).
//
// The default tester shells out to the "head" command so that we don't leak
// stuck goroutines.
func (p *Putter) SetFileReadTester(tester FileReadTester) {
	p.fileReadTester = tester
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
// You MUST call this before Put(), even if you think all collections have
// already been created.
//
// Tries to create all needed collections, potentially returning multiple errors
// wrapped in to one.
func (p *Putter) CreateCollections() error {
	dirs := p.getUniqueRequestLeafCollections()
	pool := workerpool.New(workerPoolSizeCollections)
	errCh := make(chan error, len(dirs))

	for _, dir := range dirs {
		coll := dir

		pool.Submit(func() {
			errCh <- p.handler.EnsureCollection(coll)
		})
	}

	pool.StopWait()
	close(errCh)

	var merr *multierror.Error

	for err := range errCh {
		if err != nil {
			merr = multierror.Append(merr, err)
		}
	}

	cdErr := p.handler.CollectionsDone()
	if cdErr != nil {
		merr = multierror.Append(merr, cdErr)
	}

	return merr.ErrorOrNil()
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
// Requests that need to be uploaded are first sent on the first returned
// channel, with Status set to "uploading".
//
// Each of those will then be send on the second returned channel, with Status
// set like:
//
//	 "uploaded":   a new object was uploaded to iRODS
//		"replaced":   an existing object was replaced in iRODS, because Local was
//		              newer than Remote
//		"failed":     An upload attempt failed; see Error
//
// The third return channel will receive Requests that didn't need to be
// uploaded, with Status set like:
//
//	"unmodified": Local and Remote had the same modification time, so nothing
//	              was done
//	"missing":    Local path could not be accessed, upload skipped; see Error
//
// By calling SetFileStatusCallback() you can decide additional files to not
// upload.
func (p *Putter) Put() (chan *Request, chan *Request, chan *Request) {
	chanLen := len(p.requests) + len(p.duplicateRequests)
	uploadStartCh := make(chan *Request, chanLen)
	uploadReturnCh := make(chan *Request, chanLen)
	skipReturnCh := make(chan *Request, chanLen)

	r1, r2, r3 := p.put(p.requests)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		cloneChannel(r1, uploadStartCh)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		cloneChannel(r2, uploadReturnCh)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		cloneChannel(r3, skipReturnCh)
	}()

	wg.Wait()

	r1, r2, r3 = p.put(p.duplicateRequests)

	go func() {
		cloneChannel(r1, uploadStartCh)
		close(uploadStartCh)
	}()

	go func() {
		cloneChannel(r2, uploadReturnCh)
		close(uploadReturnCh)
	}()

	go func() {
		cloneChannel(r3, skipReturnCh)
		close(skipReturnCh)
	}()

	return uploadStartCh, uploadReturnCh, skipReturnCh
}

func (p *Putter) put(requests []*Request) (chan *Request, chan *Request, chan *Request) {
	chanLen := len(requests)
	uploadStartCh := make(chan *Request, chanLen)
	uploadReturnCh := make(chan *Request, chanLen)
	skipReturnCh := make(chan *Request, chanLen)
	putCh := make(chan *Request, chanLen)

	var wg sync.WaitGroup

	wg.Add(numPutGoroutines)

	go p.pickFilesToPut(&wg, requests, putCh, skipReturnCh)
	go p.putFilesInIRODS(&wg, putCh, uploadStartCh, uploadReturnCh, skipReturnCh)

	go func() {
		wg.Wait()
		close(uploadReturnCh)
		close(skipReturnCh)
	}()

	return uploadStartCh, uploadReturnCh, skipReturnCh
}

func cloneChannel(source, dest chan *Request) {
	for r := range source {
		dest <- r
	}
}

// pickFilesToPut goes through all our Requests, immediately returns bad ones
// (eg. local file doesn't exist) and ones we don't need to do (hasn't been
// modified since last uploaded) via the returnCh, and sends the remainder to
// the putCh.
func (p *Putter) pickFilesToPut(wg *sync.WaitGroup, requests []*Request, putCh chan *Request, skipReturnCh chan *Request) {
	defer wg.Done()

	pool := workerpool.New(workerPoolSizeStats)

	for _, request := range requests {
		thisRequest := request

		pool.Submit(func() {
			p.statPathsAndReturnOrPut(thisRequest, putCh, skipReturnCh)
		})
	}

	pool.StopWait()
	close(putCh)
}

// statPathsAndReturnOrPut stats the Local and Remote paths. On error, sends the
// request to the skipReturnCh straight away. If mtime unchanged, doesn't do the
// put, sending to skipReturnCh if metadata unchanged, otherwise to putCh but
// with a note to skip the actual put and just do metadata. Otherwise, sends
// them to the putCh normally.
func (p *Putter) statPathsAndReturnOrPut(request *Request, putCh chan *Request, skipReturnCh chan *Request) {
	lInfo, err := Stat(request.Local)
	if err != nil {
		sendRequest(request, RequestStatusMissing, err, skipReturnCh)

		return
	}

	request.Size = lInfo.Size

	rInfo, err := p.handler.Stat(request)
	if err != nil {
		sendRequest(request, RequestStatusFailed, err, skipReturnCh)

		return
	}

	if request.Hardlink != "" {
		originalRequest := request.Clone()
		originalRequest.Remote = request.origRemote
		orInfo, err := p.handler.Stat(originalRequest)
		if err != nil {
			sendRequest(request, RequestStatusFailed, err, skipReturnCh)

			return
		}

		request.addStandardMeta(lInfo.Meta, orInfo.Meta)

		request.originalRemoteMeta = request.remoteMeta
		request.remoteMeta = make(map[string]string)
	}

	request.addStandardMeta(lInfo.Meta, rInfo.Meta)

	if sendForUploadOrUnmodified(request, lInfo, rInfo, putCh, skipReturnCh) {
		return
	}

	sendRequest(request, RequestStatusReplaced, nil, putCh)
}

// sendForUploadOrUnmodified sends the request to putCh if remote doesn't exist
// or if the mtime hasn't change but the metadata has, or the skipReturnCh if
// the metadata hasn't changed. Returns true in one of those cases, or false if
// the request needs to be uploaded again because the mtime changed.
func sendForUploadOrUnmodified(request *Request, lInfo, rInfo *ObjectInfo, putCh, skipReturnCh chan *Request) bool {
	if !rInfo.Exists {
		sendRequest(request, RequestStatusUploaded, nil, putCh)

		return true
	}

	if lInfo.HasSameModTime(rInfo) {
		ch := skipReturnCh

		if request.needsMetadataUpdate() {
			ch = putCh
		}

		sendRequest(request, RequestStatusUnmodified, nil, ch)

		return true
	}

	return false
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
// on uploadStartCh, then when it completes it is sent on the returnCh.
func (p *Putter) putFilesInIRODS(wg *sync.WaitGroup, putCh, uploadStartCh, uploadReturnCh, skipReturnCh chan *Request) {
	defer wg.Done()

	metaCh := make(chan *Request, cap(uploadReturnCh))
	metaDoneCh := make(chan struct{})

	go p.applyMetadataConcurrently(metaCh, uploadReturnCh, skipReturnCh, metaDoneCh)

	p.processPutCh(putCh, uploadStartCh, uploadReturnCh, metaCh)

	close(uploadStartCh)
	close(metaCh)
	<-metaDoneCh
}

func (p *Putter) applyMetadataConcurrently(metaCh, uploadReturnCh, skipReturnCh chan *Request,
	metaDoneCh chan struct{}) {
	for request := range metaCh {
		toRemove, toAdd := request.determineMetadataToRemoveAndAdd()

		returnCh := uploadReturnCh

		if request.skipPut {
			returnCh = skipReturnCh
		}

		if err := p.removeMeta(request.Remote, toRemove); err != nil {
			p.sendFailedRequest(request, err, returnCh)

			continue
		}

		if err := p.addMeta(request.Remote, toAdd); err != nil {
			p.sendFailedRequest(request, err, returnCh)

			continue
		}

		returnCh <- request
	}

	close(metaDoneCh)
}

func (p *Putter) removeMeta(path string, toRemove map[string]string) error {
	if len(toRemove) == 0 {
		return nil
	}

	return p.handler.RemoveMeta(path, toRemove)
}

// sendFailedRequest adds the err details to the request and sends it on the
// given channel.
func (p *Putter) sendFailedRequest(request *Request, err error, uploadReturnCh chan *Request) {
	sendRequest(request, RequestStatusFailed, err, uploadReturnCh)
}

func (p *Putter) addMeta(path string, toAdd map[string]string) error {
	if len(toAdd) == 0 {
		return nil
	}

	return p.handler.AddMeta(path, toAdd)
}

func (p *Putter) processPutCh(putCh, uploadStartCh, uploadReturnCh, metaCh chan *Request) {
	for request := range putCh {
		if request.skipPut {
			metaCh <- request

			continue
		}

		uploading := request.Clone()
		uploading.Status = RequestStatusUploading
		uploadStartCh <- uploading

		if err := p.testRead(request); err != nil {
			p.sendFailedRequest(request, err, uploadReturnCh)

			continue
		}

		if err := p.handler.Put(request); err != nil {
			p.sendFailedRequest(request, err, uploadReturnCh)

			continue
		}

		if request.origRemote != "" {
			empty := request.Clone()
			empty.Remote = request.origRemote
			empty.remoteMeta = request.originalRemoteMeta
			empty.Meta[MetaKeyRemoteHardlink] = request.Hardlink
			empty.Meta[MetaKeyHardlink] = request.Local
			empty.Local = os.DevNull

			if err := p.handler.Put(empty); err != nil {
				p.sendFailedRequest(request, err, uploadReturnCh)

				continue
			}

			toRemove, toAdd := empty.determineMetadataToRemoveAndAdd()

			if err := p.removeMeta(empty.Remote, toRemove); err != nil {
				p.sendFailedRequest(empty, err, uploadReturnCh)

				continue
			}

			if err := p.addMeta(empty.Remote, toAdd); err != nil {
				p.sendFailedRequest(empty, err, uploadReturnCh)

				continue
			}
		}

		metaCh <- request
	}
}

// testRead tests to see if we can open and read the request's local file,
// cancelling the read after a timeout if not and returning an error.
func (p *Putter) testRead(request *Request) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	timer := time.NewTimer(p.fileReadTimeout)
	readCh := make(chan error, 1)

	go func() {
		readCh <- p.fileReadTester(ctx, request.Local)
	}()

	errCh := make(chan error)

	go func() {
		select {
		case <-timer.C:
			errCh <- Error{ErrReadTimeout, request.Local}
		case err := <-readCh:
			timer.Stop()
			errCh <- err
		}
	}()

	return <-errCh
}
