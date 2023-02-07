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
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

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
	ErrLocalNotAbs   = "local path could not be made absolute"
	ErrRemoteNotAbs  = "remote path not absolute"
	ErrReadTimeout   = "local file read timed out"
	minDirsForUnique = 2
	numPutGoroutines = 2
	fileReadTimeout  = 10 * time.Second

	// workerPoolSizeStats is the max number of concurrent file stats we'll do
	// during Put().
	workerPoolSizeStats = 16
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
	handler Handler
}

// New returns a *Putter that will use the given Handler to Put() all the
// requests in iRODS. You should defer Cleanup() on the return value.
func New(handler Handler) (*Putter, error) {
	err := handler.Connect()
	if err != nil {
		return nil, err
	}

	return &Putter{handler: handler}, nil
}

// Cleanup should be deferred after making a New Putter. It handles things like
// disconnecting.
func (p *Putter) Cleanup() error {
	return p.handler.Cleanup()
}

// CreateCollections will determine the minimal set of collections that need to
// be created to support future Put()s for the given requests, checks if those
// collections exist in iRODS, and creates them if not.
//
// You should call this before using Put(), unless you're sure all collections
// already exist.
//
// Tries to create all needed collections, potentially returning multiple errors
// wrapped in to one.
func (p *Putter) CreateCollections(requests []*Request) error {
	dirs := getUniqueRequestLeafCollections(requests)

	var merr *multierror.Error

	for _, dir := range dirs {
		if err := p.handler.EnsureCollection(dir); err != nil {
			merr = multierror.Append(merr, err)
		}
	}

	return merr.ErrorOrNil()
}

func getUniqueRequestLeafCollections(requests []*Request) []string {
	dirs := getSortedRequestCollections(requests)
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

func getSortedRequestCollections(requests []*Request) []string {
	dirs := make([]string, len(requests))

	for i, request := range requests {
		dirs[i] = filepath.Dir(request.Remote)
	}

	sort.Strings(dirs)

	return dirs
}

func noLeavesOrNewLeaf(uniqueLeafs []string, last string) bool {
	return len(uniqueLeafs) == 0 || last != uniqueLeafs[len(uniqueLeafs)-1]
}

// Validate checks the request paths are valid, and that the Local path exists.
// If the Remote already exists, compares mtimes and metadata.
//
// Modifies the request with an Error and new Status as appropriate, and returns
// true if it should be passed to Put().
//
// If only a metadata update is required, adds a private note to the Request
// that will tell Put() to only do a metadata update, and returns a second true.
//
// The Status set is the Status that would be true after a successful Put(), or
// that is true now if it shouldn't be Put():
//
//	"uploaded":   a new object was uploaded to iRODS
//	"replaced":   an existing object was replaced in iRODS, because Local was
//			      newer than Remote
//	"unmodified": Local and Remote had the same modification time, so nothing
//	              was done
//	"missing":    Local path could not be accessed, upload skipped; see Error
func (p *Putter) Validate(request *Request) (needsPut bool, metadataChangeOnly bool) {
	if err := request.ValidatePaths(); err != nil {
		updateRequest(request, RequestStatusNotAbs, err)

		return
	}

	lInfo, err := Stat(request.Local)
	if err != nil {
		updateRequest(request, RequestStatusMissing, err)

		return
	}

	request.Size = lInfo.Size

	rInfo, err := p.handler.Stat(request)
	if err != nil {
		updateRequest(request, RequestStatusFailed, err)

		return
	}

	request.addStandardMeta(lInfo.Meta, rInfo.Meta)

	return determineUploadOrUnmodified(request, lInfo, rInfo)
}

// updateRequest sets the given status and err on the given request.
func updateRequest(request *Request, status RequestStatus, err error) {
	request.Status = status

	if err != nil {
		request.Error = err.Error()
	} else {
		request.Error = ""
	}
}

// determineUploadOrUnmodified returns true if remote doesn't exist or if the
// mtime has changed. Also returns true if the mtime hasn't changed, but the
// metadata has, in which case the second bool will be true as well.
func determineUploadOrUnmodified(request *Request, lInfo, rInfo *ObjectInfo) (needsPut bool, metadataChangeOnly bool) {
	if !rInfo.Exists {
		updateRequest(request, RequestStatusUploaded, nil)

		return true, false
	}

	if lInfo.HasSameModTime(rInfo) {
		updateRequest(request, RequestStatusUnmodified, nil)

		if request.needsMetadataUpdate() {
			return true, true
		}

		return false, false
	}

	updateRequest(request, RequestStatusReplaced, nil)

	return true, false
}

// Put will upload the given request Local file to iRODS at the Remote location.
// You ought to call CreateCollections() before calling this, and you should
// only call this if Validate() returned true.
//
// MD5 checksums will be calculated locally and remotely and compared to ensure
// a perfect upload. The request metadata will then replace any existing
// metadata with the same keys on the Remote object.
//
// If the put or metadata update fails, the Status of the request will be
// altered to:
//
//	"failed": An upload attempt failed
//
// Error on the Request will also be set.
func (p *Putter) Put(request *Request) {
	if request.skipPut {
		p.applyMetadata(request)

		return
	}

	if err := p.testRead(request); err != nil {
		updateRequest(request, RequestStatusFailed, err)

		return
	}

	if err := p.handler.Put(request); err != nil {
		updateRequest(request, RequestStatusFailed, err)

		return
	}

	p.applyMetadata(request)
}

func (p *Putter) applyMetadata(request *Request) {
	toRemove, toAdd := request.determineMetadataToRemoveAndAdd()

	if err := p.removeMeta(request.Remote, toRemove); err != nil {
		updateRequest(request, RequestStatusFailed, err)

		return
	}

	if err := p.addMeta(request.Remote, toAdd); err != nil {
		updateRequest(request, RequestStatusFailed, err)

		return
	}
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

// testRead tests to see if we can open and read the request's local file,
// cancelling the read after a 10s timeout if not and returning an error.
func (p *Putter) testRead(request *Request) error {
	ctx, cancel := context.WithTimeout(context.Background(), fileReadTimeout)
	defer cancel()

	err := exec.CommandContext(ctx, "head", "-c", "1", request.Local).Run() //nolint:gosec
	if err != nil && strings.Contains(err.Error(), "killed") {
		err = Error{ErrReadTimeout, request.Local}
	}

	return err
}
