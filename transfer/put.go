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

package transfer

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gammazero/workerpool"
	"github.com/hashicorp/go-multierror"
	"github.com/wtsi-hgi/ibackup/baton/meta"
	"github.com/wtsi-hgi/ibackup/errs"
	"github.com/wtsi-hgi/ibackup/internal/logger"
)

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

	// WorkerPoolSizeCollections is the max number of concurrent collection
	// creations we'll do during CreateCollections().
	WorkerPoolSizeCollections = 2
)

// Handler is something that knows how to communicate with iRODS and carry out
// certain operations.
type Handler interface {
	// EnsureCollection checks if the given collection exists in iRODS, creates
	// it if not, then double-checks it now exists. Must support being called
	// concurrently.
	EnsureCollection(collection string) error

	// CollectionsDone is called after all collections have been created. This
	// method cleans up connections created for collection creation.
	CollectionsDone() error

	// Stat checks if the provided Remote object exists. If it does, records its
	// metadata and returns it. Returns an error if there was a problem finding
	// out information (but not if the object does not exist).
	Stat(remote string) (bool, map[string]string, error)

	// Put uploads the Local file to the Remote location, overwriting any
	// existing object, and ensuring that a locally calculated and remotely
	// calculated md5 checksum match.
	Put(local, remote string) error

	Get(local, remote string) error

	// RemoveMeta deletes the given metadata from the given object.
	RemoveMeta(path string, meta map[string]string) error

	// AddMeta adds the given metadata to the given object. Given metadata keys
	// should already have been removed with RemoveMeta() from the remote
	// object.
	AddMeta(path string, meta map[string]string) error

	// Cleanup stops any connections created earlier and does any other cleanup
	// needed.
	Cleanup()

	// GetMeta returns the meta for a given path in iRODS.
	GetMeta(path string) (map[string]string, error)
}

// FileReadTester is a function that attempts to open and read the given path,
// returning any error doing so. Should stop and clean up if the given ctx
// becomes done before the open and read succeeds.
type FileReadTester func(ctx context.Context, path string) error

// FileStatusCallback returns RequestStatusPending if the file is to be
// uploaded, and returns any other RequestStatus, such as RequestStatusHardlink
// and RequestStatusSymlink, to not be uploaded.
type FileStatusCallback func(absPath string, fi os.FileInfo) RequestStatus

// Putter is used to Put() files in iRODS.
type Putter struct {
	handler                    Handler
	logger                     logger.Logger
	fileReadTimeout            time.Duration
	fileReadTester             FileReadTester
	requests                   []*Request
	duplicateRequests          []*Request
	stat                       func(p *Putter, request *Request, putCh chan *Request, skipReturnCh chan *Request)
	transfer                   func(r *Request, handler Handler) error
	applyMetadata              func(r *Request, handler Handler) error
	overwrite, hardlinksNormal bool
}

// New returns a *Putter that will use the given Handler to Put() all the
// requests in iRODS. You should defer Cleanup() on the return value. All the
// incoming requests will have their paths validated (they must be absolute).
//
// Requests with Hardlink set will be uploaded to the Hardlink remote location,
// and an empty file uploaded to the Remote location, with metadata pointing to
// the Hardlink location.
func New(handler Handler, requests []*Request) (*Putter, error) {
	rs, dups, err := dedupAndPrepareRequests(requests)
	if err != nil {
		return nil, err
	}

	return &Putter{
		handler:           handler,
		fileReadTimeout:   defaultFileReadTimeout,
		fileReadTester:    headRead,
		requests:          rs,
		duplicateRequests: dups,
		stat:              (*Putter).statPathsAndReturnOrPut,
		transfer:          (*Request).Put,
		applyMetadata:     (*Request).RemoveAndAddMetadata,
	}, nil
}

// NewGetter returns a *Putter that will use the given Handler to Put() all the
// requests from iRODS to the local disk. You should defer Cleanup() on the
// return value. All the incoming requests will have their paths validated (they
// must be absolute).
func NewGetter(handler Handler, requests []*Request, overwrite, hardlinksNormal bool) (*Putter, error) {
	rs, dups, err := dedupAndPrepareRequests(requests)
	if err != nil {
		return nil, err
	}

	return &Putter{
		handler:           handler,
		fileReadTimeout:   defaultFileReadTimeout,
		fileReadTester:    noRead,
		requests:          rs,
		duplicateRequests: dups,
		stat:              (*Putter).getMetadataAndReturnOrPut,
		transfer:          (*Request).Get,
		applyMetadata:     (*Request).SetMeta,
		overwrite:         overwrite,
		hardlinksNormal:   hardlinksNormal,
	}, nil
}

// SetLogger sets an optional logger for deep tracing.
func (p *Putter) SetLogger(l logger.Logger) {
	p.logger = l
	if p.logger == nil || p.handler == nil {
		return
	}

	// If the underlying handler supports deep tracing, propagate the logger so
	// it can emit baton-level request/response details.
	//
	// Note: we do this before we potentially wrap the handler so we can reach the
	// actual implementation.
	if sl, ok := p.handler.(interface{ SetLogger(l logger.Logger) }); ok {
		sl.SetLogger(l)
	}

	if lh, ok := p.handler.(*loggingHandler); ok {
		if sl, ok := lh.base.(interface{ SetLogger(l logger.Logger) }); ok {
			sl.SetLogger(l)
		}
	}

	if lh, ok := p.handler.(*loggingHandler); ok {
		lh.logger = p.logger

		return
	}

	p.handler = &loggingHandler{base: p.handler, logger: p.logger}
}

func (p *Putter) debug(msg string, ctx ...interface{}) {
	if p.logger != nil {
		p.logger.Debug(msg, ctx...)
	}
}

func (p *Putter) info(msg string, ctx ...interface{}) {
	if p.logger != nil {
		p.logger.Info(msg, ctx...)
	}
}

func (p *Putter) warn(msg string, ctx ...interface{}) {
	if p.logger != nil {
		p.logger.Warn(msg, ctx...)
	}
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
func (p *Putter) Cleanup() {
	p.handler.Cleanup()
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
	p.info("create collections", "collections", len(dirs))
	pool := workerpool.New(WorkerPoolSizeCollections)
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

	err := error(nil)
	if merr != nil {
		err = merr.ErrorOrNil()
	}

	p.info("create collections finished", "err", err)

	return err
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

func noLeavesOrNewLeaf(uniqueLeafs []string, last string) bool {
	return len(uniqueLeafs) == 0 || last != uniqueLeafs[len(uniqueLeafs)-1]
}

func (p *Putter) getSortedRequestCollections() []string {
	dirs := make([]string, 0, len(p.requests))

	for _, request := range p.requests {
		for _, remote := range request.Remotes() {
			dirs = append(dirs, filepath.Dir(remote))
		}
	}

	sort.Strings(dirs)

	return dirs
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
	p.info(
		"put starting",
		"requests", len(p.requests),
		"duplicates", len(p.duplicateRequests),
		"file_read_timeout", p.fileReadTimeout,
	)

	chanLen := len(p.requests) + len(p.duplicateRequests)
	transferStartCh := make(chan *Request, chanLen)
	transferReturnCh := make(chan *Request, chanLen)
	skipReturnCh := make(chan *Request, chanLen)

	go func() {
		started := time.Now()
		p.putRequests(p.requests, transferStartCh, transferReturnCh, skipReturnCh)

		for i := range p.duplicateRequests {
			p.info("processing duplicate request", requestLogCtx(p.duplicateRequests[i])...)
			p.putRequests(p.duplicateRequests[i:i+1], transferStartCh, transferReturnCh, skipReturnCh)
		}

		close(transferStartCh)
		close(transferReturnCh)
		close(skipReturnCh)
		p.info("put finished", "took", time.Since(started))
	}()

	return transferStartCh, transferReturnCh, skipReturnCh
}

func (p *Putter) putRequests(requests []*Request, transferStartCh, transferReturnCh,
	skipReturnCh chan *Request) {
	r1, r2, r3 := p.put(requests)

	var wg sync.WaitGroup

	wg.Add(1)

	go func() {
		defer wg.Done()

		cloneChannel(r1, transferStartCh)
	}()

	wg.Add(1)

	go func() {
		defer wg.Done()

		cloneChannel(r2, transferReturnCh)
	}()

	wg.Add(1)

	go func() {
		defer wg.Done()

		cloneChannel(r3, skipReturnCh)
	}()

	wg.Wait()
}

func cloneChannel(source, dest chan *Request) {
	for r := range source {
		dest <- r
	}
}

func (p *Putter) put(requests []*Request) (chan *Request, chan *Request, chan *Request) {
	chanLen := len(requests)
	transferStartCh := make(chan *Request, chanLen)
	transferReturnCh := make(chan *Request, chanLen)
	skipReturnCh := make(chan *Request, chanLen)
	putCh := make(chan *Request, chanLen)

	var wg sync.WaitGroup

	wg.Add(numPutGoroutines)

	go p.pickFilesToPut(&wg, requests, putCh, skipReturnCh)
	go p.transferFilesInIRODS(&wg, putCh, transferStartCh, transferReturnCh, skipReturnCh)

	go func() {
		wg.Wait()
		close(transferReturnCh)
		close(skipReturnCh)
	}()

	return transferStartCh, transferReturnCh, skipReturnCh
}

// pickFilesToPut goes through all our Requests, immediately returns bad ones
// (eg. local file doesn't exist) and ones we don't need to do (hasn't been
// modified since last uploaded) via the returnCh, and sends the remainder to
// the putCh.
func (p *Putter) pickFilesToPut(wg *sync.WaitGroup, requests []*Request,
	putCh chan *Request, skipReturnCh chan *Request) {
	defer wg.Done()

	p.debug("pickFilesToPut starting", "requests", len(requests))

	pool := workerpool.New(workerPoolSizeStats)

	for _, request := range requests {
		thisRequest := request

		pool.Submit(func() {
			p.stat(p, thisRequest, putCh, skipReturnCh)
		})
	}

	pool.StopWait()
	close(putCh)
	p.debug("pickFilesToPut finished")
}

// statPathsAndReturnOrPut stats the Local and Remote paths. On error, sends the
// request to the skipReturnCh straight away. If mtime unchanged, doesn't do the
// put, sending to skipReturnCh if metadata unchanged, otherwise to putCh but
// with a note to skip the actual put and just do metadata. Otherwise, sends
// them to the putCh normally.
func (p *Putter) statPathsAndReturnOrPut(request *Request, putCh chan *Request, skipReturnCh chan *Request) {
	p.debug("stat local+remote", requestLogCtx(request)...)
	lInfo, err := Stat(request.Local)
	if err != nil {
		p.warn("stat local failed", append(requestLogCtx(request), "err", err)...)
		sendRequest(request, RequestStatusFailed, err, skipReturnCh)

		return
	}

	request.Size = lInfo.Size

	h := p.handler
	if lh, ok := h.(logCtxHandler); ok {
		h = lh.WithLogCtx(requestHandlerLogCtx(request)...)
	}

	rInfo, err := request.StatAndAssociateStandardMetadata(lInfo, h)
	if err != nil {
		p.warn("stat remote/associate metadata failed", append(requestLogCtx(request), "err", err)...)
		sendRequest(request, RequestStatusFailed, err, skipReturnCh)

		return
	}

	p.info(
		"stat results",
		append(
			requestLogCtx(request),
			"local_exists", lInfo.Exists,
			"remote_exists", rInfo.Exists,
			"local_size", request.Size,
			"local_mtime", formatMetaTime(lInfo.ModTime()),
			"remote_mtime", formatMetaTime(rInfo.ModTime()),
			"remote_meta_keys", len(rInfo.Meta),
		)...,
	)

	if !lInfo.Exists {
		p.info(
			"local missing/orphaned",
			append(requestLogCtx(request), "remote_exists", rInfo.Exists)...,
		)
		sendRequest(request, getStatusBasedOnInfo(rInfo.Exists), nil, skipReturnCh)

		return
	}

	if p.sendForUploadOrUnmodified(request, lInfo, rInfo, putCh, skipReturnCh) {
		p.debug(
			"decided upload/unmodified",
			append(requestLogCtx(request), "status", request.Status, "skip_put", request.skipPut)...,
		)
		return
	}

	p.logDecision(request, RequestStatusReplaced, "remote mtime differs or remote missing metadata after stat")
	sendRequest(request, RequestStatusReplaced, nil, putCh)
}

func requestLogCtx(r *Request) []interface{} {
	if r == nil {
		return []interface{}{"rid", ""}
	}

	return []interface{}{
		"rid", r.ID(),
		"local", r.Local,
		"remote", r.Remote,
	}
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

// requestHandlerLogCtx returns the per-request context that should be used for
// iRODS operation logs.
//
// We intentionally avoid keys like "local" and "remote" here, because those are
// used by individual iRODS operations (eg. Put(local, remote)) and would
// otherwise appear twice in log records. This is especially confusing for
// hardlink uploads where the operation remote differs from the request remote.
func requestHandlerLogCtx(r *Request) []any {
	if r == nil {
		return []any{"rid", ""}
	}

	return []any{
		"rid", r.ID(),
		"req_local", r.Local,
		"req_remote", r.Remote,
	}
}

func formatMetaTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}

	return t.UTC().Format(time.RFC3339Nano)
}

func getStatusBasedOnInfo(remoteExists bool) RequestStatus {
	if remoteExists {
		return RequestStatusOrphaned
	}

	return RequestStatusMissing
}

func (p *Putter) logDecision(r *Request, status RequestStatus, reason string, extraCtx ...interface{}) {
	ctx := append(requestLogCtx(r), "status", status, "reason", reason)
	ctx = append(ctx, extraCtx...)
	p.info("request decision", ctx...)
}

// sendForUploadOrUnmodified sends the request to putCh if remote doesn't exist
// or if the mtime hasn't change but the metadata has, or the skipReturnCh if
// the metadata hasn't changed. Returns true in one of those cases, or false if
// the request needs to be uploaded again because the mtime changed.
func (p *Putter) sendForUploadOrUnmodified(
	request *Request,
	lInfo *ObjectInfo,
	rInfo *ObjectInfo,
	putCh chan *Request,
	skipReturnCh chan *Request,
) bool {
	if !rInfo.Exists {
		p.logDecision(request, RequestStatusUploaded, "remote missing - scheduling upload")
		sendRequest(request, RequestStatusUploaded, nil, putCh)

		return true
	}

	if !lInfo.HasSameModTime(rInfo) {
		return false
	}

	p.handleUnmodifiedRequest(request, putCh, skipReturnCh)

	return true
}

func (p *Putter) handleUnmodifiedRequest(request *Request, putCh, skipReturnCh chan *Request) {
	ch := skipReturnCh

	request.skipPut = request.Meta.needsMetadataUpdate()
	if request.skipPut {
		p.logDecision(
			request,
			RequestStatusUnmodified,
			"remote up to date; metadata needs refresh",
			"skip_put", request.skipPut,
		)

		ch = putCh
	} else {
		p.logDecision(request, RequestStatusUnmodified, "remote up to date; metadata unchanged")
	}

	sendRequest(request, RequestStatusUnmodified, nil, ch)
}

func (p *Putter) getMetadataAndReturnOrPut(request *Request, putCh chan *Request, skipReturnCh chan *Request) {
	p.debug("get metadata", requestLogCtx(request)...)
	lInfo, err := Stat(request.Local)
	if skipIfLocalFileIsNotEmptyAndNotOverwriting(lInfo, err, p.overwrite) {
		sendRequest(request, RequestStatusUnmodified, err, skipReturnCh)

		return
	}

	h := p.handler
	if lh, ok := h.(logCtxHandler); ok {
		h = lh.WithLogCtx(requestHandlerLogCtx(request)...)
	}

	rInfo, err := request.GetRemoteMetadata(h)
	if err != nil {
		p.warn("get remote metadata failed", append(requestLogCtx(request), "err", err)...)
		sendRequest(request, RequestStatusFailed, err, skipReturnCh)

		return
	}

	request.Meta.LocalMeta = request.Meta.remoteMeta

	p.sendGetRequest(request, lInfo, rInfo, putCh, skipReturnCh, p.hardlinksNormal)
}

func skipIfLocalFileIsNotEmptyAndNotOverwriting(lInfo *ObjectInfo, err error, overwrite bool) bool {
	return err == nil && lInfo.Size != 0 && !overwrite
}

func (p *Putter) sendGetRequest(request *Request, lInfo, rInfo *ObjectInfo,
	putCh, skipReturnCh chan *Request, hardlinksNormal bool) {
	if hardlink, ok := request.Meta.remoteMeta[MetaKeyRemoteHardlink]; ok {
		request.Hardlink = hardlink

		if !hardlinksNormal {
			p.logDecision(request, RequestStatusHardlinkSkipped, "hardlink present but restore in normal mode disabled")
			sendRequest(request, RequestStatusHardlinkSkipped, nil, skipReturnCh)

			return
		}

		request.Remote = hardlink
	}

	addRemoteMetaForGetRequest(request)

	if lInfo == nil || !lInfo.Exists {
		sendRequest(request, RequestStatusUploaded, nil, putCh)

		return
	}

	if !p.sendForUploadOrUnmodified(request, lInfo, rInfo, putCh, skipReturnCh) {
		sendRequest(request, RequestStatusReplaced, nil, putCh)
	}
}

func addRemoteMetaForGetRequest(request *Request) {
	if symlink, ok := request.Meta.remoteMeta[MetaKeySymlink]; ok {
		request.Symlink = symlink
	}

	_, hasMtime := request.Meta.remoteMeta[MetaKeyMtime]
	remoteMtime, hasRemoteMtime := request.Meta.remoteMeta[meta.MetaKeyRemoteMtime]

	if !hasMtime && hasRemoteMtime {
		request.Meta.remoteMeta[MetaKeyMtime] = remoteMtime
	}
}

// transferFilesInIRODS uses our handler to Put() requests sent on the given putCh in
// to iRODS sequentially, and concurrently ReplaceMetadata() on those objects
// once they're in. When each request is about to start uploading, it is sent
// on uploadStartCh, then when it completes it is sent on the returnCh.
func (p *Putter) transferFilesInIRODS(wg *sync.WaitGroup, putCh, uploadStartCh,
	uploadReturnCh, skipReturnCh chan *Request) {
	defer wg.Done()

	p.debug("transferFilesInIRODS starting")

	metaCh := make(chan *Request, cap(uploadReturnCh))
	metaDoneCh := make(chan struct{})

	go p.applyMetadataConcurrently(metaCh, uploadReturnCh, skipReturnCh, metaDoneCh)

	p.processPutCh(putCh, uploadStartCh, uploadReturnCh, metaCh)

	close(uploadStartCh)
	close(metaCh)
	<-metaDoneCh
	p.debug("transferFilesInIRODS finished")
}

func (p *Putter) applyMetadataConcurrently(metaCh, uploadReturnCh, skipReturnCh chan *Request,
	metaDoneCh chan struct{}) {
	for request := range metaCh {
		start := time.Now()
		toRemove, toAdd := request.Meta.determineMetadataToRemoveAndAdd()

		p.debug(
			"apply metadata starting",
			append(requestLogCtx(request), "skip_put", request.skipPut)...,
		)
		p.info(
			"metadata plan",
			append(
				requestLogCtx(request),
				"skip_put", request.skipPut,
				"remove_keys", sortedMapKeys(toRemove),
				"add_keys", sortedMapKeys(toAdd),
			)...,
		)
		returnCh := uploadReturnCh

		if request.skipPut {
			returnCh = skipReturnCh
		}

		h := p.handler
		if lh, ok := h.(logCtxHandler); ok {
			h = lh.WithLogCtx(requestHandlerLogCtx(request)...)
		}

		if err := p.applyMetadata(request, h); err != nil { //nolint:nestif
			p.warn(
				"apply metadata failed",
				append(requestLogCtx(request), "took", time.Since(start), "err", err)...,
			)
			if pe := new(fs.PathError); errors.As(err, &pe) && pe.Op == "lchown" {
				sendRequest(request, RequestStatusWarning, err, returnCh)
			} else {
				p.sendFailedRequest(request, err, returnCh)
			}

			continue
		}

		p.info(
			"apply metadata finished",
			append(
				requestLogCtx(request),
				"took", time.Since(start),
				"remove_keys", len(toRemove),
				"add_keys", len(toAdd),
			)...,
		)
		returnCh <- request
	}

	close(metaDoneCh)
}

func sortedMapKeys(m map[string]string) []string {
	if len(m) == 0 {
		return nil
	}

	keys := make([]string, 0, len(m))

	for k := range m {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	return keys
}

// sendFailedRequest adds the err details to the request and sends it on the
// given channel.
func (p *Putter) sendFailedRequest(request *Request, err error, uploadReturnCh chan *Request) {
	sendRequest(request, RequestStatusFailed, err, uploadReturnCh)
}

func (p *Putter) processPutCh(putCh, uploadStartCh, uploadReturnCh, metaCh chan *Request) {
	for request := range putCh {
		if request.skipPut {
			p.info("skip put; metadata only", requestLogCtx(request)...)
			metaCh <- request

			continue
		}

		uploading := request.Clone()
		uploading.Status = RequestStatusUploading

		ctx := append(requestLogCtx(request), "size_bytes", request.Size)
		if request.Hardlink != "" {
			ctx = append(
				ctx,
				"hardlink", request.Hardlink,
				"only_empty", request.onlyUploadEmptyFile,
				"inode_remote", request.inodeRequest.Remote,
				"empty_remote", request.emptyFileRequest.Remote,
			)
		}

		p.info("upload starting", ctx...)
		uploadStartCh <- uploading

		rstart := time.Now()
		if err := p.testRead(request); err != nil {
			p.warn(
				"pre-read test failed",
				append(requestLogCtx(request), "took", time.Since(rstart), "err", err)...,
			)
			p.sendFailedRequest(request, err, uploadReturnCh)

			continue
		}

		p.debug(
			"pre-read test ok",
			append(requestLogCtx(request), "took", time.Since(rstart))...,
		)

		tstart := time.Now()

		h := p.handler
		if lh, ok := h.(logCtxHandler); ok {
			h = lh.WithLogCtx(requestHandlerLogCtx(request)...)
		}

		if err := p.transfer(request, h); err != nil {
			p.warn(
				"transfer failed",
				append(requestLogCtx(request), "took", time.Since(tstart), "err", err)...,
			)
			p.sendFailedRequest(request, err, uploadReturnCh)

			continue
		}

		p.info(
			"transfer finished",
			append(requestLogCtx(request), "took", time.Since(tstart))...,
		)
		metaCh <- request
	}
}

// testRead tests to see if we can open and read the request's local file,
// cancelling the read after a timeout if not and returning an error.
func (p *Putter) testRead(request *Request) error {
	if request.Symlink != "" {
		return nil
	}

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
			errCh <- errs.PathError{Msg: ErrReadTimeout, Path: request.Local}
		case err := <-readCh:
			timer.Stop()
			errCh <- err
		}
	}()

	return <-errCh
}

// headRead is a FileReadTester that uses the "head" command and kills it if the
// ctx becomes done. It's the default FileReadTester used for Putters.
func headRead(ctx context.Context, path string) error {
	out, err := exec.CommandContext(ctx, "head", "-c", "1", path).CombinedOutput()
	if err != nil && len(out) > 0 {
		err = errs.PathError{Msg: string(out)}
	}

	return err
}

func noRead(_ context.Context, _ string) error { return nil }

// dedupRequests splits the given requests in to a slice requests that have
// unique Remote values, and a slice with duplicates. Also "prepares" all
// requests, ensuring they have valid paths and that hardlinks will be handled
// appropriately later.
func dedupAndPrepareRequests(requests []*Request) ([]*Request, []*Request, error) {
	unique := make([]*Request, 0, len(requests))
	seen := make(map[string]bool)

	var dups []*Request

	for _, r := range requests {
		err := r.Prepare()
		if err != nil {
			return nil, nil, err
		}

		if seen[r.RemoteDataPath()] {
			dups = append(dups, r)
		} else {
			unique = append(unique, r)
		}

		seen[r.RemoteDataPath()] = true
	}

	return unique, dups, nil
}
