/*******************************************************************************
 * Copyright (c) 2022, 2023, 2025 Genome Research Ltd.
 *
 * Author: Sendu Bala <sb10@sanger.ac.uk>
 * Author: Rosie Kern <rk18@sanger.ac.uk>
 * Author: Iaroslav Popov <ip13@sanger.ac.uk>
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

// this file lets you use baton, via extendo.

package baton

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/wtsi-hgi/ibackup/baton/meta"
	"github.com/wtsi-hgi/ibackup/errs"
	"github.com/wtsi-hgi/ibackup/internal"
	"github.com/wtsi-hgi/ibackup/internal/logger"
	ex "github.com/wtsi-npg/extendo/v2"
	logs "github.com/wtsi-npg/logshim"
	"github.com/wtsi-npg/logshim-zerolog/zlog"
	"github.com/wtsi-ssg/wr/backoff"
	btime "github.com/wtsi-ssg/wr/backoff/time"
	"github.com/wtsi-ssg/wr/retry"
)

const (
	ErrOperationTimeout = "iRODS operation timed out"

	extendoLogLevel        = logs.ErrorLevel
	numCollClients         = 2
	collClientMaxIndex     = numCollClients - 1
	putClientIndex         = collClientMaxIndex + 1
	metaClientIndex        = putClientIndex + 1
	extendoNotExist        = "does not exist"
	operationMinBackoff    = 5 * time.Second
	operationMaxBackoff    = 30 * time.Second
	operationBackoffFactor = 1.1
	operationTimeout       = 15 * time.Second
	operationRetries       = 6
	putStillRunningLogFreq = 2 * time.Minute
	getStillRunningLogFreq = 2 * time.Minute

	maxReplicateDetails = 10
	checksumPrefixLen   = 12
)

type loggingSleeper struct {
	logger logger.Logger
	ctx    []any
	base   *btime.Sleeper
}

func (s *loggingSleeper) Sleep(ctx context.Context, d time.Duration) {
	if s.logger != nil {
		s.logger.Info("baton retry backoff", append(s.ctx, "sleep", d)...)
	}

	s.base.Sleep(ctx, d)
}

// Baton is a Handler that uses Baton (via extendo) to interact with iRODS.
type Baton struct {
	collPool     *ex.ClientPool
	collClients  []*ex.Client
	collRunning  bool
	collCh       chan string
	collErrCh    chan error
	collMu       sync.Mutex
	putClient    *ex.Client
	metaClient   *ex.Client
	removeClient *ex.Client
	logger       logger.Logger
}

// GetBatonHandler returns a Handler that uses Baton to interact with iRODS. If
// you don't have baton-do in your PATH, you'll get an error.
func GetBatonHandler() (*Baton, error) {
	setupExtendoLogger()

	_, err := ex.FindBaton()

	return &Baton{}, err
}

// SetLogger sets an optional logger for deep tracing.
func (b *Baton) SetLogger(l logger.Logger) {
	b.logger = l
}

func (b *Baton) debug(msg string, ctx ...interface{}) {
	if b.logger != nil {
		b.logger.Debug(msg, ctx...)
	}
}

func (b *Baton) info(msg string, ctx ...interface{}) {
	if b.logger != nil {
		b.logger.Info(msg, ctx...)
	}
}

func (b *Baton) warn(msg string, ctx ...interface{}) {
	if b.logger != nil {
		b.logger.Warn(msg, ctx...)
	}
}

func (b *Baton) logIRODSError(err error, ctx ...interface{}) {
	if b.logger == nil || err == nil {
		return
	}

	if ex.IsRodsError(err) {
		code, cerr := ex.RodsErrorCode(err)
		if cerr == nil {
			b.warn("iRODS error", append(ctx, "code", code, "err", err)...)

			return
		}
	}

	b.warn("iRODS error", append(ctx, "err", err)...)
}

func (b *Baton) replicateSummaries(reps []ex.Replicate) []string {
	if len(reps) == 0 {
		return nil
	}

	limit := len(reps)
	if limit > maxReplicateDetails {
		limit = maxReplicateDetails
	}

	out := make([]string, 0, limit)
	for i := range limit {
		r := reps[i]

		ck := r.Checksum
		if len(ck) > checksumPrefixLen {
			ck = ck[:checksumPrefixLen]
		}

		out = append(out, fmt.Sprintf("%s@%s#%d valid=%t cksum=%s", r.Resource, r.Location, r.Number, r.Valid, ck))
	}

	if len(reps) > limit {
		out = append(out, fmt.Sprintf("... (%d more)", len(reps)-limit))
	}

	return out
}

func (b *Baton) listItemDetails(
	client *ex.Client,
	remote string,
) (exists bool, it ex.RodsItem, took time.Duration, err error) {
	start := time.Now()
	args := ex.Args{Timestamp: true, Size: true, Checksum: true, Replicate: true}

	err = timeoutOp(func() error {
		var errl error

		it, errl = client.ListItem(args, *requestToRodsItem("", remote))

		return errl
	}, "debug list failed: "+remote)

	took = time.Since(start)

	if err != nil {
		if strings.Contains(err.Error(), extendoNotExist) {
			return false, ex.RodsItem{}, took, nil
		}

		return false, ex.RodsItem{}, took, err
	}

	return true, it, took, nil
}

// timeoutOp carries out op, returning any error from it. Has an operationTimeout
// timeout on running op, and will return a timeout error instead if exceeded.
func timeoutOp(op retry.Operation, path string) error {
	errCh := make(chan error, 1)

	go func() {
		errCh <- op()
	}()

	timer := time.NewTimer(operationTimeout)

	var err error

	select {
	case err = <-errCh:
		timer.Stop()
	case <-timer.C:
		err = errs.PathError{Msg: ErrOperationTimeout, Path: path}
	}

	return err
}

// requestToRodsItem converts a Request in to an extendo RodsItem without AVUs.
// If you provide an empty local path, it will not be set.
func requestToRodsItem(local, remote string) *ex.RodsItem {
	item := &ex.RodsItem{
		IPath: filepath.Dir(remote),
		IName: filepath.Base(remote),
	}

	if local != "" {
		item.IDirectory = filepath.Dir(local)
		item.IFile = filepath.Base(local)
	}

	return item
}

// Snapshot returns a best-effort snapshot of an iRODS data object.
//
// Intended for higher-level callers (eg. transfer logging) that already have a
// per-request correlation id (rid) and want to log iRODS state before/after a
// put.
func (b *Baton) Snapshot(
	remote string,
) (exists bool, size uint64, checksum string, replicas int, replicateDetails []string, err error) {
	err = b.setClientIfNotExists(&b.metaClient)
	if err != nil {
		return false, 0, "", 0, nil, err
	}

	exists, it, _, err := b.listItemDetails(b.metaClient, remote)
	if err != nil {
		return false, 0, "", 0, nil, err
	}

	if !exists {
		return false, 0, "", 0, nil, nil
	}

	return true, it.ISize, it.IChecksum, len(it.IReplicates), b.replicateSummaries(it.IReplicates), nil
}

// EnsureCollection ensures the given collection exists in iRODS, creating it if
// necessary. You must call Connect() before calling this.
//
// This is safe for calling concurrently, and uses multiple connections. But an
// artefact is that the error you get might be for a different
// EnsureCollection() call you made for a different collection. This shouldn't
// make much difference if you just collect all your errors and don't care about
// order.
func (b *Baton) EnsureCollection(collection string) error {
	b.collMu.Lock()

	if !b.collRunning {
		if err := b.makeCollConnections(); err != nil {
			b.collMu.Unlock()

			return err
		}

		b.startCreatingCollections()

		b.collRunning = true
	}

	collCh := b.collCh
	errCh := b.collErrCh
	b.collMu.Unlock()

	collCh <- collection

	return <-errCh
}

// startCreatingCollections creates b.collCh and creates any collection sent to
// that channel in a goroutine.
func (b *Baton) startCreatingCollections() {
	b.collCh = make(chan string)
	b.collErrCh = make(chan error)

	go func(collCh chan string, errCh chan error) {
		for index := range b.collClients {
			go func(index int) {
				for collection := range collCh {
					errCh <- b.ensureCollection(index, ex.RodsItem{IPath: collection})
				}
			}(index)
		}
	}(b.collCh, b.collErrCh)
}

// makeCollConnections creates connections for making collections, if we don't
// already have them.
func (b *Baton) makeCollConnections() error {
	if b.collClients != nil {
		return nil
	}

	pool, clientCh, err := b.connect(numCollClients)
	if err != nil {
		return err
	}

	b.collPool = pool
	b.collClients = make([]*ex.Client, 0, numCollClients)

	for client := range clientCh {
		b.collClients = append(b.collClients, client)
	}

	return nil
}

// connect creates a connection pool and prepares the given number of
// connections to iRODS concurrently, for later use by other methods.
//
// Returns a pool you should later close, and a channel containing numClients
// clients.
func (b *Baton) connect(numClients uint8) (*ex.ClientPool, chan *ex.Client, error) {
	params := ex.DefaultClientPoolParams
	params.MaxSize = numClients
	pool := ex.NewClientPool(params, "")

	clientCh, err := b.GetClientsFromPoolConcurrently(pool, numClients)

	return pool, clientCh, err
}

// GetClientsFromPoolConcurrently gets numClients clients from the pool
// concurrently.
func (b *Baton) GetClientsFromPoolConcurrently(pool *ex.ClientPool, numClients uint8) (chan *ex.Client, error) {
	clientCh := make(chan *ex.Client, numClients)
	errCh := make(chan error, numClients)

	var wg sync.WaitGroup

	for range numClients {
		wg.Add(1)

		go func() {
			defer wg.Done()

			client, err := pool.Get()
			if err != nil {
				errCh <- err
			}

			clientCh <- client
		}()
	}

	wg.Wait()
	close(errCh)
	close(clientCh)

	return clientCh, <-errCh
}

func (b *Baton) ensureCollection(clientIndex int, ri ex.RodsItem) error {
	err := timeoutOp(func() error {
		_, errl := b.collClients[clientIndex].ListItem(ex.Args{}, ri)

		return errl
	}, "collection failed: "+ri.IPath)
	if err == nil {
		return nil
	}

	if errors.Is(err, errs.PathError{Msg: ErrOperationTimeout, Path: ri.IPath}) {
		return err
	}

	return b.createCollectionWithTimeoutAndRetries(clientIndex, ri)
}

// createCollectionWithTimeoutAndRetries tries to make and confirm the given
// collection, retrying with a backoff because it can fail for no good reason,
// then work later.
func (b *Baton) createCollectionWithTimeoutAndRetries(clientIndex int, ri ex.RodsItem) error {
	return b.doWithTimeoutAndRetries(func() error {
		_, err := b.collClients[clientIndex].MkDir(ex.Args{Recurse: true}, ri)
		if err != nil {
			return err
		}

		_, err = b.collClients[clientIndex].ListItem(ex.Args{}, ri)

		return err
	}, clientIndex, ri.IPath)
}

// doWithTimeoutAndRetries does op, but times it out. On timeout or error,
// retries a few times with backoff, getting a new baton client for each try.
func (b *Baton) doWithTimeoutAndRetries(op retry.Operation, clientIndex int, path string) error {
	attempt := 0
	opName := "MkDir"

	wrapped := func() error {
		attempt++
		b.info("baton retry attempt", "op", opName, "path", path, "attempt", attempt, "client_index", clientIndex)

		err := b.timeoutOpAndMakeNewClientOnError(op, clientIndex, path)()
		if err != nil {
			b.logIRODSError(err, "op", opName, "path", path, "attempt", attempt, "client_index", clientIndex)

			if errors.Is(err, errs.PathError{Msg: ErrOperationTimeout, Path: path}) {
				b.warn(
					"baton operation timed out (may still be running in background)",
					"op", opName,
					"path", path,
					"attempt", attempt,
				)
			}

			return err
		}

		b.info("baton retry attempt succeeded", "op", opName, "path", path, "attempt", attempt, "client_index", clientIndex)

		return nil
	}

	status := retry.Do(
		context.Background(),
		wrapped,
		retry.Untils{&retry.UntilLimit{Max: operationRetries}, &retry.UntilNoError{}},
		&backoff.Backoff{
			Min:    operationMinBackoff,
			Max:    operationMaxBackoff,
			Factor: operationBackoffFactor,
			Sleeper: &loggingSleeper{
				logger: b.logger,
				ctx:    []any{"op", opName, "path", path, "client_index", clientIndex},
				base:   &btime.Sleeper{},
			},
		},
		opName,
	)

	return status.Err
}

// timeoutOpAndMakeNewClientOnError wraps the given op with a timeout, and
// makes a new client on timeout or error.
func (b *Baton) timeoutOpAndMakeNewClientOnError(op retry.Operation, clientIndex int, path string) retry.Operation {
	return func() error {
		err := timeoutOp(op, path)
		if err != nil {
			b.warn("baton op failed; replacing client", "path", path, "client_index", clientIndex, "err", err)
			b.logIRODSError(err, "path", path, "client_index", clientIndex)

			if errors.Is(err, errs.PathError{Msg: ErrOperationTimeout, Path: path}) {
				b.warn("baton op timed out (may still complete later)", "path", path, "client_index", clientIndex)
			}

			pool := ex.NewClientPool(ex.DefaultClientPoolParams, "")

			client, errp := pool.Get()
			if errp == nil {
				go func(oldClient *ex.Client) {
					timeoutOp(func() error { //nolint:errcheck
						oldClient.StopIgnoreError()

						return nil
					}, "")
				}(b.getClientByIndex(clientIndex))

				b.setClientByIndex(clientIndex, client)
				b.info("baton client replaced", "path", path, "client_index", clientIndex)
				pool.Close()
			} else {
				b.warn("baton failed to create replacement client", "path", path, "client_index", clientIndex, "err", errp)
			}
		}

		return err
	}
}

func (b *Baton) getClientByIndex(clientIndex int) *ex.Client {
	switch clientIndex {
	case putClientIndex:
		return b.putClient
	case metaClientIndex:
		return b.metaClient
	default:
		return b.collClients[clientIndex]
	}
}

func (b *Baton) setClientByIndex(clientIndex int, client *ex.Client) {
	switch clientIndex {
	case putClientIndex:
		b.putClient = client
	case metaClientIndex:
		b.metaClient = client
	default:
		b.collClients[clientIndex] = client
	}
}

// CollectionsDone closes the connections used for connection creation.
func (b *Baton) CollectionsDone() error {
	b.collMu.Lock()
	defer b.collMu.Unlock()

	b.closeConnections(b.collClients)
	b.collClients = nil
	b.collPool.Close()

	close(b.collCh)
	close(b.collErrCh)
	b.collRunning = false

	err := b.setClientIfNotExists(&b.putClient)
	if err != nil {
		return err
	}

	return b.setClientIfNotExists(&b.metaClient)
}

func (b *Baton) setClientIfNotExists(client **ex.Client) error {
	if *client != nil && (*client).IsRunning() {
		return nil
	}

	b.debug("baton creating new client")

	newClient, err := b.getNewClient()
	if err != nil {
		return err
	}

	*client = newClient

	return nil
}

func (b *Baton) getNewClient() (*ex.Client, error) {
	pool, clientCh, err := b.connect(1)
	if err != nil {
		return nil, err
	}

	client := <-clientCh

	pool.Close()

	return client, err
}

// closeConnections closes the given connections, with a timeout, ignoring
// errors.
func (b *Baton) closeConnections(clients []*ex.Client) {
	for _, client := range clients {
		if client == nil {
			continue
		}

		timeoutOp(func() error { //nolint:errcheck
			client.StopIgnoreError()

			return nil
		}, "close error")
	}
}

// Stat gets mtime and metadata info for the request Remote object. It creates a
// new meta client if necessary so after calling this function you must
// eventually call Cleanup().
func (b *Baton) Stat(remote string) (exists bool, m map[string]string, err error) {
	finish := logger.StartOperation(b.logger, "baton stat", 0, "remote", remote)

	defer func() { finish(err) }()

	err = b.setClientIfNotExists(&b.metaClient)
	if err != nil {
		return false, nil, err
	}

	it, exists, err := b.listItemForStat(remote)
	if err != nil {
		b.logIRODSError(err, "op", "stat", "remote", remote)

		return false, nil, err
	}

	if !exists {
		return false, nil, nil
	}

	b.debugStatDetails(remote, it)

	return true, RodsItemToMeta(it), nil
}

// RodsItemToMeta pulls out the AVUs from a RodsItem and returns them as a map.
func RodsItemToMeta(it ex.RodsItem) map[string]string {
	m := make(map[string]string, len(it.IAVUs))

	for _, iavu := range it.IAVUs {
		m[iavu.Attr] = iavu.Value
	}

	m[meta.MetaKeyRemoteSize] = strconv.FormatUint(it.ISize, 10)

	var created, modified time.Time

	for _, ts := range it.ITimestamps {
		if ts.Created.After(created) {
			created = ts.Created
		}

		if ts.Modified.After(modified) {
			modified = ts.Modified
		}
	}

	m[meta.MetaKeyRemoteMtime], _ = internal.TimeToMeta(modified) //nolint:errcheck
	m[meta.MetaKeyRemoteCtime], _ = internal.TimeToMeta(created)  //nolint:errcheck

	return m
}

func (b *Baton) statArgsForTracing() ex.Args {
	args := ex.Args{Timestamp: true, AVU: true}
	if b.logger != nil {
		// Only request heavyweight details (checksum/replicates) when deep tracing
		// is enabled.
		args.Checksum = true
		args.Replicate = true
		args.Size = true
	}

	return args
}

func (b *Baton) listItemForStat(remote string) (it ex.RodsItem, exists bool, err error) {
	err = timeoutOp(func() error {
		var errl error

		it, errl = b.metaClient.ListItem(b.statArgsForTracing(), *requestToRodsItem("", remote))

		return errl
	}, "stat failed: "+remote)
	if err != nil {
		if strings.Contains(err.Error(), extendoNotExist) {
			return ex.RodsItem{}, false, nil
		}

		return ex.RodsItem{}, false, err
	}

	return it, true, nil
}

func (b *Baton) debugStatDetails(remote string, it ex.RodsItem) {
	if b.logger == nil {
		return
	}

	b.debug(
		"baton stat details",
		"remote", remote,
		"checksum", it.IChecksum,
		"replicas", len(it.IReplicates),
		"replicate_details", b.replicateSummaries(it.IReplicates),
	)
}

// Put uploads request Local to the Remote object, overwriting it if it already
// exists. It calculates and stores the md5 checksum remotely, comparing to the
// local checksum. It creates a new put client if necessary so after calling
// this function you must eventually call Cleanup().
func (b *Baton) Put(local, remote string) (err error) {
	finish := logger.StartOperation(b.logger, "baton put", putStillRunningLogFreq, "local", local, "remote", remote)

	defer func() { finish(err) }()

	err = b.setClientIfNotExists(&b.putClient)
	if err != nil {
		return err
	}

	item, cleanup, err := requestToRodsItemForPut(local, remote)
	if err != nil {
		return err
	}
	defer cleanup()

	items, err := b.putClient.Put(b.putArgsForTracing(), *item)
	if err != nil {
		b.logIRODSError(err, "op", "put", "local", local, "remote", remote)

		return err
	}

	// Log the returned details without attempting to add per-request ids here;
	// higher-level callers can correlate using their own ctx.
	b.debugPutItems(local, remote, items)

	return nil
}

func requestToRodsItemForPut(local, remote string) (*ex.RodsItem, func(), error) {
	item := requestToRodsItem(local, remote)
	cleanup := func() {}

	// iRODS treats /dev/null specially, so unless that changes we have to check
	// for it and create a temporary empty file in its place.
	if filepath.Join(item.IDirectory, item.IFile) != os.DevNull {
		return item, cleanup, nil
	}

	fileName, err := getTempFile()
	if err != nil {
		return nil, cleanup, err
	}

	item.IDirectory = filepath.Dir(fileName)
	item.IFile = filepath.Base(fileName)
	cleanup = func() { os.Remove(fileName) }

	return item, cleanup, nil
}

func (b *Baton) putArgsForTracing() ex.Args {
	args := ex.Args{Force: true, Verify: true}
	if b.logger != nil {
		args.Checksum = true
		args.Replicate = true
		args.Size = true
		args.Timestamp = true
	}

	return args
}

func (b *Baton) debugPutItems(local, remote string, items []ex.RodsItem) {
	if b.logger == nil {
		return
	}

	if len(items) == 0 {
		b.warn("baton put returned no items", "local", local, "remote", remote)

		return
	}

	it := items[0]
	b.info(
		"baton put result",
		"local", local,
		"remote", remote,
		"size", it.ISize,
		"checksum", it.IChecksum,
		"replicas", len(it.IReplicates),
	)
	b.debug(
		"baton put result details",
		"local", local,
		"remote", remote,
		"replicate_details", b.replicateSummaries(it.IReplicates),
	)
}

func (b *Baton) Get(local, remote string) (err error) {
	finish := logger.StartOperation(b.logger, "baton get", getStillRunningLogFreq, "local", local, "remote", remote)

	defer func() { finish(err) }()

	err = b.setClientIfNotExists(&b.putClient)
	if err != nil {
		return err
	}

	localDir, localFile := filepath.Split(local)
	tmpLocal := filepath.Join(localDir, fmt.Sprintf(".ibackup.get.%X", sha256.Sum256([]byte(localFile))))

	_, err = b.putClient.Get(
		ex.Args{
			Force:  true,
			Verify: true,
			Save:   true,
		},
		*requestToRodsItem(tmpLocal, remote),
	)
	if err != nil {
		b.logIRODSError(err, "op", "get", "local", local, "remote", remote)
		os.Remove(tmpLocal)

		return err
	}

	return os.Rename(tmpLocal, local)
}

// RemoveMeta removes the given metadata from a given object in iRODS. It
// creates a new meta client if necessary so after calling this function you
// must eventually call Cleanup().
func (b *Baton) RemoveMeta(path string, meta map[string]string) (err error) {
	return b.metaChange(
		"baton remove meta",
		"remove meta error: ",
		path,
		meta,
		func(item *ex.RodsItem) error {
			_, errl := b.metaClient.MetaRem(ex.Args{}, *item)

			return errl
		},
	)
}

func (b *Baton) metaChange(
	opName string,
	errMsgPrefix string,
	path string,
	meta map[string]string,
	op func(item *ex.RodsItem) error,
) (err error) {
	finish := logger.StartOperation(b.logger, opName, 0, "path", path, "keys", len(meta))

	defer func() { finish(err) }()

	err = b.setClientIfNotExists(&b.metaClient)
	if err != nil {
		return err
	}

	it := RemotePathToRodsItem(path)

	it.IAVUs = metaToAVUs(meta)
	if b.logger != nil {
		b.debug("baton meta change", "op", opName, "path", path, "keys", len(meta))
	}

	return b.runMetaOp(opName, errMsgPrefix, path, meta, it, op)
}

// RemotePathToRodsItem converts a path in to an extendo RodsItem.
func RemotePathToRodsItem(path string) *ex.RodsItem {
	return &ex.RodsItem{
		IPath: filepath.Dir(path),
		IName: filepath.Base(path),
	}
}

func metaToAVUs(meta map[string]string) []ex.AVU {
	avus := make([]ex.AVU, len(meta))
	i := 0

	for k, v := range meta {
		avus[i] = ex.AVU{Attr: k, Value: v}
		i++
	}

	return avus
}

func (b *Baton) runMetaOp(
	opName string,
	errMsgPrefix string,
	path string,
	meta map[string]string,
	it *ex.RodsItem,
	op func(item *ex.RodsItem) error,
) error {
	err := timeoutOp(func() error {
		return op(it)
	}, errMsgPrefix+path)
	if err == nil {
		return nil
	}

	b.logIRODSError(err, "op", opName, "path", path, "keys", len(meta))

	if errors.Is(err, errs.PathError{Msg: ErrOperationTimeout, Path: path}) {
		b.warn("baton meta change timed out (may still complete later)", "op", opName, "path", path)
	}

	return err
}

// GetMeta gets all the metadata for the given object in iRODS. It creates a new
// meta client if necessary so after calling this function you must eventually
// call Cleanup().
func (b *Baton) GetMeta(path string) (meta map[string]string, err error) {
	finish := logger.StartOperation(b.logger, "baton get meta", getStillRunningLogFreq, "path", path)

	defer func() { finish(err, "keys", len(meta)) }()

	err = b.setClientIfNotExists(&b.metaClient)
	if err != nil {
		return nil, err
	}

	it, err := b.listMetaItem(path)

	if err != nil && strings.Contains(err.Error(), extendoNotExist) {
		return nil, errs.PathError{Msg: internal.ErrFileDoesNotExist, Path: path}
	}

	if err != nil {
		b.logIRODSError(err, "op", "get meta", "path", path)
	}

	b.debugGetMetaDetails(path, it, err)

	meta = RodsItemToMeta(it)

	return meta, err
}

func (b *Baton) metaListArgsForTracing() ex.Args {
	args := ex.Args{AVU: true, Timestamp: true, Size: true}
	if b.logger != nil {
		args.Checksum = true
		args.Replicate = true
	}

	return args
}

func (b *Baton) listMetaItem(path string) (ex.RodsItem, error) {
	var it ex.RodsItem

	err := timeoutOp(func() error {
		var errl error

		it, errl = b.metaClient.ListItem(b.metaListArgsForTracing(), ex.RodsItem{
			IPath: filepath.Dir(path),
			IName: filepath.Base(path),
		})

		return errl
	}, "get meta error: "+path)

	return it, err
}

func (b *Baton) debugGetMetaDetails(path string, it ex.RodsItem, err error) {
	if b.logger == nil || err != nil {
		return
	}

	b.debug(
		"baton get meta details",
		"path", path,
		"checksum", it.IChecksum,
		"replicas", len(it.IReplicates),
		"replicate_details", b.replicateSummaries(it.IReplicates),
	)
}

// AddMeta adds the given metadata to a given object in iRODS. It
// creates a new meta client if necessary so after calling this function you
// must eventually call Cleanup().
func (b *Baton) AddMeta(path string, meta map[string]string) (err error) {
	return b.metaChange(
		"baton add meta",
		"add meta error: ",
		path,
		meta,
		func(item *ex.RodsItem) error {
			_, errl := b.metaClient.MetaAdd(ex.Args{}, *item)

			return errl
		},
	)
}

// Cleanup stops our clients and closes our client pool.
func (b *Baton) Cleanup() {
	b.closeConnections(append(b.collClients, b.putClient, b.removeClient, b.metaClient))

	b.collMu.Lock()
	defer b.collMu.Unlock()

	if b.collRunning {
		close(b.collCh)
		close(b.collErrCh)
		b.collPool.Close()
		b.collRunning = false
	}
}

// RemoveFile removes the given file from iRODS. It creates a new remove client
// if necessary so after calling this function you must eventually call
// Cleanup().
func (b *Baton) RemoveFile(path string) (err error) {
	finish := logger.StartOperation(b.logger, "baton remove file", 0, "path", path)

	defer func() { finish(err) }()

	err = b.setClientIfNotExists(&b.removeClient)
	if err != nil {
		return err
	}

	it := RemotePathToRodsItem(path)

	err = timeoutOp(func() error {
		_, errl := b.removeClient.RemObj(ex.Args{}, *it)

		return errl
	}, "remove file error: "+path)

	if err != nil && strings.Contains(err.Error(), "CAT_NO_ROWS_FOUND") {
		return errs.PathError{Msg: internal.ErrFileDoesNotExist, Path: path}
	}

	if err != nil {
		b.logIRODSError(err, "op", "remove file", "path", path)
	}

	return err
}

// RemoveDir removes the given directory from iRODS given it is empty. It
// creates a new remove client if necessary so after calling this function you
// must eventually call Cleanup().
func (b *Baton) RemoveDir(path string) (err error) {
	finish := logger.StartOperation(b.logger, "baton remove dir", 0, "path", path)

	defer func() { finish(err) }()

	err = b.setClientIfNotExists(&b.removeClient)
	if err != nil {
		return err
	}

	it := &ex.RodsItem{
		IPath: path,
	}

	err = timeoutOp(func() error {
		_, errl := b.removeClient.RemDir(ex.Args{}, *it)

		return errl
	}, "remove meta error: "+path)

	if err != nil && strings.Contains(err.Error(), "CAT_COLLECTION_NOT_EMPTY") {
		return errs.NewDirNotEmptyError(path)
	}

	if err != nil {
		b.logIRODSError(err, "op", "remove dir", "path", path)
	}

	return err
}

// AllClientsStopped returns true if all our clients are stopped.
func (b *Baton) AllClientsStopped() bool {
	for _, client := range append(b.collClients, b.putClient, b.metaClient, b.removeClient) {
		if client != nil && client.IsRunning() {
			return false
		}
	}

	return true
}

// QueryMeta return paths to all objects with given metadata inside the provided
// scope. It creates a new meta client if necessary so after calling this
// function you must eventually call Cleanup().
func (b *Baton) QueryMeta(dirToSearch string, meta map[string]string) (paths []string, err error) {
	finish := logger.StartOperation(b.logger, "baton query meta", 0, "scope", dirToSearch, "keys", len(meta))

	defer func() { finish(err, "results", len(paths)) }()

	err = b.setClientIfNotExists(&b.metaClient)
	if err != nil {
		return nil, err
	}

	it := &ex.RodsItem{
		IPath: dirToSearch,
		IAVUs: metaToAVUs(meta),
	}

	var items []ex.RodsItem

	err = timeoutOp(func() error {
		items, err = b.metaClient.MetaQuery(ex.Args{Object: true}, *it)

		return err
	}, "query meta error: "+dirToSearch)

	paths = make([]string, len(items))

	for i, item := range items {
		paths[i] = filepath.Join(item.IPath, item.IName)
	}

	return paths, err
}

func getTempFile() (string, error) {
	file, err := os.CreateTemp("", "ibackup-put-empty-*")
	if err != nil {
		return "", err
	}

	file.Close()

	return file.Name(), nil
}

// setupExtendoLogger sets up a STDERR logger that the extendo library will use.
// (We don't actually care about what it might log, but extendo doesn't work
// without this.)
func setupExtendoLogger() {
	logs.InstallLogger(zlog.New(zerolog.SyncWriter(os.Stderr), extendoLogLevel))
}
