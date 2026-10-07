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
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"github.com/wtsi-hgi/ibackup/baton/meta"
	"github.com/wtsi-hgi/ibackup/errs"
	"github.com/wtsi-hgi/ibackup/internal"
	ex "github.com/wtsi-npg/extendo/v3"
	logs "github.com/wtsi-npg/logshim"
	"github.com/wtsi-npg/logshim-zerolog/zlog"
	"github.com/wtsi-ssg/wr/backoff"
	btime "github.com/wtsi-ssg/wr/backoff/time"
	"github.com/wtsi-ssg/wr/retry"
)

const (
	ErrOperationTimeout   = "iRODS operation timed out"
	ErrCollectionsStopped = "collection creation was stopped"

	extendoLogLevel        = logs.ErrorLevel
	numCollClients         = 2
	extendoNotExist        = "does not exist"
	operationMinBackoff    = 5 * time.Second
	operationMaxBackoff    = 30 * time.Second
	operationBackoffFactor = 1.1
	operationTimeout       = 60 * time.Second
	operationRetries       = 6

	errSysCopyLen = -27000
)

// collRequest asks for a collection to be created, with the result sent on
// reply, which must have a buffer of 1.
type collRequest struct {
	collection string
	reply      chan<- error
}

// Baton is a Handler that uses Baton (via extendo) to interact with iRODS.
type Baton struct {
	collPool     *ex.ClientPool
	collClients  []*ex.Client
	collRunning  bool
	collCh       chan collRequest
	collDone     <-chan struct{}
	collStop     context.CancelFunc
	collMu       sync.Mutex
	clientMu     sync.Mutex
	putClient    atomic.Pointer[ex.Client]
	metaClient   atomic.Pointer[ex.Client]
	removeClient atomic.Pointer[ex.Client]
}

// GetBatonHandler returns a Handler that uses Baton to interact with iRODS. If
// you don't have baton-do in your PATH, you'll get an error.
func GetBatonHandler() (*Baton, error) {
	setupExtendoLogger()

	_, err := ex.FindBaton()

	return &Baton{}, err
}

// getClients returns a snapshot of all our clients, any of which may be nil.
func (b *Baton) getClients() []*ex.Client {
	return append(b.getCollClients(), b.putClient.Load(), b.removeClient.Load(), b.metaClient.Load())
}

func (b *Baton) getCollClients() []*ex.Client {
	b.collMu.Lock()
	defer b.collMu.Unlock()

	return slices.Clone(b.collClients)
}

// createCollections creates collections requested on collCh using the
// collection client at the given index, sending each result to its request's
// reply channel, until ctx is cancelled.
func (b *Baton) createCollections(ctx context.Context, index int, collCh <-chan collRequest) {
	for {
		select {
		case req := <-collCh:
			req.reply <- b.ensureCollection(ctx, index, ex.RodsItem{IPath: req.collection})
		case <-ctx.Done():
			return
		}
	}
}

// getCollClient returns the collection client at the given index. collMu
// guards it, since a failed operation can replace it while Cleanup() or
// AllClientsStopped() read it. Returns ctx's error instead once collection
// creation has been stopped, since CollectionsDone() discards the clients.
func (b *Baton) getCollClient(ctx context.Context, clientIndex int) (*ex.Client, error) {
	b.collMu.Lock()
	defer b.collMu.Unlock()

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	return b.collClients[clientIndex], nil
}

// swapCollClient stores the given client at the given collection client index,
// returning the client it replaced, which should be stopped. If ctx has been
// cancelled, it stores nothing and returns the given client instead.
func (b *Baton) swapCollClient(ctx context.Context, clientIndex int, client *ex.Client) *ex.Client {
	b.collMu.Lock()
	defer b.collMu.Unlock()

	if ctx.Err() != nil {
		return client
	}

	old := b.collClients[clientIndex]
	b.collClients[clientIndex] = client

	return old
}

// setupExtendoLogger sets up a STDERR logger that the extendo library will use.
// (We don't actually care about what it might log, but extendo doesn't work
// without this.)
func setupExtendoLogger() {
	logs.InstallLogger(zlog.New(zerolog.SyncWriter(os.Stderr), extendoLogLevel))
}

// EnsureCollection ensures the given collection exists in iRODS, creating it if
// necessary. You must call Connect() before calling this.
//
// This is safe for calling concurrently, and uses multiple connections. Each
// call returns the result for its own collection.
//
// Calls still waiting when Cleanup() or CollectionsDone() is called return an
// ErrCollectionsStopped error.
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

	collCh, done := b.collCh, b.collDone
	b.collMu.Unlock()

	stopped := errs.PathError{Msg: ErrCollectionsStopped, Path: collection}
	reply := make(chan error, 1)

	select {
	case collCh <- collRequest{collection: collection, reply: reply}:
	case <-done:
		return stopped
	}

	select {
	case err := <-reply:
		return err
	case <-done:
		return stopped
	}
}

// startCreatingCollections creates b.collCh and starts a goroutine per
// collection client that creates any collection sent to that channel, until
// b.collStop() is called.
func (b *Baton) startCreatingCollections() {
	b.collCh = make(chan collRequest)

	ctx, stop := context.WithCancel(context.Background())
	b.collDone = ctx.Done()
	b.collStop = stop

	for index := range b.collClients {
		go b.createCollections(ctx, index, b.collCh)
	}
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

func (b *Baton) ensureCollection(ctx context.Context, clientIndex int, ri ex.RodsItem) error {
	err := timeoutOp(func() error {
		client, errg := b.getCollClient(ctx, clientIndex)
		if errg != nil {
			return errg
		}

		_, errl := client.ListItem(ex.Args{}, ri)

		return errl
	}, "collection failed: "+ri.IPath)
	if err == nil {
		return nil
	}

	if errors.Is(err, errs.PathError{Msg: ErrOperationTimeout, Path: ri.IPath}) {
		return err
	}

	return b.createCollectionWithTimeoutAndRetries(ctx, clientIndex, ri)
}

// timeoutOp carries out op, returning any error from it. Has an
// operationTimeout timeout on running op, and will return a timeout error
// instead if exceeded.
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

// createCollectionWithTimeoutAndRetries tries to make and confirm the given
// collection, retrying with a backoff because it can fail for no good reason,
// then work later.
func (b *Baton) createCollectionWithTimeoutAndRetries(ctx context.Context, clientIndex int, ri ex.RodsItem) error {
	return b.doWithTimeoutAndRetries(ctx, func() error {
		client, err := b.getCollClient(ctx, clientIndex)
		if err != nil {
			return err
		}

		_, err = client.MkDir(ex.Args{Recurse: true}, ri)
		if err != nil {
			return err
		}

		_, err = client.ListItem(ex.Args{}, ri)

		return err
	}, clientIndex, ri.IPath)
}

// doWithTimeoutAndRetries does op, but times it out. On timeout or error,
// retries a few times with backoff, getting a new baton client for each try,
// until ctx is cancelled.
func (b *Baton) doWithTimeoutAndRetries(ctx context.Context, op retry.Operation, clientIndex int, path string) error {
	status := retry.Do(
		ctx,
		b.timeoutOpAndMakeNewClientOnError(ctx, op, clientIndex, path),
		retry.Untils{&retry.UntilLimit{Max: operationRetries}, &retry.UntilNoError{}},
		&backoff.Backoff{
			Min:     operationMinBackoff,
			Max:     operationMaxBackoff,
			Factor:  operationBackoffFactor,
			Sleeper: &btime.Sleeper{},
		},
		"MkDir",
	)

	return status.Err
}

// timeoutOpAndMakeNewClientOnError wraps the given op with a timeout, and
// makes a new collection client on timeout or error.
func (b *Baton) timeoutOpAndMakeNewClientOnError(
	ctx context.Context, op retry.Operation, clientIndex int, path string,
) retry.Operation {
	return func() error {
		err := timeoutOp(op, path)
		if err != nil && ctx.Err() == nil {
			pool := ex.NewClientPool(ex.DefaultClientPoolParams, "")

			client, errp := pool.Get()
			if errp == nil {
				go func(oldClient *ex.Client) {
					timeoutOp(func() error { //nolint:errcheck
						oldClient.StopIgnoreError()

						return nil
					}, "")
				}(b.swapCollClient(ctx, clientIndex, client))

				pool.Close()
			}
		}

		return err
	}
}

// CollectionsDone closes the connections used for connection creation.
func (b *Baton) CollectionsDone() error {
	b.collMu.Lock()
	defer b.collMu.Unlock()

	b.closeConnections(b.collClients)
	b.collClients = nil
	b.collPool.Close()

	b.collStop()
	b.collRunning = false

	err := b.setClientIfNotExists(&b.putClient)
	if err != nil {
		return err
	}

	return b.setClientIfNotExists(&b.metaClient)
}

// setClientIfNotExists stores a new client in the given pointer if it doesn't
// already hold a running one. clientMu serialises creation; the pointer is
// atomic so Cleanup() can read it without waiting on a creation in progress.
func (b *Baton) setClientIfNotExists(client *atomic.Pointer[ex.Client]) error {
	b.clientMu.Lock()
	defer b.clientMu.Unlock()

	if c := client.Load(); c != nil && c.IsRunning() {
		return nil
	}

	newClient, err := b.getNewClient()
	if err != nil {
		return err
	}

	client.Store(newClient)

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
func (b *Baton) Stat(remote string) (bool, map[string]string, error) {
	err := b.setClientIfNotExists(&b.metaClient)
	if err != nil {
		return false, nil, err
	}

	var it ex.RodsItem

	err = timeoutOp(func() error {
		var errl error

		it, errl = b.metaClient.Load().ListItem(ex.Args{Timestamp: true, AVU: true}, *requestToRodsItem("", remote))

		return errl
	}, "stat failed: "+remote)

	if err != nil {
		if strings.Contains(err.Error(), extendoNotExist) {
			return false, nil, nil
		}

		return false, nil, err
	}

	return true, RodsItemToMeta(it), nil
}

// ReplicaCounts returns best-effort counts of replicas for the given remote
// data object.
//
// It is intended for low-volume logging around uploads, and so it returns
// (exists=false, err=nil) if the object does not exist.
func (b *Baton) ReplicaCounts(remote string) (bool, int, int, error) {
	it, exists, err := b.listItemWithReplicates(remote)
	if err != nil || !exists {
		return exists, 0, 0, err
	}

	if len(it.IReplicates) == 0 {
		return true, 0, 0, errs.PathError{Msg: "no replicate information returned", Path: remote}
	}

	good, bad := 0, 0

	for _, r := range it.IReplicates {
		if r.Valid {
			good++
		} else {
			bad++
		}
	}

	return true, good, bad, nil
}

func (b *Baton) listItemWithReplicates(remote string) (ex.RodsItem, bool, error) {
	if err := b.setClientIfNotExists(&b.metaClient); err != nil {
		return ex.RodsItem{}, false, err
	}

	var it ex.RodsItem

	err := timeoutOp(func() error {
		var errl error

		it, errl = b.metaClient.Load().ListItem(
			ex.Args{Replicate: true, Checksum: true},
			*requestToRodsItem("", remote),
		)

		return errl
	}, "replica number failed: "+remote)
	if err != nil {
		if strings.Contains(err.Error(), extendoNotExist) {
			return ex.RodsItem{}, false, nil
		}

		return ex.RodsItem{}, false, err
	}

	return it, true, nil
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

// Put uploads request Local to the Remote object, overwriting it if it already
// exists. It calculates and stores the md5 checksum remotely, comparing to the
// local checksum. It creates a new put client if necessary so after calling
// this function you must eventually call Cleanup().
//
// In the event of a SYS_COPY_LEN_ERR error, which might indicate an issue
// overwriting a file due to limited storage available, the remote file will be
// removed and the upload will be retried.
func (b *Baton) Put(local, remote string) error {
	err := b.setClientIfNotExists(&b.putClient)
	if err != nil {
		return err
	}

	item := requestToRodsItem(local, remote)

	// iRODS treats /dev/null specially, so unless that changes we have to check
	// for it and create a temporary empty file in its place.
	if filepath.Join(item.IDirectory, item.IFile) == os.DevNull {
		fileName, errt := getTempFile()
		if errt != nil {
			return errt
		}

		item.IDirectory = filepath.Dir(fileName)
		item.IFile = filepath.Base(fileName)

		defer os.Remove(fileName)
	}

	_, err = b.putClient.Load().Put(
		ex.Args{
			Force:  true,
			Verify: true,
		},
		*item,
	)
	if re, ok := errors.AsType[*ex.RodsError](err); ok && re.Code() == errSysCopyLen {
		err = b.removeAndRetry(item)
	}

	return err
}

func (b *Baton) removeAndRetry(item *ex.RodsItem) error {
	if err := timeoutOp(func() error {
		_, err := b.putClient.Load().RemObj(ex.Args{}, *item)

		return err
	}, path.Join(item.IDirectory, item.IFile)); err != nil {
		return err
	}

	_, err := b.putClient.Load().Put(ex.Args{Force: true, Verify: true}, *item)

	return err
}

func (b *Baton) Get(local, remote string) error {
	err := b.setClientIfNotExists(&b.putClient)
	if err != nil {
		return err
	}

	localDir, localFile := filepath.Split(local)
	tmpLocal := filepath.Join(localDir, fmt.Sprintf(".ibackup.get.%X", sha256.Sum256([]byte(localFile))))

	_, err = b.putClient.Load().Get(
		ex.Args{
			Force:  true,
			Verify: true,
			Save:   true,
		},
		*requestToRodsItem(tmpLocal, remote),
	)
	if err != nil {
		os.Remove(tmpLocal)

		return err
	}

	return os.Rename(tmpLocal, local)
}

func getTempFile() (string, error) {
	file, err := os.CreateTemp("", "ibackup-put-empty-*")
	if err != nil {
		return "", err
	}

	file.Close()

	return file.Name(), nil
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

// RemoveMeta removes the given metadata from a given object in iRODS. It
// creates a new meta client if necessary so after calling this function you
// must eventually call Cleanup().
func (b *Baton) RemoveMeta(path string, meta map[string]string) error {
	err := b.setClientIfNotExists(&b.metaClient)
	if err != nil {
		return err
	}

	it := RemotePathToRodsItem(path)
	it.IAVUs = metaToAVUs(meta)

	err = timeoutOp(func() error {
		_, errl := b.metaClient.Load().MetaRem(ex.Args{}, *it)

		return errl
	}, "remove meta error: "+path)

	return err
}

// GetMeta gets all the metadata for the given object in iRODS. It creates a new
// meta client if necessary so after calling this function you must eventually
// call Cleanup().
func (b *Baton) GetMeta(path string) (map[string]string, error) {
	err := b.setClientIfNotExists(&b.metaClient)
	if err != nil {
		return nil, err
	}

	it, err := b.metaClient.Load().ListItem(ex.Args{AVU: true, Timestamp: true, Size: true}, ex.RodsItem{
		IPath: filepath.Dir(path),
		IName: filepath.Base(path),
	})

	if err != nil && strings.Contains(err.Error(), extendoNotExist) {
		return nil, errs.PathError{Msg: internal.ErrFileDoesNotExist, Path: path}
	}

	return RodsItemToMeta(it), err
}

// RemotePathToRodsItem converts a path in to an extendo RodsItem.
func RemotePathToRodsItem(path string) *ex.RodsItem {
	return &ex.RodsItem{
		IPath: filepath.Dir(path),
		IName: filepath.Base(path),
	}
}

// AddMeta adds the given metadata to a given object in iRODS. It
// creates a new meta client if necessary so after calling this function you
// must eventually call Cleanup().
func (b *Baton) AddMeta(path string, meta map[string]string) error {
	err := b.setClientIfNotExists(&b.metaClient)
	if err != nil {
		return err
	}

	it := RemotePathToRodsItem(path)
	it.IAVUs = metaToAVUs(meta)

	err = timeoutOp(func() error {
		_, errl := b.metaClient.Load().MetaAdd(ex.Args{}, *it)

		return errl
	}, "add meta error: "+path)

	return err
}

// Cleanup stops our clients and closes our client pool. It is safe to call
// concurrently with methods that lazily create the put, meta and remove
// clients: a client created during the Cleanup is either stopped by it or left
// usable for a later Cleanup(). It is also safe to call concurrently with
// EnsureCollection(): calls still waiting return an ErrCollectionsStopped
// error, and later calls start collection creation again.
func (b *Baton) Cleanup() {
	b.closeConnections(b.getClients())

	b.collMu.Lock()
	defer b.collMu.Unlock()

	if b.collRunning {
		b.collStop()

		// Discard the stopped clients so the next EnsureCollection() makes
		// new ones, also stopping any client swapped in since our snapshot.
		b.closeConnections(b.collClients)
		b.collClients = nil
		b.collPool.Close()
		b.collRunning = false
	}
}

// RemoveFile removes the given file from iRODS. It creates a new remove client
// if necessary so after calling this function you must eventually call
// Cleanup().
func (b *Baton) RemoveFile(path string) error {
	err := b.setClientIfNotExists(&b.removeClient)
	if err != nil {
		return err
	}

	it := RemotePathToRodsItem(path)

	err = timeoutOp(func() error {
		_, errl := b.removeClient.Load().RemObj(ex.Args{}, *it)

		return errl
	}, "remove file error: "+path)

	if err != nil && strings.Contains(err.Error(), "CAT_NO_ROWS_FOUND") {
		return errs.PathError{Msg: internal.ErrFileDoesNotExist, Path: path}
	}

	return err
}

// RemoveDir removes the given directory from iRODS given it is empty. It
// creates a new remove client if necessary so after calling this function you
// must eventually call Cleanup().
func (b *Baton) RemoveDir(path string) error {
	err := b.setClientIfNotExists(&b.removeClient)
	if err != nil {
		return err
	}

	it := &ex.RodsItem{
		IPath: path,
	}

	err = timeoutOp(func() error {
		_, errl := b.removeClient.Load().RemDir(ex.Args{}, *it)

		return errl
	}, "remove meta error: "+path)

	if err != nil && strings.Contains(err.Error(), "CAT_COLLECTION_NOT_EMPTY") {
		return errs.NewDirNotEmptyError(path)
	}

	return err
}

// AllClientsStopped returns true if all our clients are stopped.
func (b *Baton) AllClientsStopped() bool {
	for _, client := range append(b.getCollClients(), b.putClient.Load(), b.metaClient.Load(), b.removeClient.Load()) {
		if client != nil && client.IsRunning() {
			return false
		}
	}

	return true
}

// QueryMeta return paths to all objects with given metadata inside the provided
// scope. It creates a new meta client if necessary so after calling this
// function you must eventually call Cleanup().
func (b *Baton) QueryMeta(dirToSearch string, meta map[string]string) ([]string, error) {
	err := b.setClientIfNotExists(&b.metaClient)
	if err != nil {
		return nil, err
	}

	it := &ex.RodsItem{
		IPath: dirToSearch,
		IAVUs: metaToAVUs(meta),
	}

	var items []ex.RodsItem

	err = timeoutOp(func() error {
		items, err = b.metaClient.Load().MetaQuery(ex.Args{Object: true}, *it)

		return err
	}, "query meta error: "+dirToSearch)

	paths := make([]string, len(items))

	for i, item := range items {
		paths[i] = filepath.Join(item.IPath, item.IName)
	}

	return paths, err
}
