/*******************************************************************************
 * Copyright (c) 2022, 2023 Genome Research Ltd.
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

// this file implements a Handler using baton, via extendo.

package baton

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/wtsi-hgi/ibackup/internal"
	ex "github.com/wtsi-npg/extendo/v2"
	logs "github.com/wtsi-npg/logshim"
	"github.com/wtsi-npg/logshim-zerolog/zlog"
	"github.com/wtsi-ssg/wr/backoff"
	btime "github.com/wtsi-ssg/wr/backoff/time"
	"github.com/wtsi-ssg/wr/retry"
)

const (
	ErrOperationTimeout = "iRODS operation timed out"

	extendoLogLevel           = logs.ErrorLevel
	workerPoolSizeCollections = 2
	numCollClients            = workerPoolSizeCollections
	numPutRemoveMetaClients   = 3
	collClientMaxIndex        = numCollClients - 1
	putClientIndex            = collClientMaxIndex + 1
	metaClientIndex           = putClientIndex + 1
	extendoNotExist           = "does not exist"
	operationMinBackoff       = 5 * time.Second
	operationMaxBackoff       = 30 * time.Second
	operationBackoffFactor    = 1.1
	operationTimeout          = 15 * time.Second
	operationRetries          = 6
)

// Baton is a Handler that uses Baton (via extendo) to interact with iRODS.
type Baton struct {
	collPool    *ex.ClientPool
	collClients []*ex.Client
	collRunning bool
	collCh      chan string
	collErrCh   chan error
	collMu      sync.Mutex
	//PutMetaPool  *ex.ClientPool
	putClient    *ex.Client
	metaClient   *ex.Client
	removeClient *ex.Client
}

// GetBatonHandler returns a Handler that uses Baton to interact with iRODS. If
// you don't have baton-do in your PATH, you'll get an error.
func GetBatonHandler() (*Baton, error) {
	setupExtendoLogger()

	_, err := ex.FindBaton()

	return &Baton{}, err
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
// This is safe for calling concurrently, and uses multiple connections. But an
// artefact is that the error you get might be for a different
// EnsureCollection() call you made for a different collection. This shouldn't
// make much difference if you just collect all your errors and don't care about
// order.
func (b *Baton) EnsureCollection(collection string) error {
	b.collMu.Lock()

	if !b.collRunning {
		if err := b.makeCollConnections(); err != nil {
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
		b.collMu.Unlock()

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

	for i := 0; i < int(numClients); i++ {
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
	err := TimeoutOp(func() error {
		_, errl := b.collClients[clientIndex].ListItem(ex.Args{}, ri)

		return errl
	}, "collection failed: "+ri.IPath)
	if err == nil {
		return nil
	}

	if errors.Is(err, internal.Error{ErrOperationTimeout, ri.IPath}) {
		return err
	}

	return b.createCollectionWithTimeoutAndRetries(clientIndex, ri)
}

// TimeoutOp carries out op, returning any error from it. Has a 10s timeout on
// running op, and will return a timeout error instead if exceeded.
func TimeoutOp(op retry.Operation, path string) error {
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
		err = internal.Error{ErrOperationTimeout, path}
	}

	return err
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
	status := retry.Do(
		context.Background(),
		b.timeoutOpAndMakeNewClientOnError(op, clientIndex, path),
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
// makes a new client on timeout or error.
func (b *Baton) timeoutOpAndMakeNewClientOnError(op retry.Operation, clientIndex int, path string) retry.Operation {
	return func() error {
		err := TimeoutOp(op, path)
		if err != nil {
			pool := ex.NewClientPool(ex.DefaultClientPoolParams, "")

			client, errp := pool.Get()
			if errp == nil {
				go func(oldClient *ex.Client) {
					TimeoutOp(func() error { //nolint:errcheck
						oldClient.StopIgnoreError()

						return nil
					}, "")
				}(b.getClientByIndex(clientIndex))

				b.setClientByIndex(clientIndex, client)
				pool.Close()
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

//TODO

// CollectionsDone closes the connections used for connection creation, and
// creates new ones for doing puts and metadata operations.
func (b *Baton) CollectionsDone() error {
	b.collMu.Lock()
	defer b.collMu.Unlock()

	b.closeConnections(b.collClients)
	b.collClients = nil
	b.collPool.Close()

	close(b.collCh)
	close(b.collErrCh)
	b.collRunning = false

	return nil

	// return b.createPutRemoveMetaClients()
}

func (b *Baton) createPutRemoveMetaClients() error {
	pool, clientCh, err := b.connect(numPutRemoveMetaClients)
	if err != nil {
		b.collMu.Unlock()

		return err
	}

	b.putClient = <-clientCh
	b.metaClient = <-clientCh
	b.removeClient = <-clientCh

	pool.Close()

	return nil
}

// InitClients sets three clients if they do not currently exist. Returns error
// if some exist and some do not.
func (b *Baton) InitClients() error {
	if b.putClient == nil && b.metaClient == nil && b.removeClient == nil {
		return b.createPutRemoveMetaClients()
	}

	if b.putClient != nil && b.metaClient != nil && b.removeClient != nil {
		if !b.putClient.IsRunning() && !b.metaClient.IsRunning() && !b.removeClient.IsRunning() {
			return b.createPutRemoveMetaClients()
		}

		return nil
	}

	return internal.Error{"Clients are not in the same state", ""}
}

// CloseClients closes remove, put and meta clients.
func (b *Baton) CloseClients() {
	var openClients []*ex.Client

	for _, client := range []*ex.Client{b.removeClient, b.putClient, b.metaClient} {
		if client != nil {
			openClients = append(openClients, client)
		}
	}

	b.closeConnections(openClients)
}

// closeConnections closes the given connections, with a timeout, ignoring
// errors.
func (b *Baton) closeConnections(clients []*ex.Client) {
	for _, client := range clients {
		TimeoutOp(func() error { //nolint:errcheck
			client.StopIgnoreError()

			return nil
		}, "close error")
	}
}

// Stat gets mtime and metadata info for the request Remote object.
func (b *Baton) Stat(remote string) (bool, map[string]string, error) {
	var it ex.RodsItem

	err := TimeoutOp(func() error {
		var errl error
		it, errl = b.metaClient.ListItem(ex.Args{Timestamp: true, AVU: true}, *requestToRodsItem("", remote))

		return errl
	}, "stat failed: "+remote)

	if err != nil {
		if strings.Contains(err.Error(), extendoNotExist) {
			return false, map[string]string{}, nil
		}

		return false, nil, err
	}

	return true, RodsItemToMeta(it), nil
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
	meta := make(map[string]string, len(it.IAVUs))

	for _, iavu := range it.IAVUs {
		meta[iavu.Attr] = iavu.Value
	}

	return meta
}

// Put uploads request Local to the Remote object, overwriting it if it already
// exists. It calculates and stores the md5 checksum remotely, comparing to the
// local checksum.
func (b *Baton) Put(local, remote string) error {
	item := requestToRodsItem(local, remote)

	// iRODS treats /dev/null specially, so unless that changes we have to check
	// for it and create a temporary empty file in its place.
	if filepath.Join(item.IDirectory, item.IFile) == os.DevNull {
		file, err := os.CreateTemp("", "ibackup-put-empty-*")
		if err != nil {
			return err
		}

		file.Close()

		name := file.Name()

		item.IDirectory = filepath.Dir(name)
		item.IFile = filepath.Base(name)

		defer os.Remove(name)
	}

	_, err := b.putClient.Put(
		ex.Args{
			Force:  true,
			Verify: true,
		},
		*item,
	)

	return err
}

func MetaToAVUs(meta map[string]string) []ex.AVU {
	avus := make([]ex.AVU, len(meta))
	i := 0

	for k, v := range meta {
		avus[i] = ex.AVU{Attr: k, Value: v}
		i++
	}

	return avus
}

// RemoveMeta removes the given metadata from a given object in iRODS.
func (b *Baton) RemoveMeta(path string, meta map[string]string) error {
	it := RemotePathToRodsItem(path)
	it.IAVUs = MetaToAVUs(meta)

	err := TimeoutOp(func() error {
		_, errl := b.metaClient.MetaRem(ex.Args{}, *it)

		return errl
	}, "remove meta error: "+path)

	return err
}

// GetMeta gets all the metadata for the given object in iRODS.
func (b *Baton) GetMeta(path string) (map[string]string, error) {
	it, err := b.metaClient.ListItem(ex.Args{AVU: true, Timestamp: true, Size: true}, ex.RodsItem{
		IPath: filepath.Dir(path),
		IName: filepath.Base(path),
	})

	return RodsItemToMeta(it), err
}

// RemotePathToRodsItem converts a path in to an extendo RodsItem.
func RemotePathToRodsItem(path string) *ex.RodsItem {
	return &ex.RodsItem{
		IPath: filepath.Dir(path),
		IName: filepath.Base(path),
	}
}

// AddMeta adds the given metadata to a given object in iRODS.
func (b *Baton) AddMeta(path string, meta map[string]string) error {
	it := RemotePathToRodsItem(path)
	it.IAVUs = MetaToAVUs(meta)

	err := TimeoutOp(func() error {
		_, errl := b.metaClient.MetaAdd(ex.Args{}, *it)

		return errl
	}, "add meta error: "+path)

	return err
}

// Cleanup stops our clients and closes our client pool.
func (b *Baton) Cleanup() error {
	b.closeConnections(append(b.collClients, b.putClient, b.removeClient, b.metaClient))

	// b.PutMetaPool.Close()

	b.collMu.Lock()
	defer b.collMu.Unlock()

	if b.collRunning {
		close(b.collCh)
		close(b.collErrCh)
		b.collPool.Close()
		b.collRunning = false
	}

	return nil
}

func (b *Baton) RemoveFile(path string) error {
	it := RemotePathToRodsItem(path)

	err := TimeoutOp(func() error {
		_, errl := b.removeClient.RemObj(ex.Args{}, *it)

		return errl
	}, "remove file error: "+path)

	return err
}

// RemoveDir removes the given directory from iRODS given it is empty.
func (b *Baton) RemoveDir(path string) error {
	it := &ex.RodsItem{
		IPath: path,
	}

	err := TimeoutOp(func() error {
		_, errl := b.removeClient.RemDir(ex.Args{}, *it)

		return errl
	}, "remove meta error: "+path)

	return err
}

// QueryMeta return paths to all objects with given metadata inside the provided
// scope.
func (b *Baton) QueryMeta(dirToSearch string, meta map[string]string) ([]string, error) {
	it := &ex.RodsItem{
		IPath: dirToSearch,
		IAVUs: MetaToAVUs(meta),
	}

	var items []ex.RodsItem

	var err error

	err = TimeoutOp(func() error {
		items, err = b.metaClient.MetaQuery(ex.Args{Object: true}, *it)

		return err
	}, "query meta error: "+dirToSearch)

	paths := make([]string, len(items))

	for i, item := range items {
		paths[i] = filepath.Join(item.IPath, item.IName)
	}

	return paths, err
}

func (b *Baton) AllClientsStopped() bool {
	return !b.putClient.IsRunning() && !b.metaClient.IsRunning() && b.collClients == nil
}
