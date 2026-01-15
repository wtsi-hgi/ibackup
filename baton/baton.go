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
)

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

// timeoutOp carries out op, returning any error from it. Has a 10s timeout on
// running op, and will return a timeout error instead if exceeded.
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
		err := timeoutOp(op, path)
		if err != nil {
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
func (b *Baton) Stat(remote string) (bool, map[string]string, error) {
	err := b.setClientIfNotExists(&b.metaClient)
	if err != nil {
		return false, nil, err
	}

	var it ex.RodsItem

	err = timeoutOp(func() error {
		var errl error

		it, errl = b.metaClient.ListItem(ex.Args{Timestamp: true, AVU: true}, *requestToRodsItem("", remote))

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

		it, errl = b.metaClient.ListItem(
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

	_, err = b.putClient.Put(
		ex.Args{
			Force:  true,
			Verify: true,
		},
		*item,
	)

	return err
}

func (b *Baton) Get(local, remote string) error {
	err := b.setClientIfNotExists(&b.putClient)
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
		_, errl := b.metaClient.MetaRem(ex.Args{}, *it)

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

	it, err := b.metaClient.ListItem(ex.Args{AVU: true, Timestamp: true, Size: true}, ex.RodsItem{
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
		_, errl := b.metaClient.MetaAdd(ex.Args{}, *it)

		return errl
	}, "add meta error: "+path)

	return err
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
func (b *Baton) RemoveFile(path string) error {
	err := b.setClientIfNotExists(&b.removeClient)
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
		_, errl := b.removeClient.RemDir(ex.Args{}, *it)

		return errl
	}, "remove meta error: "+path)

	if err != nil && strings.Contains(err.Error(), "CAT_COLLECTION_NOT_EMPTY") {
		return errs.NewDirNotEmptyError(path)
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
		items, err = b.metaClient.MetaQuery(ex.Args{Object: true}, *it)

		return err
	}, "query meta error: "+dirToSearch)

	paths := make([]string, len(items))

	for i, item := range items {
		paths[i] = filepath.Join(item.IPath, item.IName)
	}

	return paths, err
}
