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

// this file implements a Handler using baton, via extendo.

package put

import (
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/rs/zerolog"
	ex "github.com/wtsi-npg/extendo/v2"
	logs "github.com/wtsi-npg/logshim"
	"github.com/wtsi-npg/logshim-zerolog/zlog"
)

const (
	extendoLogLevel   = logs.ErrorLevel
	numExtendoClients = 2
	extendoNotExist   = "does not exist"
)

// Baton is a Handler that uses Baton (via extendo) to interact with iRODS.
type Baton struct {
	pool       *ex.ClientPool
	putClient  *ex.Client
	metaClient *ex.Client
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

// Connect creates a connection pool and prepares 2 connections to iRODS
// concurrently, for later use by other methods.
func (b *Baton) Connect() error {
	b.pool = ex.NewClientPool(ex.DefaultClientPoolParams, "")

	clientCh, err := b.getClientsFromPoolConcurrently()
	if err != nil {
		return err
	}

	b.putClient = <-clientCh
	b.metaClient = <-clientCh

	return nil
}

// getClientsFromPoolConcurrently gets 2 clients from our pool concurrently.
func (b *Baton) getClientsFromPoolConcurrently() (chan *ex.Client, error) {
	clientCh := make(chan *ex.Client, numExtendoClients)
	errCh := make(chan error, numExtendoClients)

	var wg sync.WaitGroup

	for i := 0; i < numExtendoClients; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			client, err := b.pool.Get()
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

// Cleanup stops our clients and closes our client pool.
func (b *Baton) Cleanup() error {
	for _, client := range []*ex.Client{b.putClient, b.metaClient} {
		if err := client.Stop(); err != nil {
			return err
		}
	}

	b.pool.Close()

	return nil
}

// EnsureCollection ensures the given collection exists in iRODS, creating it if
// necessary. You must call Connect() before calling this.
func (b *Baton) EnsureCollection(collection string) error {
	ri := ex.RodsItem{IPath: collection}

	if _, err := b.putClient.ListItem(ex.Args{}, ri); err == nil {
		return nil
	}

	if _, err := b.putClient.MkDir(ex.Args{Recurse: true}, ri); err != nil {
		return err
	}

	if _, err := b.putClient.ListItem(ex.Args{}, ri); err != nil {
		return err
	}

	return nil
}

// Stat gets mtime and metadata info for the request Remote object.
func (b *Baton) Stat(request *Request) (*ObjectInfo, error) {
	it, err := b.metaClient.ListItem(ex.Args{Timestamp: true, AVU: true}, *requestToRodsItem(request))
	if err != nil {
		if strings.Contains(err.Error(), extendoNotExist) {
			return &ObjectInfo{Exists: false}, nil
		}

		return nil, err
	}

	return &ObjectInfo{Exists: true, Meta: rodsItemToMeta(it)}, nil
}

// requestToRodsItem converts a Request in to an extendo RodsItem without AVUs.
func requestToRodsItem(request *Request) *ex.RodsItem {
	return &ex.RodsItem{
		IDirectory: filepath.Dir(request.Local),
		IFile:      filepath.Base(request.Local),
		IPath:      filepath.Dir(request.Remote),
		IName:      filepath.Base(request.Remote),
	}
}

// rodsItemToMeta pulls out the AVUs from a RodsItem and returns them as a map.
func rodsItemToMeta(it ex.RodsItem) map[string]string {
	meta := make(map[string]string, len(it.IAVUs))

	for _, iavu := range it.IAVUs {
		meta[iavu.Attr] = iavu.Value
	}

	return meta
}

// Put uploads request Local to the Remote object, overwriting it if it already
// exists. It calculates and stores the md5 checksum remotely.
//
// NB: extendo does not yet support the "verify" mode, which we need to compare
// the remote checksum to a locally calculated one. This must be implemented and
// used before using this in production!
func (b *Baton) Put(request *Request) error {
	_, err := b.putClient.Put(
		ex.Args{
			Force:    true,
			Checksum: true,
		},
		*requestToRodsItemWithAVUs(request),
	)

	return err
}

// requestToRodsItemWithAVUs converts a Request in to an extendo RodsItem with
// AVUs.
func requestToRodsItemWithAVUs(request *Request) *ex.RodsItem {
	item := requestToRodsItem(request)
	item.IAVUs = metaToAVUs(request.Meta)

	return item
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

func (b *Baton) RemoveMeta(path string, meta map[string]string) error {
	it := remotePathToRodsItem(path)
	it.IAVUs = metaToAVUs(meta)

	_, err := b.metaClient.MetaRem(ex.Args{}, *it)

	return err
}

// remotePathToRodsItem converts a path in to an extendo RodsItem.
func remotePathToRodsItem(path string) *ex.RodsItem {
	return &ex.RodsItem{
		IPath: filepath.Dir(path),
		IName: filepath.Base(path),
	}
}

func (b *Baton) AddMeta(path string, meta map[string]string) error {
	it := remotePathToRodsItem(path)
	it.IAVUs = metaToAVUs(meta)

	_, err := b.metaClient.MetaAdd(ex.Args{}, *it)

	return err
}
