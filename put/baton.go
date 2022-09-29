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
	"sync"

	"github.com/rs/zerolog"
	ex "github.com/wtsi-npg/extendo/v2"
	logs "github.com/wtsi-npg/logshim"
	"github.com/wtsi-npg/logshim-zerolog/zlog"
)

const extendoLogLevel = logs.ErrorLevel
const numExtendoClients = 2

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
		*requestToRodsItem(request),
	)

	return err
}

// requestToRodsItem converts a Request in to an extendo RodsItem.
func requestToRodsItem(request *Request) *ex.RodsItem {
	avus := make([]ex.AVU, len(request.Meta))
	i := 0

	for k, v := range request.Meta {
		avus[i] = ex.AVU{Attr: k, Value: v}
		i++
	}

	return &ex.RodsItem{
		IDirectory: filepath.Dir(request.Local),
		IFile:      filepath.Base(request.Local),
		IPath:      filepath.Dir(request.Remote),
		IName:      filepath.Base(request.Remote),
		IAVUs:      avus,
	}
}

// ReplaceMetadata replaces any metadata on the request Remote object with the
// request's Meta.
func (b *Baton) ReplaceMetadata(request *Request) error {
	item := requestToRodsItem(request)
	avus := item.IAVUs

	it, err := b.metaClient.ListItem(ex.Args{AVU: true}, *item)
	if err != nil {
		return err
	}

	item.IAVUs = it.IAVUs
	currentAVUs := item.IAVUs

	toKeep := ex.SetIntersectAVUs(avus, currentAVUs)

	if err = removeUnneededAVUs(b.metaClient, item, avus, currentAVUs, toKeep); err != nil {
		return err
	}

	toAdd := ex.SetDiffAVUs(avus, toKeep)

	if len(toAdd) > 0 {
		add := ex.CopyRodsItem(*item)
		add.IAVUs = toAdd

		if _, err := b.metaClient.MetaAdd(ex.Args{}, add); err != nil {
			return err
		}
	}

	return nil
}

// removeUnneededAVUs removes metadata we don't need.
func removeUnneededAVUs(client *ex.Client, item *ex.RodsItem, newAVUs, currentAVUs, toKeep []ex.AVU) error {
	repAttrs := make(map[string]struct{})
	for _, avu := range newAVUs {
		repAttrs[avu.Attr] = struct{}{}
	}

	var toRemove []ex.AVU

	for _, avu := range currentAVUs {
		if _, ok := repAttrs[avu.Attr]; ok && !ex.SearchAVU(avu, toKeep) {
			toRemove = append(toRemove, avu)
		}
	}

	rem := ex.CopyRodsItem(*item)
	rem.IAVUs = toRemove

	if len(toRemove) > 0 {
		if _, err := client.MetaRem(ex.Args{}, rem); err != nil {
			return err
		}
	}

	return nil
}
