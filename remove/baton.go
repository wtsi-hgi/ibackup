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

package remove

import (
	"fmt"
	"os"

	"github.com/rs/zerolog"
	"github.com/wtsi-hgi/ibackup/put"
	ex "github.com/wtsi-npg/extendo/v2"
	logs "github.com/wtsi-npg/logshim"
	"github.com/wtsi-npg/logshim-zerolog/zlog"
)

// Baton is a Handler that uses Baton (via extendo) to interact with iRODS.
type Baton struct {
	put.Baton
}

const (
	extendoLogLevel = logs.ErrorLevel
)

// GetBatonHandlerWithMetaClient returns a Handler that uses Baton to interact
// with iRODS and contains a meta client for interacting with metadata. If you
// don't have baton-do in your PATH, you'll get an error.
func GetBatonHandlerWithMetaClient() (*Baton, error) {
	setupExtendoLogger()

	_, err := ex.FindBaton()
	if err != nil {
		return nil, err
	}

	params := ex.DefaultClientPoolParams
	params.MaxSize = 1
	pool := ex.NewClientPool(params, "")

	metaClient, err := pool.Get()
	if err != nil {
		return nil, fmt.Errorf("failed to get metaClient: %w", err)
	}

	baton := &Baton{
		Baton: put.Baton{
			PutMetaPool: pool,
			MetaClient:  metaClient,
		},
	}

	return baton, nil
}

// setupExtendoLogger sets up a STDERR logger that the extendo library will use.
// (We don't actually care about what it might log, but extendo doesn't work
// without this.)
func setupExtendoLogger() {
	logs.InstallLogger(zlog.New(zerolog.SyncWriter(os.Stderr), extendoLogLevel))
}

func (b *Baton) RemoveFile(path string) error {
	it := put.RemotePathToRodsItem(path)

	err := put.TimeoutOp(func() error {
		_, errl := b.MetaClient.RemObj(ex.Args{}, *it)

		return errl
	}, "remove file error: "+path)

	return err
}

// RemoveDir removes the given directory from iRODS given it is empty.
func (b *Baton) RemoveDir(path string) error {
	it := &ex.RodsItem{
		IPath: path,
	}

	err := put.TimeoutOp(func() error {
		_, errl := b.MetaClient.RemDir(ex.Args{}, *it)

		return errl
	}, "remove meta error: "+path)

	return err
}

func (b *Baton) QueryMeta(dirToSearch string, meta map[string]string) ([]string, error) {
	it := &ex.RodsItem{
		IPath: dirToSearch,
		IAVUs: put.MetaToAVUs(meta),
	}

	var items []ex.RodsItem

	var err error

	err = put.TimeoutOp(func() error {
		items, err = b.MetaClient.MetaQuery(ex.Args{Object: true}, *it)

		return err
	}, "query meta error: "+dirToSearch)

	paths := make([]string, len(items))

	for i, item := range items {
		paths[i] = item.IPath
	}

	return paths, err
}
