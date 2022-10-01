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

// this file implements a Handler using the pure Go go-irodsclient.

package put

import (
	"os"
	"path/filepath"

	"github.com/cyverse/go-irodsclient/fs"
	"github.com/cyverse/go-irodsclient/irods/types"
)

const gicYMLPath = ".irods/gic.yml"
const gicPutRetries = 10

// GIC is a Handler that uses the pure Go go-irodsclient to interact with iRODS.
type GIC struct {
	account  *types.IRODSAccount
	fsMkdir  *fs.FileSystem
	fsUpload *fs.FileSystem
	fsMeta   *fs.FileSystem
}

// GetGICHandler returns a Handler that uses the go-irodsclient to interact with
// iRODS. If you don't have ~/.irods/gic.yml with your iRODS account details in
// it, you'll get an error.
func GetGICHandler() (*GIC, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}

	yaml, err := os.ReadFile(filepath.Join(home, gicYMLPath))
	if err != nil {
		return nil, err
	}

	account, err := types.CreateIRODSAccountFromYAML(yaml)
	if err != nil {
		return nil, err
	}

	return &GIC{account: account}, err
}

// Connect creates filesystem layouts.
func (g *GIC) Connect() error {
	fsMkdir, err := fs.NewFileSystemWithDefault(g.account, "make_dir")
	if err != nil {
		return err
	}

	g.fsMkdir = fsMkdir

	fsUpload, err := fs.NewFileSystemWithDefault(g.account, "upload")
	if err != nil {
		return err
	}

	g.fsUpload = fsUpload

	fsMeta, err := fs.NewFileSystemWithDefault(g.account, "meta")
	if err != nil {
		return err
	}

	g.fsMeta = fsMeta

	return nil
}

// Cleanup releases our filesystem layouts.
func (g *GIC) Cleanup() error {
	g.fsMkdir.Release()
	g.fsUpload.Release()
	g.fsMeta.Release()

	return nil
}

// EnsureCollection ensures the given collection exists in iRODS, creating it if
// necessary.
func (g *GIC) EnsureCollection(collection string) error {
	return g.fsMkdir.MakeDir(collection, true)
}

// Put uploads request Local to the Remote object, overwriting it if it already
// exists. It calculates and stores the md5 checksum remotely.
func (g *GIC) Put(request *Request) error {
	var err error

	for i := 0; i < gicPutRetries; i++ {
		if err = g.fsUpload.UploadFile(request.Local, request.Remote, "", true, nil); err == nil {
			break
		}

		if err.Error() == "OVERWRITE_WITHOUT_FORCE_FLAG" {
			err = g.fsUpload.RemoveFile(request.Remote, true)
			if err != nil {
				break
			}
		}
	}

	return err
}

// ReplaceMetadata adds the request Meta to the Remote object, deleting existing
// keys of the same name first.
func (g *GIC) ReplaceMetadata(request *Request) error {
	for k, v := range request.Meta {
		if err := g.fsMeta.AddMetadata(request.Remote, k, v, ""); err != nil {
			if errd := g.fsMeta.DeleteMetadata(request.Remote, k, v, ""); errd != nil {
				return err
			}

			if err = g.fsMeta.AddMetadata(request.Remote, k, v, ""); err != nil {
				return err
			}
		}
	}

	return nil
}
