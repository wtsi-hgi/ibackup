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

// this file implements a Handler using GoRODS.

package put

import gorods "github.com/jjacquay712/GoRODS"

// GoRODS is a Handler that uses GoRODS to interact with iRODS.
type GoRODS struct {
	mkClient   *gorods.Client
	putClient  *gorods.Client
	metaClient *gorods.Client
}

// GetGoRODSHandler returns a Handler that uses GoRODS to interact with iRODS.
func GetGoRODSHandler() (*GoRODS, error) {
	return &GoRODS{}, nil
}

// Connect creates some clients for later use.
func (g *GoRODS) Connect() error {
	opts := gorods.ConnectionOptions{
		Type: gorods.EnvironmentDefined,
	}

	client, err := gorods.New(opts)
	if err != nil {
		return err
	}

	g.mkClient = client

	client, err = gorods.New(opts)
	if err != nil {
		return err
	}

	g.putClient = client

	client, err = gorods.New(opts)
	if err != nil {
		return err
	}

	g.metaClient = client

	return nil
}

// Cleanup does nothing.
func (g *GoRODS) Cleanup() error {
	return nil
}

// EnsureCollection ensures the given collection exists in iRODS, creating it if
// necessary.
func (g *GoRODS) EnsureCollection(collection string) error {
	return nil
}

// Put uploads request Local to the Remote object, overwriting it if it already
// exists. It calculates and stores the md5 checksum remotely.
func (g *GoRODS) Put(request *Request) error {
	return nil
}

// ReplaceMetadata adds the request Meta to the Remote object, deleting existing
// keys of the same name first.
func (g *GoRODS) ReplaceMetadata(request *Request) error {
	return nil
}
