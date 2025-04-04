/*******************************************************************************
 * Copyright (c) 2022, 2023 Genome Research Ltd.
 *
 * Authors:
 *	- Sendu Bala <sb10@sanger.ac.uk>
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

package cmd

import (
	"os/user"

	gas "github.com/wtsi-hgi/go-authserver"
	"github.com/wtsi-hgi/ibackup/server"
)

const jwtBasename = ".ibackup.jwt"

// newServerClient tries to get a jwt for the given server url, and returns a
// client that can interact with it.
func newServerClient(url, cert string) (*server.Client, error) {
	token, err := gasClientCLI(url, cert).GetJWT()
	if err != nil {
		return nil, err
	}

	return server.NewClient(url, cert, token), nil
}

func gasClientCLI(url, cert string) *gas.ClientCLI {
	c, err := gas.NewClientCLI(jwtBasename, serverTokenBasename, url, cert, false)
	if err != nil {
		die(err)
	}

	return c
}

func currentUsername() string {
	user, err := user.Current()
	if err != nil {
		dief("couldn't get user: %s", err)
	}

	return user.Username
}
