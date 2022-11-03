/*******************************************************************************
 * Copyright (c) 2022 Genome Research Ltd.
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

package server

import (
	"net/http"

	gas "github.com/wtsi-hgi/go-authserver"
)

// AddOrUpdateSet is a client call to a Server listening at the given
// domain:port url to add a backup set to the Server's database, so that the
// files in the set will get backed up. Returns a unique set ID assigned to the
// set.
//
// Provide a non-blank path to a certificate to force us to trust that
// certificate, eg. if the server was started with a self-signed certificate.
//
// You must first gas.Login() to get a JWT that you must supply here.
func AddOrUpdateSet(url, cert, jwt string) (string, error) {
	r := gas.NewAuthenticatedClientRequest(url, cert, jwt)

	resp, err := r.ForceContentType("application/json").
		Get(EndPointAuthSet)
	if err != nil {
		return "", err
	}

	switch resp.StatusCode() {
	case http.StatusUnauthorized, http.StatusNotFound:
		return "", gas.ErrNoAuth
	}

	return resp.String(), nil
}
