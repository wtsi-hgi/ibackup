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
	"github.com/wtsi-hgi/ibackup/set"
)

const ErrBadRequester = gas.Error("bad requester")

// AddOrUpdateSet is a client call to a Server listening at the given
// domain:port url to add details about a backup set to the Server's database.
//
// Provide a non-blank path to a certificate to force us to trust that
// certificate, eg. if the server was started with a self-signed certificate.
//
// You must first gas.Login() to get a JWT that you must supply here.
func AddOrUpdateSet(url, cert, jwt string, set *set.Set) error {
	r := gas.NewAuthenticatedClientRequest(url, cert, jwt)

	resp, err := r.ForceContentType("application/json").
		SetBody(set).
		Put(EndPointAuthSet)
	if err != nil {
		return err
	}

	switch resp.StatusCode() {
	case http.StatusUnauthorized, http.StatusNotFound:
		return gas.ErrNoAuth
	}

	return nil
}

// GetSets is a client call to a Server listening at the given
// domain:port url to get details about a given requester's backup sets from the
// Server's database.
//
// Provide a non-blank path to a certificate to force us to trust that
// certificate, eg. if the server was started with a self-signed certificate.
//
// You must first gas.Login() to get a JWT that you must supply here.
func GetSets(url, cert, jwt, requester string) ([]*set.Set, error) {
	r := gas.NewAuthenticatedClientRequest(url, cert, jwt)

	var sets []*set.Set

	resp, err := r.ForceContentType("application/json").
		SetQueryParams(map[string]string{getSetByRequesterKey: requester}).
		SetResult(&sets).
		Get(EndPointAuthSet)
	if err != nil {
		return nil, err
	}

	switch resp.StatusCode() {
	case http.StatusUnauthorized, http.StatusNotFound:
		return nil, gas.ErrNoAuth
	case http.StatusBadRequest:
		return nil, ErrBadRequester
	}

	return sets, nil
}
