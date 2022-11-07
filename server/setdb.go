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

package server

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/gin-gonic/gin"
	gas "github.com/wtsi-hgi/go-authserver"
	"github.com/wtsi-hgi/ibackup/set"
)

const (
	setPath = "/set"

	// EndPointSet is the endpoint for making set queries if authorization
	// isn't implemented.
	EndPointSet = gas.EndPointREST + setPath

	// EndPointAuthSet is the endpoint for making set queries if authorization
	// is implemented.
	EndPointAuthSet = gas.EndPointAuth + setPath

	ErrNoSetDBDirFound = gas.Error("set database directory not found")
	ErrNoRequester     = gas.Error("requester not supplied")

	getSetByRequesterKey = "requester"
)

// LoadSetDB loads the given set.db or creates it if it doesn't exist, and adds
// the /rest/v1/set PUT and GET endpoint to the REST API. If you call
// EnableAuth() first, then these endpoints will be secured and be available at
// /rest/v1/auth/set.
//
// The set put endpoint takes a set.Set encoded as JSON in the body.
// The set get endpoint takes an id or a user parameter.
func (s *Server) LoadSetDB(path string) error {
	db, err := set.New(path)
	if err != nil {
		return err
	}

	s.db = db

	authGroup := s.AuthRouter()

	if authGroup == nil {
		s.Router().PUT(EndPointSet, s.putSet)
		s.Router().GET(EndPointSet, s.getSets)
	} else {
		authGroup.PUT(setPath, s.putSet)
		authGroup.GET(setPath, s.getSets)
	}

	return nil
}

// putSet interprets the body as a JSON encoding of a set.Set and stores it in
// the database.
//
// LoadSetDB() must already have been called. This is called when there is a PUT
// on /rest/v1/set or /rest/v1/auth/set.
func (s *Server) putSet(c *gin.Context) {
	jsonData, err := io.ReadAll(c.Request.Body)
	if err != nil {
		c.AbortWithError(http.StatusBadRequest, err) //nolint:errcheck

		return
	}

	set := &set.Set{}

	err = json.Unmarshal(jsonData, set)
	if err != nil {
		c.AbortWithError(http.StatusBadRequest, err) //nolint:errcheck

		return
	}

	if !s.allowedAccess(c, set.Requester) {
		c.AbortWithError(http.StatusUnauthorized, err) //nolint:errcheck

		return
	}

	err = s.db.AddOrUpdate(set)
	if err != nil {
		c.AbortWithError(http.StatusInternalServerError, err) //nolint:errcheck

		return
	}

	c.Status(http.StatusOK)
}

// allowedAccess gets our current user if we have EnableAuth(), and returns
// true if that matches the given username. Always returns true if we have not
// EnableAuth().
func (s *Server) allowedAccess(c *gin.Context, user string) bool {
	u := s.GetUser(c)
	if u == nil {
		return true
	}

	return u.Username == user
}

// getSets returns the requester's set(s) from the database. requester parameter
// must be given. Returns the sets as a JSON encoding of a []*set.Set.
//
// LoadSetDB() must already have been called. This is called when there is a PUT
// on /rest/v1/set or /rest/v1/auth/set.
func (s *Server) getSets(c *gin.Context) {
	requester, given := c.GetQuery(getSetByRequesterKey)
	if !given {
		c.AbortWithError(http.StatusBadRequest, ErrNoRequester) //nolint:errcheck

		return
	}

	if !s.allowedAccess(c, requester) {
		c.AbortWithError(http.StatusUnauthorized, ErrBadRequester) //nolint:errcheck

		return
	}

	sets, err := s.db.GetByRequester(requester)
	if err != nil {
		c.AbortWithError(http.StatusBadRequest, err) //nolint:errcheck

		return
	}

	c.JSON(http.StatusOK, sets)
}
