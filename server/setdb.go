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
	"context"
	"errors"
	"net/http"

	"github.com/VertebrateResequencing/wr/queue"
	"github.com/gin-gonic/gin"
	gas "github.com/wtsi-hgi/go-authserver"
	"github.com/wtsi-hgi/ibackup/put"
	"github.com/wtsi-hgi/ibackup/set"
)

const (
	setPath        = "/set"
	filePath       = "/files"
	dirPath        = "/dirs"
	entryPath      = "/entries"
	discoveryPath  = "/discover"
	fileStatusPath = "/file_status"
	requestsPath   = "/requests"

	// EndPointAuthSet is the endpoint for getting and setting sets.
	EndPointAuthSet = gas.EndPointAuth + setPath

	// EndPointAuthFiles is the endpoint for setting set file paths.
	EndPointAuthFiles = gas.EndPointAuth + filePath

	// EndPointAuthDirs is the endpoint for setting set directory paths.
	EndPointAuthDirs = gas.EndPointAuth + dirPath

	// EndPointAuthEntries is the endpoint for getting set entries.
	EndPointAuthEntries = gas.EndPointAuth + entryPath

	// EndPointAuthDiscovery is the endpoint for triggering set discovery.
	EndPointAuthDiscovery = gas.EndPointAuth + discoveryPath

	// EndPointAuthFileStatus is the endpoint for updating file upload status.
	EndPointAuthFileStatus = gas.EndPointAuth + fileStatusPath

	// EndPointAuthRequests is the endpoint for getting file upload requests.
	EndPointAuthRequests = gas.EndPointAuth + requestsPath

	ErrNoAuth          = gas.Error("auth must be enabled")
	ErrNoSetDBDirFound = gas.Error("set database directory not found")
	ErrNoRequester     = gas.Error("requester not supplied")
	ErrBadRequester    = gas.Error("you are not the set requester")
	ErrNotAdmin        = gas.Error("you are not the server admin")
	ErrBadSet          = gas.Error("set with that id does not exist")
	ErrInvalidInput    = gas.Error("invalid input")
	ErrInteral         = gas.Error("internal server error")

	paramRequester = "requester"
	paramSetID     = "id"

	// defaultFileSize is 1MB in bytes, because most files on lustre volumes
	// are between 64KB and 1MB in size.
	defaultFileSize uint64 = 1024 * 1024

	// desiredFileSize is the total size of files we want to return in
	// getRequests (10GB).
	desiredFileSize uint64 = defaultFileSize * 1024 * 10
)

// LoadSetDB loads the given set.db or creates it if it doesn't exist, and adds
// a number of endpoints to the REST API for working with the set and its
// entries:
//
// PUT /rest/v1/auth/set : takes a set.Set encoded as JSON in the body to add or
// update details about the given set.
//
// GET /rest/v1/auth/set/[requester] : takes "requester" URL parameter to get
// the sets requested by this requester.
//
// PUT /rest/v1/auth/files/[id] : takes a []string of paths encoded as JSON in
// the body plus a set "id" URL parameter to store these as the files to back up
// for the set.
//
// PUT /rest/v1/auth/dirs/[id] : takes a []string of paths encoded as JSON in
// the body plus a set "id" URL parameter to store these as the directories to
// recursively back up for the set.
//
// GET /rest/v1/auth/discover/[id]: takes a set "id" URL parameter to trigger
// the discovery of files for the set.
//
// GET /rest/v1/auth/entries/[id] : takes a set "id" URL parameter and returns
// the set.Entries with backup status about each file (both set with
// /rest/v1/auth/files and discovered inside /rest/v1/auth/dirs).
//
// GET /rest/v1/auth/requests : returns about 10GB worth of upload requests from
// the global put queue. Only the user who started the server has permission to
// call this.
//
// PUT /rest/v1/auth/file_status : takes a put.Request encoded as JSON in the
// body to update the status of the corresponding set's file entry.
//
// You must call EnableAuth() before calling this method, and the endpoints will
// only let you work on sets where the Requester matches your logged-in
// username, or if the logged-in user is the same as the user who started the
// Server.
func (s *Server) LoadSetDB(path string) error {
	authGroup := s.AuthRouter()
	if authGroup == nil {
		return ErrNoAuth
	}

	db, err := set.New(path)
	if err != nil {
		return err
	}

	s.db = db

	authGroup.PUT(setPath, s.putSet)
	authGroup.GET(setPath+"/:"+paramRequester, s.getSets)

	idParam := "/:" + paramSetID

	authGroup.PUT(filePath+idParam, s.putFiles)
	authGroup.PUT(dirPath+idParam, s.putDirs)

	authGroup.GET(discoveryPath+idParam, s.triggerDiscovery)

	authGroup.GET(entryPath+idParam, s.getEntries)
	authGroup.GET(dirPath+idParam, s.getDirs)

	authGroup.GET(requestsPath, s.getRequests)

	authGroup.PUT(fileStatusPath, s.putFileStatus)

	return nil
}

// putSet interprets the body as a JSON encoding of a set.Set and stores it in
// the database.
//
// LoadSetDB() must already have been called. This is called when there is a PUT
// on /rest/v1/auth/set.
func (s *Server) putSet(c *gin.Context) {
	set := &set.Set{}

	if err := c.BindJSON(set); err != nil {
		c.AbortWithError(http.StatusBadRequest, err) //nolint:errcheck

		return
	}

	if !s.allowedAccess(c, set.Requester) {
		c.AbortWithError(http.StatusUnauthorized, ErrBadRequester) //nolint:errcheck

		return
	}

	err := s.db.AddOrUpdate(set)
	if err != nil {
		c.AbortWithError(http.StatusInternalServerError, err) //nolint:errcheck

		return
	}

	c.Status(http.StatusOK)
}

// allowedAccess gets our current user if we have EnableAuth(), and returns
// true if that matches the given username. Always returns true if we have not
// EnableAuth(), or if our current user is the user who started the Server.
// If user is blank, it's a test if the current user started the Server.
func (s *Server) allowedAccess(c *gin.Context, user string) bool {
	u := s.GetUser(c)
	if u == nil {
		return true
	}

	if u.Username == s.username {
		return true
	}

	return u.Username == user
}

// getSets returns the requester's set(s) from the database. requester URL
// parameter must be given. Returns the sets as a JSON encoding of a []*set.Set.
//
// LoadSetDB() must already have been called. This is called when there is a GET
// on /rest/v1/auth/set.
func (s *Server) getSets(c *gin.Context) {
	requester := c.Param(paramRequester)

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

// putFiles sets the file paths encoded in to the body as JSON as the files
// to be backed up for the set with the id specified in the URL parameter.
//
// LoadSetDB() must already have been called. This is called when there is a PUT
// on /rest/v1/auth/files/[id].
func (s *Server) putFiles(c *gin.Context) {
	sid, paths, ok := s.bindPathsAndValidateSet(c)
	if !ok {
		return
	}

	err := s.db.SetFileEntries(sid, paths)
	if err != nil {
		c.AbortWithError(http.StatusBadRequest, err) //nolint:errcheck

		return
	}

	c.Status(http.StatusOK)
}

// bindPathsAndValidateSet gets the paths out of the JSON body, and the set id
// from the URL parameter if Requester matches logged-in username.
func (s *Server) bindPathsAndValidateSet(c *gin.Context) (string, []string, bool) {
	var paths []string

	if err := c.BindJSON(&paths); err != nil {
		c.AbortWithError(http.StatusBadRequest, err) //nolint:errcheck

		return "", nil, false
	}

	set, ok := s.validateSet(c)
	if !ok {
		return "", nil, false
	}

	return set.ID(), paths, true
}

// validateSet gets the id parameter from the given context and checks a
// corresponding set exists and the logged-in user is the same as the set's
// Requester. If so, returns the set and true. If not, Aborts with an error
// and returns false.
func (s *Server) validateSet(c *gin.Context) (*set.Set, bool) {
	sid := c.Param(paramSetID)

	set := s.db.GetByID(sid)
	if set == nil {
		c.AbortWithError(http.StatusBadRequest, ErrBadSet) //nolint:errcheck

		return nil, false
	}

	if !s.allowedAccess(c, set.Requester) {
		c.AbortWithError(http.StatusUnauthorized, ErrBadRequester) //nolint:errcheck

		return nil, false
	}

	return set, true
}

// putDirs sets the directory paths encoded in to the body as JSON as the dirs
// to be backed up for the set with the id specified in the URL parameter.
//
// LoadSetDB() must already have been called. This is called when there is a PUT
// on /rest/v1/auth/dirs/[id].
func (s *Server) putDirs(c *gin.Context) {
	sid, paths, ok := s.bindPathsAndValidateSet(c)
	if !ok {
		return
	}

	err := s.db.SetDirEntries(sid, paths)
	if err != nil {
		c.AbortWithError(http.StatusBadRequest, err) //nolint:errcheck

		return
	}

	c.Status(http.StatusOK)
}

// getEntries gets the defined and discovered file entries for the set with the
// id specified in the URL parameter.
//
// LoadSetDB() must already have been called. This is called when there is a GET
// on /rest/v1/auth/entries/[id].
func (s *Server) getEntries(c *gin.Context) {
	set, ok := s.validateSet(c)
	if !ok {
		return
	}

	entries, err := s.db.GetFileEntries(set.ID())
	if err != nil {
		c.AbortWithError(http.StatusBadRequest, err) //nolint:errcheck

		return
	}

	c.JSON(http.StatusOK, entries)
}

// getDirs gets the defined directory entries for the set with the id specified
// in the URL parameter.
//
// LoadSetDB() must already have been called. This is called when there is a GET
// on /rest/v1/auth/dirs/[id].
func (s *Server) getDirs(c *gin.Context) {
	set, ok := s.validateSet(c)
	if !ok {
		return
	}

	entries, err := s.db.GetDirEntries(set.ID())
	if err != nil {
		c.AbortWithError(http.StatusBadRequest, err) //nolint:errcheck

		return
	}

	c.JSON(http.StatusOK, entries)
}

// getRequests gets about 10GB worth of put Requests from the global in-memory
// put-queue (as populated during set discoveries).
//
// LoadSetDB() must already have been called. This is called when there is a GET
// on /rest/v1/auth/requests. Only the user who started the Server has
// permission to call this.
func (s *Server) getRequests(c *gin.Context) {
	if !s.allowedAccess(c, "") {
		c.AbortWithError(http.StatusUnauthorized, ErrNotAdmin) //nolint:errcheck

		return
	}

	requests, err := s.reserveRequests()
	if err != nil {
		c.AbortWithError(http.StatusInternalServerError, err) //nolint:errcheck

		return
	}

	c.JSON(http.StatusOK, requests)
}

// reserveRequests keeps reserving items from our queue until we have about 10GB
// of them (assuming each file is 1MB unless we know better), or the queue is
// empty. Returns the Requests in the items.
func (s *Server) reserveRequests() ([]*put.Request, error) {
	var requests []*put.Request //nolint:prealloc

	var size uint64

	for {
		r, err := s.reserveRequest()
		if err != nil {
			return nil, err
		}

		if r == nil {
			break
		}

		requests = append(requests, r)

		s := r.Size
		if s == 0 {
			s = defaultFileSize
		}

		size += s
		if size >= desiredFileSize {
			break
		}
	}

	return requests, nil
}

// reserveRequest reserves an item from our queue and converts it to a Request.
// Returns nil and no error if the queue is empty.
func (s *Server) reserveRequest() (*put.Request, error) {
	item, err := s.queue.Reserve("", 0)
	if err != nil {
		qerr, ok := err.(queue.Error) //nolint:errorlint
		if ok && errors.Is(qerr.Err, queue.ErrNothingReady) {
			err = nil
		}

		return nil, err
	}

	r, ok := item.Data().(*put.Request)
	if !ok {
		return nil, ErrInvalidInput
	}

	return r, nil
}

// putFileStatus interprets the body as a JSON encoding of a put.Request and
// stores it in the database.
//
// Only works if a corresponding Request is currently in "run" state in our
// queue, in which case it will be removed from, released or buried in the queue
// depending on the number of failures.
//
// LoadSetDB() must already have been called. This is called when there is a PUT
// on /rest/v1/auth/file_status. Only the user who started the Server has
// permission to call this.
func (s *Server) putFileStatus(c *gin.Context) {
	r := &put.Request{}

	if err := c.BindJSON(r); err != nil {
		c.AbortWithError(http.StatusBadRequest, err) //nolint:errcheck

		return
	}

	if !s.allowedAccess(c, "") {
		c.AbortWithError(http.StatusUnauthorized, ErrNotAdmin) //nolint:errcheck

		return
	}

	if err := s.requestWasReserved(r); err != nil {
		c.AbortWithError(http.StatusBadRequest, err) //nolint:errcheck

		return
	}

	if err := s.updateFileStatus(r); err != nil {
		c.AbortWithError(http.StatusInternalServerError, err) //nolint:errcheck

		return
	}

	c.Status(http.StatusOK)
}

// requestWasReserved returns an error unless the given request is in the "run"
// sub-queue of our global put queue, ie. it was reserved by reserveRequest().
func (s *Server) requestWasReserved(r *put.Request) error {
	return s.queue.Touch(r.ID())
}

// updateFileStatus updates the request's file entry status in the db, and
// removes the request from our queue.
func (s *Server) updateFileStatus(r *put.Request) error {
	entry, err := s.db.SetEntryStatus(r)
	if err != nil {
		return err
	}

	return s.removeOrReleaseRequestFromQueue(r, entry)
}

// removeOrReleaseRequestFromQueue removes the given Request from our queue
// unless it has failed. < 3 failures results in it being released, 3 results in
// it being buried.
func (s *Server) removeOrReleaseRequestFromQueue(r *put.Request, entry *set.Entry) error {
	if r.Status == put.RequestStatusFailed {
		if entry.Attempts >= set.AttemptsToBeConsideredFailing {
			return s.queue.Bury(r.ID())
		}

		return s.queue.Release(context.Background(), r.ID())
	}

	return s.queue.Remove(context.Background(), r.ID())
}