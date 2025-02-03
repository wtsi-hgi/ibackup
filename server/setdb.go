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

package server

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/VertebrateResequencing/wr/queue"
	"github.com/gin-gonic/gin"
	gas "github.com/wtsi-hgi/go-authserver"
	"github.com/wtsi-hgi/grand"
	"github.com/wtsi-hgi/ibackup/put"
	"github.com/wtsi-hgi/ibackup/set"
	"github.com/wtsi-hgi/ibackup/slack"
)

const (
	setPath          = "/set"
	filePath         = "/files"
	dirPath          = "/dirs"
	entryPath        = "/entries"
	exampleEntryPath = "/example_entries"
	failedEntryPath  = "/failed_entries"
	discoveryPath    = "/discover"
	requestsPath     = "/requests"
	workingPath      = "/working"
	fileStatusPath   = "/file_status"
	fileRetryPath    = "/retry"
	removePathsPath  = "/remove_paths"
	removeDirsPath   = "/remove_dirs"

	// EndPointAuthSet is the endpoint for getting and setting sets.
	EndPointAuthSet = gas.EndPointAuth + setPath

	// EndPointAuthFiles is the endpoint for setting set file paths.
	EndPointAuthFiles = gas.EndPointAuth + filePath

	// EndPointAuthDirs is the endpoint for setting set directory paths.
	EndPointAuthDirs = gas.EndPointAuth + dirPath

	// EndPointAuthEntries is the endpoint for getting set entries.
	EndPointAuthEntries = gas.EndPointAuth + entryPath

	// EndPointAuthExampleEntry is the endpoint for getting set entries.
	EndPointAuthExampleEntry = gas.EndPointAuth + exampleEntryPath

	// EndPointAuthFailedEntries is the endpoint for getting some failed set
	// entries.
	EndPointAuthFailedEntries = gas.EndPointAuth + failedEntryPath

	// EndPointAuthDiscovery is the endpoint for triggering set discovery.
	EndPointAuthDiscovery = gas.EndPointAuth + discoveryPath

	// EndPointAuthRequests is the endpoint for getting file upload requests.
	EndPointAuthRequests = gas.EndPointAuth + requestsPath

	// EndPointAuthWorking is the endpoint for advising the server you're still
	// working on Requests retrieved from EndPointAuthRequests.
	EndPointAuthWorking = gas.EndPointAuth + workingPath

	// EndPointAuthFileStatus is the endpoint for updating file upload status.
	EndPointAuthFileStatus = gas.EndPointAuth + fileStatusPath

	// EndPointAuthRetryEntries is the endpoint for retrying file uploads.
	EndPointAuthRetryEntries = gas.EndPointAuth + fileRetryPath

	//
	EndPointAuthRemovePaths = gas.EndPointAuth + removePathsPath

	//
	EndPointAuthRemoveDirs = gas.EndPointAuth + removeDirsPath

	ErrNoAuth          = gas.Error("auth must be enabled")
	ErrNoSetDBDirFound = gas.Error("set database directory not found")
	ErrNoRequester     = gas.Error("requester not supplied")
	ErrBadRequester    = gas.Error("you are not the set requester")
	ErrNotAdmin        = gas.Error("you are not the server admin")
	ErrBadSet          = gas.Error("set with that id does not exist")
	ErrInvalidInput    = gas.Error("invalid input")
	ErrInternal        = gas.Error("internal server error")

	paramRequester = "requester"
	paramSetID     = "id"

	// maxRequestsToReserve is the maximum number of requests that
	// reserveRequests returns.
	maxRequestsToReserve = 100

	logTraceIDLen = 8
	allUsers      = "all"
)

// LoadSetDB loads the given set.db or creates it if it doesn't exist.
// Optionally, also provide a path to backup the database to.
//
// It adds a number of endpoints to the REST API for working with the set and
// its entries:
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
// GET /rest/v1/auth/failed_entries/[id] : takes a set "id" URL parameter and
// returns the set.Entries with backup status about each file (both set with
// /rest/v1/auth/files and discovered inside /rest/v1/auth/dirs) that has failed
// status. Up to 10 are returned in an object with Entries and Skipped, where
// Skipped is the number of failed files not returned.
//
// GET /rest/v1/auth/requests : returns about 10GB worth of upload requests from
// the global put queue. Only the user who started the server has permission to
// call this.
//
// PUT /rest/v1/auth/working : takes a []string of Request ids encoded as JSON
// in the body received from the requests endpoint to advise the server you're
// still working on uploading those requests. Only the user who started the
// server has permission to call this.
//
// PUT /rest/v1/auth/file_status : takes a put.Request encoded as JSON in the
// body to update the status of the corresponding set's file entry.
//
// GET /rest/v1/auth/retry : takes a set "id" URL parameter to trigger the retry
// of failed file uploads for the set.
//
// If the database indicates there are sets we were in the middle of working on,
// the upload requests will be added to our in-memory queue, just like during
// discovery.
//
// You must call EnableAuthWithServerToken() before calling this method, and the
// endpoints will only let you work on sets where the Requester matches your
// logged-in username, or if the logged-in user is the same as the user who
// started the Server.
func (s *Server) LoadSetDB(path, backupPath string) error {
	authGroup := s.AuthRouter()
	if authGroup == nil {
		return ErrNoAuth
	}

	err := s.setupDB(path, backupPath, authGroup)
	if err != nil {
		return err
	}

	s.statusUpdateCh = make(chan *fileStatusPacket)
	go s.handleFileStatusUpdates()

	return s.recoverQueue()
}

func (s *Server) setupDB(path, backupPath string, authGroup *gin.RouterGroup) error {
	s.sendSlackMessage(slack.Info, "server starting, loading database")

	db, err := set.New(path, backupPath)
	if err != nil {
		return err
	}

	s.sendSlackMessage(slack.Success, "server loaded database")

	go s.tellSlackStillRunning()

	db.LogSetChangesToSlack(s.slacker)

	s.db = db

	s.addDBEndpoints(authGroup)

	return nil
}

func (s *Server) sendSlackMessage(level slack.Level, msg string) {
	if s.slacker == nil {
		return
	}

	s.slacker.SendMessage(level, msg)
}

func (s *Server) tellSlackStillRunning() {
	if s.slacker == nil || s.stillRunningMsgFreq <= 0 {
		return
	}

	ticker := time.NewTicker(s.stillRunningMsgFreq)

	defer ticker.Stop()

	s.serverAliveCh = make(chan bool)

	for {
		select {
		case <-ticker.C:
			s.serverStillRunning()
		case <-s.serverAliveCh:
			return
		}
	}
}

func (s *Server) serverStillRunning() {
	s.sendSlackMessage(slack.Info, "server is still running")
}

// EnableRemoteDBBackups causes the database backup file to also be backed up to
// the remote path.
func (s *Server) EnableRemoteDBBackups(remotePath string, handler put.Handler) {
	s.db.EnableRemoteBackups(remotePath, handler)
}

// addDBEndpoints adds all the REST API endpoints to the given router group.
func (s *Server) addDBEndpoints(authGroup *gin.RouterGroup) {
	authGroup.PUT(setPath, s.putSet)
	authGroup.GET(setPath+"/:"+paramRequester, s.getSets)

	idParam := "/:" + paramSetID

	authGroup.PUT(filePath+idParam, s.putFiles)
	authGroup.PUT(dirPath+idParam, s.putDirs)

	authGroup.GET(discoveryPath+idParam, s.triggerDiscovery)

	authGroup.GET(entryPath+idParam, s.getEntries)
	authGroup.GET(exampleEntryPath+idParam, s.getExampleEntry)

	authGroup.GET(dirPath+idParam, s.getDirs)
	authGroup.GET(failedEntryPath+idParam, s.getFailedEntries)

	authGroup.GET(requestsPath, s.getRequests)

	authGroup.PUT(workingPath, s.putWorking)

	authGroup.PUT(fileStatusPath, s.putFileStatus)

	authGroup.GET(fileRetryPath+idParam, s.retryFailedEntries)

	authGroup.PUT(removePathsPath+idParam, s.removePaths)
	//authGroup.PUT(removeDirsPath+idParam, s.removeDirs)
}

// putSet interprets the body as a JSON encoding of a set.Set and stores it in
// the database.
//
// LoadSetDB() must already have been called. This is called when there is a PUT
// on /rest/v1/auth/set.
func (s *Server) putSet(c *gin.Context) {
	given := &set.Set{}

	if err := c.BindJSON(given); err != nil {
		c.AbortWithError(http.StatusBadRequest, err) //nolint:errcheck

		return
	}

	if !s.AllowedAccess(c, given.Requester) {
		c.AbortWithError(http.StatusUnauthorized, ErrBadRequester) //nolint:errcheck

		return
	}

	err := s.db.AddOrUpdate(given)
	if err != nil {
		c.AbortWithError(http.StatusInternalServerError, err) //nolint:errcheck

		return
	}

	s.handleNewlyDefinedSets(given)

	c.Status(http.StatusOK)
}

// tryBackup will backup the database if a backup path was specified by
// EnableDatabaseBackups().
func (s *Server) tryBackup() {
	go func() {
		err := s.db.Backup()
		if err != nil {
			s.Logger.Printf("error creating database backup: %s", err)
		}
	}()
}

// getSets returns the requester's set(s) from the database. requester URL
// parameter must be given. Returns the sets as a JSON encoding of a []*set.Set.
//
// LoadSetDB() must already have been called. This is called when there is a GET
// on /rest/v1/auth/set.
func (s *Server) getSets(c *gin.Context) {
	requester := c.Param(paramRequester)

	if !s.AllowedAccess(c, requester) {
		c.AbortWithError(http.StatusUnauthorized, ErrBadRequester) //nolint:errcheck

		return
	}

	var (
		sets []*set.Set
		err  error
	)

	if requester == allUsers {
		sets, err = s.db.GetAll()
	} else {
		sets, err = s.db.GetByRequester(requester)
	}

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

func (s *Server) removePaths(c *gin.Context) {
	sid, filePaths, dirPaths, ok := s.parseRemoveParamsAndValidateSet(c)
	if !ok {
		return
	}

	set := s.db.GetByID(sid)

	err := s.db.ValidateFilePaths(set, filePaths)
	if err != nil {
		c.AbortWithError(http.StatusBadRequest, err) //nolint:errcheck

		return
	}

	err = s.db.ValidateDirPaths(set, dirPaths)
	if err != nil {
		c.AbortWithError(http.StatusBadRequest, err) //nolint:errcheck

		return
	}

	err = s.db.ResetRemoveSize(sid)
	if err != nil {
		c.AbortWithError(http.StatusBadRequest, err) //nolint:errcheck

		return
	}

	s.removeFiles(set, filePaths)

	s.removeDirs(set, dirPaths)
}

func (s *Server) removeFiles(set *set.Set, paths []string) error {
	err := s.removeFileFromIRODSandDB(set, paths)
	if err != nil {
		return err
	}

	// s.removeQueue.Add(context.Background(), "key", "", "data", 0, 0, 1*time.Hour, queue.SubQueueReady, []string{})

	return nil
}

// getBatonAndTransformer returns a baton with a meta client and a transformer
// for the given set.
func (s *Server) getBatonAndTransformer(set *set.Set) (*put.Baton, put.PathTransformer, error) {
	baton, err := put.GetBatonHandlerWithMetaClient()
	if err != nil {
		return nil, nil, err
	}

	tranformer, err := set.MakeTransformer()

	return baton, tranformer, err
}

func (s *Server) removeFilesFromIRODS(set *set.Set, paths []string,
	baton *put.Baton, transformer put.PathTransformer) error {
	for _, path := range paths {
		rpath, err := transformer(path)
		if err != nil {
			return err
		}

		remoteMeta, err := baton.GetMeta(rpath)
		if err != nil {
			return err
		}

		sets, requesters, err := s.handleSetsAndRequesters(set, remoteMeta)
		if err != nil {
			return err
		}

		err = baton.RemovePathFromSetInIRODS(transformer, rpath, sets, requesters, remoteMeta)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Server) removeFileFromIRODS(set *set.Set, path string,
	baton *put.Baton, transformer put.PathTransformer) error {
	rpath, err := transformer(path)
	if err != nil {
		return err
	}

	remoteMeta, err := baton.GetMeta(rpath)
	if err != nil {
		return err
	}

	sets, requesters, err := s.handleSetsAndRequesters(set, remoteMeta)
	if err != nil {
		return err
	}

	return baton.RemovePathFromSetInIRODS(transformer, rpath, sets, requesters, remoteMeta)
}

func (s *Server) handleSetsAndRequesters(set *set.Set, meta map[string]string) ([]string, []string, error) {
	sets := strings.Split(meta[put.MetaKeySets], ",")
	requesters := strings.Split(meta[put.MetaKeyRequester], ",")

	if !slices.Contains(sets, set.Name) {
		return sets, requesters, nil
	}

	otherUserSets, userSets, err := s.getSetNamesByRequesters(requesters, set.Requester)
	if err != nil {
		return nil, nil, err
	}

	if len(userSets) == 1 && userSets[0] == set.Name {
		requesters, err = removeElementFromSlice(requesters, set.Requester)
		if err != nil {
			return nil, nil, err
		}
	}

	if slices.Contains(otherUserSets, set.Name) {
		return sets, requesters, nil
	}

	sets, err = removeElementFromSlice(sets, set.Name)

	return sets, requesters, err
}

func (s *Server) getSetNamesByRequesters(requesters []string, user string) ([]string, []string, error) {
	var (
		otherUserSets []string
		curUserSets   []string
	)

	for _, requester := range requesters {
		requesterSets, err := s.db.GetByRequester(requester)
		if err != nil {
			return nil, nil, err
		}

		if requester == user {
			curUserSets = append(curUserSets, getNamesFromSets(requesterSets)...)

			continue
		}

		otherUserSets = append(otherUserSets, getNamesFromSets(requesterSets)...)
	}

	return otherUserSets, curUserSets, nil
}

func getNamesFromSets(sets []*set.Set) []string {
	names := make([]string, len(sets))

	for i, set := range sets {
		names[i] = set.Name
	}

	return names
}

func removeElementFromSlice(slice []string, element string) ([]string, error) {
	index := slices.Index(slice, element)
	if index < 0 {
		return nil, fmt.Errorf("Element %s not in slice", element)
	}

	slice[index] = slice[len(slice)-1]
	slice[len(slice)-1] = ""

	return slice[:len(slice)-1], nil
}

func (s *Server) removeDirFromIRODS(set *set.Set, path string,
	baton *put.Baton, transformer put.PathTransformer) error {

	rpath, err := transformer(path)
	if err != nil {
		return err
	}

	err = baton.RemoveDirFromIRODS(rpath)
	if err != nil {
		return err
	}

	return nil
}

func (s *Server) removeDirs(set *set.Set, paths []string) error {
	var filepaths []string
	var err error

	for _, path := range paths {
		filepaths, err = s.db.GetFilesInDir(set.ID(), path, filepaths)
		if err != nil {
			return err
		}
	}

	err = s.removeFileFromIRODSandDB(set, filepaths)
	if err != nil {
		return err
	}

	return s.removeDirFromIRODSandDB(set, paths)
}

func (s *Server) removeFileFromIRODSandDB(userSet *set.Set, paths []string) error {
	for _, path := range paths {
		entry, err := s.db.GetFileEntryForSet(userSet.ID(), path)
		if err != nil {
			return err
		}

		baton, transformer, err := s.getBatonAndTransformer(userSet)
		if err != nil {
			return err
		}

		err = s.removeFileFromIRODS(userSet, path, baton, transformer)
		if err != nil {
			return err
		}

		err = s.db.RemoveFileEntries(userSet.ID(), path)
		if err != nil {
			return err
		}

		err = s.db.UpdateBasedOnRemovedEntry(userSet.ID(), entry)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Server) removeDirFromIRODSandDB(userSet *set.Set, paths []string) error {
	for _, path := range paths {
		baton, transformer, err := s.getBatonAndTransformer(userSet)
		if err != nil {
			return err
		}

		err = s.removeDirFromIRODS(userSet, path, baton, transformer)
		if err != nil {
			return err
		}

		err = s.db.RemoveDirEntries(userSet.ID(), path)
		if err != nil {
			return err
		}
	}

	return nil
}

// func (s *Server) removeFromIRODSandDB(userSet *set.Set, dirpaths, filepaths []string) error {
// 	err := s.removePathsFromIRODS(userSet, dirpaths, filepaths)
// 	if err != nil {
// 		return err
// 	}

// 	err = s.removePathsFromDB(userSet, dirpaths, filepaths)
// 	if err != nil {
// 		return err
// 	}

// 	return s.db.SetNewCounts(userSet.ID())

// }

// func (s *Server) removePathsFromIRODS(set *set.Set, dirpaths, filepaths []string) error {
// 	baton, transformer, err := s.getBatonAndTransformer(set)
// 	if err != nil {
// 		return err
// 	}

// 	err = s.removeFilesFromIRODS(set, filepaths, baton, transformer)
// 	if err != nil {
// 		return err
// 	}

// 	return s.removeDirsFromIRODS(set, dirpaths, baton, transformer)
// }

// func (s *Server) removePathsFromDB(set *set.Set, dirpaths, filepaths []string) error {
// 	err := s.db.RemoveDirEntries(set.ID(), dirpaths)
// 	if err != nil {
// 		return err
// 	}

// 	return s.db.RemoveFileEntries(set.ID(), filepaths)
// }

// bindPathsAndValidateSet gets the paths out of the JSON body, and the set id
// from the URL parameter if Requester matches logged-in username.
func (s *Server) bindPathsAndValidateSet(c *gin.Context) (string, []string, bool) {
	var bpaths [][]byte

	if err := c.BindJSON(&bpaths); err != nil {
		c.AbortWithError(http.StatusBadRequest, err) //nolint:errcheck

		return "", nil, false
	}

	set, ok := s.validateSet(c)
	if !ok {
		return "", nil, false
	}

	return set.ID(), bytesToStrings(bpaths), true
}

// parseRemoveParamsAndValidateSet gets the file paths and dir paths out of the
// JSON body, and the set id from the URL parameter if Requester matches
// logged-in username.
func (s *Server) parseRemoveParamsAndValidateSet(c *gin.Context) (string, []string, []string, bool) {
	bmap := make(map[string][][]byte)

	if err := c.BindJSON(&bmap); err != nil {
		c.AbortWithError(http.StatusBadRequest, err) //nolint:errcheck

		return "", nil, nil, false
	}

	set, ok := s.validateSet(c)
	if !ok {
		return "", nil, nil, false
	}

	return set.ID(), bytesToStrings(bmap[fileKeyForJSON]), bytesToStrings(bmap[dirKeyForJSON]), true
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

	if !s.AllowedAccess(c, set.Requester) {
		c.AbortWithError(http.StatusUnauthorized, ErrBadRequester) //nolint:errcheck

		return nil, false
	}

	return set, true
}

// bytesToStrings converts [][]byte to []string so that json unmarshalling
// doesn't mess with non-UTF8 characters.
func bytesToStrings(b [][]byte) []string {
	s := make([]string, len(b))

	for i, value := range b {
		s[i] = string(value)
	}

	return s
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

	entries := make([]*set.Dirent, len(paths))

	for n, path := range paths {
		entries[n] = &set.Dirent{
			Path: path,
			Mode: os.ModeDir,
		}
	}

	err := s.db.SetDirEntries(sid, entries)
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

	for _, entry := range entries {
		entry.MakeSafeForJSON()
	}

	c.JSON(http.StatusOK, entries)
}

// getExampleEntry gets the first defined file entry for the set with the
// id specified in the URL parameter.
//
// LoadSetDB() must already have been called. This is called when there is a GET
// on /rest/v1/auth/example_entry/[id].
func (s *Server) getExampleEntry(c *gin.Context) {
	set, ok := s.validateSet(c)
	if !ok {
		return
	}

	entry, err := s.db.GetDefinedFileEntry(set.ID())
	if err != nil {
		c.AbortWithError(http.StatusBadRequest, err) //nolint:errcheck

		return
	}

	if entry != nil {
		entry.MakeSafeForJSON()
	}

	c.JSON(http.StatusOK, entry)
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

	for _, entry := range entries {
		entry.MakeSafeForJSON()
	}

	c.JSON(http.StatusOK, entries)
}

// getFailedEntries gets up to 10 file entries with failed status for the set
// with the id specified in the URL parameter. Returned in a struct that also
// has Skipped, the number of failed entries not returned.
//
// LoadSetDB() must already have been called. This is called when there is a GET
// on /rest/v1/auth/failed_entries/[id].
func (s *Server) getFailedEntries(c *gin.Context) {
	set, ok := s.validateSet(c)
	if !ok {
		return
	}

	entries, skipped, err := s.db.GetFailedEntries(set.ID())
	if err != nil {
		c.AbortWithError(http.StatusBadRequest, err) //nolint:errcheck

		return
	}

	for _, entry := range entries {
		entry.MakeSafeForJSON()
	}

	c.JSON(http.StatusOK, &FailedEntries{
		Entries: entries,
		Skipped: skipped,
	})
}

// getRequests gets up to 100 Requests from the global in-memory put-queue (as
// populated during set discoveries).
//
// LoadSetDB() must already have been called. This is called when there is a GET
// on /rest/v1/auth/requests. Only the user who started the Server has
// permission to call this.
func (s *Server) getRequests(c *gin.Context) {
	if !s.AllowedAccess(c, "") {
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

// reserveRequests keeps reserving items from our queue until we have total
// requests/s.numClients (but max 100) of them, or the queue is empty.
//
// Returns the Requests in the items.
func (s *Server) reserveRequests() ([]*put.Request, error) {
	n := s.getCachedNumRequestsToReserve()
	requests := make([]*put.Request, 0, n)
	count := 0

	for {
		r, err := s.reserveRequest()
		if err != nil {
			return nil, err
		}

		if r == nil {
			break
		}

		r.MakeSafeForJSON()
		requests = append(requests, r)

		count++
		if count == n {
			break
		}
	}

	return requests, nil
}

// getCachedNumRequestsToReserve calls numRequestsToReserve and caches the
// result for the remaining numClients to use, so numClients clients all
// reserve the same amount.
func (s *Server) getCachedNumRequestsToReserve() int {
	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()

	if len(s.numRequestsCache) > 0 {
		a := s.numRequestsCache
		n, a := a[len(a)-1], a[:len(a)-1]
		s.numRequestsCache = a

		return n
	}

	n := s.numRequestsToReserve()
	if n == 0 {
		return n
	}

	s.numRequestsCache = make([]int, s.numClients-1)

	for i := range s.numRequestsCache {
		s.numRequestsCache[i] = n
	}

	return n
}

// numRequestsToReserve returns the number of requests we should reserve taking
// in to account the number of items ready to be reserved, the number of clients
// we could have running, and our maximum of maxRequestsToReserve.
func (s *Server) numRequestsToReserve() int {
	ready := s.queue.Stats().Ready
	if ready == 0 {
		return 0
	}

	n := int(math.Ceil(float64(ready) / float64(s.numClients)))

	if n > maxRequestsToReserve {
		n = maxRequestsToReserve
	}

	return n
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

// putWorking interprets the body as a JSON encoding of a []string of Request
// ids retrieved from getRequests().
//
// For each request, touches it in the queue. Due to timing issues, does not
// return an error if some of the requests were not running in the queue.
//
// LoadSetDB() must already have been called. This is called when there is a PUT
// on /rest/v1/auth/working. Only the user who started the Server has permission
// to call this.
func (s *Server) putWorking(c *gin.Context) {
	if !s.AllowedAccess(c, "") {
		c.AbortWithError(http.StatusUnauthorized, ErrNotAdmin) //nolint:errcheck

		return
	}

	var rids []string

	if err := c.BindJSON(&rids); err != nil {
		c.AbortWithError(http.StatusBadRequest, err) //nolint:errcheck

		return
	}

	var err error

	fails := 0

	for _, rid := range rids {
		if thisErr := s.touchRequest(rid); thisErr != nil {
			err = thisErr
			fails++
		}
	}

	if err != nil {
		s.Logger.Printf("failed to touch %d/%d requests; example error: %s", fails, len(rids), err)
	}

	c.Status(http.StatusOK)
}

// touchRequest returns an error unless a request with the given ID is in the
// "run" sub-queue of our global put queue, ie. it was reserved by
// reserveRequest().
//
// If no error, extends the time that some client can work on this request
// before we consider the client dead and we release it to be be reserved by a
// different client.
func (s *Server) touchRequest(rid string) error {
	return s.queue.Touch(rid)
}

// codeAndError is used for sending a http status code and error over a channel.
type codeAndError struct {
	code int
	err  error
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

	s.Logger.Printf("got a putFileStatus")

	if err := c.BindJSON(r); err != nil {
		c.AbortWithError(http.StatusBadRequest, err) //nolint:errcheck
		s.Logger.Printf("no request sent to putFileStatus")

		return
	}

	if !s.AllowedAccess(c, "") {
		c.AbortWithError(http.StatusUnauthorized, ErrNotAdmin) //nolint:errcheck
		s.Logger.Printf("denied access during file status update for %s", r.Local)

		return
	}

	r.CorrectFromJSON()

	ceCh := s.queueFileStatusUpdate(r)
	ce := <-ceCh

	if ce != nil {
		c.AbortWithError(ce.code, ce.err) //nolint:errcheck

		return
	}

	c.Status(http.StatusOK)
}

// fileStatusPacket contains a Request and codeAndError channel for sending over
// a channel.
type fileStatusPacket struct {
	r    *put.Request
	ceCh chan *codeAndError
}

// queueFileStatusUpdate queues a file status update request from a client,
// sending it to the channel handleFileStatusUpdates() reads from.
func (s *Server) queueFileStatusUpdate(r *put.Request) chan *codeAndError {
	ceCh := make(chan *codeAndError)

	go func() {
		s.statusUpdateCh <- &fileStatusPacket{
			r:    r,
			ceCh: ceCh,
		}
	}()

	return ceCh
}

// handleFileStatusUpdates linearises file status updates by continueously
// looping over update requests coming in from clients and ensuring they are
// dealt with fully before dealing with the next request.
func (s *Server) handleFileStatusUpdates() {
	for fsp := range s.statusUpdateCh {
		trace := grand.LcString(logTraceIDLen)

		s.Logger.Printf("[%s] will update status of %s", trace, fsp.r.Local)

		if err := s.touchRequest(fsp.r.ID()); err != nil {
			s.Logger.Printf("[%s] touch failed for: %s", trace, err)
			fsp.ceCh <- &codeAndError{code: http.StatusBadRequest, err: err}

			continue
		}

		if err := s.updateFileStatus(fsp.r, trace); err != nil {
			s.Logger.Printf("[%s] update failed: %s", trace, err)
			fsp.ceCh <- &codeAndError{code: http.StatusBadRequest, err: err}

			continue
		}

		s.Logger.Printf("[%s] update succeeded", trace)
		fsp.ceCh <- nil
	}
}

// updateFileStatus updates the request's file entry status in the db, and
// removes the request from our queue if not still uploading or no longer in the
// set. Possibly stuck requests are noted in the server's in-memory list of
// stuck requests.
//
// The supplied trace string is used in logging output.
func (s *Server) updateFileStatus(r *put.Request, trace string) error {
	entry, err := s.db.SetEntryStatus(r)
	if err != nil {
		var errr error

		errs := &set.Error{}
		if errors.As(err, errs) && errs.Msg == set.ErrInvalidEntry {
			errr = s.removeOrReleaseRequestFromQueue(r, entry)
		}

		return errors.Join(err, errr)
	}

	if err = s.handleNewlyCompletedSets(r); err != nil {
		return err
	}

	return s.trackUploadingAndStuckRequests(r, trace, entry)
}

// handleNewlyCompletedSets gets the set the given request is for, and if it
// has completed, carries out actions needed for newly completed sets: trigger
// the monitoring countdown, and do a database backup.
func (s *Server) handleNewlyCompletedSets(r *put.Request) error {
	completed, err := s.db.GetByNameAndRequester(r.Set, r.Requester)
	if err != nil {
		return err
	}

	if completed.Status != set.Complete {
		return nil
	}

	s.monitorSet(completed)
	s.tryBackup()

	return nil
}

func (s *Server) trackUploadingAndStuckRequests(r *put.Request, trace string, entry *set.Entry) error {
	if r.Status == put.RequestStatusUploading {
		s.uploadTracker.uploadStarting(r)

		s.Logger.Printf("[%s] uploading, called uploadStarting()", trace)

		return nil
	}

	s.uploadTracker.uploadFinished(r)

	s.Logger.Printf("[%s] will remove/release; called uploadFinished()", trace)

	return s.removeOrReleaseRequestFromQueue(r, entry)
}

// removeOrReleaseRequestFromQueue removes the given Request from our queue
// unless it has failed. < 3 failures results in it being released, 3 results in
// it being buried.
func (s *Server) removeOrReleaseRequestFromQueue(r *put.Request, entry *set.Entry) error {
	if r.Status == put.RequestStatusFailed {
		s.updateQueueItemData(r)

		if entry.Attempts%set.AttemptsToBeConsideredFailing == 0 {
			return s.queue.Bury(r.ID())
		}

		return s.queue.Release(context.Background(), r.ID())
	}

	return s.queue.Remove(context.Background(), r.ID())
}

// updateQueueItemData updates the item in our queue corresponding to the
// given request, with the request's latest properties.
func (s *Server) updateQueueItemData(r *put.Request) {
	if item, err := s.queue.Get(r.ID()); err == nil {
		stats := item.Stats()
		s.queue.Update(context.Background(), item.Key, "", r, stats.Priority, stats.Delay, stats.TTR) //nolint:errcheck
	}
}

// recoverQueue is used at startup to fill the in-memory queue with requests for
// sets we were in the middle of working on before.
func (s *Server) recoverQueue() error {
	sets, err := s.db.GetAll()
	if err != nil {
		return err
	}

	for _, given := range sets {
		err = s.recoverSet(given)
		if err != nil {
			given.RecoveryError(err)
			s.Logger.Printf("failed to recover set %s for %s: %s", given.Name, given.Requester, err)
		}
	}

	s.sendSlackMessage(slack.Success, "recovery completed")

	return nil
}

// recoverSet checks the status of the given Set and recovers its state as
// appropriate: discover it if it was previously in the middle of being
// discovered; adds its remaining upload requests if it was previously in the
// middle of uploading; otherwise do nothing.
func (s *Server) recoverSet(given *set.Set) error {
	if given.StartedDiscovery.After(given.LastDiscovery) {
		return s.discoverSet(given)
	}

	s.monitorSet(given)

	var transformer put.PathTransformer

	var err error

	if given.LastDiscovery.After(given.LastCompleted) {
		transformer, err = given.MakeTransformer()
		if err != nil {
			return err
		}

		err = s.enqueueSetFiles(given, transformer)
	}

	return err
}

// retryFailedEntries adds failed entires in the set with the id specified in
// the URL parameter back to the in-memory ready queue.
//
// LoadSetDB() must already have been called. This is called when there is a GET
// on /rest/v1/auth/retry/[id].
func (s *Server) retryFailedEntries(c *gin.Context) {
	got, ok := s.validateSet(c)
	if !ok {
		return
	}

	failed, err := s.retryFailedSetFiles(got)
	if err != nil {
		c.AbortWithError(http.StatusBadRequest, err) //nolint:errcheck

		return
	}

	c.JSON(http.StatusOK, failed)
}

// retryFailedSetFiles gets all failed entries in the given set and adds them to
// the global put queue if not already there, or kicks them if they are.
func (s *Server) retryFailedSetFiles(given *set.Set) (int, error) {
	transformer, err := given.MakeTransformer()
	if err != nil {
		return 0, err
	}

	s.RetryBuried(&BuriedFilter{
		User: given.Requester,
		Set:  given.Name,
	})

	entries, err := s.db.GetFileEntries(given.ID())
	if err != nil {
		return 0, err
	}

	var filtered []*set.Entry

	for _, entry := range entries {
		if entry.Status == set.Failed {
			filtered = append(filtered, entry)
		}
	}

	return len(filtered), s.enqueueEntries(filtered, given, transformer)
}
