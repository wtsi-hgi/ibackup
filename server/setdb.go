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
	"maps"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/VertebrateResequencing/wr/queue"
	"github.com/gin-gonic/gin"
	"github.com/viant/ptrie"
	gas "github.com/wtsi-hgi/go-authserver"
	"github.com/wtsi-hgi/ibackup/errs"
	"github.com/wtsi-hgi/ibackup/internal"
	"github.com/wtsi-hgi/ibackup/remove"
	"github.com/wtsi-hgi/ibackup/set"
	"github.com/wtsi-hgi/ibackup/slack"
	"github.com/wtsi-hgi/ibackup/transfer"
	"github.com/wtsi-hgi/ibackup/transformer"
)

const (
	setPath            = "/set"
	filePath           = "/files"
	dirPath            = "/dirs"
	entryPath          = "/entries"
	uploadedEntryPath  = "/uploaded_entries"
	lastStateEntryPath = "/laststate_entries"
	exampleEntryPath   = "/example_entries"
	failedEntryPath    = "/failed_entries"
	discoveryPath      = "/discover"
	requestsPath       = "/requests"
	workingPath        = "/working"
	fileStatusPath     = "/file_status"
	fileRetryPath      = "/retry"
	removePathsPath    = "/remove_paths"
	trashPathsPath     = "/trash_paths"
	removeExpiredPath  = "/remove_expired"

	// EndPointAuthSet is the endpoint for getting and setting sets.
	EndPointAuthSet = gas.EndPointAuth + setPath

	// EndPointAuthFiles is the endpoint for setting set file paths.
	EndPointAuthFiles = gas.EndPointAuth + filePath

	// EndPointAuthDirs is the endpoint for setting set directory paths.
	EndPointAuthDirs = gas.EndPointAuth + dirPath

	// EndPointAuthEntries is the endpoint for getting set entries.
	EndPointAuthEntries = gas.EndPointAuth + entryPath

	// EndPointAuthUploadedEntries is the endpoint for getting set uploaded
	// entries.
	EndPointAuthUploadedEntries = gas.EndPointAuth + uploadedEntryPath

	// EndPointAuthLastStateEntries is the endpoint for getting uploaded entries
	// excluding orphaned entries.
	EndPointAuthLastStateEntries = gas.EndPointAuth + lastStateEntryPath

	// EndPointAuthExampleEntry is the endpoint for getting set entries.
	EndPointAuthExampleEntry = gas.EndPointAuth + exampleEntryPath

	// EndPointAuthFailedEntries is the endpoint for getting some failed set
	// entries.
	EndPointAuthFailedEntries = gas.EndPointAuth + failedEntryPath

	// EndPointAuthDiscovery is the endpoint for triggering set discovery which
	// will ultimately result in the upload of files missing remotely or changed
	// locally and will delete remote files missing locally if the set has
	// monitor-removals turned on, or if the force_removals parameter is passed
	// as true.
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

	// EndPointAuthRemovePaths is the endpoint for removing objects from sets.
	EndPointAuthRemovePaths = gas.EndPointAuth + removePathsPath

	// EndPointAuthTrashPaths is the endpoint for trashing objects from sets.
	EndPointAuthTrashPaths = gas.EndPointAuth + trashPathsPath

	// EndPointAuthRemoveExpired is the endpoint for removing expired objects
	// from sets.
	EndPointAuthRemoveExpired = gas.EndPointAuth + removeExpiredPath

	ErrNoAuth         = gas.Error("auth must be enabled")
	ErrBadRequester   = gas.Error("you are not the set requester")
	ErrEmptyName      = gas.Error("set name cannot be empty")
	ErrTrashSetName   = gas.Error("set name cannot be prefixed with " + set.TrashPrefix)
	ErrNotTrashed     = gas.Error("you cannot permanently remove from non trashed sets")
	ErrInvalidName    = gas.Error("set name contains invalid characters")
	ErrSetNotComplete = gas.Error("set should be complete for this operation")
	ErrNotAdmin       = gas.Error("you are not the server admin")
	ErrNoPaths        = gas.Error("no paths given")
	ErrBadSet         = gas.Error("set with that id does not exist")
	ErrInvalidInput   = gas.Error("invalid input")
	ErrInternal       = gas.Error("internal server error")
	ErrRelativePath   = gas.Error("path is not absolute")

	errMatchNotAncestor = gas.Error("removed folder is not an ancestor of the merged path")

	paramRequester    = "requester"
	paramSetID        = "id"
	paramMakeWritable = "makeWritable"

	numberOfFilesForOneHardlink = 2

	// maxRequestsToReserve is the maximum number of requests that
	// reserveRequests returns.
	maxRequestsToReserve = 100

	allUsers = "all"
)

// liveSets are the sets in our database of a remote object's requesters, and
// the sets in our database (of any requester) that have its path.
type liveSets struct {
	known   []*set.Set
	holders []*set.Set
}

// withoutStaleNames returns the given set names less those in added, other
// than the given one, that are stale: one of the object's requesters has a set
// of that name, and none of our sets of that name has the path.
func (l liveSets) withoutStaleNames(names, added []string, given string) []string {
	return slices.DeleteFunc(names, func(name string) bool {
		return name != given && slices.Contains(added, name) && l.isKnown(name) &&
			!slices.ContainsFunc(l.holders, func(h *set.Set) bool { return h.Name == name })
	})
}

// isKnown returns true if one of the object's requesters has a set of the given
// name.
func (l liveSets) isKnown(name string) bool {
	return slices.ContainsFunc(l.known, func(k *set.Set) bool { return k.Name == name })
}

// withoutStaleRequesters returns the given requesters less those in added,
// other than the given one, none of whose sets has the path.
func (l liveSets) withoutStaleRequesters(requesters, added []string, given string) []string {
	return slices.DeleteFunc(requesters, func(requester string) bool {
		return requester != given && slices.Contains(added, requester) &&
			!slices.ContainsFunc(l.holders, func(h *set.Set) bool { return h.Requester == requester })
	})
}

// allKnown returns true if every one of the given names, other than the given
// exception, isKnown().
func (l liveSets) allKnown(names []string, except string) bool {
	return !slices.ContainsFunc(names, func(name string) bool { return name != except && !l.isKnown(name) })
}

// liveSetsOfPath returns the liveSets of the remote object of the given path,
// which has the given requesters, reading the sets of those requesters
// (GetByRequester()) and the sets that have the path (GetAllSetsForFile() and
// GetByID()).
func (s *Server) liveSetsOfPath(path string, requesters []string) (liveSets, error) {
	var live liveSets

	for _, requester := range requesters {
		sets, err := s.db.GetByRequester(requester)
		if err != nil {
			return liveSets{}, err
		}

		live.known = append(live.known, sets...)
	}

	holderIDs, err := s.db.GetAllSetsForFile(path)
	if err != nil {
		return liveSets{}, err
	}

	for _, id := range holderIDs {
		if holder := s.db.GetByID(id); holder != nil {
			live.holders = append(live.holders, holder)
		}
	}

	return live, nil
}

func isRemoteFileMissing(err error) bool {
	pathErr, ok := errors.AsType[errs.PathError](err)

	return ok && pathErr.Msg == internal.ErrFileDoesNotExist
}

// withRecordedRemoteMeta returns a copy of the given remote metadata with any
// sets or requester names missing from its values added from the given
// recorded metadata, since an earlier attempt that recorded them may have
// removed them and then failed to add their new values. Something else may
// also have put back only some of the names since, as a client's upload of a
// late upload's retry adds just its own set and requester (see
// undoLateUploadOfObject()).
func withRecordedRemoteMeta(remoteMeta, recorded map[string]string) map[string]string {
	meta := make(map[string]string, len(remoteMeta))
	maps.Copy(meta, remoteMeta)

	for key, value := range recorded {
		names := splitCommaSeparatedUnique(meta[key])

		for _, name := range splitCommaSeparatedUnique(value) {
			names = appendIfMissing(names, name)
		}

		meta[key] = strings.Join(names, ",")
	}

	return meta
}

// addedNames returns the names in the given key's value of merged that are not
// in that of original.
func addedNames(original, merged map[string]string, key string) []string {
	had := splitCommaSeparatedUnique(original[key])

	return slices.DeleteFunc(splitCommaSeparatedUnique(merged[key]), func(name string) bool {
		return slices.Contains(had, name)
	})
}

// requestSet returns a set with just the name and requester of the given
// request's set, enough for its ID.
func requestSet(r *transfer.Request) *set.Set {
	return &set.Set{Name: r.Set, Requester: r.Requester}
}

// lateUploadEntry returns an uploaded entry for the given request's file. Its
// inode is unknown, so a hardlink's inode file is kept or removed by looking
// for other remote hardlinks to it, as when its inode record has gone.
func lateUploadEntry(r *transfer.Request) *set.Entry {
	entry := &set.Entry{
		Path:        r.Local,
		Size:        r.UploadedSize(),
		Status:      set.Uploaded,
		Type:        set.Regular,
		LastAttempt: time.Now(),
	}

	switch {
	case r.Symlink != "":
		entry.Type = set.Symlink
		entry.Dest = r.Symlink
	case r.Hardlink != "":
		entry.Type = set.Hardlink
	}

	return entry
}

// withLiveRecordedRemoteMeta returns withRecordedRemoteMeta() of the given
// metadata of the given path's remote object, less any set or requester name
// it added back that our database shows is stale. Another set's removal may
// have taken its name off the object since the names were recorded, and adding
// that back would keep the object, held by no set, for ever.
//
// A set name is stale if one of our sets has that name and one of the
// object's requesters, and none of our sets of that name has the path. Other
// names, of deleted sets or another server's, are kept, as an object kept by
// another server's set is not ours to remove (unless our database has a set
// of the same name and requester, which we can't tell apart). A requester is
// stale if none of the requester's sets has the path, and every set name the
// object keeps is that of one of our sets with one of the object's
// requesters, since a name not known for one of them could be the
// requester's. The given set's name and requester are kept for the caller to
// decide on.
//
// A set's entry for the path goes from our database only after its removal
// has updated the remote object, so a name is only taken as stale once its
// set's update is done. A late upload (see undoLateUploadOfObject()) can add a
// set's name after that, but its undo removes the name anyway.
func (s *Server) withLiveRecordedRemoteMeta(path string, given *set.Set,
	remoteMeta, recorded map[string]string,
) (map[string]string, error) {
	meta := withRecordedRemoteMeta(remoteMeta, recorded)

	addedSets := addedNames(remoteMeta, meta, transfer.MetaKeySets)
	addedRequesters := addedNames(remoteMeta, meta, transfer.MetaKeyRequester)

	if len(addedSets)+len(addedRequesters) == 0 {
		return meta, nil
	}

	requesters := splitCommaSeparatedUnique(meta[transfer.MetaKeyRequester])

	live, err := s.liveSetsOfPath(path, requesters)
	if err != nil {
		return nil, err
	}

	names := live.withoutStaleNames(splitCommaSeparatedUnique(meta[transfer.MetaKeySets]), addedSets, given.Name)

	if live.allKnown(names, given.Name) {
		requesters = live.withoutStaleRequesters(requesters, addedRequesters, given.Requester)
	}

	meta[transfer.MetaKeySets] = strings.Join(names, ",")
	meta[transfer.MetaKeyRequester] = strings.Join(requesters, ",")

	return meta, nil
}

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

	if s.readOnly {
		return nil
	}

	s.statusUpdateCh = make(chan *fileStatusPacket)
	go s.handleFileStatusUpdates()

	s.startHungDebug()

	return s.recoverQueue()
}

func (s *Server) setupDB(path, backupPath string, authGroup *gin.RouterGroup) error {
	s.sendSlackMessage(slack.Info, "server starting, loading database")

	db, err := set.New(path, backupPath, s.readOnly)
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
func (s *Server) EnableRemoteDBBackups(remotePath string, handler transfer.Handler) {
	s.db.EnableRemoteBackups(remotePath, handler)
}

// addDBEndpoints adds all the REST API endpoints to the given router group.
func (s *Server) addDBEndpoints(authGroup *gin.RouterGroup) { //nolint:funlen
	authGroup.GET(setPath+"/:"+paramRequester, s.getSets)

	idParam := "/:" + paramSetID

	authGroup.GET(discoveryPath+idParam, s.triggerDiscovery)

	authGroup.GET(entryPath+idParam, s.getEntries)
	authGroup.GET(uploadedEntryPath+idParam, s.getUploadedEntries)
	authGroup.GET(lastStateEntryPath+idParam, s.getLastStateEntries)

	authGroup.GET(exampleEntryPath+idParam, s.getExampleEntry)

	authGroup.GET(dirPath+idParam, s.getDirs)
	authGroup.GET(failedEntryPath+idParam, s.getFailedEntries)

	authGroup.GET(requestsPath, s.getRequests)
	authGroup.GET(fileRetryPath+idParam, s.retryFailedEntries)

	if s.readOnly {
		return
	}

	authGroup.PUT(setPath, s.putSet)

	authGroup.PUT(filePath+idParam, s.putFiles)

	authGroup.PUT(dirPath+idParam, s.putDirs)

	authGroup.PUT(workingPath, s.putWorking)

	authGroup.PUT(fileStatusPath, s.putFileStatus)

	authGroup.DELETE(removePathsPath+idParam, s.removePaths)

	authGroup.PUT(trashPathsPath+idParam, s.trashPaths)

	authGroup.DELETE(removeExpiredPath, s.removeExpired)
	authGroup.DELETE(removeExpiredPath+idParam, s.removeExpired)

	authGroup.DELETE(setPath+idParam, s.deleteSet)
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

	if c.DefaultQuery(paramMakeWritable, "false") == "true" {
		given.ReadOnly = false

		err := s.makeSetWritable(c, given.ID())
		if err != nil {
			return
		}
	}

	statusCode, err := s.validatePutSetInputs(c, given)
	if err != nil {
		c.AbortWithError(statusCode, err) //nolint:errcheck

		return
	}

	err = s.db.AddOrUpdate(given)
	if err != nil {
		c.AbortWithError(addOrUpdateErrStatus(err), err) //nolint:errcheck

		return
	}

	s.handleNewlyDefinedSets(given)

	c.Status(http.StatusOK)
}

// addOrUpdateErrStatus returns the HTTP status for the given error from
// AddOrUpdate(). Its set.Errors reject the user's change, such as editing a
// read-only set or a transformer already used, so are bad requests; anything
// else is a database failure.
func addOrUpdateErrStatus(err error) int {
	if _, ok := errors.AsType[set.Error](err); ok { //nolint:errcheck
		return http.StatusBadRequest
	}

	return http.StatusInternalServerError
}

func (s *Server) validatePutSetInputs(c *gin.Context, givenSet *set.Set) (int, error) {
	if !s.AllowedAccess(c, givenSet.Requester) {
		return http.StatusUnauthorized, ErrBadRequester
	}

	if givenSet.Name == "" {
		return http.StatusBadRequest, ErrEmptyName
	}

	if givenSet.IsTrash() {
		return http.StatusBadRequest, ErrTrashSetName
	}

	if strings.ContainsRune(givenSet.Name, ',') {
		return http.StatusBadRequest, ErrInvalidName
	}

	_, err := givenSet.MakeTransformer()

	return http.StatusBadRequest, err
}

func (s *Server) makeSetWritable(c *gin.Context, sid string) error {
	if !s.AllowedAccess(c, "") {
		c.AbortWithError(http.StatusUnauthorized, ErrNotAdmin) //nolint:errcheck

		return ErrNotAdmin
	}

	err := s.db.MakeSetWritable(sid)
	if err != nil {
		c.AbortWithError(http.StatusInternalServerError, err) //nolint:errcheck
	}

	return err
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

	if !s.AllowedAccess(c, "") {
		sets = filterTrashed(sets)
	}

	if err != nil {
		c.AbortWithError(http.StatusBadRequest, err) //nolint:errcheck

		return
	}

	c.JSON(http.StatusOK, sets)
}

func filterTrashed(sets []*set.Set) []*set.Set {
	filtered := make([]*set.Set, 0, len(sets))

	for _, s := range sets {
		if strings.HasPrefix(s.Name, set.TrashPrefix) {
			continue
		}

		filtered = append(filtered, s)
	}

	return filtered
}

// putFiles sets the file paths encoded in to the body as JSON as the files to
// be backed up for the set with the id specified in the URL parameter.
//
// LoadSetDB() must already have been called. This is called when there is a PUT
// on /rest/v1/auth/files/[id].
func (s *Server) putFiles(c *gin.Context) {
	givenSet, paths, ok := s.bindPathsAndValidateSet(c)
	if !ok {
		return
	}

	err := s.db.IsSetReadyToAddFiles(givenSet)
	if err != nil {
		c.AbortWithError(http.StatusBadRequest, err) //nolint:errcheck

		return
	}

	err = s.db.MergeFileEntries(givenSet.ID(), paths)
	if err != nil {
		c.AbortWithError(http.StatusBadRequest, err) //nolint:errcheck

		return
	}

	c.Status(http.StatusOK)
}

func (s *Server) removePaths(c *gin.Context) {
	givenSet, paths, ok := s.bindPathsAndValidateSet(c)
	if !ok {
		return
	}

	filePaths, dirPaths, httpStatus, err := s.validateInputsForRemovals(givenSet, paths, c)
	if err != nil {
		c.AbortWithError(httpStatus, err) //nolint:errcheck

		return
	}

	err = s.removeFilesAndDirs(givenSet, filePaths, dirPaths, set.ToRemove)
	if err != nil {
		c.AbortWithError(http.StatusBadRequest, err) //nolint:errcheck

		return
	}
}

// validateInputsForRemovals returns an error if the provided set is not a
// trashed set, the user is not an admin, no paths are provided or any provided
// path is not in the given set. Also returns the valid paths classified into a
// slice of filepaths or dirpaths.
func (s *Server) validateInputsForRemovals(givenSet *set.Set, paths []string,
	c *gin.Context) ([]string, []string, int, error) {
	if !givenSet.IsTrash() {
		return nil, nil, http.StatusBadRequest, ErrNotTrashed
	}

	if !s.AllowedAccess(c, "") {
		return nil, nil, http.StatusUnauthorized, ErrNotAdmin
	}

	if len(paths) == 0 {
		return nil, nil, http.StatusBadRequest, ErrNoPaths
	}

	files, dirs, err := s.db.ValidateFileAndDirPaths(givenSet, paths)

	return files, dirs, http.StatusBadRequest, err
}

func (s *Server) trashPaths(c *gin.Context) {
	givenSet, paths, ok := s.bindPathsAndValidateSet(c)
	if !ok {
		return
	}

	filePaths, dirPaths, err := s.validateInputsForTrashing(givenSet, paths)
	if err != nil {
		c.AbortWithError(http.StatusBadRequest, err) //nolint:errcheck

		return
	}

	err = s.removeToTrashSet(givenSet, filePaths, dirPaths)
	if err != nil {
		c.AbortWithError(http.StatusBadRequest, err) //nolint:errcheck

		return
	}
}

func (s *Server) removeToTrashSet(givenSet *set.Set, filePaths []string, dirPaths []string) error {
	trashSet := set.BuildTrashSetFromSet(givenSet)

	err := s.db.AddOrUpdate(&trashSet)
	if err != nil {
		return err
	}

	err = s.removeFilesAndDirs(givenSet, filePaths, dirPaths, set.ToTrash)
	if err != nil {
		return err
	}

	return nil
}

func (s *Server) deleteSet(c *gin.Context) {
	set, ok := s.validateSet(c)
	if !ok {
		return
	}

	paths, err := s.getSetPaths(set.ID())
	if err != nil {
		c.AbortWithError(http.StatusInternalServerError, err) //nolint:errcheck

		return
	}

	s.registerDeletionCallback(set)

	err = s.removeToTrashSet(set, paths, nil)
	if err != nil {
		c.AbortWithError(http.StatusInternalServerError, err) //nolint:errcheck

		return
	}

	s.removeQueuedRequestsOfSet(set)
}

func (s *Server) removeExpired(c *gin.Context) {
	sid := c.Param(paramSetID)

	if !s.AllowedAccess(c, "") {
		c.AbortWithError(http.StatusUnauthorized, ErrNotAdmin) //nolint:errcheck

		return
	}

	var sets []*set.Set

	var err error

	if sid != "" { //nolint:nestif
		givenSet, ok := s.validateSet(c)
		if !ok {
			return
		}

		sets = append(sets, givenSet)
	} else {
		sets, err = s.db.GetTrashedSets()
		if err != nil {
			c.AbortWithError(http.StatusInternalServerError, err) //nolint:errcheck

			return
		}
	}

	err = s.removeExpiredEntriesFromSets(sets)
	if err != nil {
		c.AbortWithError(http.StatusInternalServerError, err) //nolint:errcheck

		return
	}
}

func (s *Server) removeExpiredEntriesFromSets(sets []*set.Set) error {
	for _, set := range sets {
		err := s.removeExpiredEntriesFromSet(set)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Server) removeExpiredEntriesFromSet(givenSet *set.Set) error {
	filter := func(e *set.Entry) bool {
		entryAge := time.Since(e.TrashDate)

		return entryAge >= s.trashLifespan
	}

	expiredFiles, err := s.db.GetFileEntries(givenSet.ID(), filter)
	if err != nil {
		return err
	}

	expiredDirs, err := s.db.GetDirEntries(givenSet.ID(), filter)
	if err != nil {
		return err
	}

	filePaths := make([]string, len(expiredFiles))
	for i, e := range expiredFiles {
		filePaths[i] = e.Path
	}

	dirPaths := make([]string, len(expiredDirs))
	for i, e := range expiredDirs {
		dirPaths[i] = e.Path
	}

	// a removal of nothing would still start a new removal, wiping the set's
	// status of its last one.
	if len(filePaths) == 0 && len(dirPaths) == 0 {
		return nil
	}

	return s.removeFilesAndDirs(givenSet, filePaths, dirPaths, set.ToRemove)
}

func (s *Server) getSetPaths(setID string) ([]string, error) {
	entries, err := s.db.GetFileEntries(setID, nil)
	if err != nil {
		return nil, err
	}

	paths := make([]string, len(entries))

	for i, entry := range entries {
		paths[i] = entry.Path
	}

	return paths, nil
}

// registerDeletionCallback arranges for the given set to be deleted from the
// database once its removals are done, along with any of its upload requests
// queued since deleteSet removed them, such as by a discovery that was running.
// It doesn't wait for such a discovery, which could take hours; one that
// queues requests after this cleanup has them dropped by updateFileStatus once
// they are uploaded.
func (s *Server) registerDeletionCallback(given *set.Set) {
	s.discoveryCoordinator.OnRemovalsDone(given.ID(), func() {
		err := s.db.Delete(given.ID())
		if err != nil {
			s.Logger.Print(err.Error())

			return
		}

		s.removeQueuedRequestsOfSet(given)
	})
}

// removeQueuedRequestsOfSet removes from our queue the upload requests of the
// given set that no client is working on, so clients don't upload the files of
// a deleted set. Results for those being worked on are dropped by
// updateFileStatus.
func (s *Server) removeQueuedRequestsOfSet(given *set.Set) {
	for _, item := range s.queue.AllItems() {
		r, ok := item.Data().(*transfer.Request)
		if !ok || r.Set != given.Name || r.Requester != given.Requester {
			continue
		}

		if err := s.removeWaitingRequest(item, r); err != nil {
			s.Logger.Printf("queued request of deleted set not removed rid=%s err=%s", item.Key, err)
		}
	}
}

// removeWaitingRequest removes the given item, holding the given upload request,
// from our queue unless a client is working on it, and releases any claim the
// request still has on a remote data object from a reservation that expired, so
// other sets' requests for it aren't held up. The check and the removal happen
// under the queue's lock, so a client can't reserve the request in between and
// then have its claim released. An item already gone from the queue is kept,
// so that a newer request queued since under the same key isn't removed.
func (s *Server) removeWaitingRequest(item *queue.Item, r *transfer.Request) error {
	removed, err := s.queue.RemoveUnless(context.Background(), item.Key, func(any) bool {
		state := item.State()

		return state == queue.ItemStateRun || state == queue.ItemStateRemoved
	})
	if removed {
		err = errors.Join(err, s.remoteClaims.release(r))
	}

	return err
}

// removeWaitingUploadOfFile removes from our queue the given set's upload
// request for the given local and remote paths, unless a client is working on
// it, so that a retry waiting there, or buried and later kicked by a retry of
// the set, doesn't upload the file again once it has been removed.
func (s *Server) removeWaitingUploadOfFile(given *set.Set, local, remote string) {
	r := &transfer.Request{Local: local, Remote: remote, Requester: given.Requester, Set: given.Name}

	item, err := s.queue.Get(r.ID())
	if err != nil {
		return
	}

	if err = s.removeWaitingRequest(item, r); err != nil {
		s.Logger.Printf("queued request of removed file not removed rid=%s err=%s", item.Key, err)
	}
}

// validateInputsForTrashing returns an error if the provided set is a trashed
// set or not complete, no paths are provided or any provided path is not in the
// given set. Also returns the valid paths classified into a slice of filepaths
// or dirpaths.
func (s *Server) validateInputsForTrashing(givenSet *set.Set, paths []string) ([]string, []string, error) {
	if givenSet.IsTrash() {
		return nil, nil, ErrTrashSetName
	}

	if givenSet.Status != set.Complete {
		return nil, nil, ErrSetNotComplete
	}

	if len(paths) == 0 {
		return nil, nil, ErrNoPaths
	}

	return s.db.ValidateFileAndDirPaths(givenSet, paths)
}

// removeFilesAndDirs queues the removal of the given paths from the given set,
// starts handling them, and adds their number to the set's removal counts. It
// holds removalSubmitMu throughout, so a handler, which waits for it after
// reserving a request, can't count a request removed before that number
// includes it.
func (s *Server) removeFilesAndDirs(set *set.Set, filePaths, dirPaths []string, action set.RemoveAction) error {
	s.removalSubmitMu.Lock()
	defer s.removalSubmitMu.Unlock()

	if filePaths == nil && dirPaths == nil {
		return s.db.ResetRemoveSize(set.ID())
	}

	numFilesSubmitted, err := s.submitFilesForRemoval(set, filePaths, action)
	if err != nil {
		return err
	}

	numObjsSubmitted, err := s.submitDirsForRemoval(set, dirPaths, action)
	if err != nil {
		return err
	}

	go s.handleRemoveRequests(set.ID())

	if s.removalQueuedHook != nil {
		s.removalQueuedHook()
	}

	return s.db.UpdateSetTotalToRemove(set.ID(), uint64(numFilesSubmitted+numObjsSubmitted)) //nolint:gosec
}

func (s *Server) submitFilesForRemoval(set *set.Set, paths []string, action set.RemoveAction) (int, error) {
	if len(paths) == 0 {
		return 0, nil
	}

	defs, remReqs := buildRemovalStructsFromPaths(set, paths, false, action)

	err := s.db.SetRemoveRequests(set.ID(), remReqs)
	if err != nil {
		return 0, err
	}

	added, _, err := s.removeQueue.AddMany(context.Background(), defs)

	return added, err
}

func buildRemovalStructsFromPaths(givenSet *set.Set, paths []string, isDir bool,
	action set.RemoveAction,
) ([]*queue.ItemDef, []set.RemoveReq) {
	remReqs := make([]set.RemoveReq, len(paths))
	defs := make([]*queue.ItemDef, len(paths))

	for i, path := range paths {
		rq := set.NewRemoveRequest(path, givenSet, isDir, action)

		remReqs[i] = rq
		defs[i] = rq.ItemDef(ttr)
	}

	return defs, remReqs
}

func (s *Server) submitDirsForRemoval(set *set.Set, paths []string, action set.RemoveAction) (int, error) {
	if len(paths) == 0 {
		return 0, nil
	}

	defs, remReqs, err := s.makeItemsDefsFromDirPaths(set, paths, action)
	if err != nil {
		return 0, err
	}

	err = s.db.SetRemoveRequests(set.ID(), remReqs)
	if err != nil {
		return 0, err
	}

	added, _, err := s.removeQueue.AddMany(context.Background(), defs)

	return added, err
}

func (s *Server) makeItemsDefsFromDirPaths(givenSet *set.Set,
	paths []string, action set.RemoveAction,
) ([]*queue.ItemDef, []set.RemoveReq, error) {
	remReqs := make([]set.RemoveReq, 0, len(paths))
	defs := make([]*queue.ItemDef, 0, len(paths))

	// a dir an older server stored both with and without a trailing slash has
	// two stored keys, which share one removed-bucket key and are deleted by
	// one removal, and GetFoldersInDir() finds a dir stored with one inside
	// itself, so each dir gets one request.
	requested := make(map[string]bool)
	isNewDir := func(path string) bool {
		key := strings.TrimSuffix(path, "/")
		isNew := !requested[key]
		requested[key] = true

		return isNew
	}

	for _, path := range paths {
		if !isNewDir(path) {
			continue
		}

		rq := set.NewRemoveRequest(path, givenSet, true, action)

		dirFilepaths, err := s.db.GetFilesInDir(givenSet.ID(), path)
		if err != nil {
			return nil, nil, err
		}

		dirFolderPaths, err := s.db.GetFoldersInDir(givenSet.ID(), path)
		if err != nil {
			return nil, nil, err
		}

		fileDefs, fileRemoveReqs := buildRemovalStructsFromPaths(givenSet, dirFilepaths, false, action)
		folderDefs, folderRemoveReqs := buildRemovalStructsFromPaths(givenSet,
			slices.DeleteFunc(dirFolderPaths, func(folder string) bool { return !isNewDir(folder) }), true, action)

		remReqs = slices.Concat(remReqs, fileRemoveReqs, folderRemoveReqs)
		defs = slices.Concat(defs, fileDefs, folderDefs)

		remReqs = append(remReqs, rq)
		defs = append(defs, rq.ItemDef(ttr))
	}

	return defs, remReqs, nil
}

// removeFileFromIRODSandDB removes the given request's file from the remote
// storage, unless an earlier attempt already did, then from the database. The
// database side is one transaction that also marks the request complete, so a
// retry after a stop between the two only redoes the database side.
func (s *Server) removeFileFromIRODSandDB(removeReq *set.RemoveReq) error {
	entry, err := s.db.GetFileEntryForSet(removeReq.Set.ID(), removeReq.Path)
	if err != nil {
		return s.removeDeletedFileFromDB(removeReq, err)
	}

	err = s.processRemoteFileRemoval(removeReq, entry)
	if err != nil && !entry.WasNotUploaded() {
		return err
	}

	_, err = s.db.RemoveFileEntry(removeReq, entry)

	return err
}

// removeDeletedFileFromDB handles the given error from getting the entry of the
// given request's file. If the error is that the set has no such file, and the
// incomplete request's remote removal started, which needed the entry, a build
// before the one-transaction removal deleted the entry and then stopped, so it
// finishes the database side without the entry. Otherwise, such as for a path
// that was never in the set, or a request already complete, it returns the
// error.
func (s *Server) removeDeletedFileFromDB(removeReq *set.RemoveReq, err error) error {
	if !errors.Is(err, set.ErrInvalidEntry) || removeReq.IsComplete || removeReq.RemoteRemovalStatus == set.NotRemoved {
		return err
	}

	s.Logger.Printf("finishing removal of %s from set %s, whose entry an older build already removed",
		removeReq.Path, removeReq.Set.ID())

	return s.db.RemoveDeletedFileEntry(removeReq)
}

// processRemoteFileRemoval removes the given request's file from the remote
// storage, or updates its remote object's metadata, unless an earlier attempt
// already did. First, even on a retry after that, it removes the file's upload
// request from our queue, so that one queued again since (by a discovery, or a
// recovery after a restart) isn't handed out after the remote removal. A retry
// whose set's transformer can no longer be made (eg. after a restart with a
// config lacking it) skips that and returns nil, so its database side runs.
func (s *Server) processRemoteFileRemoval(removeReq *set.RemoveReq, entry *set.Entry) error {
	transformer, err := removeReq.Set.MakeTransformer()
	if err != nil {
		if removeReq.RemoteRemovalStatus == set.Removed {
			return nil
		}

		return err
	}

	rpath, err := transformer(removeReq.Path)

	s.removeWaitingUploadOfFile(removeReq.Set, removeReq.Path, rpath)

	if removeReq.RemoteRemovalStatus == set.Removed {
		return nil
	}

	mayMissInRemote := removeReq.RemoteRemovalStatus == set.AboutToBeRemoved && removeReq.Action == set.ToRemove

	remoteMeta, err := s.getRemoteMeta(rpath, err)

	if erru := s.markAboutToBeRemoved(removeReq, entry, remoteMeta, err); erru != nil {
		return erru
	}

	if err == nil {
		err = s.updateOrRemoveRemoteFile(removeReq, transformer, rpath, remoteMeta, entry)
	} else {
		err = s.handleRemoteFileMetaError(err, removeReq, rpath, transformer, entry)
	}

	if err != nil && fileErrorCannotBeIgnored(err, mayMissInRemote) {
		s.setErrorOnEntry(entry, removeReq.Set.ID(), removeReq.Path, set.EntryRemovalErrorPrefix+err.Error())

		return err
	}

	removeReq.RemoteRemovalStatus = set.Removed

	return s.db.UpdateRemoveRequest(*removeReq)
}

func fileErrorCannotBeIgnored(err error, mayMissInRemote bool) bool {
	return !(mayMissInRemote && strings.Contains(err.Error(), internal.ErrFileDoesNotExist)) //nolint:staticcheck,lll
}

// getRemoteMeta returns the metadata of the remote object at the given remote
// path, or the given error from transforming a local path to it.
func (s *Server) getRemoteMeta(rpath string, transformErr error) (map[string]string, error) {
	if transformErr != nil {
		return nil, transformErr
	}

	return s.storageHandler.GetMeta(rpath)
}

// markAboutToBeRemoved stores the given request as about to be removed, before
// anything changes the remote object, along with the size of the given entry
// and, if the given metadata was read without error, its sets and requester
// values, unless an earlier attempt already recorded them. Updating the
// object's metadata removes the old values before adding the new ones, so a
// retry after adding failed takes any value now missing from the object from
// these, instead of seeing an object with no sets, which it would remove.
func (s *Server) markAboutToBeRemoved(removeReq *set.RemoveReq, entry *set.Entry,
	remoteMeta map[string]string, metaErr error,
) error {
	removeReq.RemoteRemovalStatus = set.AboutToBeRemoved

	if removeReq.ObjectSize == nil {
		size := entry.Size
		removeReq.ObjectSize = &size
	}

	if metaErr == nil && removeReq.RemoteMeta == nil {
		removeReq.RemoteMeta = map[string]string{
			transfer.MetaKeySets:      remoteMeta[transfer.MetaKeySets],
			transfer.MetaKeyRequester: remoteMeta[transfer.MetaKeyRequester],
		}
	}

	return s.db.UpdateRemoveRequest(*removeReq)
}

// updateOrRemoveRemoteFile updates the sets and requester metadata of the given
// request's remote object, or removes it if no set would hold it any more.
// When trashing a file the set never uploaded (see Entry.WasNotUploaded()), it
// leaves the object alone: the set has no object to trash, so any object at the
// remote path is another set's, and the trash set gets no entry that would
// expire a trash tag on it.
func (s *Server) updateOrRemoveRemoteFile(removeReq *set.RemoveReq, transformer transformer.PathTransformer,
	rpath string, remoteMeta map[string]string, entry *set.Entry,
) error {
	if removeReq.Action == set.ToTrash && entry.WasNotUploaded() {
		return nil
	}

	metaBeforeUpdate, err := s.withLiveRecordedRemoteMeta(removeReq.Path, removeReq.Set, remoteMeta,
		removeReq.RemoteMeta)
	if err != nil {
		return err
	}

	var sets, requesters []string

	if removeReq.Action == set.ToRemove {
		sets, requesters, err = s.handleSetsAndRequesters(removeReq.Path, removeReq.Set, metaBeforeUpdate)
	} else {
		sets, requesters, err = s.handleSetMetadataForTrash(removeReq.Set, metaBeforeUpdate)
	}

	if err != nil {
		return err
	}

	if len(sets) == 0 && remoteMeta[transfer.MetaFOFNSet] == "" {
		return s.removeRemoteFileAndHandleHardlink(removeReq.Path, rpath, remoteMeta, transformer, entry)
	}

	return remove.UpdateSetsAndRequestersOnRemoteFile(s.storageHandler, rpath, sets, requesters, remoteMeta)
}

func (s *Server) removeRemoteFileAndHandleHardlink(lpath, rpath string, meta map[string]string,
	transformer transformer.PathTransformer, entry *set.Entry,
) error {
	err := remove.RemoveFileAndParentFoldersIfEmpty(s.storageHandler, rpath)
	if err != nil {
		var dirRemovalError errs.DirRemovalError
		if !errors.As(err, &dirRemovalError) {
			return err
		}

		s.Logger.Print(err.Error())
	}

	if meta[transfer.MetaKeyHardlink] == "" {
		return nil
	}

	return s.removeInodeFileIfUnused(lpath, meta[transfer.MetaKeyRemoteHardlink], transformer, entry)
}

// handleRemoteFileMetaError handles the given error from getting the metadata
// of the given request's remote object. If the object doesn't exist and the
// entry doesn't say it was never uploaded (failed, missing or abnormal, with no
// earlier upload), the object was removed outside of ibackup (or by an earlier
// attempt), so this logs a warning and treats it as removed; when removing,
// that includes removing a hardlink's inode file if no other hardlink needs
// it. Otherwise it returns the error.
func (s *Server) handleRemoteFileMetaError(err error, removeReq *set.RemoveReq, rpath string,
	transformer transformer.PathTransformer, entry *set.Entry,
) error {
	if !isRemoteFileMissing(err) || entry.WasNotUploaded() {
		return err
	}

	s.Logger.Printf("warning: remote object %s of %s in set %s does not exist or is not readable, "+
		"so treating it as removed",
		rpath, removeReq.Path, removeReq.Set.ID())

	if removeReq.Action != set.ToRemove {
		return nil
	}

	return s.removeInodeFileOfMissingHardlink(removeReq.Path, transformer, entry)
}

// removeInodeFileOfMissingHardlink removes the remote inode file of the given
// entry, if it is a hardlink whose remote object no longer exists, unless
// another hardlink still needs it. The inode file's path comes from the entry,
// as the remote object's metadata that normally gives it is gone.
func (s *Server) removeInodeFileOfMissingHardlink(lpath string, transformer transformer.PathTransformer,
	entry *set.Entry,
) error {
	if entry.Type != set.Hardlink || s.remoteHardlinkLocation == "" || entry.InodeStoragePath() == "" {
		return nil
	}

	rInodePath := filepath.Join(s.remoteHardlinkLocation, entry.InodeStoragePath())

	err := s.removeInodeFileIfUnused(lpath, rInodePath, transformer, entry)
	if isRemoteFileMissing(err) {
		return nil
	}

	return err
}

// removeInodeFileIfUnused removes the given remote inode file of the given
// hardlink entry, unless another hardlink still needs it.
func (s *Server) removeInodeFileIfUnused(lpath, rInodePath string, transformer transformer.PathTransformer,
	entry *set.Entry,
) error {
	files, thresh, err := s.getFilesWithSameInode(lpath, entry.Inode, transformer, rInodePath)
	if err != nil {
		return err
	}

	if len(files) > thresh {
		return nil
	}

	return s.storageHandler.RemoveFile(rInodePath)
}

func (s *Server) getFilesWithSameInode(path string, inode uint64, transformer transformer.PathTransformer,
	rInodePath string,
) ([]string, int, error) {
	files, err := s.db.GetFilesFromInode(inode, s.db.GetMountPointFromPath(path))
	if err != nil {
		return nil, 0, err
	}

	if slices.Contains(files, path) {
		return files, numberOfFilesForOneHardlink, nil
	}

	files, err = remove.FindHardlinksWithInode(rInodePath, transformer, s.storageHandler)

	return files, 0, err
}

// handleSetsAndRequesters returns the sets and requesters values that the
// remote object of the given path, which has the given metadata, should have
// once the given set no longer holds it: without the set's name unless another
// set of that name holds the path, and without its requester unless another of
// the requester's sets holds the path. The requester goes even when the sets
// value already lacks the set's name, since adding the new values may have
// added the sets value and then failed, leaving a retry with the recorded old
// requester value.
func (s *Server) handleSetsAndRequesters(path string, givenSet *set.Set,
	meta map[string]string) ([]string, []string, error) {
	sets := splitCommaSeparatedUnique(meta[transfer.MetaKeySets])
	requesters := splitCommaSeparatedUnique(meta[transfer.MetaKeyRequester])

	if !slices.Contains(sets, givenSet.Name) && !slices.Contains(requesters, givenSet.Requester) {
		return sets, requesters, nil
	}

	numUserSets, numSetsWithGivenName, err := s.countSetsForPath(path, givenSet)
	if err != nil {
		return nil, nil, err
	}

	if numUserSets == 1 {
		requesters = slices.DeleteFunc(requesters, func(s string) bool { return s == givenSet.Requester })
	}

	if numSetsWithGivenName > 1 {
		return sets, requesters, nil
	}

	sets = slices.DeleteFunc(sets, func(s string) bool { return s == givenSet.Name })

	return sets, requesters, nil
}

func splitCommaSeparatedUnique(value string) []string {
	if value == "" {
		return nil
	}

	parts := strings.Split(value, ",")
	seen := make(map[string]struct{}, len(parts))
	unique := make([]string, 0, len(parts))

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		if _, ok := seen[part]; ok {
			continue
		}

		seen[part] = struct{}{}
		unique = append(unique, part)
	}

	return unique
}

func appendIfMissing(slice []string, element string) []string {
	if element == "" {
		return slice
	}

	if slices.Contains(slice, element) {
		return slice
	}

	return append(slice, element)
}

// countSetsForPath gets all sets that contain the provided path and returns the
// number of them with the given set's requester, and the number with its name.
// The given set is always counted, once, even when it no longer has the path,
// as when undoing an upload that finished after the path's removal from it.
func (s *Server) countSetsForPath(path string, givenSet *set.Set) (uint16, uint16, error) {
	setIDs, err := s.db.GetAllSetsForFile(path)
	if err != nil {
		return 0, 0, err
	}

	numSetsWithRequester, numSetsWithSetName := uint16(1), uint16(1)
	givenID := givenSet.ID()

	for _, setID := range setIDs {
		if setID == givenID {
			continue
		}

		set := s.db.GetByID(setID)

		if set.Requester == givenSet.Requester {
			numSetsWithRequester++
		}

		if set.Name == givenSet.Name {
			numSetsWithSetName++
		}
	}

	return numSetsWithRequester, numSetsWithSetName, nil
}

func (s *Server) handleSetMetadataForTrash(givenSet *set.Set, meta map[string]string) ([]string, []string, error) {
	sets := splitCommaSeparatedUnique(meta[transfer.MetaKeySets])
	requesters := splitCommaSeparatedUnique(meta[transfer.MetaKeyRequester])

	sets = appendIfMissing(sets, set.TrashPrefix+givenSet.Name)

	otherUserSets, _, err := s.getSetNamesByRequesters(requesters, givenSet.Requester)
	if err != nil {
		return nil, nil, err
	}

	if slices.Contains(otherUserSets, givenSet.Name) {
		return sets, requesters, nil
	}

	sets = slices.DeleteFunc(sets, func(s string) bool { return s == givenSet.Name })

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

func (s *Server) setErrorOnEntry(entry *set.Entry, sid, path string, errMsg string) {
	entry.LastError = errMsg

	erru := s.db.UpdateEntry(sid, path, entry)
	if erru != nil {
		s.Logger.Printf("%s", erru.Error())
	}
}

// bindPathsAndValidateSet gets the paths out of the JSON body, and the set id
// from the URL parameter if Requester matches logged-in username. The paths
// must be absolute, and are returned clean, since the set's paths are stored
// and matched (including as string prefixes of removed folders) that way.
func (s *Server) bindPathsAndValidateSet(c *gin.Context) (*set.Set, []string, bool) {
	var bpaths [][]byte

	if err := c.BindJSON(&bpaths); err != nil {
		c.AbortWithError(http.StatusBadRequest, err) //nolint:errcheck

		return nil, nil, false
	}

	set, ok := s.validateSet(c)
	if !ok {
		return nil, nil, false
	}

	paths := bytesToStrings(bpaths)

	for i, path := range paths {
		if !filepath.IsAbs(path) {
			c.AbortWithError(http.StatusBadRequest, fmt.Errorf("%w: %s", ErrRelativePath, path)) //nolint:errcheck

			return nil, nil, false
		}

		paths[i] = filepath.Clean(path)
	}

	return set, paths, true
}

// validateSet gets the id parameter from the given context and checks a
// corresponding set exists and the logged-in user is the same as the set's
// Requester. If so, returns the set and true. If not, Aborts with an error and
// returns false.
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
	givenSet, paths, ok := s.bindPathsAndValidateSet(c)
	if !ok {
		return
	}

	err := s.db.IsSetReadyToAddFiles(givenSet)
	if err != nil {
		c.AbortWithError(http.StatusBadRequest, err) //nolint:errcheck

		return
	}

	entries := make([]*set.Dirent, len(paths))

	for n, path := range paths {
		entries[n] = &set.Dirent{
			Path: path,
			Mode: os.ModeDir,
		}
	}

	err = s.db.MergeDirEntries(givenSet.ID(), entries)
	if err != nil {
		c.AbortWithError(http.StatusBadRequest, err) //nolint:errcheck

		return
	}

	err = s.updateRemovedBucket(entries, givenSet)
	if err != nil {
		c.AbortWithError(http.StatusBadRequest, err) //nolint:errcheck

		return
	}

	c.Status(http.StatusOK)
}

// updateRemovedBucket removes the entry from the removed bucket if it was
// previously removed from the given set, as it should no longer be excluded
// from discovery.
func (s *Server) updateRemovedBucket(entries []*set.Dirent, given *set.Set) error {
	sid := given.ID()

	excludeTree, err := s.buildExclusionTree(sid)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		isRemoved, match := isDirentRemovedFromSet(entry, excludeTree)
		if !isRemoved {
			continue
		}

		err = s.excludeFromRemovedBucket(entry, match, excludeTree, given)
		if err != nil {
			return err
		}

		excludeTree, err = s.buildExclusionTree(sid)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Server) buildExclusionTree(sid string) (ptrie.Trie[bool], error) {
	excludedPaths, err := s.db.GetExcludedPaths(sid)
	if err != nil {
		return nil, err
	}

	excludeTree := ptrie.New[bool]()
	for _, path := range excludedPaths {
		err = excludeTree.Put([]byte(path), true)
		if err != nil {
			return nil, err
		}
	}

	return excludeTree, nil
}

// excludeFromRemovedBucket removes the given entry from the removed bucket. If
// the entry's path is not equal to the given match (from exclude tree), the
// bucket is repopulated with the other paths represented by the match.
func (s *Server) excludeFromRemovedBucket(entry *set.Dirent, match string,
	excludeTree ptrie.Trie[bool], given *set.Set,
) error {
	sid := given.ID()

	err := s.removeChildEntriesFromRemovedBucket(entry, excludeTree, sid)
	if err != nil {
		return err
	}

	if filepath.Clean(entry.Path) == filepath.Clean(match) {
		return s.db.RemoveFromRemovedBucket(match, sid)
	}

	allOtherPaths, err := s.findAllOtherPathsToExclude(entry, match, given)
	if err != nil {
		return err
	}

	err = s.db.SetRemoveRequests(sid, allOtherPaths)
	if err != nil {
		return err
	}

	return s.db.RemoveFromRemovedBucket(match, sid)
}

// findAllOtherPathsToExclude finds every path that should be excluded but isn't
// due to match being removed from the given set's removed bucket. It climbs
// from the entry's parent up to match (a folder's match has a trailing "/").
// The records it returns are completed discovery exclusions, not removals from
// iRODS: they only keep the rest of the removed folder out of discovery.
//
// The entry's path must be absolute and clean, as bindPathsAndValidateSet
// makes it, so match, which prefixes it, is one of its ancestors. Reaching the
// root without meeting match therefore can't happen; the root stop is a
// defensive one that returns an error rather than excluding every sibling of
// every ancestor.
func (s *Server) findAllOtherPathsToExclude(entry *set.Dirent, match string,
	given *set.Set,
) ([]set.RemoveReq, error) {
	var paths []set.RemoveReq

	match = filepath.Clean(match)
	path := filepath.Dir(entry.Path)

	for {
		childPaths, err := findChildPathsToExclude(path, entry, given)
		if err != nil {
			return nil, err
		}

		paths = append(paths, childPaths...)

		if path == match {
			return paths, nil
		}

		if path == "/" {
			return nil, fmt.Errorf("%w: %s not under %s", errMatchNotAncestor, entry.Path, match)
		}

		path = filepath.Dir(path)
	}
}

func findChildPathsToExclude(path string, entry *set.Dirent, given *set.Set) ([]set.RemoveReq, error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}

	paths := make([]set.RemoveReq, 0, len(entries))
	entryDir := entry.Path + "/"

	for _, dirEntry := range entries {
		childPath := filepath.Join(path, dirEntry.Name())
		if strings.HasPrefix(entryDir, childPath+"/") {
			continue
		}

		removeReq := set.RemoveReq{
			Path:       childPath,
			Set:        given,
			IsDir:      dirEntry.IsDir(),
			IsComplete: true,
		}

		paths = append(paths, removeReq)
	}

	return paths, nil
}

func (s *Server) removeChildEntriesFromRemovedBucket(entry *set.Dirent,
	excludeTree ptrie.Trie[bool], sid string,
) error {
	paths := getChildPathsFromPTrie(entry.Path, excludeTree)

	for _, path := range paths {
		err := s.db.RemoveFromRemovedBucket(path, sid)
		if err != nil {
			return err
		}
	}

	return nil
}

// getChildPathsFromPTrie expects a Path to a folder. It accept both paths
// ending with "/" and without it. Folders in PTrie should have a trailing "/".
func getChildPathsFromPTrie(entryPath string, tree ptrie.Trie[bool]) []string {
	var paths []string

	if !strings.HasSuffix(entryPath, "/") {
		entryPath += "/"
	}

	tree.Walk(func(key []byte, _ bool) bool {
		path := string(key)

		if strings.HasPrefix(path, entryPath) {
			paths = append(paths, path)
		}

		return true
	})

	return paths
}

// getEntries gets the defined and discovered file entries for the set with the
// id specified in the URL parameter.
//
// LoadSetDB() must already have been called. This is called when there is a GET
// on /rest/v1/auth/entries/[id].
func (s *Server) getEntries(c *gin.Context) {
	s.getFileEntries(c, nil)
}

func (s *Server) getFileEntries(c *gin.Context, filter func(*set.Entry) bool) {
	set, ok := s.validateSet(c)
	if !ok {
		return
	}

	entries, err := s.db.GetFileEntries(set.ID(), filter)
	if err != nil {
		c.AbortWithError(http.StatusBadRequest, err) //nolint:errcheck

		return
	}

	for _, entry := range entries {
		entry.MakeSafeForJSON()
	}

	c.JSON(http.StatusOK, entries)
}

func (s *Server) getUploadedEntries(c *gin.Context) {
	s.getFileEntries(c, set.FileEntryFilterUploaded)
}

func (s *Server) getLastStateEntries(c *gin.Context) {
	s.getFileEntries(c, set.FileEntryFilterLastState)
}

// getExampleEntry gets the first defined file entry for the set with the id
// specified in the URL parameter.
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

	entries, err := s.db.GetDirEntries(set.ID(), nil)
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

	if len(requests) == 0 {
		// a client given no requests exits; until wr records its job as
		// complete, put jobs rac submits for newly ready requests are rejected
		// as duplicates, so trigger submission again once that should be done
		s.triggerReadyAddedCallbackAfter(clientExitGrace)
	}

	c.JSON(http.StatusOK, requests)
}

// reserveRequests keeps reserving items from our queue until we have total
// requests/s.numClients (but max 100) of them, or the queue is empty.
//
// Requests whose remote data object is being worked on by a different client
// (eg. hardlinks sharing a remote inode file, or another set's request for the
// same file) are not returned; they wait in the queue until that client is done
// with it.
//
// Returns the Requests in the items.
func (s *Server) reserveRequests() ([]*transfer.Request, error) {
	n := s.getCachedNumRequestsToReserve()
	requests := make([]*transfer.Request, 0, n)
	batch := make(map[string]bool)
	count := 0

	for {
		r, err := s.reserveRequest()
		if err != nil {
			return nil, err
		}

		if r == nil {
			break
		}

		claimed, err := s.remoteClaims.claim(r, batch)
		if err != nil {
			s.releaseUnclaimedRequest(r, err)
		}

		if !claimed {
			continue
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

// releaseUnclaimedRequest releases a reserved request whose remote data object
// couldn't be claimed, so it is retried instead of staying reserved until its
// TTR expires. Like removal retries, the release is delayed by retryDelay; this
// also stops reserveRequests immediately reserving it again. If the release
// fails, TTR expiry remains the fallback.
func (s *Server) releaseUnclaimedRequest(r *transfer.Request, claimErr error) {
	rid := r.ID()

	s.Logger.Printf("failed to claim remote data object for rid=%s: %s", rid, claimErr)

	if err := s.queue.SetDelay(rid, retryDelay); err != nil {
		s.Logger.Printf("request retry delay set failed rid=%s delay=%s err=%s", rid, retryDelay, err)
	}

	if err := s.queue.Release(context.Background(), rid); err != nil {
		s.Logger.Printf("failed to release unclaimed request rid=%s: %s", rid, err)
	}
}

// getCachedNumRequestsToReserve calls numRequestsToReserve and caches the
// result for the remaining numClients to use, so numClients clients all reserve
// the same amount.
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
func (s *Server) reserveRequest() (*transfer.Request, error) {
	if uint(s.queue.Stats().Items) < s.maxQueueLength>>1 { //nolint:gosec
		go s.refillQueue() //nolint:errcheck
	}

	item, err := s.queue.Reserve("", 0)
	if err != nil {
		qerr, ok := err.(queue.Error) //nolint:errorlint
		if ok && errors.Is(qerr.Err, queue.ErrNothingReady) {
			err = nil
		}

		return nil, err
	}

	r, ok := item.Data().(*transfer.Request)
	if !ok {
		return nil, ErrInvalidInput
	}

	s.logRequestFirstReserved(r)
	r.ReplicaLogging = s.replicaLogging

	return r, nil
}

func (s *Server) logRequestFirstReserved(r *transfer.Request) {
	if r == nil {
		return
	}

	rid := r.ID()
	if !s.markRequestLogged(rid) {
		return
	}

	// Intentionally include local+remote only once so grepping by path yields a
	// single anchor line; all subsequent server logs should be keyed by rid.
	s.Logger.Printf(
		"request reserved rid=%s local=%s remote=%s requester=%s set=%s",
		rid,
		r.Local,
		r.Remote,
		r.Requester,
		r.Set,
	)
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
	r := &transfer.Request{}

	if err := c.BindJSON(r); err != nil {
		c.AbortWithError(http.StatusBadRequest, err) //nolint:errcheck

		return
	}

	if !s.AllowedAccess(c, "") {
		c.AbortWithError(http.StatusUnauthorized, ErrNotAdmin) //nolint:errcheck
		r.CorrectFromJSON()
		s.Logger.Printf("file status denied rid=%s", r.ID())

		return
	}

	r.CorrectFromJSON()

	ceCh := s.queueFileStatusUpdate(r)
	ce := <-ceCh

	if ce != nil {
		s.Logger.Printf("file status update failed rid=%s err=%v", r.ID(), ce.err)
		c.AbortWithError(ce.code, ce.err) //nolint:errcheck

		return
	}

	c.Status(http.StatusOK)
}

// fileStatusPacket contains a Request and codeAndError channel for sending over
// a channel.
type fileStatusPacket struct {
	r    *transfer.Request
	ceCh chan *codeAndError
}

// queueFileStatusUpdate queues a file status update request from a client,
// sending it to the channel handleFileStatusUpdates() reads from.
func (s *Server) queueFileStatusUpdate(r *transfer.Request) chan *codeAndError {
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
		rid := fsp.r.ID()
		s.hungDebugNoteStatusUpdate(rid)

		if err := s.touchRequest(rid); err != nil {
			s.Logger.Printf("file status touch failed rid=%s err=%v", rid, err)
			fsp.ceCh <- &codeAndError{code: http.StatusBadRequest, err: err}

			continue
		}

		if err := s.updateFileStatus(fsp.r); err != nil {
			s.Logger.Printf("file status apply failed rid=%s err=%v", rid, err)
			fsp.ceCh <- &codeAndError{code: http.StatusBadRequest, err: err}

			continue
		}
		fsp.ceCh <- nil
	}
}

// updateFileStatus updates the request's file entry status in the db, and
// removes the request from our queue if not still uploading or no longer in the
// set. Possibly stuck requests are noted in the server's in-memory list of
// stuck requests.
func (s *Server) updateFileStatus(r *transfer.Request) error {
	entry, err := s.db.SetEntryStatus(r)
	if err != nil {
		switch {
		case errors.Is(err, set.ErrInvalidEntry):
			return s.dropRequest(r, "its file is no longer in its set")
		case errors.Is(err, set.ErrInvalidSetID):
			return s.dropRequest(r, "its set was deleted")
		}

		return err
	}

	if err = s.handleNewlyCompletedSets(r); err != nil {
		return err
	}

	if s.failedDuringRemoval(r) {
		// the pending removal still has the file, so it sees the object itself.
		s.uploadTracker.uploadFinished(r)

		return s.removeDroppedRequest(r, "its file is being removed from its set")
	}

	return s.trackUploadingAndStuckRequests(r, entry)
}

// failedDuringRemoval returns true if the given request failed while a removal
// of its file from its set is pending. That removal kept the request while it
// ran, so a retry released now would be handed out again after the removal.
func (s *Server) failedDuringRemoval(r *transfer.Request) bool {
	if r.Status != transfer.RequestStatusFailed {
		return false
	}

	removeReq, err := s.storedRemovalOfFile(r)

	return err == nil && removeReq != nil && !removeReq.IsComplete
}

// storedRemovalOfFile returns the remove request the given request's set
// stored for its file, or for the nearest of its directories, or nil if there
// is none. The exclusion records stored for the other paths of a removed
// directory when a path under it is added back (see findChildPathsToExclude())
// have no set or action, and only keep those paths out of discovery, so they
// count as none.
func (s *Server) storedRemovalOfFile(r *transfer.Request) (*set.RemoveReq, error) {
	removeReq, err := s.db.GetRemoveRequestOfFile(requestSet(r).ID(), r.Local)
	if err != nil || removeReq == nil || removeReq.Set == nil {
		return nil, err
	}

	return removeReq, nil
}

// dropRequest handles a result for the given request, whose set has been
// deleted or no longer has its file. An uploading status is ignored, keeping
// the request running so that its final result still gets here. Whatever its
// final result, any remote object left with the set's metadata, by this upload
// or an earlier attempt, has that undone (see undoLateUpload()). Then the
// request is removed from our queue (see removeDroppedRequest()). If the undo
// fails, the request is instead retried like a failed upload, so that a
// client's next attempt brings its result back here to be undone again.
func (s *Server) dropRequest(r *transfer.Request, reason string) error {
	if r.Status == transfer.RequestStatusUploading {
		return nil
	}

	s.uploadTracker.uploadFinished(r)

	if err := s.undoLateUpload(r); err != nil {
		return s.retryLateUploadUndo(r, err)
	}

	delete(s.lateUndoMeta, r.ID())

	return s.removeDroppedRequest(r, reason)
}

// removeDroppedRequest removes the given finished request from our queue
// whatever its status, and releases any claim it has on a remote data object,
// so it isn't uploaded or retried again, logging the given reason.
func (s *Server) removeDroppedRequest(r *transfer.Request, reason string) error {
	s.Logger.Printf("request dropped rid=%s status=%s: %s", r.ID(), r.Status, reason)

	return errors.Join(s.queue.Remove(context.Background(), r.ID()), s.remoteClaims.release(r))
}

// undoLateUpload undoes what a client of the given request, whose set was
// deleted or no longer has its file, did to its remote object, if the object
// still has the set's name. Any status is checked, not only those of uploads
// that leave the object with the set's metadata, since a retry after a failed
// undo can report a missing or failed file while the object an earlier
// attempt left is still there. The removal of the file (or of the set) ran
// before or during the upload, so didn't see that, and this does what it would
// have done. For a file the set trashed, including any of a deleted set, whose
// deletion trashes its files, it puts the file in the trash set and tags the
// object for it. Otherwise it takes the set (and its requester, unless another
// of the requester's sets has the file) off the object, removing it if no set
// is left.
func (s *Server) undoLateUpload(r *transfer.Request) error {
	removeReq, err := s.lateUploadRemoval(r)
	if err != nil {
		return err
	}

	meta, err := s.storageHandler.GetMeta(r.Remote)
	if isRemoteFileMissing(err) {
		return nil
	} else if err != nil {
		return err
	}

	if !slices.Contains(splitCommaSeparatedUnique(meta[transfer.MetaKeySets]), r.Set) {
		return nil
	}

	return s.undoLateUploadOfObject(r, removeReq, meta)
}

// lateUploadRemoval returns a new remove request, for the given request's file,
// with the action and set of the removal its set stored for the file (or its
// directory). Without one the set was deleted, perhaps then added again, or
// only an exclusion record of a removed directory is left, so the request is
// to trash, for the set if it exists.
func (s *Server) lateUploadRemoval(r *transfer.Request) (*set.RemoveReq, error) {
	stored, err := s.storedRemovalOfFile(r)
	if err != nil {
		return nil, err
	}

	if stored != nil {
		return &set.RemoveReq{Path: r.Local, Set: stored.Set, Action: stored.Action}, nil
	}

	given, err := s.db.GetByNameAndRequester(r.Set, r.Requester)
	if err != nil {
		return nil, err
	}

	if given == nil {
		given = requestSet(r)
	}

	return &set.RemoveReq{Path: r.Local, Set: given, Action: set.ToTrash}, nil
}

// putLateUploadInTrash puts the given entry in the trash set of the given set,
// first making that trash set if it doesn't exist, as when the set never
// trashed anything. A deleted set's trash set always exists, as its deletion
// made it.
func (s *Server) putLateUploadInTrash(given *set.Set, entry *set.Entry) error {
	trashSet := set.BuildTrashSetFromSet(given)

	if s.db.GetByID(trashSet.ID()) == nil {
		if err := s.db.AddOrUpdate(&trashSet); err != nil {
			return err
		}
	}

	return s.db.PutLateUploadInTrash(given, entry)
}

// undoLateUploadOfObject does undoLateUpload()'s work on the given request's
// remote object, which has the given metadata, for the given removal. When
// trashing, the trash set gets the file first, so that a retry after the
// object's update failed still finds the set's name on the object. When
// removing, removing a hardlink needs the set's transformer.
//
// Updating the object's metadata removes the old values before adding the new
// ones, so if adding fails the object is left with no sets or requester, and a
// client's upload of the retry then adds back only its own. So, as a removal
// does (see markAboutToBeRemoved()), the object's sets and requester values
// before the first attempt are recorded, by request ID, and the names in them
// are added to what later attempts see. The record is kept in memory, by the
// single goroutine that handles status updates, so is lost on restart, when a
// late result is rejected anyway.
func (s *Server) undoLateUploadOfObject(r *transfer.Request, removeReq *set.RemoveReq,
	meta map[string]string,
) error {
	entry := lateUploadEntry(r)

	var (
		pt  transformer.PathTransformer
		err error
	)

	if removeReq.Action == set.ToTrash {
		err = s.putLateUploadInTrash(removeReq.Set, entry)
	} else {
		pt, err = removeReq.Set.MakeTransformer()
	}

	if err != nil {
		return err
	}

	removeReq.RemoteMeta = s.recordLateUndoMeta(r.ID(), meta)

	s.Logger.Printf("undoing upload of %s to %s, which finished after its removal from set %s",
		r.Local, r.Remote, removeReq.Set.ID())

	return s.updateOrRemoveRemoteFile(removeReq, pt, r.Remote, meta, entry)
}

// recordLateUndoMeta returns the sets and requester values recorded for the
// late upload with the given request ID, first recording those of the given
// metadata if there are none yet.
func (s *Server) recordLateUndoMeta(rid string, meta map[string]string) map[string]string {
	if _, ok := s.lateUndoMeta[rid]; !ok {
		s.lateUndoMeta[rid] = map[string]string{
			transfer.MetaKeySets:      meta[transfer.MetaKeySets],
			transfer.MetaKeyRequester: meta[transfer.MetaKeyRequester],
		}
	}

	return s.lateUndoMeta[rid]
}

// retryLateUploadUndo makes the given request, whose late upload couldn't be
// undone due to the given error, fail, so it is retried like a failed upload:
// a client uploads it again (transferring nothing if the object is unchanged),
// and its result comes back to dropRequest() to be undone again. Like a failed
// upload, every third attempt buries it instead, until a retry of its set or
// an admin kicks it.
func (s *Server) retryLateUploadUndo(r *transfer.Request, undoErr error) error {
	s.Logger.Printf("request upload undo failed rid=%s err=%s", r.ID(), undoErr)

	r.Status = transfer.RequestStatusFailed
	r.Error = "undoing upload of a file no longer in its set failed: " + undoErr.Error()

	attempts := 1

	if item, err := s.queue.Get(r.ID()); err == nil {
		attempts += int(item.Stats().Releases)
	}

	return s.removeOrReleaseRequestFromQueue(r, &set.Entry{Attempts: attempts})
}

// handleNewlyCompletedSets gets the set the given request is for, and if it has
// completed, carries out actions needed for newly completed sets: trigger the
// monitoring countdown, and do a database backup. A set deleted since the
// request's status was recorded has nothing to handle.
func (s *Server) handleNewlyCompletedSets(r *transfer.Request) error {
	completed, err := s.db.GetByNameAndRequester(r.Set, r.Requester)
	if err != nil {
		return err
	}

	if completed == nil || completed.Status != set.Complete {
		return nil
	}

	s.monitorSet(completed)
	s.tryBackup()

	return nil
}

func (s *Server) trackUploadingAndStuckRequests(r *transfer.Request, entry *set.Entry) error {
	if r.Status == transfer.RequestStatusUploading {
		s.uploadTracker.uploadStarting(r)
		s.Logger.Printf("request uploading rid=%s%s", r.ID(), s.replicaLogSuffix(r))

		return nil
	}

	s.uploadTracker.uploadFinished(r)

	if err := s.removeOrReleaseRequestFromQueue(r, entry); err != nil {
		return err
	}

	if r.Status != transfer.RequestStatusFailed {
		s.requeueIfRediscovered(r, entry)
	}

	return nil
}

// requeueIfRediscovered adds the given finished request's entry back to our
// queue if a discovery completed after its result was recorded. That discovery
// wants the entry uploaded again, but its enqueue would have skipped it as a
// duplicate if it ran before we removed the request from the queue. A set
// deleted since the result was recorded has nothing to requeue. Errors are
// logged, since the result itself was recorded.
func (s *Server) requeueIfRediscovered(r *transfer.Request, recorded *set.Entry) {
	given, err := s.db.GetByNameAndRequester(r.Set, r.Requester)
	if err == nil && given != nil && given.LastDiscovery.After(recorded.LastAttempt) {
		err = s.requeueEntry(given, r.Local)
	}

	if err != nil {
		s.Logger.Printf("request requeue after rediscovery failed rid=%s err=%s", r.ID(), err)
	}
}

// requeueEntry adds the given set's entry for the given path to our queue if
// discovery would.
func (s *Server) requeueEntry(given *set.Set, path string) error {
	entry, err := s.db.GetFileEntryForSet(given.ID(), path)
	if err != nil || !entry.ShouldUpload(given) {
		return err
	}

	transformer, err := given.MakeTransformer()
	if err != nil {
		return err
	}

	_, err = s.enqueueEntries([]*set.Entry{entry}, given, transformer)

	return err
}

// removeOrReleaseRequestFromQueue removes the given Request from our queue
// unless it has failed. < 3 failures results in it being released, 3 results in
// it being buried. Either way, any claim it has on a remote data object is
// released.
func (s *Server) removeOrReleaseRequestFromQueue(r *transfer.Request, entry *set.Entry) error {
	return errors.Join(s.moveFinishedRequestInQueue(r, entry), s.remoteClaims.release(r))
}

func (s *Server) moveFinishedRequestInQueue(r *transfer.Request, entry *set.Entry) error {
	rSuffix := s.replicaLogSuffix(r)

	if r.Status == transfer.RequestStatusFailed {
		s.updateQueueItemData(r)

		attempts := entry.Attempts
		if attempts%set.AttemptsToBeConsideredFailing == 0 {
			s.Logger.Printf("request buried rid=%s attempts=%d err=%s%s", r.ID(), attempts, r.Error, rSuffix)

			return s.queue.Bury(r.ID())
		}

		delay := s.failedUploadRetryDelay
		if err := s.queue.SetDelay(r.ID(), delay); err != nil {
			s.Logger.Printf("request retry delay set failed rid=%s delay=%s err=%s", r.ID(), delay, err)
		}

		s.Logger.Printf("request retry rid=%s attempt=%d delay=%s err=%s%s", r.ID(), attempts, delay, r.Error, rSuffix)

		return s.queue.Release(context.Background(), r.ID())
	}

	s.Logger.Printf("request done rid=%s status=%s%s", r.ID(), r.Status, rSuffix)

	return s.queue.Remove(context.Background(), r.ID())
}

func (s *Server) replicaLogSuffix(r *transfer.Request) string {
	if r == nil || !r.ReplicaLogging {
		return ""
	}

	var b strings.Builder

	appendIntSuffix(&b, "replicas_before_good", r.ReplicaBeforeGood)
	appendIntSuffix(&b, "replicas_before_bad", r.ReplicaBeforeBad)
	appendIntSuffix(&b, "replicas_after_good", r.ReplicaAfterGood)
	appendIntSuffix(&b, "replicas_after_bad", r.ReplicaAfterBad)

	return b.String()
}

func appendIntSuffix(b *strings.Builder, key string, v *int) {
	if v == nil {
		return
	}

	_, _ = fmt.Fprintf(b, " %s=%d", key, *v)
}

// updateQueueItemData updates the item in our queue corresponding to the given
// request, with the request's latest properties.
func (s *Server) updateQueueItemData(r *transfer.Request) {
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

	var discoverOnly bool

	for _, given := range sets {
		if given.ReadOnly {
			continue
		}

		hitLimit, errs := s.recoverSet(given, discoverOnly)
		if errs != nil {
			given.RecoveryError(errs)
			s.Logger.Printf("failed to recover set %s for %s: %s", given.Name, given.Requester, errs)
		}

		discoverOnly = discoverOnly || hitLimit
	}

	err = s.recoverRemoveQueue()
	if err != nil {
		return err
	}

	s.sendSlackMessage(slack.Success, "recovery completed")

	return nil
}

// refillQueue will scan the database for items to add to the in-memory queue.
func (s *Server) refillQueue() { //nolint:gocognit,gocyclo,funlen
	var hitLimit bool

	if !s.queueRefilling.CompareAndSwap(false, true) {
		return
	}

	defer s.queueRefilling.Store(false)

	for !hitLimit {
		var given *set.Set

		s.queueMu.Lock()

		if len(s.queuedSets) > 0 {
			given = s.queuedSets[0]
			s.queuedSets = s.queuedSets[1:]
		}

		s.queueMu.Unlock()

		if given == nil {
			break
		}

		var err error

		hitLimit, err = s.enqueueSet(given, false)
		if err != nil {
			s.Logger.Printf("failed to refill queue from set %s for %s: %s", given.Name, given.Requester, err)
		}
	}
}

func (s *Server) recoverRemoveQueue() error {
	remReqs, err := s.db.GetIncompleteRemoveRequests()
	if err != nil {
		return err
	}

	var sids []string

	defs := make([]*queue.ItemDef, len(remReqs))

	for i, remReq := range remReqs {
		defs[i] = remReq.ItemDef(ttr)

		if !slices.Contains(sids, remReq.Set.ID()) {
			sids = append(sids, remReq.Set.ID())
		}
	}

	_, _, err = s.removeQueue.AddMany(context.Background(), defs)
	if err != nil {
		return err
	}

	for _, sid := range sids {
		go s.handleRemoveRequests(sid)
	}

	return nil
}

// recoverSet checks the status of the given Set and recovers its state as
// appropriate: discover it if it was previously in the middle of being
// discovered; adds its remaining upload requests if it was previously in the
// middle of uploading, or the requests of its in-flight failed files if it had
// completed with failures; otherwise do nothing.
//
// Returns true if the queue reached its limit while adding upload requests.
func (s *Server) recoverSet(given *set.Set, discoverOnly bool) (bool, error) {
	if given.StartedDiscovery.After(given.LastDiscovery) {
		return false, s.discoverSet(given, false)
	}

	s.monitorSet(given)

	if given.LastDiscovery.After(given.LastCompleted) || given.Failed > 0 {
		return s.enqueueSet(given, discoverOnly)
	}

	return false, nil
}

// enqueueSet adds the given set's upload requests to the queue (see
// enqueueSetFiles), or, if it has completed, only those of its in-flight
// entries (see enqueueInFlightSetFiles). With discoverOnly, it instead adds the
// set to queuedSets, for refillQueue to do this later.
//
// Returns true if the queue reached its limit while adding upload requests.
func (s *Server) enqueueSet(given *set.Set, discoverOnly bool) (bool, error) {
	if discoverOnly {
		s.queueMu.Lock()
		s.queuedSets = append(s.queuedSets, given)
		s.queueMu.Unlock()

		return false, nil
	}

	transformer, err := given.MakeTransformer()
	if err != nil {
		return false, err
	}

	if !given.LastDiscovery.After(given.LastCompleted) {
		return s.enqueueInFlightSetFiles(given, transformer)
	}

	return s.enqueueSetFiles(given, transformer)
}

// enqueueInFlightSetFiles adds the requests of the given completed set's file
// entries that are in flight (see Entry.IsInFlight). A set completes once each
// file has uploaded or failed at least once, so these are failed files waiting
// for an automatic retry, buried ones a user retried, and either of those that
// was uploading. Its other entries, eg. never-attempted missing ones, must not
// be requested again, so this doesn't use Entry.ShouldBeQueued. Since in-flight
// entries all failed, only the set's failed entries are read.
func (s *Server) enqueueInFlightSetFiles(given *set.Set,
	transformer transformer.PathTransformer) (bool, error) {
	entries, err := s.db.GetFailedFileEntries(given.ID(), func(e *set.Entry) bool {
		return e.IsInFlight()
	})
	if err != nil || len(entries) == 0 {
		return false, err
	}

	return s.enqueueEntries(entries, given, transformer)
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

	if got.IsTrash() {
		c.AbortWithError(http.StatusBadRequest, ErrTrashSetName) //nolint:errcheck

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

	// Record the retry before kicking the buried requests, so that it isn't
	// lost if the queue loses them before they run, eg. on a server restart.
	if err = s.db.MarkBuriedEntriesRetried(given.ID()); err != nil {
		return 0, err
	}

	s.kickItems(s.buriedItems(&BuriedFilter{
		User: given.Requester,
		Set:  given.Name,
	}))

	entries, err := s.db.GetFileEntries(given.ID(), nil)
	if err != nil {
		return 0, err
	}

	var filtered []*set.Entry

	for _, entry := range entries {
		if entry.Status == set.Failed {
			filtered = append(filtered, entry)
		}
	}

	if len(filtered) == 0 {
		return 0, nil
	}

	// A failed entry may already have a corresponding request sitting in the
	// global queue with a delay applied (default is long). In that case,
	// re-adding is a duplicate and doesn't force an immediate retry. Attempt to
	// clear any delay and release existing requests first, and only enqueue
	// those that aren't already present.
	toEnqueue := make([]*set.Entry, 0, len(filtered))
	for _, entry := range filtered {
		r, errr := s.entryToRequest(entry, transformer, given)
		if errr != nil {
			return 0, errr
		}

		// If the request already exists, make it runnable immediately.
		present := false
		if errd := s.queue.SetDelay(r.ID(), 0); errd == nil {
			present = true
		}

		if errr := s.queue.Release(context.Background(), r.ID()); errr == nil {
			present = true
		}

		if !present {
			toEnqueue = append(toEnqueue, entry)
		}
	}

	_, err = s.enqueueEntries(toEnqueue, given, transformer)

	return len(filtered), err
}
