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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/VertebrateResequencing/wr/queue"
	"github.com/gin-gonic/gin"
	"github.com/viant/ptrie"
	"github.com/wtsi-hgi/ibackup/set"
	"github.com/wtsi-hgi/ibackup/transfer"
	"github.com/wtsi-ssg/wrstat/v6/walk"
)

const ttr = 6 * time.Minute

// discoveryCoordinator is used to ensure removals and discoveries cannot
// happen at the same time on the same set. Discoveries have priority and will
// pause any removal that's already running until the discovery has finished.
type discoveryCoordinator struct {
	sync.Mutex
	hasDiscoveryHappened map[string]bool
	numRunningRemovals   map[string]uint8
	muMap                map[string]*sync.Mutex
}

func newDiscoveryCoordinator() *discoveryCoordinator {
	return &discoveryCoordinator{
		hasDiscoveryHappened: make(map[string]bool),
		numRunningRemovals:   make(map[string]uint8),
		muMap:                make(map[string]*sync.Mutex),
	}
}

// StartDiscovery will wait until any individual removals are complete and then
// block any future removals until the discovery is done.
func (dc *discoveryCoordinator) StartDiscovery(sid string) {
	mu := dc.getOrCreateSetMutex(sid)
	mu.Lock()
}

func (dc *discoveryCoordinator) getOrCreateSetMutex(sid string) *sync.Mutex {
	dc.Lock()
	defer dc.Unlock()

	v, exists := dc.muMap[sid]
	if !exists {
		v = &sync.Mutex{}
		dc.muMap[sid] = v
	}

	return v
}

// DiscoveryHappened tells any running removals that a discovery happened during
// their execution, then stops blocking removals.
func (dc *discoveryCoordinator) DiscoveryHappened(sid string) {
	dc.Lock()
	defer dc.Unlock()

	if dc.isRemovalRunning(sid) {
		dc.hasDiscoveryHappened[sid] = true
	}

	dc.unlockSetMutex(sid)
}

func (dc *discoveryCoordinator) unlockSetMutex(sid string) {
	dc.muMap[sid].Unlock()
}

// isRemovalRunning requires a lock to be held before calling.
func (dc *discoveryCoordinator) isRemovalRunning(sid string) bool {
	_, exists := dc.numRunningRemovals[sid]

	return exists
}

// WillRemove indicates that a removal is running on the provided set id.
func (dc *discoveryCoordinator) WillRemove(sid string) {
	dc.Lock()
	defer dc.Unlock()

	dc.numRunningRemovals[sid]++
}

// WaitForDiscovery will block while a discovery on the provided set is running,
// and it will return an indication if discovery has happened since removal
// started.
func (dc *discoveryCoordinator) WaitForDiscovery(sid string) bool {
	mu := dc.getOrCreateSetMutex(sid)
	mu.Lock()

	dc.Lock()
	_, hasDiscoveryHappened := dc.hasDiscoveryHappened[sid]
	dc.Unlock()

	return hasDiscoveryHappened
}

// AllowDiscovery indicates we finished our individual removal and a discovery
// can now start.
func (dc *discoveryCoordinator) AllowDiscovery(sid string) {
	dc.Lock()
	defer dc.Unlock()

	dc.unlockSetMutex(sid)
}

// RemovalDone indicates this set is no longer removing only if all remove
// commands containing this set have finished.
func (dc *discoveryCoordinator) RemovalDone(sid string) {
	dc.Lock()
	defer dc.Unlock()

	if dc.numRunningRemovals[sid] == 1 {
		delete(dc.numRunningRemovals, sid)
		delete(dc.hasDiscoveryHappened, sid)
	} else {
		dc.numRunningRemovals[sid]--
	}
}

// triggerDiscovery triggers the file discovery process for the set with the id
// specified in the URL parameter.
//
// LoadSetDB() must already have been called. This is called when there is a GET
// on /rest/v1/auth/discover/[id].
func (s *Server) triggerDiscovery(c *gin.Context) {
	set, ok := s.validateSet(c)
	if !ok {
		return
	}

	if err := s.discoverSet(set); err != nil {
		c.AbortWithError(http.StatusBadRequest, err) //nolint:errcheck

		return
	}

	c.Status(http.StatusOK)
}

// discoverSet discovers and stores file entry details for the given set if it is not read-only.
// Immediately tries to record in the db that discovery has started, and create
// a transformer for local->remote paths and returns any error from doing that.
// Actual discovery will then proceed asynchronously, followed by adding all
// upload requests for the set to the global put queue.
func (s *Server) discoverSet(given *set.Set) error {
	if given.ReadOnly {
		s.Logger.Printf("Ignore discovery on a read-only set %s [%s:%s]", given.ID(), given.Requester, given.Name)

		return nil
	}

	transformer, err := given.MakeTransformer()
	if err != nil {
		s.recordSetError("making transformer for %s failed: %s", given.ID(), err)

		return err
	}

	go s.discoverThenEnqueue(given, transformer)

	return nil
}

// discoverThenEnqueue updates file existence, discovers dir contents, then
// queues the set's files for uploading. Call this in a go-routine, but don't
// call it multiple times at once for the same set! This will block removals on
// the same set.
func (s *Server) discoverThenEnqueue(given *set.Set, transformer transfer.PathTransformer) {
	if given.MonitorRemovals {
		if err := s.discoverSetRemovals(given); err != nil {
			s.recordSetError("error discovering set (%s) removals: %s", given.ID(), err)

			return
		}
	}

	s.discoveryCoordinator.StartDiscovery(given.ID())
	defer s.discoveryCoordinator.DiscoveryHappened(given.ID())

	updated, err := s.doDiscovery(given)
	if err != nil {
		s.Logger.Printf("discovery error %s: %s", given.ID(), err)

		return
	}

	s.handleNewlyDefinedSets(updated)

	if err := s.enqueueSetFiles(updated, transformer); err != nil {
		s.recordSetError("queuing files for %s failed: %s", updated.ID(), err)
	}
}

func (s *Server) doDiscovery(given *set.Set) (*set.Set, error) {
	excludeTree, err := s.buildExclusionTree(given.ID())
	if err != nil {
		return nil, err
	}

	return s.db.Discover(given.ID(), s.walkDirEntries(given, excludeTree))
}

func (s *Server) discoverSetRemovals(given *set.Set) error {
	filesToRemove, err := s.findFilesToRemove(given)
	if err != nil {
		return err
	}

	dirsToRemove, err := s.findDirsToRemove(given)
	if err != nil {
		return err
	}

	return s.removeFilesAndDirs(given, filesToRemove, dirsToRemove)
}

func (s *Server) findFilesToRemove(given *set.Set) ([]string, error) {
	fileEntriesInSet, err := s.db.GetFileEntries(given.ID())
	if err != nil {
		return nil, err
	}

	return findNotExistingEntries(fileEntriesInSet)
}

func findNotExistingEntries(entries []*set.Entry) ([]string, error) {
	var missingPaths []string

	for _, entry := range entries {
		_, err := os.Stat(entry.Path)
		if errors.Is(err, os.ErrNotExist) {
			missingPaths = append(missingPaths, entry.Path)
		} else if err != nil {
			return nil, err
		}
	}

	return missingPaths, nil
}

func (s *Server) findDirsToRemove(given *set.Set) ([]string, error) {
	dirEntriesInSet, err := s.db.GetAllDirEntries(given.ID())
	if err != nil {
		return nil, err
	}

	return findNotExistingEntries(dirEntriesInSet)
}

func (s *Server) walkDirEntries(given *set.Set,
	excludeTree ptrie.Trie[bool],
) func([]*set.Entry) ([]*set.Dirent, []*set.Dirent, error) {
	return func(entries []*set.Entry) ([]*set.Dirent, []*set.Dirent, error) {
		entriesCh := make(chan *set.Dirent)
		doneCh := make(chan error)
		warnCh := make(chan error)

		go s.doSetDirWalks(entries, excludeTree, given, entriesCh, doneCh, warnCh)

		return s.processSetDirWalkOutput(given, entriesCh, doneCh, warnCh)
	}
}

func (s *Server) processSetDirWalkOutput(given *set.Set, entriesCh chan *set.Dirent,
	doneCh, warnCh chan error,
) ([]*set.Dirent, []*set.Dirent, error) {
	warnDoneCh := s.processSetDirWalkWarnings(given, warnCh)

	var ( //nolint:prealloc
		fileEntries []*set.Dirent
		dirEntries  []*set.Dirent
	)

	for entry := range entriesCh {
		if entry.IsDir() {
			dirEntries = append(dirEntries, entry)

			continue
		}

		fileEntries = append(fileEntries, entry)
	}

	err := <-doneCh

	close(warnCh)

	if err != nil {
		<-warnDoneCh

		return nil, nil, err
	}

	return fileEntries, dirEntries, <-warnDoneCh
}

func (s *Server) processSetDirWalkWarnings(given *set.Set, warnCh chan error) chan error {
	warnDoneCh := make(chan error)

	go func() {
		var warning error
		for warn := range warnCh {
			warning = errors.Join(warning, warn)
		}

		if warning != nil {
			if err := s.db.SetWarning(given.ID(), warning.Error()); err != nil {
				warnDoneCh <- err

				return
			}
		}

		warnDoneCh <- nil
	}()

	return warnDoneCh
}

// recordSetError sets the given err on the set, and logs on failure to do so.
// Also logs the given message which should include 2 %s which will be filled
// with the sid and err.
func (s *Server) recordSetError(msg, sid string, err error) {
	s.Logger.Printf(msg, sid, err)

	if err = s.db.SetError(sid, err.Error()); err != nil {
		s.Logger.Printf("setting error for %s failed: %s", sid, err)
	}
}

// handleNewlyDefinedSets is called when a set has had all its entries
// discovered and stored in the database. It then ensures the set is
// appropriately monitored, and we trigger a database backup.
func (s *Server) handleNewlyDefinedSets(given *set.Set) {
	s.monitorSet(given)
	s.tryBackup()
}

// doSetDirWalks walks the given dir entries of the given set concurrently,
// sending discovered file paths to the entriesCh. Closes the entriesCh when
// done, then sends any error on the doneCh. Non-critical warnings during the
// walk are sent to the warnChan.
func (s *Server) doSetDirWalks(entries []*set.Entry, excludeTree ptrie.Trie[bool], given *set.Set, //nolint:funlen
	entriesCh chan *set.Dirent, doneCh, warnChan chan error,
) {
	errCh := make(chan error, len(entries))

	s.dirPoolMu.Lock()

	if s.dirPool == nil {
		close(entriesCh)

		doneCh <- fs.ErrClosed

		return
	}

	existing, errg := s.db.GetExistingDirs(given.ID())
	if errg != nil {
		doneCh <- errg

		return
	}

	for _, entry := range entries {
		dir := entry.Path
		thisEntry := entry

		s.dirPool.Submit(func() {
			err := s.checkAndWalkDir(dir, filterEntries(entriesCh, excludeTree, dir), warnChan)
			errCh <- s.handleMissingDirectories(err, thisEntry, given, existing)
		})
	}

	s.dirPoolMu.Unlock()

	var err error

	for i := 0; i < len(entries); i++ {
		thisErr := <-errCh
		if thisErr != nil {
			err = thisErr
		}
	}

	close(entriesCh)

	doneCh <- err
}

// checkAndWalkDir checks if the given dir exists, and if it does, walks the
// dir using the given cb. Major errors are returned; walk errors are logged and
// permission ones sent to the warnChan.
func (s *Server) checkAndWalkDir(dir string, cb walk.PathCallback, warnChan chan error) error {
	_, err := os.Lstat(dir)
	if err != nil {
		return err
	}

	walker := walk.New(cb, true, false)

	return walker.Walk(dir, func(path string, err error) {
		s.Logger.Printf("walk found %s, but had error: %s", path, err)

		if errors.Is(err, fs.ErrPermission) {
			warnChan <- fmt.Errorf("%s: %w", path, err)
		}
	})
}

// filterEntries sends every entry found on the walk to the given entriesCh,
// except for entries that are not regular files or symlinks or dirs, which are
// silently skipped.
func filterEntries(entriesCh chan *set.Dirent, excludeTree ptrie.Trie[bool],
	parentDir string,
) func(entry *walk.Dirent) error {
	return func(entry *walk.Dirent) error {
		dirent := set.DirEntFromWalk(entry)

		isRemoved, _ := isDirentRemovedFromSet(dirent, excludeTree)

		if isRemoved {
			return nil
		}

		if !(entry.IsRegular() || entry.IsSymlink() || entry.IsDir()) ||
			dirent.Path == filepath.Clean(parentDir) { //nolint:wsl

			return nil
		}

		entriesCh <- dirent

		return nil
	}
}

// isDirentRemovedFromSet checks if the dirent has previously been removed from
// the set by using the provided prefix tree. Also returns the matched prefix.
func isDirentRemovedFromSet(dirent *set.Dirent, excludeTree ptrie.Trie[bool]) (bool, string) {
	path := dirent.Path
	if dirent.IsDir() && !strings.HasSuffix(path, "/") {
		path += "/"
	}

	var prefixMatch string

	pathBytes := []byte(path)
	excludeTree.MatchPrefix(pathBytes, func(match []byte, _ bool) bool {
		if bytes.HasSuffix(match, []byte{'/'}) {
			prefixMatch = string(match)

			return false
		}

		if bytes.Equal(match, pathBytes) {
			prefixMatch = string(match)

			return false
		}

		return true
	})

	return prefixMatch != "", prefixMatch
}

// handleMissingDirectories checks if the given error is not nil, and if so
// records in the database that the entry has problems, is missing or is
// orphaned.
func (s *Server) handleMissingDirectories(dirStatErr error, entry *set.Entry,
	given *set.Set, existing map[string]struct{}) error {
	status, errStr := determineDirStatus(dirStatErr, entry, existing)

	r := &transfer.Request{
		Local:     entry.Path,
		Requester: given.Requester,
		Set:       given.Name,
		Size:      0,
		Status:    status,
		Error:     errStr,
	}

	_, err := s.db.SetEntryStatus(r)
	if err != nil {
		return err
	}

	if os.IsNotExist(dirStatErr) {
		return nil
	}

	return dirStatErr
}

func determineDirStatus(dirStatErr error, entry *set.Entry,
	existing map[string]struct{}) (transfer.RequestStatus, string) {
	if dirStatErr == nil {
		return transfer.RequestStatusPending, ""
	}

	status := transfer.RequestStatusMissing

	if _, ok := existing[entry.Path]; ok {
		status = transfer.RequestStatusOrphaned
	}

	return status, dirStatErr.Error()
}

// enqueueSetFiles gets all the set's file entries (set and discovered), creates
// put requests for them and adds them to the global put queue for uploading.
// Skips entries that are missing or that have failed or uploaded since the
// last discovery.
func (s *Server) enqueueSetFiles(given *set.Set, transformer transfer.PathTransformer) error {
	entries, err := s.db.GetFileEntries(given.ID())
	if err != nil {
		return err
	}

	entries = uploadableEntries(entries, given)

	return s.enqueueEntries(entries, given, transformer)
}

// uploadableEntries returns the subset of given entries that are suitable for
// uploading: pending and those that were dealt with before the last discovery.
func uploadableEntries(entries []*set.Entry, given *set.Set) []*set.Entry {
	var filtered []*set.Entry

	for _, entry := range entries {
		if entry.ShouldUpload(given.LastDiscovery) {
			filtered = append(filtered, entry)
		}
	}

	return filtered
}

// enqueueEntries converts the given entries to requests, stores those in items
// and adds them the in-memory queue.
func (s *Server) enqueueEntries(entries []*set.Entry, given *set.Set, transformer transfer.PathTransformer) error {
	defs := make([]*queue.ItemDef, len(entries))

	for i, entry := range entries {
		r, err := s.entryToRequest(entry, transformer, given)
		if err != nil {
			return err
		}

		defs[i] = &queue.ItemDef{
			Key:  r.ID(),
			Data: r,
			TTR:  ttr,
		}
	}

	if len(defs) == 0 {
		return nil
	}

	_, dups, err := s.queue.AddMany(context.Background(), defs)

	if dups > 0 {
		s.markFailedEntries(given)
	}

	return err
}

// entryToRequest converts an Entry to a Request containing details of the given
// set.
func (s *Server) entryToRequest(entry *set.Entry, transformer transfer.PathTransformer,
	given *set.Set,
) (*transfer.Request, error) {
	r, err := transfer.NewRequestWithTransformedLocal(entry.Path, transformer)
	if err != nil {
		return nil, err
	}

	if err = r.ValidatePaths(); err != nil {
		return nil, err
	}

	r.Set = given.Name
	r.Requester = given.Requester

	if entry.Type == set.Symlink {
		r.Symlink = entry.Dest
		r.Meta.SetLocal(transfer.MetaKeySymlink, entry.Dest)
	}

	for k, v := range given.Metadata {
		r.Meta.SetLocal(k, v)
	}

	if entry.Type == set.Hardlink && s.remoteHardlinkLocation != "" {
		r.Hardlink = filepath.Join(s.remoteHardlinkLocation,
			entry.InodeStoragePath())
		r.Meta.SetLocal(transfer.MetaKeyHardlink, entry.Dest)
	}

	return r, nil
}

// markFailedEntries looks for buried items in the queue related to the given
// set and marks the corresponding entries as failed.
func (s *Server) markFailedEntries(given *set.Set) {
	s.forEachBuriedItem(&BuriedFilter{
		User: given.Requester,
		Set:  given.Name,
	}, func(item *queue.Item) {
		request := item.Data().(*transfer.Request) //nolint:errcheck,forcetypeassert

		for range jobRetries {
			_, err := s.db.SetEntryStatus(request)
			if err != nil {
				s.Logger.Printf("failed to mark entry as failed for buried item for set %s for %s: %s\n",
					given.Name, given.Requester, err)

				return
			}
		}
	})
}
