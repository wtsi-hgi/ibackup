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
	"fmt"
	"io/fs"
	"net/http"
	"strings"
	"time"

	"github.com/VertebrateResequencing/wr/queue"
	"github.com/gin-gonic/gin"
	"github.com/wtsi-hgi/ibackup/put"
	"github.com/wtsi-hgi/ibackup/set"
	"github.com/wtsi-ssg/wrstat/v4/walk"
)

const ttr = 6 * time.Minute

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

// discoverSet discovers and stores file entry details for the given set.
// Immediately tries to record in the db that discovery has started, and create
// a transformer for local->remote paths and returns any error from doing that.
// Actual discovery will then proceed asynchronously, followed by adding all
// upload requests for the set to the global put queue.
func (s *Server) discoverSet(given *set.Set) error {
	if err := s.db.SetDiscoveryStarted(given.ID()); err != nil {
		return err
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
// call it multiple times at once for the same set!
func (s *Server) discoverThenEnqueue(given *set.Set, transformer put.PathTransformer) {
	doneCh := make(chan bool, 1)

	go func() {
		if err := s.updateSetFileExistence(given); err != nil {
			s.recordSetError("enqueue for %s failed: %s", given.ID(), err)
		}

		close(doneCh)
	}()

	updated, err := s.discoverDirEntries(given, doneCh)
	if err != nil {
		s.recordSetError("discover dir contents for %s failed: %s", given.ID(), err)

		return
	}

	if err := s.enqueueSetFiles(updated, transformer); err != nil {
		s.recordSetError("queuing files for %s failed: %s", updated.ID(), err)
	}
}

// updateSetFileExistence gets the file entries (not discovered ones) for the
// set, checks if they all exist locally (concurrently), and updates their entry
// status in the db if they're missing.
func (s *Server) updateSetFileExistence(given *set.Set) error {
	entries, err := s.db.GetPureFileEntries(given.ID())
	if err != nil {
		return err
	}

	errCh := make(chan error, len(entries))

	for _, entry := range entries {
		path := entry.Path

		s.filePool.Submit(func() {
			_, errs := s.updateNonRegularEntries(given, path)
			errCh <- errs
		})
	}

	for i := 0; i < len(entries); i++ {
		thisErr := <-errCh
		if thisErr != nil {
			err = thisErr
		}
	}

	return err
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

// discoverDirEntries concurrently walks the set's directories, waits until
// the filesDoneCh closes (file entries have been updated), then sets the
// discovered file paths, which makes the db consider discovery to be complete.
//
// Returns the updated set and any error.
func (s *Server) discoverDirEntries(given *set.Set, filesDoneCh chan bool) (*set.Set, error) {
	entries, err := s.db.GetDirEntries(given.ID())
	if err != nil {
		return nil, err
	}

	entriesCh := make(chan *walk.Dirent)
	doneCh := make(chan error)
	warnCh := make(chan error)
	warnDoneCh := make(chan struct{})

	var warning []string

	go s.doSetDirWalks(given, entries, entriesCh, doneCh, warnCh)

	go func() {
		for warn := range warnCh {
			warning = append(warning, warn.Error())
		}

		close(warnDoneCh)
	}()

	var fileEntries []*walk.Dirent //nolint:prealloc

	for entry := range entriesCh {
		fileEntries = append(fileEntries, entry)
	}

	err = <-doneCh
	if err != nil {
		return nil, err
	}

	close(warnCh)

	<-filesDoneCh
	<-warnDoneCh

	given, err = s.db.SetDiscoveredEntries(given.ID(), fileEntries)
	if err != nil {
		return nil, err
	}

	/* for _, entry := range fileEntries {
		errw := s.updateEntryBasedOnType(given, entry)
		if errw != nil {
			warning = append(warning, errw.Error())
		}
	} */

	if len(warning) != 0 {
		err = s.db.SetWarning(given.ID(), strings.Join(warning, "\n"))
		if err != nil {
			return nil, err
		}
	}

	s.handleNewlyDefinedSets(given)

	return given, err
}

// handleNewlyDefinedSets is called when a set has had all its entries
// discovered and stored in the database. It then ensures the set is
// appropriately monitored, and we trigger a database backup.
func (s *Server) handleNewlyDefinedSets(given *set.Set) {
	s.monitorSet(given)
	s.tryBackup()
}

// doSetDirWalks walks the given dir entries of the given set concurrently,
// sending discovered file paths to the entriesCh. Closes the entriesCh when done,
// then sends any error on the doneCh. Non-critical warnings during the walk are
// sent to the warnChan.
func (s *Server) doSetDirWalks(given *set.Set, entries []*set.Entry, entriesCh chan *walk.Dirent,
	doneCh, warnChan chan error) {
	errCh := make(chan error, len(entries))

	for _, entry := range entries {
		dir := entry.Path

		s.dirPool.Submit(func() {
			errCh <- s.checkAndWalkDir(given, dir, func(entry *walk.Dirent) error {
				entriesCh <- entry

				return nil
			}, warnChan)
		})
	}

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
func (s *Server) checkAndWalkDir(given *set.Set, dir string, cb walk.PathCallback, warnChan chan error) error {
	missing, err := s.updateNonRegularEntries(given, dir)
	if err != nil {
		return err
	}

	if missing {
		return nil
	}

	walker := walk.New(cb, false, false)

	return walker.Walk(dir, func(path string, err error) {
		s.Logger.Printf("walk found %s, but had error: %s", path, err)

		if errors.Is(err, fs.ErrPermission) {
			warnChan <- err
		}
	})
}

// enqueueSetFiles gets all the set's file entries (set and discovered), creates
// put requests for them and adds them to the global put queue for uploading.
// Skips entries that are missing or that have failed or uploaded since the
// last discovery.
func (s *Server) enqueueSetFiles(given *set.Set, transformer put.PathTransformer) error {
	entries, err := s.db.GetFileEntries(given.ID())
	if err != nil {
		return err
	}

	entries = uploadableEntries(entries, given)

	return s.enqueueEntries(entries, given, transformer)
}

// uploadableEntries returns the subset of given entries that are suitable for
// uploading: pending and those that were dealth with before the the last
// discovery.
func uploadableEntries(entries []*set.Entry, given *set.Set) []*set.Entry {
	var filtered []*set.Entry

	for _, entry := range entries {
		if entry.ShouldUpload(given.LastDiscovery) {
			fmt.Printf("entry %s had status %s and so should be uploaded\n", entry.Path, entry.Status)
			filtered = append(filtered, entry)
		} else {
			fmt.Printf("entry %s had status %s and so should NOT be uploaded\n", entry.Path, entry.Status)
		}
	}

	return filtered
}

// enqueueEntries converts the given entries to requests, stores those in items
// and adds them the in-memory queue.
func (s *Server) enqueueEntries(entries []*set.Entry, given *set.Set, transformer put.PathTransformer) error {
	defs := make([]*queue.ItemDef, len(entries))

	for i, entry := range entries {
		r, err := entryToRequest(entry, transformer, given)
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

	_, _, err := s.queue.AddMany(context.Background(), defs)

	return err
}

// entryToRequest converts an Entry to a Request containing details of the given
// set.
func entryToRequest(entry *set.Entry, transformer put.PathTransformer, given *set.Set) (*put.Request, error) {
	r, err := put.NewRequestWithTransformedLocal(entry.Path, transformer)
	if err != nil {
		return nil, err
	}

	if err = r.ValidatePaths(); err != nil {
		return nil, err
	}

	r.Set = given.Name
	r.Requester = given.Requester

	return r, nil
}
