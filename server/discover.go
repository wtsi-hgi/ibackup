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
	"os"

	"github.com/gin-gonic/gin"
	"github.com/wtsi-hgi/ibackup/put"
	"github.com/wtsi-hgi/ibackup/set"
	"github.com/wtsi-ssg/wrstat/v3/walk"
)

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
// Immediately tries to record in the db that discovery has started, and returns
// any error from doing that. Actual discovery will then proceed asynchronously.
func (s *Server) discoverSet(set *set.Set) error {
	if err := s.db.SetDiscoveryStarted(set.ID()); err != nil {
		return err
	}

	go func() {
		doneCh := make(chan bool, 1)

		go func() {
			if err := s.updateSetFileExistence(set); err != nil {
				s.Logger.Printf("update file existence for %s failed: %s", set.ID(), err)
			}

			close(doneCh)
		}()

		if err := s.discoverDirEntries(set, doneCh); err != nil {
			s.Logger.Printf("discover dir contents for %s failed: %s", set.ID(), err)
		}
	}()

	return nil
}

// updateSetFileExistence gets the file entries (not discovered ones) for the
// set, checks if they all exist locally (concurrently), and if not updates the
// entry status in the db.
func (s *Server) updateSetFileExistence(set *set.Set) error {
	entries, err := s.db.GetPureFileEntries(set.ID())
	if err != nil {
		return err
	}

	errCh := make(chan error, len(entries))

	for _, entry := range entries {
		path := entry.Path

		s.filePool.Submit(func() {
			_, errs := s.setEntryMissingIfNotExist(set, path)
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

// setEntryMissingIfNotExist checks if the given path exists, and if not returns
// true and updates the corresponding entry for that path in the given set with
// a missing status.
func (s *Server) setEntryMissingIfNotExist(set *set.Set, path string) (bool, error) {
	if _, err := os.Stat(path); err == nil {
		return false, nil
	}

	r := &put.Request{
		Local:     path,
		Requester: set.Requester,
		Set:       set.Name,
		Size:      0,
		Status:    put.RequestStatusMissing,
		Error:     nil,
	}

	return true, s.db.SetEntryStatus(r)
}

// discoverDirEntries concurrently walks the set's directories, waits until
// the filesDoneCh closes (file entries have been updated), then sets the
// discovered file paths, which makes the db consider discovery to be complete.
func (s *Server) discoverDirEntries(set *set.Set, filesDoneCh chan bool) error {
	entries, err := s.db.GetDirEntries(set.ID())
	if err != nil {
		return err
	}

	pathsCh := make(chan string)
	doneCh := make(chan error)

	go s.doSetDirWalks(set, entries, pathsCh, doneCh)

	var paths []string //nolint:prealloc

	for path := range pathsCh {
		paths = append(paths, path)
	}

	err = <-doneCh
	if err != nil {
		return err
	}

	<-filesDoneCh

	return s.db.SetDiscoveredEntries(set.ID(), paths)
}

// doSetDirWalks walks the given dir entries of the given set concurrently,
// sending discovered file paths to the pathsCh. Closes the pathsCh when done,
// then sends any error on the doneCh.
func (s *Server) doSetDirWalks(set *set.Set, entries []*set.Entry, pathsCh chan string, doneCh chan error) {
	errCh := make(chan error, len(entries))

	var cb walk.PathCallback = func(path string) error {
		pathsCh <- path

		return nil
	}

	for _, entry := range entries {
		dir := entry.Path

		s.dirPool.Submit(func() {
			errCh <- s.checkAndWalkDir(set, dir, cb)
		})
	}

	var err error

	for i := 0; i < len(entries); i++ {
		thisErr := <-errCh
		if thisErr != nil {
			err = thisErr
		}
	}

	close(pathsCh)

	doneCh <- err
}

// checkAndWalkDir checks if the given dir exists, and if it does, walks the
// dir using the given cb. Major errors are returned; walk errors are logged but
// otherwise ignored.
func (s *Server) checkAndWalkDir(set *set.Set, dir string, cb walk.PathCallback) error {
	missing, err := s.setEntryMissingIfNotExist(set, dir)
	if err != nil {
		return err
	}

	if missing {
		return nil
	}

	walker := walk.New(cb, false)

	return walker.Walk(dir, s.walkErrorCallback)
}

// walkErrorCallback is for giving to Walk; we just log errors.
func (s *Server) walkErrorCallback(path string, err error) {
	s.Logger.Printf("walk found %s, but had error: %s", path, err)
}
