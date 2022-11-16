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
	"context"
	"net/http"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/wtsi-hgi/ibackup/put"
	"github.com/wtsi-hgi/ibackup/set"
	"github.com/wtsi-ssg/wrstat/v3/walk"
)

const ttr = 1 * time.Minute

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
func (s *Server) discoverSet(given *set.Set) error {
	if err := s.db.SetDiscoveryStarted(given.ID()); err != nil {
		return err
	}

	transformer, err := given.MakeTransformer()
	if err != nil {
		s.recordSetError(given.ID(), err)

		return err
	}

	go func() {
		doneCh := make(chan bool, 1)

		go func() {
			if err := s.enqueueFiles(given, transformer); err != nil {
				s.Logger.Printf("enqueue for %s failed: %s", given.ID(), err)
				s.recordSetError(given.ID(), err)
			}

			close(doneCh)
		}()

		if err := s.discoverDirEntries(given, transformer, doneCh); err != nil {
			s.Logger.Printf("discover dir contents for %s failed: %s", given.ID(), err)
			s.recordSetError(given.ID(), err)
		}
	}()

	return nil
}

// enqueueFiles gets the file entries (not discovered ones) for the set, checks
// if they all exist locally (concurrently), and adds requests for them to the
// global put queue if so. If not, updates the entry status in the db.
func (s *Server) enqueueFiles(given *set.Set, transformer put.PathTransformer) error {
	entries, err := s.db.GetPureFileEntries(given.ID())
	if err != nil {
		return err
	}

	errCh := make(chan error, len(entries))

	for _, entry := range entries {
		path := entry.Path

		s.filePool.Submit(func() {
			if _, errs := os.Stat(path); errs != nil {
				errCh <- s.setEntryMissing(given, path)

				return
			}

			errCh <- s.enqueueFile(given, path, transformer)
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
func (s *Server) recordSetError(sid string, err error) {
	if err = s.db.SetError(sid, err.Error()); err != nil {
		s.Logger.Printf("setting error for %s failed: %s", sid, err)
	}
}

// setEntryMissing updates the corresponding entry for the given path in the
// given set with a missing status.
func (s *Server) setEntryMissing(given *set.Set, path string) error {
	r := &put.Request{
		Local:     path,
		Requester: given.Requester,
		Set:       given.Name,
		Size:      0,
		Status:    put.RequestStatusMissing,
		Error:     nil,
	}

	return s.db.SetEntryStatus(r)
}

// enqueueFile makes a request for the given path in the given set and adds it
// to the global put queue for uploading.
func (s *Server) enqueueFile(given *set.Set, path string, transformer put.PathTransformer) error {
	r, err := put.NewRequestWithTransformedLocal(path, transformer)
	if err != nil {
		return err
	}

	if err = r.ValidatePaths(); err != nil {
		return err
	}

	r.Set = given.Name
	r.Requester = given.Requester

	_, err = s.queue.Add(context.Background(), "uniquekey...", "", r, 0, 0, ttr, "")

	return err
}

// discoverDirEntries concurrently walks the set's directories and adds requests
// for discovered files to the global put queue. It then waits until the
// filesDoneCh closes (file entries have been updated), then sets the discovered
// file paths, which makes the db consider discovery to be complete.
func (s *Server) discoverDirEntries(given *set.Set, transformer put.PathTransformer, filesDoneCh chan bool) error {
	entries, err := s.db.GetDirEntries(given.ID())
	if err != nil {
		return err
	}

	pathsCh := make(chan string)
	doneCh := make(chan error)

	go s.doSetDirWalks(given, entries, pathsCh, doneCh)

	paths, enqueueErr := s.readAndEnqueuePaths(pathsCh, given, transformer)

	err = <-doneCh
	if err != nil {
		return err
	}

	if enqueueErr != nil {
		return enqueueErr
	}

	<-filesDoneCh

	return s.db.SetDiscoveredEntries(given.ID(), paths)
}

// doSetDirWalks walks the given dir entries of the given set concurrently,
// sending discovered file paths to the pathsCh. Closes the pathsCh when done,
// then sends any error on the doneCh.
func (s *Server) doSetDirWalks(given *set.Set, entries []*set.Entry, pathsCh chan string, doneCh chan error) {
	errCh := make(chan error, len(entries))

	var cb walk.PathCallback = func(path string) error {
		pathsCh <- path

		return nil
	}

	for _, entry := range entries {
		dir := entry.Path

		s.dirPool.Submit(func() {
			errCh <- s.checkAndWalkDir(given, dir, cb)
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
func (s *Server) checkAndWalkDir(given *set.Set, dir string, cb walk.PathCallback) error {
	if _, err := os.Stat(dir); err != nil {
		return s.setEntryMissing(given, dir)
	}

	walker := walk.New(cb, false)

	return walker.Walk(dir, s.walkErrorCallback)
}

// walkErrorCallback is for giving to Walk; we just log errors.
func (s *Server) walkErrorCallback(path string, err error) {
	s.Logger.Printf("walk found %s, but had error: %s", path, err)
}

// readAndEnqueuePaths reads paths from the pathsCh and enqueues them. Returns
// the paths from the channel as a slice, and the last enqueue error.
func (s *Server) readAndEnqueuePaths(pathsCh chan string, given *set.Set, pt put.PathTransformer) ([]string, error) {
	var paths []string //nolint:prealloc

	var err error

	for path := range pathsCh {
		if thisErr := s.enqueueFile(given, path, pt); thisErr != nil {
			err = thisErr
		}

		paths = append(paths, path)
	}

	return paths, err
}
