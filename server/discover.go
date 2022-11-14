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
func (s *Server) discoverSet(set *set.Set) error {
	if err := s.db.SetDiscoveryStarted(set.ID()); err != nil {
		return err
	}

	if err := s.updateSetFileExistence(set); err != nil {
		return err
	}

	if err := s.discoverDirEntries(set); err != nil {
		return err
	}

	return nil
}

// updateSetFileExistence gets the file entries (not discovered ones) for the
// set, checks if they all exist locally, and if not updates the entry status
// in the db.
func (s *Server) updateSetFileExistence(set *set.Set) error {
	entries, err := s.db.GetPureFileEntries(set.ID())
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if _, err := s.setEntryMissingIfNotExist(set, entry.Path); err != nil {
			return err
		}
	}

	return nil
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

	err := s.db.SetEntryStatus(r)

	return true, err
}

func (s *Server) discoverDirEntries(set *set.Set) error {
	entries, err := s.db.GetDirEntries(set.ID())
	if err != nil {
		return err
	}

	var paths []string

	cb := func(path string) error {
		paths = append(paths, path)

		return nil
	}

	for _, entry := range entries {
		missing, err := s.setEntryMissingIfNotExist(set, entry.Path)
		if err != nil {
			return err
		}

		if missing {
			continue
		}

		walker := walk.New(cb, false)
		if err = walker.Walk(entry.Path, walkErrorCallback); err != nil {
			return err
		}
	}

	return s.db.SetDiscoveredEntries(set.ID(), paths)
}

// walkErrorCallback is for giving to Walk; we currently just ignore all errors.
func walkErrorCallback(string, error) {}
