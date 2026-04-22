/*******************************************************************************
 * Copyright (c) 2026 Genome Research Ltd.
 *
 * Authors: Michael Woolnough <mw31@sanger.ac.uk>
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

package fofn

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/wtsi-hgi/ibackup/server"
	"github.com/wtsi-hgi/ibackup/set"
	"github.com/wtsi-hgi/ibackup/transfer"
	"vimagination.zapto.org/byteio"
)

const dirPerms = 0775

// Client represents a configured `ibackup watchfofns` client.
type Client struct {
	base string
}

// NewClient create a new client for `ibackup watchfofns`, using the given
// path as the watch directory.
func NewClient(path string) *Client {
	return &Client{base: path}
}

func (c *Client) path(id string, filename string) string {
	return filepath.Join(c.base, id, filename)
}

// GetSetByName gets details about a given requesters backup set from the watch
// directory.
//
// Returns an error if the requester has no set with the given name.
func (c *Client) GetSetByName(requester, setName string) (*set.Set, error) {
	s := &set.Set{Name: setName, Requester: requester}

	config, err := readConfig(c.path(s.ID(), ""))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, server.ErrBadSet
	} else if err != nil {
		return nil, err
	}

	statusFile := c.path(s.ID(), statusFilename)
	counts, _ := parseStatusCounts(statusFile) //nolint:errcheck

	fofnInfo, err := os.Stat(c.path(s.ID(), fofnFilename))
	if err == nil {
		s.LastDiscovery = fofnInfo.ModTime()
	}

	completed, err := os.Stat(statusFile)
	if err == nil {
		s.LastCompleted = completed.ModTime()

		if s.LastCompleted.After(s.LastDiscovery) {
			s.Status = set.Complete
		}
	}

	setSetData(s, counts, config)

	return s, nil
}

func setSetData(s *set.Set, counts statusCounts, config subDirConfig) {
	s.Uploaded = uint64(counts.Uploaded)                                   //nolint:gosec
	s.Replaced = uint64(counts.Replaced)                                   //nolint:gosec
	s.Missing = uint64(counts.Missing)                                     //nolint:gosec
	s.Failed = uint64(counts.Failed)                                       //nolint:gosec
	s.Orphaned = uint64(counts.Orphaned)                                   //nolint:gosec
	s.Hardlinks = uint64(counts.Hardlinks)                                 //nolint:gosec
	s.Skipped = uint64(counts.Skipped)                                     //nolint:gosec
	s.NumFiles = uint64(counts.Failed + counts.Frozen + counts.Hardlinks + //nolint:gosec
		counts.Missing + counts.NotProcessed + counts.Orphaned + counts.Replaced +
		counts.Skipped + counts.Uploaded + counts.Warning)
	s.Transformer = config.Transformer
	s.Frozen = config.Freeze
	s.Metadata = config.Metadata

	if s.Metadata == nil {
		s.Metadata = make(map[string]string)
	}

	s.Metadata[transfer.MetaKeyReview] = config.Review
	s.Metadata[transfer.MetaKeyRemoval] = config.Remove
	s.Metadata[transfer.MetaKeyReason] = config.Reason
}

// AddOrUpdateSet adds details about a backup set to the watch directory.
func (c *Client) AddOrUpdateSet(set *set.Set) error {
	fofnPath := c.path(set.ID(), "")

	if err := createDirs(fofnPath); err != nil {
		return err
	}

	reason, review, remove, metadata := processMetadata(set)

	c.setLastDiscovery(set)

	return writeConfig(fofnPath, subDirConfig{
		Transformer: set.Transformer,
		Metadata:    metadata,
		Freeze:      set.Frozen,
		Requester:   set.Requester,
		Name:        set.Name,
		Review:      review,
		Remove:      remove,
		Reason:      reason,
	})
}

func createDirs(path string) error {
	if err := os.MkdirAll(path, dirPerms); err != nil {
		return err
	}

	if err := os.Chmod(path, dirPerms); err != nil {
		return err
	}

	return nil
}

func processMetadata(set *set.Set) (string, string, string, map[string]string) {
	metadata := make(map[string]string, len(set.Metadata))

	var reason, review, remove string

	for k, v := range set.Metadata {
		switch k {
		case transfer.MetaKeyReason:
			reason = v
		case transfer.MetaKeyReview:
			review = v
		case transfer.MetaKeyRemoval:
			remove = v
		default:
			metadata[k] = v
		}
	}

	return reason, review, remove, metadata
}

func (c *Client) setLastDiscovery(set *set.Set) {
	if !set.LastDiscovery.IsZero() {
		os.Chtimes(c.path(set.ID(), fofnFilename), set.LastDiscovery, set.LastDiscovery) //nolint:errcheck
	}
}

// MergeFiles sets the given paths as the file paths for the backup set with the
// given ID.
//
// The paths are stored in a temporary file until TriggerDiscovery is called.
func (c *Client) MergeFiles(setID string, paths []string) (err error) {
	p := make([]server.PathMTime, len(paths))

	for n, path := range paths {
		p[n] = server.PathMTime{Path: path}
	}

	return c.MergeFilesWithMTimes(setID, p)
}

// MergeFilesWithMTimes acts like MergeFiles but allows the setting of a
// non-zero mtime that will be prepended to the path.
//
// If the mtime for a path indicates that it hasn't updated since the last
// backup time, there will be no attempt to re-upload that file.
func (c *Client) MergeFilesWithMTimes(setID string, paths []server.PathMTime) (err error) {
	w, err := newBufWriter(c.path(setID, fofnFilename+".tmp"))
	if err != nil {
		return err
	}

	defer func() {
		if errr := w.Close(); err == nil {
			err = errr
		}
	}()

	lw := byteio.StickyLittleEndianWriter{Writer: w}

	for _, path := range paths {
		lw.WriteInt64(path.MTime)
		lw.WriteString0(path.Path)
	}

	return lw.Err
}

// TriggerDiscovery renames the temporary file created by the MergeFiles call so
// that `ibackup watchfofns` can find it and start the process of backing up the
// files.
func (c *Client) TriggerDiscovery(setID string, _ bool) error {
	fofnPath := c.path(setID, fofnFilename)

	return os.Rename(fofnPath+".tmp", fofnPath)
}
