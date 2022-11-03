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

package set

import (
	"fmt"
	"time"

	"github.com/dgryski/go-farm"
)

type SetStatus int

const (
	// PendingDiscovery is a Set status meaning the set's entries are pending
	// existence, size and directory content discovery.
	PendingDiscovery SetStatus = iota

	// PendingUpload is a Set status meaning discovery has completed, but no
	// entries have been uploaded since then.
	PendingUpload

	// Uploading is a Set status meaning discovery has completed and upload of
	// entries has started.
	Uploading

	// Failing is a Set status meaning at least 1 of the entries has failed to
	// upload.
	Failing

	// Complete is a Set status meaning all entries have successfully uploaded
	// since the last discovery.
	Complete
)

// Set describes a backup set; a list of files and directories to backup, plus
// some metadata. All properties are required unless otherwise noted.
type Set struct {
	// An arbitrary (short) name for this backup set.
	Name string

	// The username of the person requesting this backup.
	Requester string

	// The method of transforming local Entries paths in to remote paths, to
	// determine the upload location. "humgen" to use the put.HumgenTransformer,
	// or "prefix=local:remote" to use the put.PrefixTransformer.
	Transformer string

	// Monitor the files and directories and re-upload them whenever they
	// change. Optional, defaults to unmonitored (a one time upload of Entries).
	Monitor bool

	// An optional longer free-text description of this backup set.
	Description string

	// Delete local paths after successful upload. Optional, defaults to no
	// deletions (ie. do a backup, not a move).
	// DeleteLocal bool

	// Delete remote paths if removed from the set. Optional, defaults to no
	// deletions (ie. keep all uploads and ignore removed Entries).
	// DeleteRemote bool

	// Receive an optional notification after this date if DeleteRemote is true
	// and there are still Entries in this set.
	// Expiry time.Time

	// StartedDiscovery provides the last time that discovery started. This is a
	// read-only value.
	StartedDiscovery time.Time

	// LastDiscovery provides the last time that discovery completed. This is a
	// read-only value.
	LastDiscovery time.Time

	// NumFiles provides the total number of set and discovered files in this
	// set, as of the last discovery. This is a read-only value.
	NumFiles uint64

	// SizeFiles provides the total size (bytes) of set and discovered files in
	// this set, as of the last discovery. This is a read-only value.
	SizeFiles uint64

	// Uploaded provides the total number of set and discovered files in this
	// set that have been uploaded or confirmed uploaded since the last
	// discovery. This is a read-only value.
	Uploaded uint64

	// Status provides the current status for the set since the last discovery.
	// This is a read-only value.
	Status SetStatus

	// LastCompleted provides the last time that all uploads completed. This is
	// a read-only value.
	LastCompleted time.Time

	// LastCompletedCount provides the count of files there were uploaded on the
	// last successful upload attempt. This is a read-only value.
	LastCompletedCount uint64

	// LastCompletedSize provides the size of files (bytes) there were uploaded
	// on the last successful upload attempt. This is a read-only value.
	LastCompletedSize uint64
}

// ID returns an ID for this set, generated deterministiclly from its Name and
// Requester. Ie. it is unique between all requesters, and amongst a requester's
// differently named sets. Sets with the same Name and Requester will have the
// same ID().
func (s *Set) ID() string {
	concat := fmt.Sprintf("%s:%s", s.Requester, s.Name)

	l, h := farm.Hash128([]byte(concat))

	return fmt.Sprintf("%016x%016x", l, h)
}

// Files uses the database to retrieve our file entries, giving the backup
// status of all the file paths in this Set.
func (s *Set) Files(db *DB) ([]*Entry, error) {
	return db.getFileEntries(s.ID())
}

// Dirs uses the database to retrieve our directory entries, giving the backup
// status of all the directory paths in this Set.
func (s *Set) Dirs(db *DB) ([]*Entry, error) {
	return db.getDirEntries(s.ID())
}

// DiscoveryStarted should be called when you start discovering the existence
// of file entries and the contents of directory entries in this set. It resets
// a number of the status-type values for this Set.
func (s *Set) DiscoveryStarted(db *DB) error {
	return db.setDiscoveryStarted(s.ID())
}

// DiscoveryCompleted should be called when you finish discovering the existence
// of file entries and the contents of directory entries in this set. Provide
// the count and size (bytes) of the discovered entries.
func (s *Set) DiscoveryCompleted(db *DB, count, size uint64) error {
	return db.setDiscoveryCompleted(s.ID(), count, size)
}
