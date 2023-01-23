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
	"strings"
	"time"

	"github.com/dgryski/go-farm"
	"github.com/dustin/go-humanize" //nolint:misspell
	"github.com/wtsi-hgi/ibackup/put"
)

type Status int

const (
	dateFormat            = "2006-01-02 15:04:05"
	arPrefixParts         = 2
	ErrInvalidTransformer = "invalid transformer"
	prefixTransformerKey  = "prefix="
)

const (
	// PendingDiscovery is a Set status meaning the set's entries are pending
	// existence, size and directory content discovery.
	PendingDiscovery Status = iota

	// PendingUpload is a Set status meaning discovery has completed, but no
	// entries have been uploaded since then.
	PendingUpload

	// Uploading is a Set status meaning discovery has completed and upload of
	// entries has started.
	Uploading

	// Failing is a Set status meaning at least 1 of the entries has failed to
	// upload after 3 attempts. Other uploads are ongoing.
	Failing

	// Complete is a Set status meaning all entries have had an upload attempt
	// since the last discovery. (Some uploads may have failed, but they had
	// 3 retries.)
	Complete
)

// String lets you convert a Status to a meaningful string.
func (s Status) String() string {
	return [...]string{
		"pending discovery",
		"pending upload",
		"uploading",
		"failing",
		"complete",
	}[s]
}

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
	DeleteLocal bool

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

	// Failed provides the total number of set and discovered files in this set
	// that have failed their upload since the last discovery. This is a
	// read-only value.
	Failed uint64

	// Missing provides the total number of set and discovered files in this set
	// that no longer exist locally since the last discovery. This is a
	// read-only value.
	Missing uint64

	// Status provides the current status for the set since the last discovery.
	// This is a read-only value.
	Status Status

	// LastCompleted provides the last time that all uploads completed
	// (regardless of success or failure). This is a read-only value.
	LastCompleted time.Time

	// LastCompletedCount provides the count of files on the last upload attempt
	// (those successfully uploaded, those which failed, but not those which
	// were missing locally). This is a read-only value.
	LastCompletedCount uint64

	// LastCompletedSize provides the size of files (bytes) counted in
	// LastCompletedCount. This is a read-only value.
	LastCompletedSize uint64

	// Error holds any error that applies to the whole set, such as an issue
	// with the Transformer. This is a read-only value.
	Error string
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

// Discovered provides a string representation of when discovery last completed,
// or if it hasn't, when it started, or if it hasn't, says "not started".
func (s *Set) Discovered() string {
	discovered := ""

	switch {
	case s.StartedDiscovery.IsZero() && s.LastDiscovery.IsZero():
		discovered = "not started"
	case s.StartedDiscovery.After(s.LastDiscovery):
		discovered = fmt.Sprintf("started %s", s.StartedDiscovery.Format(dateFormat))
	default:
		discovered = fmt.Sprintf("completed %s", s.LastDiscovery.Format(dateFormat))
	}

	return discovered
}

// Count provides a string representation of NumFiles, or if 0, returns the
// LastCompletedCount with a textual note to that effect.
func (s *Set) Count() string {
	nfiles := "pending"

	if s.NumFiles > 0 {
		nfiles = fmt.Sprintf("%d", s.NumFiles)
	} else if s.LastCompletedCount != 0 {
		nfiles = fmt.Sprintf("%d (as of last completion)", s.LastCompletedCount)
	}

	return nfiles
}

// Size provides a string representation of SizeFiles in a human readable
// format, or if 0, returns the LastCompletedSize with a textual note to that
// effect.
func (s *Set) Size() string {
	sfiles := ""

	switch {
	case s.NumFiles > 0:
		sfiles = humanize.IBytes(s.SizeFiles)

		if s.Status != Complete {
			sfiles += " (and counting)"
		}
	case s.LastCompletedCount != 0:
		sfiles = fmt.Sprintf("%s (as of last completion)", humanize.IBytes(s.LastCompletedSize))
	default:
		sfiles = "pending"
	}

	return sfiles
}

// MakeTransformer turns our Transformer string in to a put.HumgenTransformer or
// a put.PrefixTransformer as appropriate.
func (s *Set) MakeTransformer() (put.PathTransformer, error) {
	if s.Transformer == "humgen" {
		return put.HumgenTransformer, nil
	}

	if !strings.HasPrefix(s.Transformer, prefixTransformerKey) {
		return nil, Error{ErrInvalidTransformer, ""}
	}

	lr := strings.TrimPrefix(s.Transformer, prefixTransformerKey)

	parts := strings.Split(lr, ":")
	if len(parts) != arPrefixParts {
		return nil, Error{ErrInvalidTransformer, ""}
	}

	return put.PrefixTransformer(parts[0], parts[1]), nil
}
