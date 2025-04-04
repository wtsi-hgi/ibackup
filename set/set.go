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
	"slices"
	"strings"
	"time"

	"github.com/dgryski/go-farm"
	"github.com/dustin/go-humanize" //nolint:misspell
	"github.com/wtsi-hgi/ibackup/put"
	"github.com/wtsi-hgi/ibackup/slack"
)

type Status int

type Slacker interface {
	SendMessage(level slack.Level, msg string)
}

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
	// change, checking for changes after the given amount of time. Optional,
	// defaults to unmonitored (a one time upload of Entries).
	MonitorTime time.Duration

	// An optional longer free-text description of this backup set.
	Description string

	// Optional additional metadata which will be applied to every file in the
	// set.
	Metadata map[string]string

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

	// SizeTotal provides the total size (bytes) of set and discovered files in
	// this set, as of the last discovery. This is a read-only value.
	SizeTotal uint64

	// Uploaded provides the total number of set and discovered files in this
	// set that have, for the first time, been uploaded or confirmed uploaded
	// since the last discovery. This is a read-only value.
	Uploaded uint64

	// Replaced is like Uploaded, but for files that had previously been
	// uploaded to iRODS, and now uploaded again because the file on local disk
	// was newer.
	Replaced uint64

	// Skipped is like Uploaded, but for files that had previously been
	// uploaded to iRODS, and were not uploaded again because the file on local
	// disk was the same age.
	Skipped uint64

	// Failed provides the total number of set and discovered files in this set
	// that have failed their upload since the last discovery. This is a
	// read-only value.
	Failed uint64

	// Missing provides the total number of set and discovered files in this set
	// that no longer exist locally since the last discovery. This is a
	// read-only value.
	Missing uint64

	// Abnormal provides the total number of set files in this set that were
	// neither regular files, nor Symlinks (ie. they were fifo or socket files
	// etc).
	Abnormal uint64

	// Symlinks provides the total number of set and discovered files in this
	// set that are symlinks. This is a read-only value.
	Symlinks uint64

	// Hardlinks provides the total number of set and discovered files in this
	// set that are hardlinks and skipped. This is a read-only value.
	Hardlinks uint64

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

	// SizeUploaded provides the size of files (bytes) actually uploaded (not
	// skipped) since the last discovery. This is a read-only value.
	SizeUploaded uint64

	// Error holds any error that applies to the whole set, such as an issue
	// with the Transformer. This is a read-only value.
	Error string

	// Warning contains errors that do not stop progression. This is a read-only
	// value.
	Warning string

	slacker Slacker
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
	if s.LastDiscovery.IsZero() {
		return "pending"
	}

	if s.NumFiles == 0 && s.LastCompletedCount != 0 {
		return fmt.Sprintf("%d (as of last completion)", s.LastCompletedCount)
	}

	return fmt.Sprintf("%d", s.NumFiles)
}

// Size provides a string representation of SizeFiles in a human readable
// format, or if 0, returns the LastCompletedSize with a textual note to that
// effect.
func (s *Set) Size() string {
	if s.LastDiscovery.IsZero() {
		return "pending"
	}

	if s.NumFiles == 0 && s.LastCompletedCount != 0 {
		return humanize.IBytes(s.LastCompletedSize) + " (as of last completion)" //nolint:misspell
	}

	sfiles := humanize.IBytes(s.SizeTotal) //nolint:misspell

	if s.Status != Complete {
		sfiles += " (and counting)"
	}

	return sfiles
}

// UploadedSize provides a string representation of SizeUploaded in a human
// readable format. This is the size of actual uploads (excluding skipped,
// unlike Size()) since the last discovery.
func (s *Set) UploadedSize() string {
	return humanize.IBytes(s.SizeUploaded) //nolint:misspell
}

func (s *Set) TransformPath(path string) (string, error) {
	transformer, err := s.MakeTransformer()
	if err != nil {
		return "", err
	}

	dest, err := transformer(path)
	if err != nil {
		return "", err
	}

	return dest, nil
}

// MakeTransformer turns our Transformer string in to a put.HumgenTransformer or
// a put.PrefixTransformer as appropriate.
func (s *Set) MakeTransformer() (put.PathTransformer, error) {
	if s.Transformer == "humgen" {
		return put.HumgenTransformer, nil
	}

	if s.Transformer == "gengen" {
		return put.GengenTransformer, nil
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

// Incomplete returns true if our Status is not Complete, or if we
// HasProblems(). (Status can be "Complete" even if there are upload failures,
// because "Complete" status only means we're not trying to upload any more.)
func (s *Set) Incomplete() bool {
	return s.Status != Complete || s.HasProblems()
}

// HasProblems returns true if any uploads have failed, if there's an error on
// the set itself, or if our transformer doesn't work.
//
// Currently does NOT check if all of the user's desired local paths can be
// transformed, so there might actually be problems.
func (s *Set) HasProblems() bool {
	_, err := s.MakeTransformer()

	return s.Failed > 0 || s.Error != "" || err != nil
}

// Queued returns true if we're either pending discovery or upload. Ie. the
// set was recently added or updated, but uploads haven't begun yet probably due
// to uploads for other sets being first in the queue.
func (s *Set) Queued() bool {
	return s.Status == PendingDiscovery || s.Status == PendingUpload
}

// countsValid tells you if our Uploaded, Replaced, Skipped, Failed and Missing
// counts are valid (0..NumFiles).
func (s *Set) countsValid() bool {
	// we can't just do the final summed test, because if the numbers are close
	// to max uint64 value from a wrapping bug, they'll wrap back around and
	// pass the test
	if s.Uploaded > s.NumFiles {
		return false
	}

	if s.Replaced > s.NumFiles {
		return false
	}

	if s.Skipped > s.NumFiles {
		return false
	}

	if s.Failed > s.NumFiles {
		return false
	}

	if s.Missing > s.NumFiles {
		return false
	}

	if s.Abnormal > s.NumFiles {
		return false
	}

	if s.Symlinks > s.NumFiles {
		return false
	}

	if s.Hardlinks > s.NumFiles {
		return false
	}

	return s.Uploaded+s.Replaced+s.Skipped+s.Failed+s.Missing+s.Abnormal <= s.NumFiles
}

func (s *Set) adjustBasedOnEntry(entry *Entry) {
	if entry.Type == Symlink {
		s.Symlinks--
	} else if entry.Type == Hardlink {
		s.Hardlinks--
	}

	if entry.newSize {
		s.SizeTotal += entry.Size
		s.SizeUploaded += entry.Size
	}

	if entry.Status == Skipped {
		s.SizeUploaded -= entry.Size
	}

	if entry.unFailed {
		s.Failed--

		if s.Failed <= 0 {
			s.Status = Uploading
		}
	}

	s.entryToSetCounts(entry)
}

// entryToSetCounts increases set Uploaded, Failed or Missing based on
// set.Status.
func (s *Set) entryToSetCounts(entry *Entry) {
	s.entryStatusToSetCounts(entry)
	s.entryTypeToSetCounts(entry)
}

func (s *Set) entryStatusToSetCounts(entry *Entry) { //nolint:gocyclo
	switch entry.Status { //nolint:exhaustive
	case Uploaded:
		s.Uploaded++
	case Replaced:
		s.Replaced++
	case Skipped:
		s.Skipped++
	case Failed:
		if entry.newFail {
			s.Failed++
		}

		if entry.Attempts >= AttemptsToBeConsideredFailing {
			s.Status = Failing
			s.sendSlackMessage(slack.Error, "has failed uploads")
		}
	case Missing:
		s.Missing++
	case AbnormalEntry:
		s.Abnormal++
	}
}

func (s *Set) sendSlackMessage(level slack.Level, msg string) {
	if s.slacker == nil {
		return
	}

	s.slacker.SendMessage(level, s.createSlackMessage(msg))
}

func (s *Set) createSlackMessage(msg string) string {
	return fmt.Sprintf("`%s.%s` %s", s.Requester, s.Name, msg)
}

func (s *Set) entryTypeToSetCounts(entry *Entry) {
	switch entry.Type { //nolint:exhaustive
	case Symlink:
		s.Symlinks++
	case Hardlink:
		s.Hardlinks++
	}
}

// LogChangesToSlack will cause the set to use the slacker when significant
// events happen to the set.
func (s *Set) LogChangesToSlack(slacker Slacker) {
	s.slacker = slacker
}

// SuccessfullyStoredInDB should be called when you successfully store the set
// in DB.
func (s *Set) SuccessfullyStoredInDB() {
	s.sendSlackMessage(slack.Info, "stored in db")
}

// DiscoveryCompleted should be called when you complete discovering a set. Pass
// in the number of files you discovered.
func (s *Set) DiscoveryCompleted(numFiles uint64) {
	s.LastDiscovery = time.Now()
	s.NumFiles = numFiles

	if s.NumFiles == 0 || (s.Missing+s.Abnormal == s.NumFiles) {
		s.Status = Complete
		s.LastCompleted = time.Now()

		s.sendSlackMessage(slack.Warn, "completed discovery and backup due to no files")

		return
	}

	s.Status = PendingUpload

	s.sendSlackMessage(slack.Info, fmt.Sprintf("completed discovery: %d files", numFiles))
}

// UpdateBasedOnEntry updates set status values based on an updated Entry
// from updateFileEntry(), assuming that request is for one of set's file
// entries.
func (s *Set) UpdateBasedOnEntry(entry *Entry, getFileEntries func(string) ([]*Entry, error)) error {
	s.checkIfUploading()

	s.adjustBasedOnEntry(entry)

	err := s.fixCounts(entry, getFileEntries)
	if err != nil {
		return err
	}

	s.checkIfComplete()

	return nil
}

func (s *Set) checkIfUploading() {
	if !(s.Status == PendingDiscovery || s.Status == PendingUpload) {
		return
	}

	s.Status = Uploading

	s.sendSlackMessage(slack.Info, "started uploading files")
}

func (s *Set) checkIfComplete() {
	if !(s.Uploaded+s.Replaced+s.Skipped+s.Failed+s.Missing+s.Abnormal == s.NumFiles) {
		return
	}

	s.Status = Complete
	s.LastCompleted = time.Now()
	s.LastCompletedCount = s.Uploaded + s.Replaced + s.Skipped + s.Failed
	s.LastCompletedSize = s.SizeTotal

	s.sendSlackMessage(slack.Success, fmt.Sprintf("completed backup "+
		"(%d newly uploaded; %d replaced; %d skipped; %d failed; %d missing; %d abnormal; %s data uploaded)",
		s.Uploaded, s.Replaced, s.Skipped, s.Failed, s.Missing, s.Abnormal, s.UploadedSize()))
}

// fixCounts resets the set counts to 0 and goes through all the entries for
// the set in the db to recaluclate them. The supplied entry should be one you
// newly updated and that wasn't in the db before the transaction we're in.
func (s *Set) fixCounts(entry *Entry, getFileEntries func(string) ([]*Entry, error)) error {
	if s.countsValid() {
		return nil
	}

	entries, err := getFileEntries(s.ID())
	if err != nil {
		return err
	}

	s.Uploaded = 0
	s.Replaced = 0
	s.Skipped = 0
	s.Failed = 0
	s.Missing = 0
	s.Abnormal = 0
	s.Symlinks = 0
	s.Hardlinks = 0

	s.updateAllCounts(entries, entry)

	return nil
}

// updateAllCounts should be called after setting all counts to 0 (because they
// had become invalid), and then recalculates the counts. Also marks the given
// entry as newFail if any entry in entries is Failed.
func (s *Set) updateAllCounts(entries []*Entry, entry *Entry) {
	for _, e := range entries {
		if e.Path == entry.Path {
			e = entry
		}

		if e.Status == Failed {
			e.newFail = true
		}

		s.entryToSetCounts(e)
	}
}

// SetError records the given error against the set, indicating it wont work.
func (s *Set) SetError(errMsg string) {
	s.Error = errMsg

	s.sendSlackMessage(slack.Error, "is invalid: "+errMsg)
}

// SetWarning records the given warning against the set, indicating it has an
// issue.
func (s *Set) SetWarning(warnMsg string) {
	s.Warning = warnMsg

	s.sendSlackMessage(slack.Warn, "has an issue: "+warnMsg)
}

func (s *Set) RecoveryError(err error) {
	s.sendSlackMessage(slack.Error, "could not be recovered: "+err.Error())
}

// copyUserProperties copies data from one set into another.
func (s *Set) copyUserProperties(copySet *Set) {
	s.Transformer = copySet.Transformer
	s.MonitorTime = copySet.MonitorTime
	s.DeleteLocal = copySet.DeleteLocal
	s.Description = copySet.Description
	s.Error = copySet.Error
	s.Warning = copySet.Warning
	s.Metadata = copySet.Metadata
}

// reset puts the Set data back to zero/initial/empty values.
func (s *Set) reset() {
	s.StartedDiscovery = time.Now()
	s.NumFiles = 0
	s.SizeTotal = 0
	s.SizeUploaded = 0
	s.Uploaded = 0
	s.Replaced = 0
	s.Skipped = 0
	s.Failed = 0
	s.Missing = 0
	s.Abnormal = 0
	s.Symlinks = 0
	s.Hardlinks = 0
	s.Status = PendingDiscovery
	s.Error = ""
	s.Warning = ""
}

func (s *Set) UserMetadata() string {
	var keyArr []string //nolint:prealloc

	for k := range s.Metadata {
		if !strings.HasPrefix(k, put.MetaUserNamespace) {
			continue
		}

		keyArr = append(keyArr, k)
	}

	slices.Sort(keyArr)

	metaArr := make([]string, 0, len(keyArr))

	for _, k := range keyArr {
		metaArr = append(metaArr, fmt.Sprintf("%s=%s", k[len(put.MetaUserNamespace):], s.Metadata[k]))
	}

	return strings.Join(metaArr, ";")
}
