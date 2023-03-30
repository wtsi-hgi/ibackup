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

package set

import (
	"time"
)

type EntryStatus int

const (
	// Pending is an Entry status meaning the file has not yet had an upload
	// attempt.
	Pending EntryStatus = iota

	// UploadingEntry is an Entry status meaning the file has started to upload.
	UploadingEntry

	// Uploaded is an Entry status meaning the file has been uploaded
	// successfully.
	Uploaded

	// Failed is an Entry status meaning the file failed to upload due to some
	// remote issue.
	Failed

	// Failed is an Entry status meaning the local file is missing so can't be
	// uploaded. Symlinks are considered to be "missing" since they can't be
	// uploaded.
	Missing
)

// String lets you convert a EntryStatus to a meaningful string.
func (e EntryStatus) String() string {
	return [...]string{
		"pending",
		"uploading",
		"uploaded",
		"failed",
		"missing",
	}[e]
}

// Entry holds the status of an entry in a backup set.
type Entry struct {
	Path        string
	PathForJSON []byte // set by MakeSafeForJSON(); do not set this yourself.
	Size        uint64 // size of local file in bytes.
	Status      EntryStatus
	LastError   string
	LastAttempt time.Time
	Attempts    int

	newSize  bool
	newFail  bool
	unFailed bool
	isDir    bool
}

// MakeSafeForJSON copies Path to PathForJSON, so that if this Entry struct is
// encoded as JSON, non-UTF8 characters will be preserved. On decode be sure to
// use CorrectFromJSON().
func (e *Entry) MakeSafeForJSON() {
	e.PathForJSON = []byte(e.Path)
}

// CorrectFromJSON copies PathForJSON to Path, so that this value is correct
// following a decode from JSON.
func (e *Entry) CorrectFromJSON() {
	e.Path = string(e.PathForJSON)
}

// ShouldUpload returns true if this Entry is pending or the last attempt was
// before the given time.
func (e *Entry) ShouldUpload(reuploadAfter time.Time) bool {
	if e.Status == Pending {
		return true
	}

	return !e.LastAttempt.After(reuploadAfter)
}
