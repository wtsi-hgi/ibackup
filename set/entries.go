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

import "time"

type EntryStatus int

const (
	// Pending is an Entry status meaning the file has not yet had an upload
	// attempt.
	Pending EntryStatus = iota

	// Uploaded is an Entry status meaning the file has been uploaded
	// successfully.
	Uploaded

	// Failed is an Entry status meaning the file failed to upload due to some
	// remote issue.
	Failed

	// Failed is an Entry status meaning the local file is missing so can't be
	// uploaded.
	Missing
)

// String lets you convert a EntryStatus to a meaningful string.
func (e EntryStatus) String() string {
	return [...]string{
		"pending",
		"uploaded",
		"failed",
		"missing",
	}[e]
}

// Entry holds the status of an entry in a backup set.
type Entry struct {
	Path        string
	Size        uint64 // size of local file in bytes.
	Status      EntryStatus
	LastError   string
	LastAttempt time.Time
	Attempts    int

	newFail  bool
	unFailed bool
	isDir    bool
}

// ShouldUpload returns true if this Entry is pending or has failed less than 3
// times or was previously uploaded before the given time.
func (e *Entry) ShouldUpload(reuploadAfter time.Time) bool {
	switch e.Status {
	case Missing:
		return false
	case Failed:
		if e.Attempts >= attemptsToBeConsideredFailing {
			return false
		}
	case Uploaded:
		if e.LastAttempt.After(reuploadAfter) {
			return false
		}
	case Pending:
	}

	return true
}
