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

// package server provides a web server for a REST API and website.

package server

import (
	"sync"
	"time"

	"github.com/wtsi-hgi/ibackup/put"
	"github.com/wtsi-hgi/ibackup/slack"
)

type uploadTracker struct {
	sync.RWMutex
	uploading     map[string]*put.Request
	stuckRequests map[string]*put.Request

	highestNumDebouncer *slack.HighestNumDebouncer
}

func newUploadTracker(slacker slack.Slacker, debounce time.Duration) *uploadTracker {
	ut := &uploadTracker{
		uploading:           make(map[string]*put.Request),
		stuckRequests:       make(map[string]*put.Request),
		highestNumDebouncer: slack.NewHighestNumDebouncer(slacker, debounce, "clients uploading"),
	}

	return ut
}

func (ut *uploadTracker) uploadStarting(r *put.Request) {
	ut.Lock()
	defer ut.Unlock()

	if r.Stuck != nil {
		ut.stuckRequests[r.ID()] = r
	}

	if _, ok := ut.uploading[r.ID()]; ok {
		return
	}

	ut.uploading[r.ID()] = r

	ut.highestNumDebouncer.SendDebounceMsg(len(ut.uploading))
}

func (ut *uploadTracker) uploadFinished(r *put.Request) {
	ut.Lock()
	defer ut.Unlock()

	delete(ut.uploading, r.ID())
	delete(ut.stuckRequests, r.ID())

	ut.highestNumDebouncer.SendDebounceMsg(len(ut.uploading))
}

func (ut *uploadTracker) currentlyUploading() []*put.Request {
	ut.RLock()
	defer ut.RUnlock()

	uploading := make([]*put.Request, len(ut.uploading))
	i := 0

	for _, r := range ut.uploading {
		uploading[i] = r
		i++
	}

	return uploading
}

func (ut *uploadTracker) isUploading(r *put.Request) bool {
	ut.RLock()
	defer ut.RUnlock()

	_, ok := ut.uploading[r.ID()]

	return ok
}

func (ut *uploadTracker) numUploading() int {
	ut.RLock()
	defer ut.RUnlock()

	return len(ut.uploading)
}

func (ut *uploadTracker) currentlyStuck() []*put.Request {
	ut.RLock()
	defer ut.RUnlock()
	stuck := make([]*put.Request, len(ut.stuckRequests))
	i := 0

	for _, r := range ut.stuckRequests {
		stuck[i] = r
		i++
	}

	return stuck
}

func (ut *uploadTracker) numStuck() int {
	ut.RLock()
	defer ut.RUnlock()

	return len(ut.stuckRequests)
}
