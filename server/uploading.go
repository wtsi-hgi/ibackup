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
	"fmt"
	"sync"
	"time"

	"github.com/wtsi-hgi/ibackup/put"
	"github.com/wtsi-hgi/ibackup/set"
	"github.com/wtsi-hgi/ibackup/slack"
)

// debounceTracker holds values for debouncing slack messages.
type debounceTracker struct {
	sync.Mutex
	slacker         set.Slacker
	debounceTimeout time.Duration
	bouncing        bool

	msg     string
	lastNum int
	curNum  int
}

type uploadTracker struct {
	sync.RWMutex
	uploading     map[string]*put.Request
	stuckRequests map[string]*put.Request

	debounceTracker debounceTracker
}

func newUploadTracker(slacker set.Slacker, debounce time.Duration) *uploadTracker {
	ut := &uploadTracker{
		uploading:     make(map[string]*put.Request),
		stuckRequests: make(map[string]*put.Request),
		debounceTracker: debounceTracker{
			slacker:         slacker,
			debounceTimeout: debounce,
			msg:             "clients uploading",
		},
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

	ut.createAndSendSlackMsg()
}

func (ut *uploadTracker) createAndSendSlackMsg() {
	ut.debounceTracker.Lock()
	defer ut.debounceTracker.Unlock()

	ut.debounceTracker.sendSlackMsg(len(ut.uploading))
}

func (dt *debounceTracker) sendSlackMsg(num int) {
	if num > dt.curNum || dt.curNum == dt.lastNum {
		dt.curNum = num
	}

	if dt.slacker == nil || dt.bouncing || num == dt.lastNum {
		return
	}

	dt.slacker.SendMessage(slack.Info, fmt.Sprintf("%d %s", num, dt.msg))
	dt.lastNum = num
	dt.bouncing = true
	debounce := dt.debounceTimeout

	go func() {
		<-time.After(debounce)

		dt.Lock()
		defer dt.Unlock()
		dt.bouncing = false

		nextNum := dt.curNum
		dt.curNum = 0

		dt.sendSlackMsg(nextNum)
	}()
}

func (ut *uploadTracker) uploadFinished(r *put.Request) {
	ut.Lock()
	defer ut.Unlock()

	delete(ut.uploading, r.ID())
	delete(ut.stuckRequests, r.ID())

	ut.createAndSendSlackMsg()
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
