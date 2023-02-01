/*******************************************************************************
 * Copyright (c) 2023 Genome Research Ltd.
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

	"github.com/VertebrateResequencing/wr/queue"
	"github.com/gin-gonic/gin"
	gas "github.com/wtsi-hgi/go-authserver"
	"github.com/wtsi-hgi/ibackup/put"
)

const (
	queueStatusPath    = "/status"
	queueBuriedPath    = "/buried"
	queueUploadingPath = "/uploading"

	// EndPointAuthQueueStatus is the endpoint for getting queue status.
	EndPointAuthQueueStatus = gas.EndPointAuth + queueStatusPath

	// EndPointAuthQueueBuried is the endpoint for dealing with buried items in
	// the queue.
	EndPointAuthQueueBuried = gas.EndPointAuth + queueBuriedPath

	// EndPointAuthQueueUploading is the endpoint for getting items currently
	// uploading from the queue.
	EndPointAuthQueueUploading = gas.EndPointAuth + queueUploadingPath
)

// MakeQueueEndPoints adds a number of endpoints to the REST API for working
// with the global in-memory put request queue:
//
// GET /rest/v1/auth/status : get the summary queue status.
//
// GET /rest/v1/auth/buried: get details about buried items in the queue.
//
// PUT /rest/v1/auth/buried: retry all buried items in the queue, or if there is
// a QueueRequest encoded as JSON in the body, limit to just those items that
// correspond.
//
// DELETE /rest/v1/auth/buried: delete all buried items in the queue, or if
// there is a QueueRequest encoded as JSON in the body, limit to just those
// items that correspond.
//
// GET /rest/v1/auth/uploading: get details about uploading items in the queue.
//
// You must call EnableAuth() before calling this method, and the non-GET
// endpoints will only work if the logged-in user is the same as the user who
// started the Server.
func (s *Server) MakeQueueEndPoints() error {
	authGroup := s.AuthRouter()
	if authGroup == nil {
		return ErrNoAuth
	}

	authGroup.GET(queueStatusPath, s.getQueueStatus)

	authGroup.GET(queueBuriedPath, s.getBuried)
	authGroup.DELETE(queueBuriedPath, s.removeBuried)
	authGroup.PUT(queueBuriedPath, s.retryBuried)

	authGroup.GET(queueUploadingPath, s.getUploading)

	return nil
}

// getQueueStatus gets the server's QueueStatus.
//
// MakeQueueEndPoints() must already have been called. This is called when there
// is a GET on /rest/v1/auth/status.
func (s *Server) getQueueStatus(c *gin.Context) {
	c.JSON(http.StatusOK, s.QueueStatus())
}

// QStatus describes the status of a Server's in-memory put request queue.
type QStatus struct {
	Total     int
	Reserved  int
	Uploading int
	Failed    int
	Stuck     []*put.Request
}

// GetQueueStatus gets information about the server's queue.
func (c *Client) GetQueueStatus() (*QStatus, error) {
	var qs *QStatus

	err := c.getThing(EndPointAuthQueueStatus, &qs)

	return qs, err
}

// QueueStatus returns current information about the queue's stats and any
// possibly stuck requests.
func (s *Server) QueueStatus() *QStatus {
	s.mapMu.RLock()
	defer s.mapMu.RUnlock()

	stats := s.queue.Stats()

	stuck := make([]*put.Request, len(s.stuckRequests))
	i := 0

	for _, r := range s.stuckRequests {
		stuck[i] = r
		i++
	}

	return &QStatus{
		Total:     stats.Items,
		Reserved:  stats.Running,
		Uploading: len(s.uploading),
		Failed:    stats.Buried,
		Stuck:     stuck,
	}
}

// getBuried gets the server's BuriedRequests.
//
// MakeQueueEndPoints() must already have been called. This is called when there
// is a GET on /rest/v1/auth/buried.
func (s *Server) getBuried(c *gin.Context) {
	c.JSON(http.StatusOK, s.BuriedRequests())
}

// BuriedRequests gets the details of put requests that have failed multiple
// times and will no longer be retried.
func (c *Client) BuriedRequests() ([]*put.Request, error) {
	var rs []*put.Request

	err := c.getThing(EndPointAuthQueueBuried, &rs)

	return rs, err
}

// BuriedRequests returns the put requests that are currently buried in the
// global put queue.
func (s *Server) BuriedRequests() []*put.Request {
	var buried []*put.Request

	s.forEachBuriedItem(nil, func(item *queue.Item) {
		buried = append(buried, item.Data().(*put.Request)) //nolint:forcetypeassert
	})

	return buried
}

// BuriedFilter describes a filter to be used when retrying or deleting buried
// requests, to only act of requests that match the set properties of this.
type BuriedFilter struct {
	User string
	Set  string
	Path string
}

// RequestPasses returns true if the given Request matches our filter details.
// Also returns true if User isn't set.
func (b *BuriedFilter) RequestPasses(r *put.Request) bool {
	if b.User == "" {
		return true
	}

	if b.User != r.Requester {
		return false
	}

	if b.Set != "" && b.Set != r.Set {
		return false
	}

	return b.Path == "" || b.Path == r.Local
}

// forEachBuriedItem runs cb on each currently buried item, optionally
// filtering the items to those for Requests matching the given filter.
func (s *Server) forEachBuriedItem(bf *BuriedFilter, cb func(*queue.Item)) {
	items := s.queue.AllItems()

	for _, item := range items {
		if bf != nil {
			if !bf.RequestPasses(item.Data().(*put.Request)) { //nolint:forcetypeassert
				continue
			}
		}

		if item.State() == queue.ItemStateBury {
			cb(item)
		}
	}
}

// retryBuried retries the server's Buried.
//
// MakeQueueEndPoints() must already have been called. This is called when there
// is a PUT on /rest/v1/auth/buried.
func (s *Server) retryBuried(c *gin.Context) {
	bf, ok := s.getBuriedFilterFromContext(c)
	if !ok {
		return
	}

	c.JSON(http.StatusOK, s.RetryBuried(bf))
}

// RetryBuried retries put requests that have failed multiple times, optionally
// filtered according to the BuriedFilter. Returns the number of items retried.
func (c *Client) RetryBuried(bf *BuriedFilter) (int, error) {
	var n int

	err := c.putThing(EndPointAuthQueueBuried, bf, &n)

	return n, err
}

// RetryBuried retries all buried items in the global put queue, and returns
// the number retried.
func (s *Server) RetryBuried(bf *BuriedFilter) int {
	var n int

	s.forEachBuriedItem(bf, func(item *queue.Item) {
		if err := s.queue.Kick(context.Background(), item.Key); err == nil {
			n++
		}
	})

	return n
}

// removeBuried removes the server's Buried.
//
// MakeQueueEndPoints() must already have been called. This is called when there
// is a DELETE on /rest/v1/auth/buried.
func (s *Server) removeBuried(c *gin.Context) {
	bf, ok := s.getBuriedFilterFromContext(c)
	if !ok {
		return
	}

	c.JSON(http.StatusOK, s.RemoveBuried(bf))
}

// getBuriedFilterFromContext gets the BuriedFilter from c and returns it. The
// returned bool will be false if we aborted with an error.
func (s *Server) getBuriedFilterFromContext(c *gin.Context) (*BuriedFilter, bool) {
	if !s.allowedAccess(c, "") {
		c.AbortWithError(http.StatusUnauthorized, ErrNotAdmin) //nolint:errcheck

		return nil, false
	}

	bf := &BuriedFilter{}

	if err := c.BindJSON(bf); err != nil {
		c.AbortWithError(http.StatusBadRequest, err) //nolint:errcheck

		return nil, false
	}

	return bf, true
}

// RemoveBuried removes put requests that have failed multiple times from the
// queue, optionally filtered according to the BuriedFilter. Returns the number
// of items removed.
func (c *Client) RemoveBuried(bf *BuriedFilter) (int, error) {
	var n int

	err := c.deleteThing(EndPointAuthQueueBuried, bf, &n)

	return n, err
}

// deleteThing sends thing encoded as JSON in the body via a DELETE to the given
// url. The response JSON is decoded in to responseThing.
func (c *Client) deleteThing(url string, thing, responseThing interface{}) error {
	req := c.setBodyAndOptionalResult(thing, responseThing)

	resp, err := req.Delete(url)
	if err != nil {
		return err
	}

	return responseToErr(resp)
}

// RemoveBuried removes all buried items in the global put queue, and returns
// the number removed.
func (s *Server) RemoveBuried(bf *BuriedFilter) int {
	var n int

	s.forEachBuriedItem(bf, func(item *queue.Item) {
		if err := s.queue.Remove(context.Background(), item.Key); err == nil {
			n++
		}
	})

	return n
}

// getUploading gets the server's UploadingRequests.
//
// MakeQueueEndPoints() must already have been called. This is called when there
// is a GET on /rest/v1/auth/uploading.
func (s *Server) getUploading(c *gin.Context) {
	c.JSON(http.StatusOK, s.UploadingRequests())
}

// UploadingRequests gets the details of put requests that are currently
// uploading.
func (c *Client) UploadingRequests() ([]*put.Request, error) {
	var rs []*put.Request

	err := c.getThing(EndPointAuthQueueUploading, &rs)

	return rs, err
}

// UploadingRequests returns the put requests that are currently uploading from
// the global put queue.
func (s *Server) UploadingRequests() []*put.Request {
	s.mapMu.RLock()
	defer s.mapMu.RUnlock()

	uploading := make([]*put.Request, len(s.uploading))
	i := 0

	for _, r := range s.uploading {
		uploading[i] = r
		i++
	}

	return uploading
}