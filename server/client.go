/*******************************************************************************
 * Copyright (c) 2022, 2023 Genome Research Ltd.
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
	"sync"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/inconshreveable/log15"
	gas "github.com/wtsi-hgi/go-authserver"
	"github.com/wtsi-hgi/ibackup/put"
	"github.com/wtsi-hgi/ibackup/set"
)

const (
	touchFrequency       = ttr / 3
	bytesInMiB           = 1024 * 1024
	millisecondsInSecond = 1000
	numHandlers          = 2
)

// Client is used to interact with the Server over the network, with
// authentication.
type Client struct {
	url      string
	cert     string
	jwt      string
	toTouch  *put.Request
	touchMu  sync.Mutex
	touching bool
	touchErr error
	logger   log15.Logger
}

// NewClient returns a Client you can use to call methods on a Server listening
// at the given domain:port url.
//
// Provide a non-blank path to a certificate to force us to trust that
// certificate, eg. if the server was started with a self-signed certificate.
//
// You must first gas.Login() to get a JWT that you must supply here.
func NewClient(url, cert, jwt string) *Client {
	return &Client{
		url:  url,
		cert: cert,
		jwt:  jwt,
	}
}

func (c *Client) request() *resty.Request {
	return gas.NewAuthenticatedClientRequest(c.url, c.cert, c.jwt)
}

// AddOrUpdateSet adds details about a backup set to the Server's database.
func (c *Client) AddOrUpdateSet(set *set.Set) error {
	return c.putThing(EndPointAuthSet, set)
}

// putThing sends thing encoded as JSON in the body via a PUT to the given url.
// If optionalResponseThing is defined, gets that decoded from the JSON
// response.
func (c *Client) putThing(url string, thing interface{}, optionalResponseThing ...interface{}) error {
	req := c.setBodyAndOptionalResult(thing, optionalResponseThing...)

	resp, err := req.Put(url)
	if err != nil {
		return err
	}

	return responseToErr(resp)
}

func (c *Client) setBodyAndOptionalResult(thing interface{}, optionalResponseThing ...interface{}) *resty.Request {
	req := c.request().ForceContentType("application/json").SetBody(thing)

	if len(optionalResponseThing) == 1 {
		req = req.SetResult(&optionalResponseThing[0])
	}

	return req
}

// responseToErr converts a response's status code to one of our errors, or nil
// if there's no problem.
func responseToErr(resp *resty.Response) error {
	switch resp.StatusCode() {
	case http.StatusUnauthorized:
		return gas.ErrNoAuth
	case http.StatusBadRequest:
		return ErrInvalidInput
	case http.StatusNotFound:
		return gas.ErrNeedsAuth
	case http.StatusOK:
		return nil
	default:
		return ErrInteral
	}
}

// GetSets gets details about a given requester's backup sets from the
// Server's database.
func (c *Client) GetSets(requester string) ([]*set.Set, error) {
	var sets []*set.Set

	err := c.getThing(EndPointAuthSet+"/"+requester, &sets)

	return sets, err
}

// getThing gets thing decoded from JSON from the given url.
func (c *Client) getThing(url string, thing interface{}) error {
	resp, err := c.request().ForceContentType("application/json").
		SetResult(&thing).
		Get(url)
	if err != nil {
		return err
	}

	return responseToErr(resp)
}

// GetSetByID gets details about a given requester's backup set from the
// Server's database. This is a convienience function that calls GetSets() and
// filters on the given set ID. Returns an error if the requester has no set
// with the given ID.
func (c *Client) GetSetByID(requester, setID string) (*set.Set, error) {
	return c.getFilteredSet(requester, func(set *set.Set) bool {
		return set.ID() == setID
	})
}

// getFilteredSet calls GetSets() and returns the set that the given checker
// returned true for.
func (c *Client) getFilteredSet(requester string, checker func(*set.Set) bool) (*set.Set, error) {
	sets, err := c.GetSets(requester)
	if err != nil {
		return nil, err
	}

	for _, set := range sets {
		if checker(set) {
			return set, nil
		}
	}

	return nil, ErrBadSet
}

// GetSetByName gets details about a given requester's backup set from the
// Server's database. This is a convienience function that calls GetSets() and
// filters on the given set name. Returns an error if the requester has no set
// with the given name.
func (c *Client) GetSetByName(requester, setName string) (*set.Set, error) {
	return c.getFilteredSet(requester, func(set *set.Set) bool {
		return set.Name == setName
	})
}

// SetFiles sets the given paths as the file paths for the backup set with the
// given ID.
func (c *Client) SetFiles(setID string, paths []string) error {
	return c.putThing(EndPointAuthFiles+"/"+setID, paths)
}

// SetDirs sets the given paths as the directory paths for the backup set with
// the given ID.
func (c *Client) SetDirs(setID string, paths []string) error {
	return c.putThing(EndPointAuthDirs+"/"+setID, paths)
}

// TriggerDiscovery tells the server that you've called SetFiles() and SetDirs()
// for the given set, and now want it to discover the files that exist and
// discover the contents of the directories, and start the process of backing up
// the files.
func (c *Client) TriggerDiscovery(setID string) error {
	resp, err := c.request().Get(EndPointAuthDiscovery + "/" + setID)
	if err != nil {
		return err
	}

	return responseToErr(resp)
}

// GetFiles gets the defined and discovered file paths and their backup status
// for the given set.
func (c *Client) GetFiles(setID string) ([]*set.Entry, error) {
	var entries []*set.Entry

	err := c.getThing(EndPointAuthEntries+"/"+setID, &entries)

	return entries, err
}

// GetDirs gets the directories for the given set that were supplied to
// SetDirs().
func (c *Client) GetDirs(setID string) ([]*set.Entry, error) {
	var entries []*set.Entry

	err := c.getThing(EndPointAuthDirs+"/"+setID, &entries)

	return entries, err
}

// GetUploadRequest gets an upload Requests from the global put queue and
// returns it, moving it "ready" status in the queue to "running".
//
// This automatically handles regularly telling the server knows we're still
// working on it, stopping when you UpdateFileStatus().
//
// Only the user who started the server has permission to call this.
func (c *Client) GetUploadRequest() (*put.Request, error) {
	var request *put.Request

	err := c.getThing(EndPointAuthRequests, &request)

	if request != nil {
		c.startTouching(request)
	}

	return request, err
}

// startTouching adds the given request's ID to our touch map, and starts a
// goroutine to regularly do the touching if not already started.
func (c *Client) startTouching(r *put.Request) {
	c.touchMu.Lock()
	defer c.touchMu.Unlock()

	c.toTouch = r

	if !c.touching {
		c.touching = true
		go c.touchRegularly()
	}
}

// touchRegularly will periodically (more frequently than the ttr) tell the
// server we're still working on a Request.
func (c *Client) touchRegularly() {
	ticker := time.NewTicker(touchFrequency)

	var err error

	defer func() {
		ticker.Stop()
		c.touchMu.Lock()
		c.touchErr = err
		c.touching = false
		c.touchMu.Unlock()
	}()

	for range ticker.C {
		c.touchMu.Lock()

		if c.toTouch == nil {
			c.touchMu.Unlock()

			continue
		}

		if err = c.stillWorkingOnRequest(); err != nil {
			c.touchMu.Unlock()

			return
		}

		c.touchMu.Unlock()
	}
}

// stillWorkingOnRequest should be called more frequently than the ttr to tell
// the server that you're still working on the request received from
// GetUploadRequest().
//
// Only the user who started the server has permission to call this.
func (c *Client) stillWorkingOnRequest() error {
	if c.toTouch == nil {
		return nil
	}

	return c.putThing(EndPointAuthWorking, c.toTouch.ID())
}

// UploadRequests continuously calls GetUploadRequest(), uses the given Putter
// to put the requests in to iRODS, and updates the server with results.
//
// If an upload seems to take too long, based on the given minimum MB/s upload
// speed, tells the server the upload might be stuck, but continues to wait.
// Does not complain about being stuck until at least minTimeForUpload has
// passed.
//
// Stops after more than maxMB worth of files have been dealt with (or an error
// dealing with the server is encountered), and returns the total number of
// Requests processed.
//
// Logs server issues to the given logger.
func (c *Client) UploadRequests(p *put.Putter, maxMB int, //nolint:gocognit
	minMBperSecondUploadSpeed float64, minTimeForUpload time.Duration, logger log15.Logger) (int, error) {
	c.logger = logger

	var (
		count   int
		totalMB float64
		err     error
		request *put.Request
	)

	for {
		request, err = c.GetUploadRequest()
		if request == nil || err != nil {
			break
		}

		count++

		if err = c.handleRequest(p, request, minMBperSecondUploadSpeed, minTimeForUpload); err != nil {
			break
		}

		totalMB += bytesToMB(request.Size)
		if totalMB >= float64(maxMB) {
			break
		}
	}

	return count, err
}

// handleRequest validates the given request, puts it if necessary, and updates
// the server with the new file status.
func (c *Client) handleRequest(p *put.Putter, request *put.Request,
	minMBperSecondUploadSpeed float64, minTimeForUpload time.Duration) error {
	shouldPut, metaOnly := p.Validate(request)
	if shouldPut {
		if err := c.handlePut(p, request, minMBperSecondUploadSpeed, minTimeForUpload, metaOnly); err != nil {
			return err
		}
	}

	if err := c.UpdateFileStatus(request); err != nil {
		c.logger.Warn("failed to update file status to complete", "err", err, "path", request.Local)

		return err
	}

	return nil
}

// handlePut uses the Putter to put the request in iRODS. If it actually needs
// uploading, tracks upload time to generate stuck warnings.
func (c *Client) handlePut(p *put.Putter, request *put.Request,
	minMBperSecondUploadSpeed float64, minTimeForUpload time.Duration, metaOnly bool) error {
	uploadDoneCh := make(chan struct{})

	var stuckResolvedCh chan struct{}

	if !metaOnly {
		var err error

		stuckResolvedCh, err = c.handleUploadTracking(request, minMBperSecondUploadSpeed, minTimeForUpload, uploadDoneCh)
		if err != nil {
			return err
		}
	}

	p.Put(request)

	if !metaOnly {
		close(uploadDoneCh)
		<-stuckResolvedCh
	}

	return nil
}

// handleUploadTracking starts timing an upload and sends stuck message to
// server if it takes too long. Wait for the returned channel to close before
// doing any further UpdateFileStatus calls for this request.
func (c *Client) handleUploadTracking(request *put.Request,
	minMBperSecondUploadSpeed float64, minTimeForUpload time.Duration, doneCh chan struct{}) (chan struct{}, error) {
	ru := request.Clone()
	ru.Status = put.RequestStatusUploading

	if err := c.UpdateFileStatus(ru); err != nil {
		c.logger.Warn("failed to update file status to uploading", "err", err, "path", ru.Local)

		return nil, err
	}

	c.logger.Info("started upload", "path", ru.Local)

	stuckResolvedCh := c.stuckIfUploadTakesTooLong(minMBperSecondUploadSpeed, minTimeForUpload, ru, doneCh)

	return stuckResolvedCh, nil
}

// stuckIfUploadTakesTooLong will send stuck info to the server after some time
// based on the size of the request, unless the given channel is closed to
// indicate the upload completed. You should wait for the returned channel to be
// closed before doing any further UpdateFileStatus calls for this request.
func (c *Client) stuckIfUploadTakesTooLong(minMBperSecondUploadSpeed float64, minTimeForUpload time.Duration,
	request *put.Request, doneCh chan struct{}) chan struct{} {
	started := time.Now()
	timer := time.NewTimer(maxTimeForUpload(request, minMBperSecondUploadSpeed, minTimeForUpload))
	stuckResolvedCh := make(chan struct{})

	go func() {
		defer func() {
			close(stuckResolvedCh)
		}()

		select {
		case <-doneCh:
			timer.Stop()
		case <-timer.C:
			request.Stuck = put.NewStuck(started)
			c.logger.Warn("upload stuck?", "path", request.Local)

			if err := c.UpdateFileStatus(request); err != nil {
				c.logger.Warn("failed to update file status to stuck", "err", err, "path", request.Local)
			}
		}
	}()

	return stuckResolvedCh
}

// maxTimeForUpload assumes uploads happen in at least our
// minMBperSecondUploadSpeed, and returns a duration based on that and the
// request Size. The minimum duration returned is 1 second.
func maxTimeForUpload(r *put.Request, minMBperSecondUploadSpeed float64, minTimeForUpload time.Duration) time.Duration {
	mb := bytesToMB(r.Size)
	seconds := mb / minMBperSecondUploadSpeed
	d := time.Duration(seconds*millisecondsInSecond) * time.Millisecond

	if d < minTimeForUpload {
		d = minTimeForUpload
	}

	return d
}

// bytesToMB converts bytes to number of MB.
func bytesToMB(bytes uint64) float64 {
	return float64(bytes) / bytesInMiB
}

// UpdateFileStatus updates a file's status in the DB based on the given
// Request's status.
//
// If the status isn't "uploading", this also tells the server we're no longer
// working on the request. The db update will not be carried out if we're not
// currently touching a corresponding request due to a prior
// GetSomeUploadRequests().
//
// Only the user who started the server has permission to call this.
func (c *Client) UpdateFileStatus(r *put.Request) error {
	if r.Status != put.RequestStatusUploading {
		c.touchMu.Lock()
		c.toTouch = nil
		c.touchMu.Unlock()
	}

	return c.putThing(EndPointAuthFileStatus, r)
}
