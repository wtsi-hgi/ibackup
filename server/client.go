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
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/hashicorp/go-multierror"
	"github.com/inconshreveable/log15"
	gas "github.com/wtsi-hgi/go-authserver"
	"github.com/wtsi-hgi/ibackup/set"
	"github.com/wtsi-hgi/ibackup/transfer"
)

const (
	touchFrequency       = ttr / 3
	minTimeForUpload     = 1 * time.Minute
	bytesInMiB           = 1024 * 1024
	numHandlers          = 2
	millisecondsInSecond = 1000

	ErrKilledDueToStuck = gas.Error(transfer.ErrStuckTimeout)
)

// Client is used to interact with the Server over the network, with
// authentication.
type Client struct {
	url                       string
	cert                      string
	jwt                       string
	toTouch                   map[string]bool
	touchMu                   sync.Mutex
	touching                  bool
	minMBperSecondUploadSpeed float64
	minTimeForUpload          time.Duration
	maxStuckTime              time.Duration
	uploadsErrCh              chan error
	logger                    log15.Logger
	heartbeatQuitCh           chan bool
}

// NewClient returns a Client you can use to call methods on a Server listening
// at the given domain:port url.
//
// Provide a non-blank path to a certificate to force us to trust that
// certificate, eg. if the server was started with a self-signed certificate.
//
// You must first gas.GetJWT() to get a JWT that you must supply here.
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
	var err error

	switch resp.StatusCode() {
	case http.StatusUnauthorized:
		err = gas.ErrNoAuth
	case http.StatusBadRequest:
		err = ErrInvalidInput
	case http.StatusNotFound:
		err = gas.ErrNeedsAuth
	case http.StatusOK:
		return nil
	default:
		err = ErrInternal
	}

	body := string(resp.Body())

	if body != "" {
		err = fmt.Errorf("%w: %s", err, body)
	}

	return err
}

// GetSets gets details about a given requester's backup sets from the
// Server's database. If you started the server, the user "all" will return
// all sets in the system.
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
	return c.putThing(EndPointAuthFiles+"/"+setID, stringsToBytes(paths))
}

// stringsToBytes converts []string to [][]byte so that json marshalling
// doesn't mess with non-UTF8 characters.
func stringsToBytes(s []string) [][]byte {
	b := make([][]byte, len(s))

	for i, value := range s {
		b[i] = []byte(value)
	}

	return b
}

// SetDirs sets the given paths as the directory paths for the backup set with
// the given ID.
func (c *Client) SetDirs(setID string, paths []string) error {
	return c.putThing(EndPointAuthDirs+"/"+setID, stringsToBytes(paths))
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

	for _, entry := range entries {
		entry.CorrectFromJSON()
	}

	return entries, err
}

// GetExampleFile gets an example (not discovered) file for the given set that
// was supplied to SetFiles(). If there are no undiscovered files, returns a
// nil entry and error.
func (c *Client) GetExampleFile(setID string) (*set.Entry, error) {
	var entry *set.Entry

	err := c.getThing(EndPointAuthExampleEntry+"/"+setID, &entry)

	if entry != nil {
		entry.CorrectFromJSON()
	}

	return entry, err
}

// GetDirs gets the directories for the given set that were supplied to
// SetDirs().
func (c *Client) GetDirs(setID string) ([]*set.Entry, error) {
	var entries []*set.Entry

	err := c.getThing(EndPointAuthDirs+"/"+setID, &entries)

	for _, entry := range entries {
		entry.CorrectFromJSON()
	}

	return entries, err
}

// FailedEntries holds failed Entries and the number of failed ones that were
// skipped because there were more than 10.
type FailedEntries struct {
	Entries []*set.Entry
	Skipped int
}

// GetFailedFiles gets up to 10 examples of GetFiles() Entries that have a
// failed status, along with a count of how many other failed Entries there
// were.
func (c *Client) GetFailedFiles(setID string) ([]*set.Entry, int, error) {
	fentries := &FailedEntries{}

	err := c.getThing(EndPointAuthFailedEntries+"/"+setID, &fentries)

	if fentries.Entries != nil {
		for _, entry := range fentries.Entries {
			entry.CorrectFromJSON()
		}
	}

	return fentries.Entries, fentries.Skipped, err
}

// GetSomeUploadRequests gets up to 100 Requests from the global put queue and
// returns them, moving from "ready" status in the queue to "running".
//
// This automatically handles regularly telling the server knows we're still
// working on them, stopping when you UpdateFileStatus().
//
// Only the user who started the server has permission to call this.
func (c *Client) GetSomeUploadRequests() ([]*transfer.Request, error) {
	var requests []*transfer.Request

	err := c.getThing(EndPointAuthRequests, &requests)

	for _, r := range requests {
		r.CorrectFromJSON()
	}

	c.startTouching(requests)

	return requests, err
}

// startTouching adds the given request's IDs to our touch map, and starts a
// goroutine to regularly do the touching if not already started.
func (c *Client) startTouching(requests []*transfer.Request) {
	c.touchMu.Lock()
	defer c.touchMu.Unlock()

	c.toTouch = make(map[string]bool)

	for _, r := range requests {
		c.toTouch[r.ID()] = true
	}

	if len(c.toTouch) == 0 {
		c.touching = false

		return
	}

	if !c.touching {
		c.touching = true
		go c.touchRegularly()
	}
}

// touchRegularly will periodically (more frequently than the ttr) tell the
// server we're still working on all the Request IDs in our touch map. Ends if
// the map is empty.
func (c *Client) touchRegularly() {
	ticker := time.NewTicker(touchFrequency)

	var err error

	defer func() {
		ticker.Stop()
		c.touchMu.Lock()
		c.touching = false
		c.touchMu.Unlock()
	}()

	for range ticker.C {
		c.touchMu.Lock()
		rids := c.getRequestIDsToTouch()

		if len(rids) == 0 {
			c.touchMu.Unlock()

			return
		}

		if err = c.stillWorkingOnRequests(rids); err != nil {
			c.touchMu.Unlock()

			return
		}

		c.touchMu.Unlock()
	}
}

// getRequestIDsToTouch converts our toTouch map to a slice of its keys.
func (c *Client) getRequestIDsToTouch() []string {
	rids := make([]string, len(c.toTouch))
	i := 0

	for rid := range c.toTouch {
		rids[i] = rid
		i++
	}

	return rids
}

// stillWorkingOnRequests should be called more frequently than the ttr to tell
// the server that you're still working on the requests received from
// GetSomeUploadRequests().
//
// Only the user who started the server has permission to call this.
func (c *Client) stillWorkingOnRequests(rids []string) error {
	return c.putThing(EndPointAuthWorking, rids)
}

// SendPutResultsToServer reads from the given channels (as returned by
// put.Putter.Put()) and sends the results to the server, which will deal with
// any failures and update its database. Could return an error related to not
// being able to update the server with the results.
//
// If an upload seems to take too long, based on the given minimum MB/s upload
// speed, tells the server the upload might be stuck, but continues to wait an
// additional maxStuckTime before giving up and returning an error.
// (Note that uploads may still actually be occurring; we only stop updating the
// server; you should exit your process to stop the uploads.)
//
// Do not call this concurrently!
func (c *Client) SendPutResultsToServer(uploadStarts, uploadResults, skipResults chan *transfer.Request,
	minMBperSecondUploadSpeed float64, minTimeForUpload, maxStuckTime time.Duration, logger log15.Logger) error {
	c.minMBperSecondUploadSpeed = minMBperSecondUploadSpeed
	c.minTimeForUpload = minTimeForUpload
	c.maxStuckTime = maxStuckTime
	c.logger = logger
	c.uploadsErrCh = make(chan error)

	var wg sync.WaitGroup

	wg.Add(numHandlers)

	go c.handleUploadTracking(&wg, uploadStarts, uploadResults)
	go c.handleSendingSkipResults(&wg, skipResults)

	go func() {
		wg.Wait()
		close(c.uploadsErrCh)
	}()

	merr := c.combineUploadErrors()

	c.logger.Info("finished sending put results to server")

	return merr.ErrorOrNil()
}

// handleUploadTracking starts timing uploads and sends stuck message to server
// if they take too long (and errors if they take super long). When uploads
// complete and are sent on the results chan, updates server again.
func (c *Client) handleUploadTracking(wg *sync.WaitGroup, uploadStarts, uploadResults chan *transfer.Request) {
	defer wg.Done()

	for ru := range uploadStarts {
		c.logger.Info("started upload", "path", ru.Local)

		if err := c.UpdateFileStatus(ru); err != nil {
			c.logger.Warn("failed to update file status to uploading", "err", err, "path", ru.Local)
			c.uploadsErrCh <- err

			continue
		}

		stopStuckTimer := c.stuckIfUploadTakesTooLong(ru)

		rr := <-uploadResults

		c.logger.Info("finished upload", "path", ru.Local)
		close(stopStuckTimer)

		if err := c.UpdateFileStatus(rr); err != nil {
			c.logger.Warn("failed to update file status to complete", "err", err, "path", ru.Local)
			c.uploadsErrCh <- err
		}
	}
}

// stuckIfUploadTakesTooLong will send stuck info to the server after some time
// based on the size of the request, unless the returned channel is closed to
// indicate the upload completed.
func (c *Client) stuckIfUploadTakesTooLong(request *transfer.Request) chan bool {
	started := time.Now()
	stuckTimer := time.NewTimer(c.maxTimeForUpload(request))

	doneCh := make(chan bool)

	go func() {
		defer stuckTimer.Stop()

		select {
		case <-doneCh:
			return
		case <-stuckTimer.C:
			request.Stuck = transfer.NewStuck(started)
			c.logger.Warn("upload stuck?", "path", request.Local)

			if err := c.UpdateFileStatus(request); err != nil {
				c.uploadsErrCh <- err
			}

			go c.killStuckIfTakesTooLong(request, doneCh)
		}
	}()

	return doneCh
}

// maxTimeForUpload assumes uploads happen in at least our
// minMBperSecondUploadSpeed, and returns a duration based on that and the
// request Size. The minimum duration returned is 1 second.
func (c *Client) maxTimeForUpload(r *transfer.Request) time.Duration {
	mb := bytesToMB(r.Size)
	seconds := mb / c.minMBperSecondUploadSpeed
	d := time.Duration(seconds*millisecondsInSecond) * time.Millisecond

	if d < c.minTimeForUpload {
		d = c.minTimeForUpload
	}

	return d
}

// bytesToMB converts bytes to number of MB.
func bytesToMB(bytes uint64) float64 {
	return float64(bytes) / bytesInMiB
}

// killStuckIfTakesTooLong logs that a stuck upload has breached the
// maxStuckTime, sets the requests status to failed in the db, and signals that
// we should stop sending put results to the server.
func (c *Client) killStuckIfTakesTooLong(request *transfer.Request, doneCh chan bool) {
	timeout := time.NewTimer(c.maxStuckTime)

	select {
	case <-timeout.C:
		c.logger.Warn("upload stuck for a long time, giving up", "path", request.Local)
		request.Status = transfer.RequestStatusFailed
		request.Error = transfer.ErrStuckTimeout

		if err := c.UpdateFileStatus(request); err != nil {
			c.uploadsErrCh <- err
		}

		c.uploadsErrCh <- ErrKilledDueToStuck
	case <-doneCh:
		return
	}
}

// handleSendingSkipResults updates file status for each completed request that
// didn't need to start uploading first.
func (c *Client) handleSendingSkipResults(wg *sync.WaitGroup, results chan *transfer.Request) {
	defer wg.Done()

	for r := range results {
		c.logger.Info("skipped upload", "path", r.Local)

		if err := c.UpdateFileStatus(r); err != nil {
			c.logger.Warn("failed to update file status for skipped", "err", err, "path", r.Local)
			c.uploadsErrCh <- err
		}
	}
}

func (c *Client) combineUploadErrors() *multierror.Error {
	var merr *multierror.Error

	for err := range c.uploadsErrCh {
		merr = multierror.Append(merr, err)

		if errors.Is(err, ErrKilledDueToStuck) {
			break
		}
	}

	return merr
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
func (c *Client) UpdateFileStatus(r *transfer.Request) error {
	if r.Status != transfer.RequestStatusUploading {
		c.touchMu.Lock()
		delete(c.toTouch, r.ID())
		c.touchMu.Unlock()
	}

	r.MakeSafeForJSON()

	return c.putThing(EndPointAuthFileStatus, r)
}

// RetryFailedSetUploads initiates the retry of any failed uploads in the given
// set.
//
// Only the user who started the server can retry sets of other users.
//
// Returns the number of failed uploads the retry was initiated for.
func (c *Client) RetryFailedSetUploads(id string) (int, error) {
	retried := 0
	err := c.getThing(EndPointAuthRetryEntries+"/"+id, &retried)

	return retried, err
}

// RemoveFilesAndDirs removes the given paths from the backup set with the given
// ID.
func (c *Client) RemoveFilesAndDirs(setID string, paths []string) error {
	return c.putThing(EndPointAuthRemovePaths+"/"+setID, stringsToBytes(paths))
}
