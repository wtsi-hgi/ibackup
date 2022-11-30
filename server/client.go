/*******************************************************************************
 * Copyright (c) 2022 Genome Research Ltd.
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
	gas "github.com/wtsi-hgi/go-authserver"
	"github.com/wtsi-hgi/ibackup/put"
	"github.com/wtsi-hgi/ibackup/set"
)

const touchFrequency = ttr / 2

// Client is used to interact with the Server over the network, with
// authentication.
type Client struct {
	url      string
	cert     string
	jwt      string
	toTouch  map[string]bool
	touchMu  sync.Mutex
	touching bool
	touchErr error
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
		url:     url,
		cert:    cert,
		jwt:     jwt,
		toTouch: make(map[string]bool),
	}
}

func (c *Client) request() *resty.Request {
	return gas.NewAuthenticatedClientRequest(c.url, c.cert, c.jwt)
}

// AddOrUpdateSet adds details about a backup set to the Server's database.
func (c *Client) AddOrUpdateSet(set *set.Set) error {
	return c.putThing(EndPointAuthSet, set)
}

// putThing sends thing encoded as JSON in the body via a PUT to the given
// url.
func (c *Client) putThing(url string, thing interface{}) error {
	resp, err := c.request().ForceContentType("application/json").
		SetBody(thing).
		Put(url)
	if err != nil {
		return err
	}

	return responseToErr(resp)
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

// GetSomeUploadRequests gets some (approx 10GB worth of) upload Requests from
// the global put queue and returns them, moving from "ready" status in the
// queue to "running".
//
// This automatically handles regularly telling the server knows we're still
// working on them, stopping when you UpdateFileStatus().
//
// Only the user who started the server has permission to call this.
func (c *Client) GetSomeUploadRequests() ([]*put.Request, error) {
	var requests []*put.Request

	err := c.getThing(EndPointAuthRequests, &requests)

	c.startTouching(requests)

	return requests, err
}

// startTouching adds the given request's IDs to our touch map, and starts a
// goroutine to regularly do the touching if not already started.
func (c *Client) startTouching(requests []*put.Request) {
	c.touchMu.Lock()
	defer c.touchMu.Unlock()

	for _, r := range requests {
		c.toTouch[r.ID()] = true
	}

	if len(c.toTouch) == 0 {
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
		c.touchErr = err
		c.touching = false
		c.touchMu.Unlock()
	}()

	for range ticker.C {
		rids := c.getRequestIDsToTouch()

		if len(rids) == 0 {
			return
		}

		if err = c.stillWorkingOnRequests(rids); err != nil {
			return
		}
	}
}

// getRequestIDsToTouch converts our toTouch map to a slice of its keys.
func (c *Client) getRequestIDsToTouch() []string {
	c.touchMu.Lock()
	defer c.touchMu.Unlock()

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

// UpdateFileStatus updates a file's status in the DB based on the given
// Request's status.
//
// This also tells the server we're no longer working on the request. The db
// update will not be carried out if we're not currently touching a
// corresponding request due to a prior GetSomeUploadRequests().
//
// Only the user who started the server has permission to call this.
func (c *Client) UpdateFileStatus(r *put.Request) error {
	c.touchMu.Lock()
	delete(c.toTouch, r.ID())
	c.touchMu.Unlock()

	return c.putThing(EndPointAuthFileStatus, r)
}
