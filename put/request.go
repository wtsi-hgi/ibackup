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

package put

import (
	"fmt"
	"path/filepath"
	"strings"
	"time"
)

type RequestStatus string

const (
	RequestStatusUploaded   RequestStatus = "uploaded"
	RequestStatusReplaced   RequestStatus = "replaced"
	RequestStatusUnmodified RequestStatus = "unmodified"
	RequestStatusMissing    RequestStatus = "missing"
	RequestStatusFailed     RequestStatus = "failed"
	ErrNotHumgenLustre                    = "not a valid humgen lustre path"
	metaListSeparator                     = ","
)

// Request represents a local file you would like transferred to a remote iRODS
// path, and any extra metadata (beyond the defaults which include user, group,
// mtime, upload date) you'd like to associate with it. Setting Requester and
// Set will add these to the requesters and sets metadata on upload.
type Request struct {
	Local      string
	Remote     string
	Requester  string
	Set        string
	Meta       map[string]string
	Status     RequestStatus
	Error      error
	remoteMeta map[string]string
	skipPut    bool
}

// ValidatePaths checks that both Local and Remote paths are absolute. (It does
// NOT check that either path exists.)
func (r *Request) ValidatePaths() error {
	local, err := filepath.Abs(r.Local)
	if err != nil {
		return Error{ErrLocalNotAbs, r.Local}
	}

	r.Local = local

	if !filepath.IsAbs(r.Remote) {
		return Error{ErrRemoteNotAbs, r.Remote}
	}

	return nil
}

// addStandardMeta ensures our Meta is unique to us, and adds key vals from the
// diskMeta map (which should be from a Stat().Meta call) to our own Meta,
// replacing exisiting keys.
//
// It sets our remoteMeta to the given remoteMeta. remoteMeta is used to
// determine which keys need to be removed, and which can be left untouched,
// when updating the metadata for an existing object.
//
// Finally, it adds the remaining standard metadata we apply, replacing existing
// values: date, using the current date, and requesters and sets, appending
// Requester and Set to any existing values in the remoteMeta.
func (r *Request) addStandardMeta(diskMeta, remoteMeta map[string]string) {
	r.cloneMeta()

	for k, v := range diskMeta {
		r.Meta[k] = v
	}

	r.remoteMeta = remoteMeta

	r.addDate()

	r.appendMeta(metaKeyRequester, r.Requester)
	r.appendMeta(metaKeySets, r.Set)
}

// cloneMeta is used to ensure that our Meta is unique to us, so that if we
// alter it, we don't alter any other Request's Meta.
func (r *Request) cloneMeta() {
	clone := make(map[string]string, len(r.Meta))

	for k, v := range r.Meta {
		clone[k] = v
	}

	r.Meta = clone
}

// addDate adds the current date to Meta, replacing any exisiting value.
func (r *Request) addDate() {
	date, _ := timeToMeta(time.Now()) //nolint:errcheck

	r.Meta[metaKeyDate] = date
}

// appendMeta appends the given value to the given key value in our remoteMeta,
// and sets it for our Meta.
func (r *Request) appendMeta(key, val string) {
	if val == "" {
		return
	}

	appended := val

	if rval, exists := r.remoteMeta[key]; exists {
		rvals := strings.Split(rval, metaListSeparator)
		appended = appendValIfNotInList(val, rvals)
	}

	r.Meta[key] = appended
}

// appendValIfNotInList appends val to list if not already in list. Returns the
// list as a comma separated string.
func appendValIfNotInList(val string, list []string) string {
	found := false

	for _, v := range list {
		if v == val {
			found = true

			break
		}
	}

	if !found {
		list = append(list, val)
	}

	return strings.Join(list, metaListSeparator)
}

// needsMetadataUpdate returns true if requesters or sets is different between
// our Meta and remoteMeta. Call this only after confirming a put isn't needed
// by comparing mtimes; this sets skipPut if returning true. Also sets our date
// metadata to the remote value, since we're not uploading now.
func (r *Request) needsMetadataUpdate() bool {
	need := false
	defer func() {
		r.skipPut = need
		r.Meta[metaKeyDate] = r.remoteMeta[metaKeyDate]
	}()

	need = r.valForMetaKeyDifferentOnRemote(metaKeyRequester)
	if need {
		return need
	}

	need = r.valForMetaKeyDifferentOnRemote(metaKeySets)

	return need
}

// valForMetaKeyDifferentOnRemote returns false if key has no remote value.
// Returns true if the remote value is different to ours.
func (r *Request) valForMetaKeyDifferentOnRemote(key string) bool {
	if rval, defined := r.remoteMeta[key]; defined {
		if rval != r.Meta[key] {
			return true
		}
	}

	return false
}

// determineMetadataToRemoveAndAdd compares our Meta to our remoteMeta and
// returns a map of entries where both share a key but have a different value
// (remove these), and a map of those key vals, plus key vals unique to
// wantedMeta (add these).
func (r *Request) determineMetadataToRemoveAndAdd() (map[string]string, map[string]string) {
	toRemove := make(map[string]string)
	toAdd := make(map[string]string)

	for attr, wanted := range r.Meta {
		if remote, exists := r.remoteMeta[attr]; exists { //nolint:nestif
			if wanted != remote {
				toRemove[attr] = remote
				toAdd[attr] = wanted
			}
		} else {
			toAdd[attr] = wanted
		}
	}

	return toRemove, toAdd
}

// PathTransformer is a function that given a local path, returns the
// corresponding remote path.
type PathTransformer func(local string) (remote string, err error)

// NewRequestWithTransformedLocal takes a local path string, uses the given
// PathTransformer to generate the corresponding remote path, and returns a
// Request with Local and Remote set.
func NewRequestWithTransformedLocal(local string, pt PathTransformer) (*Request, error) {
	remote, err := pt(local)
	if err != nil {
		return nil, err
	}

	return &Request{Local: local, Remote: remote}, nil
}

// PrefixTransformer returns a PathTransformer that will replace localPrefix
// in any path given to it with remotePrefix, and return the result.
//
// If the given path does not contain localPrefix, returns the path prefixed
// with remotePrefix (treating the given path as relative to localPrefix).
func PrefixTransformer(localPrefix, remotePrefix string) PathTransformer {
	return func(local string) (string, error) {
		remote := strings.Replace(local, localPrefix, remotePrefix, 1)

		if remote == local {
			remote = filepath.Join(remotePrefix, local)
		}

		return remote, nil
	}
}

// HumgenTransformer is a PathTransformer that will convert a local "lustre"
// path to a "canonical" path in the humgen iRODS zone.
//
// This transform is specific to the "humgen" group at the Sanger Institute.
func HumgenTransformer(local string) (string, error) {
	parts := strings.Split(local, "/")
	ptuPart := -1

	for i, part := range parts {
		if dirIsProjectOrTeamOrUsers(part) {
			ptuPart = i

			break
		}
	}

	if !dirIsLustreWithPTUSubDir(parts[1], ptuPart, len(parts)) {
		return "", Error{ErrNotHumgenLustre, local}
	}

	return fmt.Sprintf("/humgen/%s/%s/%s/%s", parts[ptuPart], parts[ptuPart+1], parts[2],
		strings.Join(parts[ptuPart+2:], "/")), nil
}

// dirIsProjectOrTeamOrUsers returns true if the given directory is projects,
// teams or users.
func dirIsProjectOrTeamOrUsers(dir string) bool {
	return dir == "projects" || dir == "teams" || dir == "users"
}

// dirIsLustreWithPTUSubDir returns true if the given dir is lustre, and you
// found the project|team|users directory at subdirectory 4 or higher, but not
// at the leaf or its parent.
func dirIsLustreWithPTUSubDir(dir string, ptuPart, numParts int) bool {
	return dir == "lustre" && ptuPart >= 4 && ptuPart+2 <= numParts-1
}
