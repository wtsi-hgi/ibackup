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
)

type RequestStatus string

const (
	RequestStatusUploaded   RequestStatus = "uploaded"
	RequestStatusReplaced   RequestStatus = "replaced"
	RequestStatusUnmodified RequestStatus = "unmodified"
	RequestStatusMissing    RequestStatus = "missing"
	RequestStatusFailed     RequestStatus = "failed"
	ErrNotHumgenLustre                    = "not a valid humgen lustre path"
)

// Request represents a local file you would like transferred to a remote iRODS
// path, and the metadata you'd like to associate with it.
type Request struct {
	Local      string
	Remote     string
	Meta       map[string]string
	Status     RequestStatus
	Error      error
	remoteMeta map[string]string
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

// AddMeta ensures our Meta is unique to us, and adds key vals from the given
// map to our own Meta, replacing exisiting keys.
func (r *Request) AddMeta(meta map[string]string) {
	r.cloneMeta()

	for k, v := range meta {
		r.Meta[k] = v
	}
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

// SetRemoteMeta sets our remoteMeta to the given map. remoteMeta is used
// to determine which keys need to be removed, and which can be left untouched,
// when updating the metadata for an existing object.
func (r *Request) SetRemoteMeta(meta map[string]string) {
	r.remoteMeta = meta
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
