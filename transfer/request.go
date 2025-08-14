/*******************************************************************************
 * Copyright (c) 2022, 2023, 2025 Genome Research Ltd.
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

package transfer

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/dgryski/go-farm"
	"github.com/wtsi-hgi/ibackup/errs"
	"golang.org/x/sys/unix"
)

type RequestStatus string

const (
	RequestStatusPending         RequestStatus = "pending"
	RequestStatusReserved        RequestStatus = "reserved"
	RequestStatusUploading       RequestStatus = "uploading"
	RequestStatusUploaded        RequestStatus = "uploaded"
	RequestStatusReplaced        RequestStatus = "replaced"
	RequestStatusUnmodified      RequestStatus = "unmodified"
	RequestStatusMissing         RequestStatus = "missing"
	RequestStatusOrphaned        RequestStatus = "orphaned"
	RequestStatusFailed          RequestStatus = "failed"
	RequestStatusWarning         RequestStatus = "warning"
	RequestStatusHardlinkSkipped RequestStatus = "hardlink"
	stuckTimeFormat                            = "02/01/06 15:04 MST"
	defaultDirPerms                            = 0777
)

// Stuck is used to provide details of a potentially "stuck" upload Request.
type Stuck struct {
	UploadStarted time.Time
	Host          string
	PID           int
}

// NewStuck returns a Stuck with the given UploadStarted and the current Host
// and PID.
func NewStuck(uploadStarted time.Time) *Stuck {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}

	return &Stuck{
		UploadStarted: uploadStarted,
		Host:          hostname,
		PID:           os.Getpid(),
	}
}

// String returns a message explaining our stuck details.
func (s *Stuck) String() string {
	return fmt.Sprintf("upload stuck? started %s on host %s, PID %d",
		s.UploadStarted.Format(stuckTimeFormat), s.Host, s.PID)
}

// Request represents a local file you would like transferred to a remote iRODS
// path, and any extra metadata (beyond the defaults which include user, group,
// mtime, upload date) you'd like to associate with it. Setting Requester and
// Set will add these to the requesters and sets metadata on upload.
type Request struct {
	Local               string
	Remote              string
	LocalForJSON        []byte // set by MakeSafeForJSON(); do not set this yourself.
	RemoteForJSON       []byte // set by MakeSafeForJSON(); do not set this yourself.
	Requester           string
	Set                 string
	Meta                *Meta
	Status              RequestStatus
	Symlink             string // contains symlink path if request represents a symlink.
	Hardlink            string // contains first seen path if request represents a hard-linked file.
	Size                uint64 // size of Local in bytes, set for you on returned Requests.
	Error               string
	Stuck               *Stuck
	skipPut             bool
	emptyFileRequest    *Request
	inodeRequest        *Request
	onlyUploadEmptyFile bool
}

// MakeSafeForJSON copies Local and Remote to LocalForJSON and RemoteForJSON,
// so that if this Request struct is encoded as JSON, non-UTF8 characters will
// be preserved. On decode be sure to use CorrectFromJSON().
func (r *Request) MakeSafeForJSON() {
	r.LocalForJSON = []byte(r.Local)
	r.RemoteForJSON = []byte(r.Remote)
}

// CorrectFromJSON copies LocalForJSON and RemoteForJSON to Local and Remote,
// so that these values are correct following a decode from JSON.
func (r *Request) CorrectFromJSON() {
	r.Local = string(r.LocalForJSON)
	r.Remote = string(r.RemoteForJSON)
}

// ID returns a deterministic identifier that is unique to this Request's
// combination of Local, Remote, Requester and Set.
func (r *Request) ID() string {
	concat := strings.Join([]string{
		r.Local,
		r.Remote,
		r.Requester,
		r.Set,
	}, ":")

	l, h := farm.Hash128([]byte(concat))

	return fmt.Sprintf("%016x%016x", l, h)
}

// Prepare calls ValidatePaths and returns its error, if any. Also prepares
// the request for correct hardlink handling, if Hardlink is set.
func (r *Request) Prepare() error {
	if err := r.ValidatePaths(); err != nil {
		return err
	}

	if r.Hardlink == "" {
		return nil
	}

	emptyFileRequest := r.Clone()
	emptyFileRequest.Local = os.DevNull
	emptyFileRequest.Meta.setHardlinks(r.Local, r.Hardlink)
	emptyFileRequest.Hardlink = ""

	inodeRequest := r.Clone()
	inodeRequest.Remote = r.Hardlink
	inodeRequest.Hardlink = ""

	r.emptyFileRequest = emptyFileRequest
	r.inodeRequest = inodeRequest

	return nil
}

// ValidatePaths checks that both Local and Remote paths are absolute. (It does
// NOT check that either path exists.)
func (r *Request) ValidatePaths() error {
	local, err := filepath.Abs(r.Local)
	if err != nil {
		return errs.PathError{Msg: ErrLocalNotAbs, Path: r.Local}
	}

	r.Local = local

	if !filepath.IsAbs(r.Remote) {
		return errs.PathError{Msg: ErrRemoteNotAbs, Path: r.Remote}
	}

	return nil
}

// Remotes normally returns Remote, but if we have a Hardlink, also returns that
// as a remote.
func (r *Request) Remotes() []string {
	remotes := []string{r.Remote}

	if r.Hardlink != "" {
		remotes = append(remotes, r.Hardlink)
	}

	return remotes
}

// RemoteDataPath should be used if you want to know where the actual data for
// this request would get uploaded to. Normally returns Remote, but if a
// hardlink, returns Hardlink.
func (r *Request) RemoteDataPath() string {
	if r.Hardlink == "" {
		return r.Remote
	}

	return r.Hardlink
}

// LocalDataPath should be used instead of Request.Local for upload purposes.
//
// For symlinks, returns a zero-sized file as iRODS doesn't handle
// links appropriately (there will be metadata allowing its recreation).
// Otherwise returns Request.Local.
func (r *Request) LocalDataPath() string {
	if r.Symlink == "" {
		return r.Local
	}

	return os.DevNull
}

// UploadedSize returns the number of bytes that were uploaded if
// Request.UploadPath was used.
func (r *Request) UploadedSize() uint64 {
	if r.Symlink == "" && r.Hardlink == "" {
		return r.Size
	}

	return 0
}

// Clone returns a copy of this request that can safely be altered without
// affecting the original.
func (r *Request) Clone() *Request {
	clone := &Request{
		Local:     r.Local,
		Remote:    r.Remote,
		Requester: r.Requester,
		Set:       r.Set,
		Meta:      r.Meta,
		Status:    r.Status,
		Symlink:   r.Symlink,
		Hardlink:  r.Hardlink,
		Size:      r.Size,
		Error:     r.Error,
		Stuck:     r.Stuck,
		skipPut:   r.skipPut,
	}

	clone.Meta = r.Meta.Clone()

	return clone
}

// StatAndAssociateStandardMetadata uses the handler to "stat" our Remote, and
// the given diskMeta to apply standard metadata to ourselves.
//
// For hardlinks, does the same for our Hardlink and empty file to capture that
// info.
func (r *Request) StatAndAssociateStandardMetadata(lInfo *ObjectInfo, handler Handler) (*ObjectInfo, error) {
	diskMeta := lInfo.Meta

	if r.Hardlink == "" {
		return statAndAssociateStandardMetadata(r, diskMeta, handler)
	}

	rInfoEmpty, err := statAndAssociateStandardMetadata(r.emptyFileRequest, diskMeta, handler)
	if err != nil {
		return nil, err
	}

	rInfo, err := statAndAssociateStandardMetadata(r.inodeRequest, diskMeta, handler)
	if err != nil {
		return nil, err
	}

	r.Meta = r.inodeRequest.Meta.Clone()

	if lInfo.HasSameModTime(rInfo) && !rInfoEmpty.Exists {
		rInfo = rInfoEmpty
		r.onlyUploadEmptyFile = true
	}

	return rInfo, nil
}

func statAndAssociateStandardMetadata(request *Request, diskMeta map[string]string,
	handler Handler) (*ObjectInfo, error) {
	exists, meta, err := handler.Stat(request.Remote)
	if err != nil {
		return nil, err
	}

	rInfo := ObjectInfo{Exists: exists, Meta: meta}

	request.Meta.addStandardMeta(diskMeta, rInfo.Meta, request.Requester, request.Set)

	return &rInfo, nil
}

func (r *Request) GetRemoteMetadata(handler Handler) (*ObjectInfo, error) {
	return statAndAssociateStandardMetadata(r, nil, handler)
}

// RemoveAndAddMetadata removes and adds metadata on our Remote based on the
// disk and remote metadata discovered during
// StatAndAssociateStandardMetadata().
//
// Handles hardlinks (their Hardlink remote and empty file) appropriately.
func (r *Request) RemoveAndAddMetadata(handler Handler) error {
	if r.Hardlink == "" {
		return removeAndAddMetadata(r, handler)
	}

	if err := removeAndAddMetadata(r.emptyFileRequest, handler); err != nil {
		return err
	}

	if r.onlyUploadEmptyFile && !r.inodeRequest.skipPut {
		r.inodeRequest.skipPut = r.inodeRequest.Meta.needsMetadataUpdate()
		if !r.inodeRequest.skipPut {
			return nil
		}
	}

	return removeAndAddMetadata(r.inodeRequest, handler)
}

// SetMeta will set the MTime and GID on a locally restored file.
func (r *Request) SetMeta(_ Handler) error {
	mtime, ok := r.Meta.remoteMeta[MetaKeyMtime]
	if ok {
		if err := setTimes(r.Local, mtime); err != nil {
			return err
		}
	}

	group, ok := r.Meta.remoteMeta[MetaKeyGroup]
	if ok {
		return setGroup(r.Local, group)
	}

	return nil
}

func setTimes(file, mtime string) error {
	var t time.Time

	if err := t.UnmarshalText([]byte(mtime)); err != nil {
		return err
	}

	tv := unix.Timeval{Sec: t.Unix()}

	return unix.Lutimes(file, []unix.Timeval{tv, tv})
}

func setGroup(file, group string) error {
	u, err := user.Current()
	if err != nil {
		return err
	}

	uid, err := strconv.ParseUint(u.Uid, 10, 64)
	if err != nil {
		return err
	}

	g, err := user.LookupGroup(group)
	if err != nil {
		g = &user.Group{Gid: u.Gid}
	}

	gid, err := strconv.ParseUint(g.Gid, 10, 64)
	if err != nil {
		return err
	}

	return os.Lchown(file, int(uid), int(gid)) //nolint:gosec
}

func removeAndAddMetadata(r *Request, handler Handler) error {
	toRemove, toAdd := r.Meta.determineMetadataToRemoveAndAdd()

	if err := r.removeMeta(handler, toRemove); err != nil {
		return err
	}

	return r.addMeta(handler, toAdd)
}

func (r *Request) removeMeta(handler Handler, toRemove map[string]string) error {
	if len(toRemove) == 0 {
		return nil
	}

	return handler.RemoveMeta(r.Remote, toRemove)
}

func (r *Request) addMeta(handler Handler, toAdd map[string]string) error {
	if len(toAdd) == 0 {
		return nil
	}

	return handler.AddMeta(r.Remote, toAdd)
}

// Put uses the given handler to upload our Local file to Remote. This has
// special handling for hardlinks: uploads Local to Hardlink, and an empty file
// to Remote, with linking metadata.
func (r *Request) Put(handler Handler) error {
	if r.Hardlink == "" {
		return handler.Put(r.LocalDataPath(), r.Remote)
	}

	if !r.onlyUploadEmptyFile {
		if err := handler.Put(r.inodeRequest.LocalDataPath(), r.inodeRequest.Remote); err != nil {
			return err
		}
	}

	return handler.Put(r.emptyFileRequest.LocalDataPath(), r.emptyFileRequest.Remote)
}

// Get uses the given handler to download our Remote file to Local. This has
// special handling for symlinks; it creates the symlink without consulting
// the handler.
func (r *Request) Get(handler Handler) error {
	if err := os.MkdirAll(filepath.Dir(r.Local), defaultDirPerms); err != nil {
		return err
	}

	if r.Symlink != "" {
		return r.createSymlinkIfRequired()
	}

	return handler.Get(r.Local, r.Remote)
}

func (r *Request) createSymlinkIfRequired() error {
	symlink, err := os.Readlink(r.Local)
	if symlink == r.Symlink { //nolint:nestif
		return nil
	} else if !errors.Is(err, fs.ErrNotExist) {
		if err = os.Remove(r.Local); err != nil {
			return err
		}
	}

	return os.Symlink(r.Symlink, r.Local)
}

// NewRequestWithTransformedLocal takes a local path string, uses the given
// PathTransformer to generate the corresponding remote path, and returns a
// Request with Local and Remote set.
func NewRequestWithTransformedLocal(local string, pt PathTransformer) (*Request, error) {
	remote, err := pt(local)
	if err != nil {
		return nil, err
	}

	return &Request{Local: local, Remote: remote, Meta: NewMeta()}, nil
}
