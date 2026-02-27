/*******************************************************************************
 * Copyright (c) 2025 Genome Research Ltd.
 *
 * Author: Sendu Bala <sb10@sanger.ac.uk>
 * Author: Rosie Kern <rk18@sanger.ac.uk>
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
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/wtsi-hgi/ibackup/internal"
)

const (
	MetaNamespace     = "ibackup:"
	MetaUserNamespace = MetaNamespace + "user:"
	MetaFOFNNamespace = MetaNamespace + "fofn:"
	MetaFOFNSet       = MetaFOFNNamespace + "set"
	MetaKeyMtime      = MetaNamespace + "mtime"   // mtime of source file, 1sec truncated UTC RFC 3339
	MetaKeyOwner      = MetaNamespace + "owner"   // a username
	MetaKeyGroup      = MetaNamespace + "group"   // a unix group name
	MetaKeyDate       = MetaNamespace + "date"    // date upload initiated, 1sec truncated UTC RFC 3339
	MetaKeyReason     = MetaNamespace + "reason"  // storage reason: backup|archive|quarantine
	MetaKeyReview     = MetaNamespace + "review"  // a date for review
	MetaKeyRemoval    = MetaNamespace + "removal" // a date for removal
	// a comma sep list of usernames of the people who reqested the backup.
	MetaKeyRequester = MetaNamespace + "requesters"
	// a comma sep list of backup set names this file belongs to.
	MetaKeySets    = MetaNamespace + "sets"
	MetaKeySymlink = MetaNamespace + "symlink" // symlink destination if file is a symlink
	// the first path seen with this inode if file is a hardlink.
	MetaKeyHardlink = MetaNamespace + "hardlink"
	// iRODS path that contains the data for this hardlink.
	MetaKeyRemoteHardlink = MetaNamespace + "remotehardlink"
	MetaKeyFofn           = MetaNamespace + "fofn"
	validMetaParts        = 2
	validMetaKeyDividers  = 2
	metaListSeparator     = ","

	errInvalidMetaNamespace    = "namespace is incorrect, must be 'ibackup:user:', 'ibackup:fofn:', or empty"
	errInvalidMetaLength       = "meta must be provided in the form key=value"
	ErrInvalidReason           = "reason must be 'backup', 'archive', 'quarantine'"
	ErrInvalidDurationFormat   = "duration must be in the form <number><unit>, or YYYY-MM-DD"
	ErrInvalidReviewRemoveDate = "--review duration must be smaller than --removal duration"
)

var Reasons = []string{"unset", "backup", "archive", "quarantine"} //nolint:gochecknoglobals

type Reason int

const (
	Unset Reason = iota
	Backup
	Archive
	Quarantine
)

type MetaError struct {
	err    string
	keyVal string
}

func (e MetaError) Error() string {
	return fmt.Sprintf("invalid meta: '%s' %v", e.keyVal, e.err)
}

func (e MetaError) Is(err error) bool {
	var metaErr *MetaError
	if errors.As(err, &metaErr) {
		return metaErr.err == e.err
	}

	return false
}

func (r *Reason) Set(value string) error {
	index := slices.Index(Reasons, value)
	if index != -1 {
		*r = Reason(index)

		return nil
	}

	return MetaError{ErrInvalidReason, value}
}

// ValidateAndCreateUserMetadata takes a key=value string, validates it as a
// metadata value then returns the key prefixed with the user namespace,
// 'ibackup:user:', and the value. Returns an error if the meta is invalid.
func ValidateAndCreateUserMetadata(kv string) (string, string, error) {
	parts := strings.Split(kv, "=")
	if len(parts) != validMetaParts {
		return "", "", MetaError{errInvalidMetaLength, kv}
	}

	key, err := handleNamespace(parts[0])
	value := parts[1]

	return key, value, err
}

// handleNamespace prefixes the user namespace 'ibackup:user:' onto the key if
// it isn't already included. Returns an error if the key contains an invalid
// namespace.
func handleNamespace(key string) (string, error) {
	keyDividers := strings.Count(key, ":")

	switch {
	case keyDividers == 0:
		return MetaUserNamespace + key, nil
	case keyDividers != validMetaKeyDividers:
		return "", MetaError{errInvalidMetaNamespace, key}
	case strings.HasPrefix(key, MetaUserNamespace), strings.HasPrefix(key, MetaFOFNNamespace):
		return key, nil
	default:
		return "", MetaError{errInvalidMetaNamespace, key}
	}
}

// createBackupMetadata adds the backup metadata values to the local meta map if
// the provided inputs are valid.
func createBackupMetadata(reason Reason, review, removal string, mm *Meta) error {
	if reason == Unset {
		reason = Backup
	}

	review, removal = setReviewAndRemovalDurations(reason, review, removal)

	removalDate, err := getFutureDateFromDurationOrDate(removal)
	if err != nil {
		return err
	}

	reviewDate, err := getFutureDateFromDurationOrDate(review)
	if err != nil {
		return err
	}

	if reviewDate.After(removalDate) {
		return MetaError{ErrInvalidReviewRemoveDate, fmt.Sprintf("%s is after %s", review, removal)}
	}

	reviewStr, removalStr, err := reviewRemovalDatesToMeta(reviewDate, removalDate)
	if err != nil {
		return err
	}

	mm.setBackupMeta(reason.String(), reviewStr, removalStr)

	return nil
}

// setReviewAndRemovalDurations returns the review and removal durations for a
// given reason. If the user has not set a duration, the function will return
// the default corresponding to the provided reason.
func setReviewAndRemovalDurations(reason Reason, review, removal string) (string, string) {
	defaultReviewDuration, defaultRemovalDuration := getDefaultReviewAndRemovalDurations(reason)
	if review == "" {
		review = defaultReviewDuration
	}

	if removal == "" {
		removal = defaultRemovalDuration
	}

	return review, removal
}

func getDefaultReviewAndRemovalDurations(reason Reason) (string, string) {
	switch reason {
	case Archive:
		return "1y", "2y"
	case Quarantine:
		return "2m", "3m"
	default:
		return "6m", "1y"
	}
}

func reviewRemovalDatesToMeta(review, removal time.Time) (string, string, error) {
	reviewStr, err := internal.TimeToMeta(review)
	if err != nil {
		return "", "", err
	}

	removalStr, err := internal.TimeToMeta(removal)

	return reviewStr, removalStr, err
}

// getFutureDateFromDurationOrDate calculates the future date based on the time
// string provided. Returns an error if duration is not in the format
// '<number><unit>', e.g. '1y', '12m' or YYYY-MM-DD.
func getFutureDateFromDurationOrDate(t string) (time.Time, error) {
	date, err := time.Parse("2006-01-02", t)
	if err == nil {
		return date, nil
	}

	num, err := strconv.Atoi(t[:len(t)-1])
	if err != nil {
		return time.Time{}, MetaError{ErrInvalidDurationFormat, t}
	}

	switch t[len(t)-1] {
	case 'y':
		return time.Now().AddDate(num, 0, 0), nil
	case 'm':
		return time.Now().AddDate(0, num, 0), nil
	default:
		return time.Time{}, MetaError{ErrInvalidDurationFormat, t}
	}
}

func (r Reason) String() string {
	return Reasons[r]
}

func (r Reason) Type() string {
	return "Reason"
}

func areBackupInputsAllBlank(reason Reason, review, removal string) bool {
	return reason == Unset && review == "" && removal == ""
}

type Meta struct {
	LocalMeta  map[string]string
	remoteMeta map[string]string
}

func NewMeta() *Meta {
	return &Meta{
		LocalMeta:  make(map[string]string),
		remoteMeta: make(map[string]string),
	}
}

// handleMeta takes the user provided meta and the backup meta inputs and
// returns a Meta containing all valid metadata.
func HandleMeta(meta string, reason Reason, review, removal string, existingMeta map[string]string) (*Meta, error) {
	mm, err := ParseMetaString(meta, existingMeta)
	if err != nil {
		return nil, err
	}

	if areBackupInputsAllBlank(reason, review, removal) && existingMeta != nil {
		mm.setBackupMeta(existingMeta[MetaKeyReason], existingMeta[MetaKeyReview],
			existingMeta[MetaKeyRemoval])

		return mm, nil
	}

	err = createBackupMetadata(reason, review, removal, mm)

	return mm, err
}

// ParseMetaString takes a string of key=value pairs and map of existing
// metadata and returns a Meta. If the metadata string is nonblank and valid, it
// will be used to populate the Meta, otherwise the existing meta will be used.
func ParseMetaString(meta string, existingMetadata map[string]string) (*Meta, error) {
	kvs := strings.Split(meta, ";")
	mm := make(map[string]string, len(kvs))

	if meta == "" {
		return applyExistingMetadata(mm, existingMetadata), nil
	}

	for _, kv := range kvs {
		key, value, err := ValidateAndCreateUserMetadata(kv)
		if err != nil {
			return nil, MetaError{err: err.Error(), keyVal: kv}
		}

		mm[key] = value
	}

	return &Meta{LocalMeta: mm}, nil
}

func applyExistingMetadata(mm map[string]string, existingMetadata map[string]string) *Meta {
	for k, v := range existingMetadata {
		if strings.HasPrefix(k, MetaUserNamespace) {
			mm[k] = v
		}
	}

	return &Meta{LocalMeta: mm, remoteMeta: make(map[string]string)}
}

func (m *Meta) setBackupMeta(reason, review, removal string) {
	m.LocalMeta[MetaKeyReason] = reason
	m.LocalMeta[MetaKeyReview] = review
	m.LocalMeta[MetaKeyRemoval] = removal
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
func (m *Meta) addStandardMeta(diskMeta, remoteMeta map[string]string, requester, set string) {
	m.uniquify()

	maps.Copy(m.LocalMeta, diskMeta)

	m.remoteMeta = remoteMeta

	m.addDate()

	m.appendMeta(MetaKeyRequester, requester)
	m.appendMeta(MetaKeySets, set)
}

// uniquify is used to ensure that our Meta is unique to us, so that if we
// alter it, we don't alter any other Request's Meta.
func (m *Meta) uniquify() {
	m.LocalMeta = maps.Clone(m.LocalMeta)
	m.remoteMeta = maps.Clone(m.remoteMeta)
}

// Clone returns a new Meta containing a Clone of our maps inside the provided
// Meta.
func (m *Meta) Clone() *Meta {
	newMeta := Meta{}
	newMeta.LocalMeta = maps.Clone(m.LocalMeta)
	newMeta.remoteMeta = maps.Clone(m.remoteMeta)

	return &newMeta
}

// addDate adds the current date to localMeta, replacing any exisiting value.
func (m *Meta) addDate() {
	date, _ := internal.TimeToMeta(time.Now()) //nolint:errcheck

	m.LocalMeta[MetaKeyDate] = date
}

// appendMeta appends the given value to the given key value in our remoteMeta,
// and sets it for our Meta.
func (m *Meta) appendMeta(key, val string) {
	if val == "" {
		return
	}

	appended := val

	if rval, exists := m.remoteMeta[key]; exists {
		rvals := strings.Split(rval, metaListSeparator)
		appended = appendValIfNotInList(val, rvals)
	}

	m.LocalMeta[key] = appended
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
// by comparing mtimes; request.skipPut should be set to the return value. Also
// sets our date metadata to the remote value, since we're not uploading now.
func (m *Meta) needsMetadataUpdate() bool {
	if remoteDate := m.remoteMeta[MetaKeyDate]; remoteDate != "" {
		m.LocalMeta[MetaKeyDate] = remoteDate
	}

	return m.valForMetaKeyDifferentOnRemote(MetaKeyRequester) ||
		m.valForMetaKeyDifferentOnRemote(MetaKeySets)
}

// valForMetaKeyDifferentOnRemote returns false if key has no remote value.
// Returns true if the remote value is different to ours.
func (m *Meta) valForMetaKeyDifferentOnRemote(key string) bool {
	if rval, defined := m.remoteMeta[key]; defined {
		if rval != m.LocalMeta[key] {
			return true
		}
	}

	return false
}

// determineMetadataToRemoveAndAdd compares our localMeta to our remoteMeta and
// returns a map of entries where both share a key but have a different value
// (remove these), and a map of those key vals, plus key vals unique to
// wantedMeta (add these).
func (m *Meta) determineMetadataToRemoveAndAdd() (map[string]string, map[string]string) {
	toRemove := make(map[string]string)
	toAdd := make(map[string]string)

	for attr, wanted := range m.LocalMeta {
		if remote, exists := m.remoteMeta[attr]; exists { //nolint:nestif
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

// setHardlinks takes the local and remote hardlinks and sets them in localMeta.
func (m *Meta) setHardlinks(local, remote string) {
	m.LocalMeta[MetaKeyHardlink] = local
	m.LocalMeta[MetaKeyRemoteHardlink] = remote
}

// Metadata returns a clone of a Meta's localMeta.
func (m *Meta) Metadata() map[string]string {
	return maps.Clone(m.LocalMeta)
}

// SetLocal sets a Meta's localMeta given a key and a value.
func (m *Meta) SetLocal(key, value string) {
	m.LocalMeta[key] = value
}
