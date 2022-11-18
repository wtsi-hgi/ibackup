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

package set

import (
	"bytes"
	"fmt"
	"strings"
	"time"

	"github.com/ugorji/go/codec"
	"github.com/wtsi-hgi/ibackup/put"
	bolt "go.etcd.io/bbolt"
)

type Error struct {
	msg string
	id  string
}

func (e Error) Error() string {
	if e.id != "" {
		return fmt.Sprintf("%s [%s]", e.msg, e.id)
	}

	return e.msg
}

const (
	ErrInvalidSetID   = "invalid set ID"
	ErrInvalidRequest = "request lacks Requester or Set"
	ErrInvalidEntry   = "invalid set entry"

	setsBucket                    = "sets"
	userToSetBucket               = "userLookup"
	subBucketPrefix               = "~!~"
	fileBucket                    = subBucketPrefix + "files"
	dirBucket                     = subBucketPrefix + "dirs"
	discoveredBucket              = subBucketPrefix + "discovered"
	dbOpenMode                    = 0600
	separator                     = ":!:"
	attemptsToBeConsideredFailing = 3
)

// DB is used to create and query a database for storing backup sets (lists of
// files a user wants to have backed up) and their backup status.
type DB struct {
	db *bolt.DB
	ch codec.Handle
}

// New returns a *DB that can be used to create or query a set database. Provide
// the path to the database file.
//
// Returns an error if path exists but can't be opened, or if it doesn't exist
// and can't be created.
func New(path string) (*DB, error) {
	db, err := bolt.Open(path, dbOpenMode, &bolt.Options{
		NoFreelistSync: true,
		NoGrowSync:     true,
		FreelistType:   bolt.FreelistMapType,
	})
	if err != nil {
		return nil, err
	}

	err = db.Update(func(tx *bolt.Tx) error {
		_, errc := tx.CreateBucketIfNotExists([]byte(setsBucket))
		if errc != nil {
			return errc
		}

		_, errc = tx.CreateBucketIfNotExists([]byte(userToSetBucket))

		return errc
	})

	return &DB{
		db: db,
		ch: new(codec.BincHandle),
	}, err
}

// Close closes the database. Be sure to call this to finalise any writes to
// disk correctly.
func (d *DB) Close() error {
	return d.db.Close()
}

// AddOrUpdate adds or updates the given Set to the database.
func (d *DB) AddOrUpdate(set *Set) error {
	err := d.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(setsBucket))

		id := set.ID()
		bid := []byte(id)

		if existing := b.Get(bid); existing != nil {
			eset := d.decodeSet(existing)
			eset.Transformer = set.Transformer
			eset.Monitor = set.Monitor
			eset.Description = set.Description
			set = eset
		}

		errp := b.Put(bid, d.encodeToBytes(set))
		if errp != nil {
			return errp
		}

		b = tx.Bucket([]byte(userToSetBucket))

		return b.Put([]byte(set.Requester+separator+id), bid)
	})

	return err
}

// encodeToBytes encodes the given thing as a byte slice, suitable for storing
// in a database.
func (d *DB) encodeToBytes(thing interface{}) []byte {
	var encoded []byte
	enc := codec.NewEncoderBytes(&encoded, d.ch)
	enc.MustEncode(thing)

	return encoded
}

// SetFileEntries sets the file paths for the given backup set. Only supply
// absolute paths to files.
func (d *DB) SetFileEntries(setID string, paths []string) error {
	return d.setEntries(setID, paths, fileBucket)
}

// setEntries sets the paths for the given backup set in a sub bucket with the
// given prefix. Only supply absolute paths.
//
// *** Currently ignores old entries that are not in the given paths.
func (d *DB) setEntries(setID string, paths []string, bucketName string) error {
	return d.db.Update(func(tx *bolt.Tx) error {
		subBucketName := []byte(bucketName + separator + setID)
		b, existing, err := d.getAndDeleteExistingEntries(tx, subBucketName)
		if err != nil {
			return err
		}

		for _, path := range paths {
			e := d.existingOrNewEncodedEntry(path, existing)

			if errp := b.Put([]byte(path), e); errp != nil {
				return errp
			}
		}

		return nil
	})
}

// getAndDeleteExistingEntries gets existing entries in the given sub bucket
// of the setsBucket, then deletes and recreates the sub bucket. Returns the
// empty sub bucket and any old values.
func (d *DB) getAndDeleteExistingEntries(tx *bolt.Tx, subBucketName []byte) (*bolt.Bucket, map[string][]byte, error) {
	setsBucket := tx.Bucket([]byte(setsBucket))
	existing := make(map[string][]byte)

	b, err := setsBucket.CreateBucketIfNotExists(subBucketName)
	if err != nil {
		return b, existing, err
	}

	err = b.ForEach(func(k, v []byte) error {
		existing[string(k)] = v

		return nil
	})
	if err != nil {
		return b, existing, err
	}

	if len(existing) > 0 {
		if err = setsBucket.DeleteBucket(subBucketName); err != nil {
			return b, existing, err
		}

		b, err = setsBucket.CreateBucket(subBucketName)
	}

	return b, existing, err
}

// existingOrNewEncodedEntry returns the encoded entry from the given map with
// the given path key, or creates a new Entry for path and returns its encoding.
func (d *DB) existingOrNewEncodedEntry(path string, existing map[string][]byte) []byte {
	e := existing[path]
	if e == nil {
		e = d.encodeToBytes(&Entry{Path: path})
	}

	return e
}

// SetDirEntries sets the directory paths for the given backup set. Only supply
// absolute paths to directories.
func (d *DB) SetDirEntries(setID string, paths []string) error {
	return d.setEntries(setID, paths, dirBucket)
}

// SetDiscoveryStarted updates StartedDiscovery and resets some status values
// for the given set. Returns an error if the setID isn't in the database.
func (d *DB) SetDiscoveryStarted(setID string) error {
	return d.db.Update(func(tx *bolt.Tx) error {
		set, bid, b, err := d.getSetByID(tx, setID)
		if err != nil {
			return err
		}

		set.StartedDiscovery = time.Now()
		set.NumFiles = 0
		set.SizeFiles = 0
		set.Uploaded = 0
		set.Failed = 0
		set.Missing = 0
		set.Status = PendingDiscovery
		set.Error = ""

		return b.Put(bid, d.encodeToBytes(set))
	})
}

// getSetByID returns the Set with the given ID from the database, along with
// the byte slice version of the set id and the sets bucket so you can easily
// put the set back again after making changes. Returns an error of setID isn't
// in the database.
func (d *DB) getSetByID(tx *bolt.Tx, setID string) (*Set, []byte, *bolt.Bucket, error) {
	b := tx.Bucket([]byte(setsBucket))
	bid := []byte(setID)

	v := b.Get(bid)
	if v == nil {
		return nil, nil, nil, Error{ErrInvalidSetID, setID}
	}

	set := d.decodeSet(v)

	return set, bid, b, nil
}

// SetDiscoveredEntries sets discovered file paths for the given backup set's
// directory entries. Only supply absolute paths to files.
//
// It also updates LastDiscovery, sets NumFiles and sets status to
// PendingUpload.
//
// Returns an error if the setID isn't in the database.
func (d *DB) SetDiscoveredEntries(setID string, paths []string) error {
	if err := d.setEntries(setID, paths, discoveredBucket); err != nil {
		return err
	}

	return d.db.Update(func(tx *bolt.Tx) error {
		set, bid, b, err := d.getSetByID(tx, setID)
		if err != nil {
			return err
		}

		var numFiles uint64
		cb := func([]byte) {
			numFiles++
		}

		getEntriesViewFunc(tx, setID, fileBucket, cb)
		getEntriesViewFunc(tx, setID, discoveredBucket, cb)

		set.LastDiscovery = time.Now()
		set.NumFiles = numFiles
		set.Status = PendingUpload

		return b.Put(bid, d.encodeToBytes(set))
	})
}

// SetEntryStatus finds the set Entry corresponding to the given Request's
// Local path, Requester and Set name, and updates its status in the database,
// and also updates summary status for the Set. Returns an error if a set or
// entry corresponding to the Request can't be found.
func (d *DB) SetEntryStatus(r *put.Request) error {
	setID, err := requestToSetID(r)
	if err != nil {
		return err
	}

	return d.db.Update(func(tx *bolt.Tx) error {
		set, bid, b, err := d.getSetByID(tx, setID)
		if err != nil {
			return err
		}

		entry, err := d.updateFileEntry(tx, setID, r, set.LastDiscovery)
		if err != nil {
			return err
		}

		if entry.isDir {
			return nil
		}

		updateSetBasedOnEntry(set, entry)

		return b.Put(bid, d.encodeToBytes(set))
	})
}

// requestToSetID returns a setID for the Request. Returns an error if the
// Request doesn't have a Requester and Set defined.
func requestToSetID(r *put.Request) (string, error) {
	if r.Requester == "" || r.Set == "" {
		return "", Error{ErrInvalidRequest, r.Local}
	}

	set := &Set{Requester: r.Requester, Name: r.Set}

	return set.ID(), nil
}

// updateFileEntry updates the file entry for the set based on the request's
// info.
//
// Returns the updated entry with private properties indicating if the its a
// dir, or otherwise if upload failed, and this was the first upload attempt
// since the last success, and if this was the first successful upload after a
// failure.
//
// If setDiscoveryTime is later than the entry's last attempt, resets the
// entries Attempts to 0.
func (d *DB) updateFileEntry(tx *bolt.Tx, setID string, r *put.Request, setDiscoveryTime time.Time) (*Entry, error) {
	entry, b, err := d.getEntry(tx, setID, r.Local)
	if err != nil {
		return nil, err
	}

	if setDiscoveryTime.After(entry.LastAttempt) {
		entry.Attempts = 0
		entry.LastError = ""
	}

	entry.LastAttempt = time.Now()
	entry.Size = r.Size
	requestStatusToEntryStatus(r, entry)

	return entry, b.Put([]byte(r.Local), d.encodeToBytes(entry))
}

// requestStatusToEntryStatus converts Request.Status and stores it as a Status
// on the entry. Also sets entry.Attempts, unFailed and newFail as appropriate.
func requestStatusToEntryStatus(r *put.Request, entry *Entry) {
	oldAttempts := entry.Attempts
	entry.Attempts++
	entry.newFail = false
	entry.unFailed = false

	switch r.Status {
	case put.RequestStatusUploaded, put.RequestStatusUnmodified, put.RequestStatusReplaced:
		entry.Status = Uploaded
		entry.unFailed = oldAttempts > 1
	case put.RequestStatusFailed:
		entry.Status = Failed
		entry.LastError = r.Error.Error()
		entry.newFail = oldAttempts == 0
	case put.RequestStatusMissing:
		entry.Status = Missing
		entry.unFailed = oldAttempts > 1
	}
}

// getEntry finds the Entry for the given path in the given set. Returns it
// along with the bucket it was in, so you can alter the Entry and put it back.
// Returns an error if the entry can't be found.
func (d *DB) getEntry(tx *bolt.Tx, setID, path string) (*Entry, *bolt.Bucket, error) {
	setsBucket := tx.Bucket([]byte(setsBucket))

	var (
		entry *Entry
		b     *bolt.Bucket
	)

	for _, kind := range []string{fileBucket, discoveredBucket, dirBucket} {
		entry, b = d.getEntryFromSubbucket(kind, setID, path, setsBucket)
		if entry != nil {
			break
		}
	}

	if entry == nil {
		return nil, nil, Error{ErrInvalidEntry, "set " + setID + " has no path " + path}
	}

	return entry, b, nil
}

// getEntryFromSubbucket gets an Entry for the given path from a sub-bucket of
// the setsBucket for the kind (fileBucket, discoveredBucket or dirBucket) and
// set ID. If it doesn't exist, just returns nil. Also returns the subbucket it
// was in. The entry will have isDir true if kind is dirBucket.
func (d *DB) getEntryFromSubbucket(kind, setID, path string, setsBucket *bolt.Bucket) (*Entry, *bolt.Bucket) {
	subBucketName := []byte(kind + separator + setID)

	b := setsBucket.Bucket(subBucketName)
	if b == nil {
		return nil, nil
	}

	v := b.Get([]byte(path))
	if v == nil {
		return nil, nil
	}

	entry := d.decodeEntry(v)
	entry.isDir = kind == dirBucket

	return entry, b
}

// updateSetBasedOnEntry updates set status values based on an updated Entry
// from updateFileEntry(), assuming that request is for one of set's file
// entries.
func updateSetBasedOnEntry(set *Set, entry *Entry) {
	if set.Status == PendingDiscovery || set.Status == PendingUpload {
		set.Status = Uploading
	}

	if entry.Attempts == 1 {
		set.SizeFiles += entry.Size
	}

	if entry.unFailed {
		set.Failed--

		if set.Failed <= 0 {
			set.Status = Uploading
		}
	}

	entryStatusToSetCounts(entry, set)

	if set.Uploaded+set.Failed+set.Missing == set.NumFiles {
		set.Status = Complete
		set.LastCompleted = time.Now()
		set.LastCompletedCount = set.Uploaded + set.Failed
		set.LastCompletedSize = set.SizeFiles
	}
}

// entryStatusToSetCounts increases set Uploaded, Failed or Missing based on
// set.Status.
func entryStatusToSetCounts(entry *Entry, set *Set) {
	switch entry.Status { //nolint:exhaustive
	case Uploaded:
		set.Uploaded++
	case Failed:
		if entry.newFail {
			set.Failed++
		}

		if entry.Attempts >= attemptsToBeConsideredFailing {
			set.Status = Failing
		}
	case Missing:
		set.Missing++
	}
}

// GetAll returns all the Sets previously added to the database.
func (d *DB) GetAll() ([]*Set, error) {
	var sets []*Set

	err := d.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(setsBucket))

		return b.ForEach(func(k, v []byte) error {
			if strings.HasPrefix(string(k), subBucketPrefix) {
				return nil
			}

			sets = append(sets, d.decodeSet(v))

			return nil
		})
	})

	return sets, err
}

// decodeSet takes a byte slice representation of a Set as stored in the db by
// AddOrUpdate(), and converts it back in to a *Set.
func (d *DB) decodeSet(v []byte) *Set {
	dec := codec.NewDecoderBytes(v, d.ch)

	var set *Set

	dec.MustDecode(&set)

	return set
}

// GetByRequester returns all the Sets previously added to the database by the
// given requester.
func (d *DB) GetByRequester(requester string) ([]*Set, error) {
	var sets []*Set

	err := d.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket([]byte(userToSetBucket)).Cursor()
		b := tx.Bucket([]byte(setsBucket))

		prefix := []byte(requester + separator)
		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			encodedSet := b.Get(v)
			if encodedSet == nil {
				continue
			}

			sets = append(sets, d.decodeSet(encodedSet))
		}

		return nil
	})

	return sets, err
}

// GetByID returns the Sets with the given ID previously added to the database.
// Returns nil if such a set does not exist.
func (d *DB) GetByID(id string) *Set {
	var set *Set

	d.db.View(func(tx *bolt.Tx) error { //nolint:errcheck
		b := tx.Bucket([]byte(setsBucket))

		v := b.Get([]byte(id))
		if v != nil {
			set = d.decodeSet(v)
		}

		return nil
	})

	return set
}

// GetFileEntries returns all the file entries for the given set (both
// SetFileEntries and SetDiscoveredEntries).
func (d *DB) GetFileEntries(setID string) ([]*Entry, error) {
	entries, err := d.getEntries(setID, fileBucket)
	if err != nil {
		return nil, err
	}

	entries2, err := d.getEntries(setID, discoveredBucket)
	if err != nil {
		return nil, err
	}

	return append(entries, entries2...), nil
}

// getEntries returns all the entries for the given set from the given sub
// bucket prefix.
func (d *DB) getEntries(setID, bucketName string) ([]*Entry, error) {
	var entries []*Entry

	cb := func(v []byte) {
		entries = append(entries, d.decodeEntry(v))
	}

	err := d.db.View(func(tx *bolt.Tx) error {
		getEntriesViewFunc(tx, setID, bucketName, cb)

		return nil
	})

	return entries, err
}

type getEntriesViewCallBack func(v []byte)

func getEntriesViewFunc(tx *bolt.Tx, setID, bucketName string, cb getEntriesViewCallBack) {
	subBucketName := []byte(bucketName + separator + setID)
	setsBucket := tx.Bucket([]byte(setsBucket))

	entriesBucket := setsBucket.Bucket(subBucketName)
	if entriesBucket == nil {
		return
	}

	entriesBucket.ForEach(func(_, v []byte) error { //nolint:errcheck
		cb(v)

		return nil
	})
}

// decodeEntry takes a byte slice representation of an Entry as stored in the db
// by Set*Entries(), and converts it back in to an *Entry.
func (d *DB) decodeEntry(v []byte) *Entry {
	dec := codec.NewDecoderBytes(v, d.ch)

	var entry *Entry

	dec.MustDecode(&entry)

	return entry
}

// GetPureFileEntries returns all the file entries for the given set (only
// SetFileEntries, not SetDiscoveredEntries).
func (d *DB) GetPureFileEntries(setID string) ([]*Entry, error) {
	return d.getEntries(setID, fileBucket)
}

// GetDirEntries returns all the dir entries for the given set.
func (d *DB) GetDirEntries(setID string) ([]*Entry, error) {
	return d.getEntries(setID, dirBucket)
}

// SetError updates a set with the given error message. Returns an error if the
// setID isn't in the database.
func (d *DB) SetError(setID, errMsg string) error {
	return d.db.Update(func(tx *bolt.Tx) error {
		set, bid, b, err := d.getSetByID(tx, setID)
		if err != nil {
			return err
		}

		set.Error = errMsg

		return b.Put(bid, d.encodeToBytes(set))
	})
}
