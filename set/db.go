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
	ErrInvalidSetID = "invalid set ID"

	setsBucket      = "sets"
	userToSetBucket = "userLookup"
	fileBucket      = "files"
	dirBucket       = "dirs"
	dbOpenMode      = 0600
	separator       = ":!:"
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

// setDiscoveryStarted updates StartedDiscovery and resets some status values
// for the given set. Returns an error if the setID isn't in the database.
func (d *DB) setDiscoveryStarted(setID string) error {
	return d.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(setsBucket))
		bid := []byte(setID)

		v := b.Get(bid)
		if v == nil {
			return Error{ErrInvalidSetID, setID}
		}

		set := d.decodeSet(v)
		set.StartedDiscovery = time.Now()
		set.NumFiles = 0
		set.SizeFiles = 0
		set.Uploaded = 0
		set.Status = PendingDiscovery

		return b.Put(bid, d.encodeToBytes(set))
	})
}

// setDiscoveryCompleted updates LastDiscovery, sets numFiles and sizeFiles and
// sets status to PendingUpload. Returns an error if the setID isn't in the
// database.
func (d *DB) setDiscoveryCompleted(setID string, numFiles, sizeFiles uint64) error {
	return d.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(setsBucket))
		bid := []byte(setID)

		v := b.Get(bid)
		if v == nil {
			return Error{ErrInvalidSetID, setID}
		}

		set := d.decodeSet(v)
		set.LastDiscovery = time.Now()
		set.NumFiles = numFiles
		set.SizeFiles = sizeFiles
		set.Uploaded = 0
		set.Status = PendingUpload

		return b.Put(bid, d.encodeToBytes(set))
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

// GetAll returns all the Sets previously added to the database.
func (d *DB) GetAll() ([]*Set, error) {
	var sets []*Set

	err := d.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(setsBucket))

		return b.ForEach(func(k, v []byte) error {
			switch {
			case strings.HasPrefix(string(k), dirBucket), strings.HasPrefix(string(k), fileBucket):
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

// getFileEntries returns all the file entries for the given set.
func (d *DB) getFileEntries(setID string) ([]*Entry, error) {
	return d.getEntries(setID, fileBucket)
}

// getEntries returns all the entries for the given set from the given sub
// bucket prefix.
func (d *DB) getEntries(setID, bucketName string) ([]*Entry, error) {
	var entries []*Entry

	err := d.db.View(func(tx *bolt.Tx) error {
		subBucketName := []byte(bucketName + separator + setID)
		setsBucket := tx.Bucket([]byte(setsBucket))

		entriesBucket := setsBucket.Bucket(subBucketName)
		if entriesBucket == nil {
			return nil
		}

		return entriesBucket.ForEach(func(_, v []byte) error {
			entries = append(entries, d.decodeEntry(v))

			return nil
		})
	})

	return entries, err
}

// decodeEntry takes a byte slice representation of an Entry as stored in the db
// by Set*Entries(), and converts it back in to an *Entry.
func (d *DB) decodeEntry(v []byte) *Entry {
	dec := codec.NewDecoderBytes(v, d.ch)

	var entry *Entry

	dec.MustDecode(&entry)

	return entry
}

// getDirEntries returns all the dir entries for the given set.
func (d *DB) getDirEntries(setID string) ([]*Entry, error) {
	return d.getEntries(setID, dirBucket)
}
