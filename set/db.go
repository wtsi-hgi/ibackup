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


A set should have properties:
- status: pendingDiscovery (waiting on existence, size and dir content discovery) | pendingUpload (waiting on at least 1 of its entries to be non-pending) | uploading (not all entries have uploaded) | complete | failing (there are failed entries)
- total size and number of files
- date of last attempt

A set has a nested bucket with the set entries, each of which has properties:
- status: pendingDiscovery (waiting on existence check and size discovery)  | pendingUpload (waiting on reservation) | uploading (reserved by put client) | uploaded | replaced | skipped | missing | failed
- size
- date of last attempt
- last error
- number of retries
- primary bool (if true, a file in the original set; if false, a file discovered to be in one of the set's directories)

There are lookup buckets to find sets by name and user.


 ******************************************************************************/

package set

import (
	"github.com/ugorji/go/codec"
	bolt "go.etcd.io/bbolt"
)

const (
	setsBucket    = "sets"
	entriesBucket = "entries"
	dbOpenMode    = 0600
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

		return b.Put([]byte(set.ID()), set.encodeToBytes(d.ch))
	})

	return err
}

// GetAll returns all the Sets previously added to the database.
func (d *DB) GetAll() ([]*Set, error) {
	var sets []*Set

	err := d.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(setsBucket))

		return b.ForEach(func(_, v []byte) error {
			dec := codec.NewDecoderBytes(v, d.ch)
			var set *Set
			dec.MustDecode(&set)

			sets = append(sets, set)

			return nil
		})
	})

	return sets, err
}

// // decodeChildBytes converts the byte slice returned by encodeChildren() back
// // in to a []string.
// func (d *DB) decodeChildrenBytes(encoded []byte) []string {
// 	dec := codec.NewDecoderBytes(encoded, d.ch)

// 	var children []string

// 	dec.MustDecode(&children)

// 	return children
// }

// // encodeChildren returns converts the given string slice into a []byte suitable
// // for storing on disk.
// func (d *DB) encodeChildren(dirs []string) []byte {
// 	var encoded []byte
// 	enc := codec.NewEncoderBytes(&encoded, d.ch)
// 	enc.MustEncode(dirs)

// 	return encoded
// }

// // storeDGUTs stores the current batch of DGUTs in the db.
// func (d *DB) storeDGUTs(tx *bolt.Tx) error {
// 	b := tx.Bucket([]byte(gutBucket))

// 	dir, guts := dgut.encodeToBytes(d.ch)

// 	return b.Put(dir, guts)
// }
