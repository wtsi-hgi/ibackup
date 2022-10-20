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
- unique ID
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
	"fmt"

	"github.com/dgryski/go-farm"
	"github.com/ugorji/go/codec"
	bolt "go.etcd.io/bbolt"
)

const (
	setsBucket    = "sets"
	entriesBucket = "entries"
	dbOpenMode    = 0600
)

// Set describes a backup set; a list of files and directories to backup, plus
// some metadata. All properties are required unless otherwise noted.
type Set struct {
	// An arbitrary (short) name for this backup set.
	Name string

	// The username of the person requesting this backup.
	Requester string

	// The list of local file and directory paths you want uploaded.
	Entries []string

	// The method of transforming local Entries paths in to remote paths, to
	// determine the upload location. "humgen" to use the put.HumgenTransformer,
	// or "prefix=local:remote" to use the put.PrefixTransformer.
	Transformer string

	// Monitor the files and directories and re-upload them whenever they
	// change. Optional, defaults to unmonitored (a one time upload of Entries).
	Monitor bool

	// An optional longer free-text description of this backup set.
	Description string

	// Delete local paths after successful upload. Optional, defaults to no
	// deletions (ie. do a backup, not a move).
	// DeleteLocal bool

	// Delete remote paths if removed from the set. Optional, defaults to no
	// deletions (ie. keep all uploads and ignore changes to the Entries).
	// DeleteRemote bool

	// Receive an optional notification after this date if DeleteRemote is true
	// and there are still Entries in this set.
	// Expiry time.Time
}

// ID returns an ID for this set, generated deterministiclly from its Name and
// Requester. Ie. it is unique between all requesters, and amongst a requester's
// differently named sets. Sets with the same Name and Requester will have the
// same ID().
func (s *Set) ID() string {
	concat := fmt.Sprintf("%s:%s", s.Requester, s.Name)

	l, h := farm.Hash128([]byte(concat))

	return fmt.Sprintf("%016x%016x", l, h)
}

// DB is used to create and query a database for storing backup sets (lists of
// files a user wants to have backed up) and their backup status.
type DB struct {
	db *bolt.DB
	ch codec.Handle
}

// New returns a *DB that can be used to create or query a set database. Provide
// the path to the database file.
//
// Returns an error if path exists but can't be opened	, or if it doesn't exist
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
