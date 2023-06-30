/*******************************************************************************
 * Copyright (c) 2022, 2023 Genome Research Ltd.
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
	"os"
	"strconv"
	"time"

	"github.com/wtsi-ssg/wrstat/v4/walk"
	bolt "go.etcd.io/bbolt"
)

type EntryStatus int

const (
	// Pending is an Entry status meaning the file has not yet had an upload
	// attempt.
	Pending EntryStatus = iota

	// UploadingEntry is an Entry status meaning the file has started to upload.
	UploadingEntry

	// Uploaded is an Entry status meaning the file has been uploaded
	// successfully.
	Uploaded

	// Failed is an Entry status meaning the file failed to upload due to some
	// remote issue.
	Failed

	// Missing is an Entry status meaning the local file is missing so can't be
	// uploaded.
	Missing

	// AbnormalEntry is an Entry status meaning the local files is neither
	// regular nor a symlink (ie. it's a fifo or socket etc.), so shouldn't be
	// uploaded.
	AbnormalEntry
)

type EntryType int

const (
	Regular EntryType = iota
	Hardlink
	Symlink
	Directory
	Abnormal // fifos and sockets etc. that we shouldn't upload

	// Unknown is an Entry type meaning the local file is either missing or
	// there was an error trying to get its type.
	Unknown = -1
)

// String lets you convert a EntryStatus to a meaningful string.
func (e EntryStatus) String() string {
	return [...]string{
		"pending",
		"uploading",
		"uploaded",
		"failed",
		"missing",
	}[e]
}

// Entry holds the status of an entry in a backup set.
type Entry struct {
	Path        string
	PathForJSON []byte // set by MakeSafeForJSON(); do not set this yourself.
	Size        uint64 // size of local file in bytes.
	Status      EntryStatus
	Type        EntryType
	LastError   string
	LastAttempt time.Time
	Attempts    int
	Inode       uint64
	Dest        string

	newSize  bool
	newFail  bool
	unFailed bool
	isDir    bool
}

// MakeSafeForJSON copies Path to PathForJSON, so that if this Entry struct is
// encoded as JSON, non-UTF8 characters will be preserved. On decode be sure to
// use CorrectFromJSON().
func (e *Entry) MakeSafeForJSON() {
	e.PathForJSON = []byte(e.Path)
}

// CorrectFromJSON copies PathForJSON to Path, so that this value is correct
// following a decode from JSON.
func (e *Entry) CorrectFromJSON() {
	e.Path = string(e.PathForJSON)
}

// ShouldUpload returns true if this Entry is pending or the last attempt was
// before the given time. Always returns false if the Type is Abnormal.
func (e *Entry) ShouldUpload(reuploadAfter time.Time) bool {
	if e.Type == Abnormal {
		return false
	}

	if e.Status == Pending {
		return true
	}

	return !e.LastAttempt.After(reuploadAfter)
}

// setTypeForNoInode sets our type based on the given dirent's Type, for the
// case that the the dirent has no Inode (is missing or is a directory).
func (e *Entry) setTypeForNoInode(dirent *walk.Dirent) {
	if dirent.Type == os.ModeIrregular {
		e.Status = Missing
	}

	if dirent.Type.IsDir() {
		e.Type = Directory
	}
}

func (e *Entry) updateTypeDestAndInode(newEntry *Entry) bool {
	if e.hasSameCoreProperties(newEntry) {
		return false
	}

	e.Type = newEntry.Type
	e.Dest = newEntry.Dest
	e.Inode = newEntry.Inode
	e.Status = newEntry.Status

	return true
}

func (e *Entry) hasSameCoreProperties(other *Entry) bool {
	return e.Type == other.Type && e.Dest == other.Dest && e.Inode == other.Inode && e.Status == other.Status
}

type entryCreator struct {
	db              *DB
	tx              *bolt.Tx
	bucket          *bolt.Bucket
	existingEntries map[string][]byte
	setID           []byte
	setBucket       *bolt.Bucket
	set             *Set
	transformerID   string
}

// newEntryCreator returns an entryCreator that will create new entries in the
// given bucket from dirents passed to UpdateOrCreateEntries(), basing them on
// the supplied existing ones. The bucket is expected to be empty (so get
// existing ones and then delete the bucket before calling this).
func newEntryCreator(db *DB, tx *bolt.Tx, bucket *bolt.Bucket, existing map[string][]byte,
	setID string) (*entryCreator, error) {
	got, setIDb, setBucket, err := db.getSetByID(tx, setID)
	if err != nil {
		return nil, err
	}

	c := &entryCreator{
		db:              db,
		tx:              tx,
		bucket:          bucket,
		existingEntries: existing,
		setID:           setIDb,
		setBucket:       setBucket,
		set:             got,
	}

	err = c.setTransformer()
	if err != nil {
		return nil, err
	}

	return c, nil
}

func (c *entryCreator) setTransformer() error {
	b := c.tx.Bucket([]byte(transformerToIDBucket))

	v := b.Get([]byte(c.set.Transformer))
	if v != nil {
		c.transformerID = string(v)

		return nil
	}

	id, err := b.NextSequence()
	if err != nil {
		return err
	}

	c.transformerID = strconv.FormatUint(id, 10)

	err = b.Put([]byte(c.set.Transformer), []byte(c.transformerID))
	if err != nil {
		return err
	}

	b = c.tx.Bucket([]byte(transformerFromIDBucket))

	return b.Put([]byte(c.transformerID), []byte(c.set.Transformer))
}

// UpdateOrCreateEntries creates or updates (if already in the existing map)
// entries in the database based on properties of each given dirent. It handles
// associating hard and symlink info on the resulting entries.
func (c *entryCreator) UpdateOrCreateEntries(dirents []*walk.Dirent) error {
	for _, dirent := range dirents {
		err := c.updateOrCreateEntryFromDirent(dirent)
		if err != nil {
			return err
		}
	}

	return c.setBucket.Put(c.setID, c.db.encodeToBytes(c.set))
}

func (c *entryCreator) updateOrCreateEntryFromDirent(dirent *walk.Dirent) error {
	entry, err := c.existingOrNewEncodedEntry(dirent)
	if err != nil {
		return err
	}

	return c.bucket.Put([]byte(dirent.Path), entry)
}

func (e *Entry) setTypeAndDetermineDest(eType EntryType) error {
	e.Type = eType

	var err error

	if eType == Symlink {
		e.Dest, err = os.Readlink(e.Path)
	} else if eType != Hardlink {
		e.Dest = ""
	}

	if eType == Abnormal {
		e.Status = AbnormalEntry
	}

	return err
}

func (c *entryCreator) newEntryFromDirent(dirent *walk.Dirent) (*Entry, error) {
	entry := &Entry{
		Path:  dirent.Path,
		Inode: dirent.Inode,
	}

	if dirent.Inode == 0 {
		entry.setTypeForNoInode(dirent)

		return entry, nil
	}

	eType, hardlink, err := c.determineEntryType(dirent)
	if err != nil {
		return nil, err
	}

	entry.Dest = hardlink

	err = entry.setTypeAndDetermineDest(eType)
	if err != nil {
		return nil, err
	}

	return entry, nil
}

func (c *entryCreator) existingOrNewEncodedEntry(dirent *walk.Dirent) ([]byte, error) {
	entry, err := c.newEntryFromDirent(dirent)
	if err != nil {
		return nil, err
	}

	c.set.entryToSetCounts(entry)

	e := c.existingEntries[dirent.Path]
	if e != nil {
		dbEntry := c.db.decodeEntry(e)
		if !dbEntry.updateTypeDestAndInode(entry) {
			return e, nil
		}

		entry = dbEntry
	}

	e = c.db.encodeToBytes(entry)

	return e, nil
}

func (c *entryCreator) determineEntryType(dirent *walk.Dirent) (EntryType, string, error) {
	if dirent.IsDir() {
		return Directory, "", nil
	}

	return c.direntToEntryType(dirent)
}

func (c *entryCreator) direntToEntryType(de *walk.Dirent) (EntryType, string, error) {
	eType := Regular

	switch {
	case de.Inode == 0:
		eType = Unknown
	case de.IsSymlink():
		eType = Symlink
	case !(de.IsRegular() || de.IsDir()):
		eType = Abnormal
	default:
		hardLink, err := c.db.handleInode(c.tx, de, c.transformerID)
		if err != nil {
			return eType, "", err
		}

		if hardLink != "" {
			return Hardlink, hardLink, nil
		}
	}

	return eType, "", nil
}
