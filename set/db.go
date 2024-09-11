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
	"bytes"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gammazero/workerpool"
	"github.com/ugorji/go/codec"
	"github.com/wtsi-hgi/ibackup/put"
	"github.com/wtsi-ssg/wrstat/v4/walk"
	bolt "go.etcd.io/bbolt"
)

type Error struct {
	Msg string
	id  string
}

func (e Error) Error() string {
	if e.id != "" {
		return fmt.Sprintf("%s [%s]", e.Msg, e.id)
	}

	return e.Msg
}

const (
	ErrInvalidSetID           = "invalid set ID"
	ErrInvalidRequest         = "request lacks Requester or Set"
	ErrInvalidEntry           = "invalid set entry"
	ErrInvalidTransformerPath = "invalid transformer path concatenation"
	ErrNoAddDuringDiscovery   = "can't add set while set is being discovered"

	setsBucket                    = "sets"
	userToSetBucket               = "userLookup"
	inodeBucket                   = "inodes"
	transformerToIDBucket         = "transformerIDs"
	transformerFromIDBucket       = "transformers"
	subBucketPrefix               = "~!~"
	fileBucket                    = subBucketPrefix + "files"
	dirBucket                     = subBucketPrefix + "dirs"
	discoveredBucket              = subBucketPrefix + "discovered"
	failedBucket                  = "failed"
	dbOpenMode                    = 0600
	separator                     = ":!:"
	AttemptsToBeConsideredFailing = 3
	maxFailedEntries              = 10
	hexBase                       = 16

	backupExt = ".backingup"

	// workerPoolSizeFiles is the max number of concurrent file stats we'll do
	// during discovery.
	workerPoolSizeFiles = 16
)

// DBRO is the read-only component of the DB struct.
type DBRO struct {
	db *bolt.DB

	ch codec.Handle

	slacker Slacker
}

// NewRO returns a *DBRO that can be used to query a set database. Provide
// the path to the database file.
//
// Returns an error if database can't be opened.
func NewRO(path string) (*DBRO, error) {
	boltDB, err := bolt.Open(path, dbOpenMode, &bolt.Options{
		ReadOnly: true,
		OpenFile: func(name string, _ int, _ os.FileMode) (*os.File, error) {
			return os.Open(name)
		},
	})
	if err != nil {
		return nil, err
	}

	return &DBRO{
		db: boltDB,
		ch: new(codec.BincHandle),
	}, nil
}

// DB is used to create and query a database for storing backup sets (lists of
// files a user wants to have backed up) and their backup status.
type DB struct {
	DBRO

	mountList []string
	filePool  *workerpool.WorkerPool

	mu                    sync.Mutex
	backupPath            string
	minTimeBetweenBackups time.Duration
	remoteBackupPath      string
	remoteBackupHandler   put.Handler

	rebackup atomic.Bool
}

// New returns a *DB that can be used to create or query a set database. Provide
// the path to the database file.
//
// Optionally, also provide a path to backup the database to.
//
// Returns an error if path exists but can't be opened, or if it doesn't exist
// and can't be created.
func New(path, backupPath string) (*DB, error) {
	boltDB, err := initDB(path)
	if err != nil {
		return nil, err
	}

	db := &DB{
		DBRO: DBRO{
			db: boltDB,
			ch: new(codec.BincHandle),
		},
		backupPath:            backupPath,
		minTimeBetweenBackups: 1 * time.Second,

		filePool: workerpool.New(workerPoolSizeFiles),
	}

	err = db.getMountPoints()
	if err != nil {
		return nil, err
	}

	return db, nil
}

func initDB(path string) (*bolt.DB, error) {
	boltDB, err := bolt.Open(path, dbOpenMode, &bolt.Options{
		NoFreelistSync: true,
		NoGrowSync:     true,
		FreelistType:   bolt.FreelistMapType,
		MmapFlags:      syscall.MAP_POPULATE,
	})
	if err != nil {
		return nil, err
	}

	err = boltDB.Update(func(tx *bolt.Tx) error {
		for _, bucket := range [...]string{setsBucket, failedBucket, inodeBucket,
			userToSetBucket, userToSetBucket, transformerToIDBucket, transformerFromIDBucket} {
			if _, errc := tx.CreateBucketIfNotExists([]byte(bucket)); errc != nil {
				return errc
			}
		}

		return nil
	})

	return boltDB, err
}

// Close closes the database. Be sure to call this to finalise any writes to
// disk correctly.
func (d *DB) Close() error {
	d.filePool.StopWait()

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

			if err := updateDatabaseSetWithUserSetDetails(eset, set); err != nil {
				return err
			}

			set = eset
		}

		errp := b.Put(bid, d.encodeToBytes(set))
		if errp != nil {
			return errp
		}

		b = tx.Bucket([]byte(userToSetBucket))

		return b.Put([]byte(set.Requester+separator+id), bid)
	})

	if err == nil {
		err = set.SuccessfullyStoredInDB()
	}

	return err
}

func updateDatabaseSetWithUserSetDetails(dbSet, userSet *Set) error {
	if dbSet.StartedDiscovery.After(dbSet.LastDiscovery) {
		return Error{Msg: ErrNoAddDuringDiscovery, id: dbSet.ID()}
	}

	dbSet.Transformer = userSet.Transformer
	dbSet.MonitorTime = userSet.MonitorTime
	dbSet.DeleteLocal = userSet.DeleteLocal
	dbSet.Description = userSet.Description
	dbSet.Error = userSet.Error
	dbSet.Warning = userSet.Warning

	return nil
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
	entries := make([]*walk.Dirent, len(paths))

	for n, path := range paths {
		entries[n] = &walk.Dirent{
			Path: path,
		}
	}

	return d.setEntries(setID, entries, fileBucket)
}

// setEntries sets the paths for the given backup set in a sub bucket with the
// given prefix. Only supply absolute paths.
//
// *** Currently ignores old entries that are not in the given paths.
func (d *DB) setEntries(setID string, dirents []*walk.Dirent, bucketName string) error {
	return d.db.Update(func(tx *bolt.Tx) error {
		b, existing, err := d.getAndDeleteExistingEntries(tx, bucketName, setID)
		if err != nil {
			return err
		}

		// this sort is critical to database write speed.
		sort.Slice(dirents, func(i, j int) bool {
			return dirents[i].Path < dirents[j].Path
		})

		ec, err := newEntryCreator(d, tx, b, existing, setID)
		if err != nil {
			return err
		}

		return ec.UpdateOrCreateEntries(dirents)
	})
}

// getAndDeleteExistingEntries gets existing entries in the given sub bucket
// of the setsBucket, then deletes and recreates the sub bucket. Returns the
// empty sub bucket and any old values.
func (d *DB) getAndDeleteExistingEntries(tx *bolt.Tx, subBucketName string, setID string) (*bolt.Bucket,
	map[string][]byte, error) {
	existing := make(map[string][]byte)

	sfsb, err := d.newSetFileBucket(tx, subBucketName, setID)
	if err != nil {
		return nil, existing, err
	}

	bFailed := tx.Bucket([]byte(failedBucket))

	err = sfsb.Bucket.ForEach(func(k, v []byte) error {
		path := string(k)
		existing[path] = v

		return bFailed.Delete([]byte(setID + separator + path))
	})
	if err != nil {
		return sfsb.Bucket, existing, err
	}

	if len(existing) > 0 {
		if err = sfsb.DeleteBucket(); err != nil {
			return sfsb.Bucket, existing, err
		}

		err = sfsb.CreateBucket()
	}

	return sfsb.Bucket, existing, err
}

type setFileSubBucket struct {
	Name      []byte
	Bucket    *bolt.Bucket
	SetBucket *bolt.Bucket
}

func (s *setFileSubBucket) DeleteBucket() error {
	return s.SetBucket.DeleteBucket(s.Name)
}

func (s *setFileSubBucket) CreateBucket() error {
	var err error

	s.Bucket, err = s.SetBucket.CreateBucket(s.Name)

	return err
}

func (d *DB) newSetFileBucket(tx *bolt.Tx, kindOfFileBucket, setID string) (*setFileSubBucket, error) {
	subBucketName := []byte(kindOfFileBucket + separator + setID)
	setBucket := tx.Bucket([]byte(setsBucket))

	subBucket, err := setBucket.CreateBucketIfNotExists(subBucketName)

	return &setFileSubBucket{
		Name:      subBucketName,
		Bucket:    subBucket,
		SetBucket: setBucket,
	}, err
}

// newDirentFromPath returns a Dirent for the path. If it doesn't exist, returns
// a fake one with no Inode and Type of ModeIrregular.
func newDirentFromPath(path string) *walk.Dirent {
	dirent := &walk.Dirent{
		Path: path,
		Type: os.ModeIrregular,
	}

	info, err := os.Lstat(path)
	if err != nil {
		return dirent
	}

	dirent.Type = info.Mode().Type()

	statt, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return dirent
	}

	dirent.Inode = statt.Ino

	return dirent
}

// SetDirEntries sets the directory paths for the given backup set. Only supply
// absolute paths to directories.
func (d *DB) SetDirEntries(setID string, entries []*walk.Dirent) error {
	return d.setEntries(setID, entries, dirBucket)
}

// getSetByID returns the Set with the given ID from the database, along with
// the byte slice version of the set id and the sets bucket so you can easily
// put the set back again after making changes. Returns an error of setID isn't
// in the database.
func (d *DBRO) getSetByID(tx *bolt.Tx, setID string) (*Set, []byte, *bolt.Bucket, error) {
	b := tx.Bucket([]byte(setsBucket))
	bid := []byte(setID)

	v := b.Get(bid)
	if v == nil {
		return nil, nil, nil, Error{ErrInvalidSetID, setID}
	}

	set := d.decodeSet(v)

	return set, bid, b, nil
}

// DiscoverCallback will receive the sets directory entries and return a list of
// the files discovered in those directories.
type DiscoverCallback func([]*Entry) ([]*walk.Dirent, error)

// Discover discovers and stores file entry details for the given set.
// Immediately tries to record in the db that discovery has started and returns
// any error from doing that. Actual discovery using your callback on
// directories will then proceed, as will the stat'ing of pure files, and return
// the updated set.
func (d *DB) Discover(setID string, cb DiscoverCallback) (*Set, error) {
	err := d.setDiscoveryStarted(setID)
	if err != nil {
		return nil, err
	}

	s, err := d.discover(setID, cb)
	if err != nil {
		errb := d.SetError(setID, err.Error())
		err = errors.Join(err, errb)
	}

	return s, err
}

// setDiscoveryStarted updates StartedDiscovery and resets some status values
// for the given set. Returns an error if the setID isn't in the database.
func (d *DB) setDiscoveryStarted(setID string) error {
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
		set.Abnormal = 0
		set.Symlinks = 0
		set.Hardlinks = 0
		set.Status = PendingDiscovery
		set.Error = ""
		set.Warning = ""

		return b.Put(bid, d.encodeToBytes(set))
	})
}

func (d *DB) discover(setID string, cb DiscoverCallback) (*Set, error) {
	errCh := make(chan error, 1)

	go func() {
		errCh <- d.statPureFileEntries(setID)
	}()

	entries, err := d.GetDirEntries(setID)
	if err != nil {
		return nil, err
	}

	var fileEntries []*walk.Dirent

	if cb != nil {
		fileEntries, err = cb(entries)
	}

	err = errors.Join(err, <-errCh)
	if err != nil {
		return nil, err
	}

	return d.setDiscoveredEntries(setID, fileEntries)
}

func (d *DB) statPureFileEntries(setID string) error {
	return d.db.Update(func(tx *bolt.Tx) error {
		sfsb, err := d.newSetFileBucket(tx, fileBucket, setID)
		if err != nil {
			return err
		}

		direntCh := make(chan *walk.Dirent)
		entryCh := make(chan []byte)
		numEntries := 0

		sfsb.Bucket.ForEach(func(k, v []byte) error { //nolint:errcheck
			numEntries++

			d.filePool.Submit(func() {
				path := string(k)
				direntCh <- newDirentFromPath(path)
				entryCh <- v
			})

			return nil
		})

		return d.handleFilePoolResults(tx, sfsb, setID, direntCh, entryCh, numEntries)
	})
}

func (d *DB) handleFilePoolResults(tx *bolt.Tx, sfsb *setFileSubBucket, setID string,
	direntCh chan *walk.Dirent, entryCh chan []byte,
	numEntries int) error {
	dirents := make([]*walk.Dirent, numEntries)
	existing := make(map[string][]byte, numEntries)

	for n := range dirents {
		dirent := <-direntCh
		dirents[n] = dirent
		existing[dirent.Path] = <-entryCh
	}

	sort.Slice(dirents, func(i, j int) bool {
		return dirents[i].Path < dirents[j].Path
	})

	ec, err := newEntryCreator(d, tx, sfsb.Bucket, existing, setID)
	if err != nil {
		return err
	}

	return ec.UpdateOrCreateEntries(dirents)
}

// setDiscoveredEntries sets discovered file paths for the given backup set's
// directory entries. Only supply absolute paths to files.
//
// It also updates LastDiscovery, sets NumFiles and sets status to
// PendingUpload unless the set contains no files, in which case it sets status
// to Complete.
//
// Returns the updated set and an error if the setID isn't in the database.
func (d *DB) setDiscoveredEntries(setID string, dirents []*walk.Dirent) (*Set, error) {
	if err := d.setEntries(setID, dirents, discoveredBucket); err != nil {
		return nil, err
	}

	return d.updateSetAfterDiscovery(setID)
}

// updateSetAfterDiscovery updates LastDiscovery, sets NumFiles and sets status
// to PendingUpload unless the set contains no files, in which case it sets
// status to Complete.
//
// Makes the assumption that pure files and discovered files have already been
// recorded in the database prior to calling this.
//
// Returns the updated set and an error if the setID isn't in the database.
func (d *DB) updateSetAfterDiscovery(setID string) (*Set, error) {
	var updatedSet *Set

	err := d.db.Update(func(tx *bolt.Tx) error {
		set, bid, b, err := d.getSetByID(tx, setID)
		if err != nil {
			return err
		}

		err = set.DiscoveryCompleted(d.countAllFilesInSet(tx, setID))
		if err != nil {
			return err
		}

		updatedSet = set

		return b.Put(bid, d.encodeToBytes(set))
	})

	return updatedSet, err
}

func (d *DBRO) countAllFilesInSet(tx *bolt.Tx, setID string) uint64 {
	var numFiles uint64

	cb := func([]byte) {
		numFiles++
	}

	getEntriesViewFunc(tx, setID, fileBucket, cb)
	getEntriesViewFunc(tx, setID, discoveredBucket, cb)

	return numFiles
}

// SetEntryStatus finds the set Entry corresponding to the given Request's Local
// path, Requester and Set name, and updates its status in the database, and
// also updates summary status for the Set.
//
// Returns the Entry, which is a reflection of the Request, but contains
// additional information such as the number of Attempts if the Request is
// failing and you keep retrying.
//
// Returns an error if a set or entry corresponding to the Request can't be
// found.
func (d *DB) SetEntryStatus(r *put.Request) (*Entry, error) {
	setID, err := requestToSetID(r)
	if err != nil {
		return nil, err
	}

	var entry *Entry

	err = d.db.Update(func(tx *bolt.Tx) error {
		got, bid, b, errt := d.getSetByID(tx, setID)
		if errt != nil {
			return errt
		}

		entry, errt = d.updateFileEntry(tx, setID, r, got.LastDiscovery)
		if errt != nil {
			return errt
		}

		if entry.isDir {
			return nil
		}

		d.updateSetBasedOnEntry(got, entry)

		return b.Put(bid, d.encodeToBytes(got))
	})

	return entry, err
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
		entry.Status = Pending
	}

	entry.LastAttempt = time.Now()
	entry.Size = r.UploadedSize()

	if entry.Status == Pending || entry.Status == Failed {
		entry.Attempts++
		entry.newSize = entry.Attempts == 1
	}

	requestStatusToEntryStatus(r, entry)

	err = d.updateFailedLookup(tx, setID, r.Local, entry)
	if err != nil {
		return nil, err
	}

	return entry, b.Put([]byte(r.Local), d.encodeToBytes(entry))
}

// getEntry finds the Entry for the given path in the given set. Returns it
// along with the bucket it was in, so you can alter the Entry and put it back.
// Returns an error if the entry can't be found.
func (d *DBRO) getEntry(tx *bolt.Tx, setID, path string) (*Entry, *bolt.Bucket, error) {
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
func (d *DBRO) getEntryFromSubbucket(kind, setID, path string, setsBucket *bolt.Bucket) (*Entry, *bolt.Bucket) {
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

// requestStatusToEntryStatus converts Request.Status and stores it as a Status
// on the entry. Also sets entry.Attempts, unFailed and newFail as appropriate.
func requestStatusToEntryStatus(r *put.Request, entry *Entry) {
	entry.newFail = false
	entry.unFailed = false

	switch r.Status { //nolint:exhaustive
	case put.RequestStatusUploading:
		entry.Status = UploadingEntry

		if r.Stuck != nil {
			entry.LastError = r.Stuck.String()
		}
	case put.RequestStatusUploaded, put.RequestStatusUnmodified, put.RequestStatusReplaced:
		entry.Status = Uploaded
		entry.unFailed = entry.Attempts > 1
		entry.LastError = ""
	case put.RequestStatusFailed:
		entry.Status = Failed

		if r.Error != "" {
			entry.LastError = r.Error
		}

		entry.newFail = entry.Attempts == 1
	case put.RequestStatusMissing:
		entry.Status = Missing
		entry.unFailed = entry.Attempts > 1
	}
}

// updateFailedLookup adds or removes the given entry to our failed lookup
// bucket, for quick retieval of just failed entries later.
func (d *DB) updateFailedLookup(tx *bolt.Tx, setID, path string, entry *Entry) error {
	if entry.unFailed {
		return d.removeFailedLookup(tx, setID, path)
	}

	if entry.Status == Failed {
		return d.addFailedLookup(tx, setID, path, entry)
	}

	return nil
}

// removeFailedLookup removes the given path for the given set from our failed
// lookup bucket.
func (d *DB) removeFailedLookup(tx *bolt.Tx, setID, path string) error {
	b, lookupKey := d.getBucketAndKeyForFailedLookup(tx, setID, path)

	return b.Delete(lookupKey)
}

// getBucketAndKeyForFailedLookup returns our failedBucket and a lookup key.
func (d *DBRO) getBucketAndKeyForFailedLookup(tx *bolt.Tx, setID, path string) (*bolt.Bucket, []byte) {
	return tx.Bucket([]byte(failedBucket)), []byte(setID + separator + path)
}

// addFailedLookup adds the given path for the given set from our failed lookup
// bucket. For speed of retrieval, it's not actually just a lookup, but we
// duplicate the entry data in the failedBucket.
func (d *DB) addFailedLookup(tx *bolt.Tx, setID, path string, entry *Entry) error {
	b, lookupKey := d.getBucketAndKeyForFailedLookup(tx, setID, path)

	return b.Put(lookupKey, d.encodeToBytes(entry))
}

// updateSetBasedOnEntry updates set status values based on an updated Entry
// from updateFileEntry(), assuming that request is for one of set's file
// entries.
func (d *DB) updateSetBasedOnEntry(set *Set, entry *Entry) {
	if set.Status == PendingDiscovery || set.Status == PendingUpload {
		set.Status = Uploading
	}

	set.adjustBasedOnEntry(entry)

	d.fixSetCounts(entry, set)

	if set.Uploaded+set.Failed+set.Missing+set.Abnormal == set.NumFiles {
		set.Status = Complete
		set.LastCompleted = time.Now()
		set.LastCompletedCount = set.Uploaded + set.Failed
		set.LastCompletedSize = set.SizeFiles
	}
}

// fixSetCounts resets the set counts to 0 and goes through all the entries for
// the set in the db to recaluclate them. The supplied entry should be one you
// newly updated and that wasn't in the db before the transaction we're in.
func (d *DB) fixSetCounts(entry *Entry, set *Set) {
	if set.countsValid() {
		return
	}

	entries, err := d.GetFileEntries(set.ID())
	if err != nil {
		return
	}

	set.Uploaded = 0
	set.Failed = 0
	set.Missing = 0
	set.Abnormal = 0
	set.Symlinks = 0
	set.Hardlinks = 0

	for _, e := range entries {
		if e.Path == entry.Path {
			e = entry
		}

		if e.Status == Failed {
			e.newFail = true
		}

		set.entryToSetCounts(e)
	}
}

// GetAll returns all the Sets previously added to the database.
func (d *DBRO) GetAll() ([]*Set, error) {
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
func (d *DBRO) decodeSet(v []byte) *Set {
	dec := codec.NewDecoderBytes(v, d.ch)

	var set *Set

	dec.MustDecode(&set)

	set.LogChangesToSlack(d.slacker)

	return set
}

// GetByRequester returns all the Sets previously added to the database by the
// given requester.
func (d *DBRO) GetByRequester(requester string) ([]*Set, error) {
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

// GetByNameAndRequester returns the set with the given name and requester.
//
// Returns nil error when no set found.
func (d *DBRO) GetByNameAndRequester(name, requester string) (*Set, error) {
	sets, err := d.GetByRequester(requester)
	if err != nil {
		return nil, err
	}

	for _, set := range sets {
		if set.Name == name {
			return set, nil
		}
	}

	return nil, nil //nolint:nilnil
}

// GetByID returns the Sets with the given ID previously added to the database.
// Returns nil if such a set does not exist.
func (d *DBRO) GetByID(id string) *Set {
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
func (d *DBRO) GetFileEntries(setID string) ([]*Entry, error) {
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

// GetFileEntryForSet returns the file entry for the given path in the given
// set.
func (d *DBRO) GetFileEntryForSet(setID, filePath string) (*Entry, error) {
	var entry *Entry

	if err := d.db.View(func(tx *bolt.Tx) error {
		var err error

		entry, _, err = d.getEntry(tx, setID, filePath)

		return err
	}); err != nil {
		return nil, err
	}

	return entry, nil
}

// GetDefinedFileEntry returns the first defined file entry for the given set.
//
// Will return nil Entry if SetFileEntries hasn't been called.
func (d *DBRO) GetDefinedFileEntry(setID string) (*Entry, error) {
	entry, err := d.getDefinedFileEntry(setID)
	if err != nil {
		return nil, err
	}

	return entry, nil
}

// getEntries returns all the entries for the given set from the given sub
// bucket prefix.
func (d *DBRO) getEntries(setID, bucketName string) ([]*Entry, error) {
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

// getEntries returns all the entries for the given set from the given sub
// bucket prefix.
func (d *DBRO) getDefinedFileEntry(setID string) (*Entry, error) {
	var entry *Entry

	err := d.db.View(func(tx *bolt.Tx) error {
		subBucketName := []byte(fileBucket + separator + setID)
		setsBucket := tx.Bucket([]byte(setsBucket))

		entriesBucket := setsBucket.Bucket(subBucketName)
		if entriesBucket == nil {
			return nil
		}

		_, v := entriesBucket.Cursor().First()
		if len(v) == 0 {
			return nil
		}

		entry = d.decodeEntry(v)

		return nil
	})

	return entry, err
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
func (d *DBRO) decodeEntry(v []byte) *Entry {
	dec := codec.NewDecoderBytes(v, d.ch)

	var entry *Entry

	dec.MustDecode(&entry)

	return entry
}

// GetFailedEntries returns up to 10 of the file entries for the given set (both
// SetFileEntries and SetDiscoveredEntries) that have a failed status. Also
// returns the number of failed entries that were not returned.
func (d *DBRO) GetFailedEntries(setID string) ([]*Entry, int, error) {
	entries := make([]*Entry, 0, maxFailedEntries)
	skipped := 0

	err := d.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket([]byte(failedBucket)).Cursor()
		prefix := []byte(setID + separator)

		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			if len(entries) < maxFailedEntries {
				entries = append(entries, d.decodeEntry(v))
			} else {
				skipped++
			}
		}

		return nil
	})

	return entries, skipped, err
}

// GetPureFileEntries returns all the file entries for the given set (only
// SetFileEntries, not SetDiscoveredEntries).
func (d *DBRO) GetPureFileEntries(setID string) ([]*Entry, error) {
	return d.getEntries(setID, fileBucket)
}

// GetDirEntries returns all the dir entries for the given set.
func (d *DBRO) GetDirEntries(setID string) ([]*Entry, error) {
	return d.getEntries(setID, dirBucket)
}

// SetError updates a set with the given error message. Returns an error if the
// setID isn't in the database.
func (d *DB) SetError(setID, errMsg string) error {
	return d.updateSetProperties(setID, func(got *Set) {
		got.Error = errMsg
	})
}

// updateSetProperties retrives a set from the database and gives it to your
// callback, allowing you to change properties on it. The altered set will then
// be stored back in the database.
func (d *DB) updateSetProperties(setID string, cb func(*Set)) error {
	return d.db.Update(func(tx *bolt.Tx) error {
		set, bid, b, err := d.getSetByID(tx, setID)
		if err != nil {
			return err
		}

		cb(set)

		return b.Put(bid, d.encodeToBytes(set))
	})
}

// SetWarning updates a set with the given warning message. Returns an error if
// the setID isn't in the database.
func (d *DB) SetWarning(setID, warnMsg string) error {
	return d.updateSetProperties(setID, func(got *Set) {
		got.Warning = warnMsg
	})
}

// Backup does an on-line backup of the database to the given file path. There's
// at least 1 second in between backups, and incoming backup requests are noted
// during an on-going backup, with 1 further backup occurring after the on-going
// one completes.
func (d *DB) Backup() error {
	if d.backupPath == "" {
		return nil
	}

	if !d.mu.TryLock() {
		d.rebackup.Store(true)

		return nil
	}
	defer d.mu.Unlock()

	for {
		if err := d.doBackup(); err != nil {
			d.rebackup.Store(false)

			return err
		}

		<-time.After(d.minTimeBetweenBackups)

		if !d.rebackup.CompareAndSwap(true, false) {
			break
		}
	}

	return nil
}

func (d *DB) doBackup() error {
	backingUp := d.backupPath + backupExt

	f, err := os.Create(backingUp)
	if err != nil {
		return err
	}

	err = d.db.View(func(tx *bolt.Tx) error {
		_, errw := tx.WriteTo(f)

		return errw
	})

	errc := f.Close()

	err = errors.Join(err, errc)
	if err != nil {
		return err
	}

	err = os.Rename(backingUp, d.backupPath)
	if err != nil {
		return err
	}

	return d.doRemoteBackup()
}

func (d *DB) doRemoteBackup() (err error) {
	if d.remoteBackupPath == "" {
		return nil
	}

	putter, err := put.New(d.remoteBackupHandler, []*put.Request{{
		Local:  d.backupPath,
		Remote: d.remoteBackupPath,
	}})
	if err != nil {
		return err
	}

	defer func() {
		if errc := putter.Cleanup(); err == nil {
			err = errc
		}
	}()

	if err = putter.CreateCollections(); err != nil {
		return err
	}

	started, finished, skipped := putter.Put()

	drainChannel(started)
	drainChannel(finished)
	drainChannel(skipped)

	return err
}

func drainChannel(ch chan *put.Request) {
	for range ch { //nolint:revive
	}
}

// SetMinimumTimeBetweenBackups sets the minimum time between successive
// backups. Defaults to 1 second if this method not called.
func (d *DB) SetMinimumTimeBetweenBackups(dur time.Duration) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.minTimeBetweenBackups = dur
}

// SetBackupPath sets the backup path, for if you created the DB without
// providing one.
func (d *DB) SetBackupPath(path string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.backupPath = path
}

// EnableRemoteBackups causes the backup file to also be backed up to the
// remote path.
func (d *DB) EnableRemoteBackups(remotePath string, handler put.Handler) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.remoteBackupPath = remotePath
	d.remoteBackupHandler = handler
}

func (d *DBRO) LogSetChangesToSlack(slacker Slacker) {
	d.slacker = slacker
}
