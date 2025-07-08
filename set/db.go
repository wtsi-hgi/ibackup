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
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/VertebrateResequencing/wr/queue"
	"github.com/gammazero/workerpool"
	"github.com/ugorji/go/codec"
	"github.com/wtsi-hgi/ibackup/transfer"
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
	ErrInvalidSetID              = "invalid set ID"
	ErrInvalidRequest            = "request lacks Requester or Set"
	ErrInvalidEntry              = "invalid set entry"
	ErrInvalidTransformerPath    = "invalid transformer path concatenation"
	ErrNoAddDuringDiscovery      = "can't add set while set is being discovered"
	ErrPathNotInSet              = "path(s) do not belong to the backup set"
	ErrRemovalWhenSetNotComplete = "you can only remove from completed sets"
	ErrSetIsNotWritable          = "the set is read-only, you cannot change it"
	ErrTransformerAlreadyUsed    = "you cannot edit the transformer on a set with uploaded files"
	ErrTransformerInUse          = "you cannot edit the transformer on a set with unfinished uploads"
	ErrPendingRemovals           = "the set has unfinished removals, you cannot change it"

	setsBucket                    = "sets"
	userToSetBucket               = "userLookup"
	inodeBucket                   = "inodes"
	transformerToIDBucket         = "transformerIDs"
	transformerFromIDBucket       = "transformers"
	subBucketPrefix               = "~!~"
	fileBucket                    = subBucketPrefix + "files"
	dirBucket                     = subBucketPrefix + "dirs"
	discoveredBucket              = subBucketPrefix + "discovered"
	discoveredFoldersBucket       = subBucketPrefix + "discoveredFolders"
	removedBucket                 = subBucketPrefix + "removed"
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
	db      *bolt.DB
	ch      codec.Handle
	slacker Slacker
}

type RemovalStatus int8

const (
	NotRemoved       RemovalStatus = iota // 0
	AboutToBeRemoved                      // 1
	Removed                               // 2
)

// RemoveReq contains information about a remove request for a path.
type RemoveReq struct {
	Path                string
	Set                 *Set
	IsDir               bool
	RemoteRemovalStatus RemovalStatus
	IsComplete          bool
}

func (rq RemoveReq) Key() string {
	return strings.Join([]string{rq.Set.ID(), rq.Path}, ":")
}

// ItemDef returns a queue.ItemDef for the remove request.
func (rq RemoveReq) ItemDef(ttr time.Duration) *queue.ItemDef {
	return &queue.ItemDef{
		Key:          rq.Key(),
		Data:         rq,
		TTR:          ttr,
		ReserveGroup: rq.Set.ID(),
	}
}

// NewRemoveRequest creates a remove requests using the provided information.
func NewRemoveRequest(path string, set *Set, isDir bool) RemoveReq {
	return RemoveReq{
		Path:                path,
		Set:                 set,
		IsDir:               isDir,
		RemoteRemovalStatus: NotRemoved,
	}
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
	remoteBackupHandler   transfer.Handler

	rebackup atomic.Bool
}

// New returns a *DB that can be used to create or query a set database. Provide
// the path to the database file.
//
// Optionally, also provide a path to backup the database to.
//
// Returns an error if path exists but can't be opened, or if it doesn't exist
// and can't be created.
func New(path, backupPath string, readonly bool) (*DB, error) {
	boltDB, err := initDB(path, readonly)
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

func initDB(path string, readonly bool) (*bolt.DB, error) {
	boltDB, err := bolt.Open(path, dbOpenMode, &bolt.Options{
		NoFreelistSync: true,
		NoGrowSync:     true,
		FreelistType:   bolt.FreelistMapType,
		ReadOnly:       readonly,
	})
	if err != nil {
		return nil, err
	}

	if readonly {
		return boltDB, nil
	}

	err = boltDB.Update(func(tx *bolt.Tx) error {
		for _, bucket := range [...]string{
			setsBucket, failedBucket, inodeBucket,
			userToSetBucket, userToSetBucket, transformerToIDBucket, transformerFromIDBucket,
		} {
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

// AddOrUpdate adds or updates the given Set to the database. Errors if the set
// is read-only, or if the set is being discovered.
func (d *DB) AddOrUpdate(set *Set) error {
	err := d.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(setsBucket))

		id := set.ID()
		bid := []byte(id)

		if existing := b.Get(bid); existing != nil {
			eset := d.decodeSet(existing)

			if eset.ReadOnly {
				return Error{Msg: ErrSetIsNotWritable, id: id}
			}

			if set.Transformer != eset.Transformer {
				if eset.Uploaded+eset.Skipped+eset.Replaced+eset.Orphaned > 0 {
					return Error{Msg: ErrTransformerAlreadyUsed, id: id}
				}

				if eset.Status != Complete && eset.Error == "" {
					return Error{Msg: ErrTransformerInUse, id: id}
				}
			}

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
		set.SuccessfullyStoredInDB()
	}

	return err
}

func updateDatabaseSetWithUserSetDetails(dbSet, userSet *Set) error {
	if dbSet.StartedDiscovery.After(dbSet.LastDiscovery) {
		return Error{Msg: ErrNoAddDuringDiscovery, id: dbSet.ID()}
	}

	dbSet.copyUserProperties(userSet)

	return nil
}

func (d *DB) deleteSubBucket(tx *bolt.Tx, setID, subBucket string) error {
	setsBucket := tx.Bucket([]byte(setsBucket))
	subBucketName := []byte(subBucket + separator + setID)

	if setsBucket.Bucket(subBucketName) == nil {
		return nil
	}

	return setsBucket.DeleteBucket(subBucketName)
}

// Hide marks the given Set as hidden in the database. This bypasses the normal
// AddOrUpdate method, working on read-only sets.
func (d *DB) Hide(set *Set) error {
	return d.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(setsBucket))

		id := set.ID()
		bid := []byte(id)

		existing := b.Get(bid)
		if existing == nil {
			return Error{Msg: ErrInvalidSetID, id: id}
		}

		eset := d.decodeSet(existing)
		if eset.Hide {
			return nil
		}

		eset.Hide = true

		return b.Put(bid, d.encodeToBytes(eset))
	})
}

// DeleteSubBucket deletes the provided sub bucket from the set with the
// provided id.
func (d *DB) DeleteDiscoveredFoldersBucket(setID string) error {
	return d.db.Update(func(tx *bolt.Tx) error {
		return d.deleteSubBucket(tx, setID, discoveredFoldersBucket)
	})
}

// UpdateEntry puts the updated entry into the database for the given set.
func (d *DB) UpdateEntry(sid, key string, entry *Entry) error {
	return d.db.Update(func(tx *bolt.Tx) error {
		_, b, err := d.getEntry(tx, sid, key)
		if err != nil {
			return err
		}

		return b.Put([]byte(key), d.encodeToBytes(entry))
	})
}

// encodeToBytes encodes the given thing as a byte slice, suitable for storing
// in a database.
func (d *DB) encodeToBytes(thing interface{}) []byte {
	var encoded []byte
	enc := codec.NewEncoderBytes(&encoded, d.ch)
	enc.MustEncode(thing)

	return encoded
}

// ValidateRemoveInputs returns an error if the provided set is not complete or
// if any provided path is not in the given set. Also returns the valid paths
// classified into a slice of filepaths or dirpaths.
func (d *DB) ValidateRemoveInputs(set *Set, paths []string) ([]string, []string, error) {
	err := d.validateSet(set)
	if err != nil {
		return nil, nil, err
	}

	return d.validateFileAndDirPaths(set, paths)
}

func (d *DB) validateSet(set *Set) error {
	if set.Status == Complete {
		return nil
	}

	return Error{Msg: ErrRemovalWhenSetNotComplete, id: set.Name}
}

// IsSetReadyToAddFiles checks if the set is ready to have files/dirs added to it.
func (d *DB) IsSetReadyToAddFiles(set *Set) error {
	if set.ReadOnly {
		return Error{Msg: ErrSetIsNotWritable, id: set.ID()}
	}

	remReqs, err := d.GetRemoveRequests(set.ID())
	if err != nil {
		return err
	}

	for _, r := range remReqs {
		if !r.IsComplete {
			return Error{Msg: ErrPendingRemovals, id: set.ID()}
		}
	}

	return nil
}

// validateFileAndDirPaths returns an error if any provided path is not in the
// given set. Also classifies the valid paths into a slice of filepaths or
// dirpaths.
func (d *DB) validateFileAndDirPaths(set *Set, paths []string) ([]string, []string, error) {
	filePaths, notFilePaths, err := d.validateFilePaths(set, paths)
	if err != nil {
		return nil, nil, err
	}

	dirPaths, invalidPaths, err := d.validateDirPaths(set, notFilePaths)
	if err != nil {
		return nil, nil, err
	}

	if len(invalidPaths) == 0 {
		return filePaths, dirPaths, err
	}

	dirPaths, invalidPaths, err = d.processSetIfOld(set.ID(), dirPaths, invalidPaths)

	if len(invalidPaths) > 0 {
		err = Error{Msg: fmt.Sprintf("%s : %v", ErrPathNotInSet, invalidPaths), id: set.Name}

		return nil, nil, err
	}

	return filePaths, dirPaths, err
}

func (d *DB) processSetIfOld(sid string, dirPaths, pathsToCheck []string) ([]string, []string, error) {
	discoveredFoldersBucketExists, err := d.checkIfDiscoveredFoldersBucketExists(sid)
	if err != nil {
		return nil, nil, err
	}

	if discoveredFoldersBucketExists {
		return dirPaths, pathsToCheck, nil
	}

	discoveredFolderPaths, invalidPaths, errc := d.checkForDiscoveredFolders(pathsToCheck, sid)
	if errc != nil {
		return nil, nil, errc
	}

	dirPaths = slices.Concat(dirPaths, discoveredFolderPaths)

	return dirPaths, invalidPaths, nil
}

func (d *DB) checkIfDiscoveredFoldersBucketExists(sid string) (bool, error) {
	var exists bool

	err := d.db.View(func(tx *bolt.Tx) error {
		subBucketName := []byte(discoveredFoldersBucket + separator + sid)
		setsBucket := tx.Bucket([]byte(setsBucket))

		entriesBucket := setsBucket.Bucket(subBucketName)

		exists = entriesBucket != nil

		return nil
	})

	return exists, err
}

func (d *DB) checkForDiscoveredFolders(paths []string, sid string) ([]string, []string, error) {
	discoveredFolders, err := d.getDiscoveredFoldersForOldSets(sid)
	if err != nil {
		return nil, nil, err
	}

	validPaths := make([]string, 0, len(paths))
	invalidPaths := make([]string, 0, len(paths))

	for _, path := range paths {
		seenBefore, ok := discoveredFolders[path]
		if !ok {
			invalidPaths = append(invalidPaths, path)
		}

		if seenBefore {
			continue
		}

		validPaths = append(validPaths, path)
		discoveredFolders[path] = true
	}

	return validPaths, invalidPaths, nil
}

func (d *DB) getDiscoveredFoldersForOldSets(sid string) (map[string]bool, error) {
	dirEntries, err := d.GetDirEntries(sid)
	if err != nil {
		return nil, err
	}

	baseFolders := make(map[string]bool, len(dirEntries))

	for _, dirEntry := range dirEntries {
		baseFolders[dirEntry.Path] = true
	}

	discoveredFolders := make(map[string]bool)

	fileEntries, err := d.getEntries(sid, discoveredBucket)
	if err != nil {
		return nil, err
	}

	for _, file := range fileEntries {
		discoveredFolders = d.getAllDiscoveredFoldersFromFile(file.Path, baseFolders, discoveredFolders)
	}

	return discoveredFolders, nil
}

func (d *DB) getAllDiscoveredFoldersFromFile(path string, baseFolders,
	discoveredFolders map[string]bool) map[string]bool {
	dir := filepath.Dir(path)

	if _, ok := baseFolders[dir]; ok {
		return discoveredFolders
	}

	discoveredFolders[dir] = false

	return d.getAllDiscoveredFoldersFromFile(dir, baseFolders, discoveredFolders)
}

func (d *DB) validateFilePaths(set *Set, paths []string) ([]string, []string, error) {
	return d.validatePaths(set, fileBucket, discoveredBucket, paths)
}

// validatePaths checks if the provided paths are in atleast one of the given
// buckets for the set. Returns a slice of all valid paths and a slice of all
// invalid paths.
func (d *DB) validatePaths(set *Set, bucket1, bucket2 string, paths []string) ([]string, []string, error) {
	entriesMap, err := d.getPathToEntryMapFromBuckets([]string{bucket1, bucket2}, set.ID())
	if err != nil {
		return nil, nil, err
	}

	var ( //nolint:prealloc
		validPaths   []string
		invalidPaths []string
	)

	for _, path := range paths {
		if _, ok := entriesMap[path]; ok {
			validPaths = append(validPaths, path)

			continue
		}

		invalidPaths = append(invalidPaths, path)
	}

	return validPaths, invalidPaths, nil
}

func (d *DB) getPathToEntryMapFromBuckets(buckets []string, sid string) (map[string]bool, error) {
	entriesMap := make(map[string]bool)

	for _, bucket := range buckets {
		entries, err := d.getEntries(sid, bucket)
		if err != nil {
			return nil, err
		}

		for _, entry := range entries {
			entriesMap[entry.Path] = false
		}
	}

	return entriesMap, nil
}

func (d *DB) validateDirPaths(set *Set, paths []string) ([]string, []string, error) {
	return d.validatePaths(set, dirBucket, discoveredFoldersBucket, paths)
}

// RemoveFileEntry removes the provided file from a given set.
func (d *DB) RemoveFileEntry(setID string, path string) error {
	err := d.removeEntry(setID, path, fileBucket)
	if err != nil {
		return err
	}

	return d.removeEntry(setID, path, discoveredBucket)
}

// removeEntry removes the entry with the provided entry key from a given
// bucket of a given set.
func (d *DB) removeEntry(setID string, entryKey string, bucketName string) error {
	return d.db.Update(func(tx *bolt.Tx) error {
		subBucketName := []byte(bucketName + separator + setID)
		setsBucket := tx.Bucket([]byte(setsBucket))

		entriesBucket := setsBucket.Bucket(subBucketName)
		if entriesBucket == nil {
			return nil
		}

		return entriesBucket.Delete([]byte(entryKey))
	})
}

// RemoveDirEntry removes the provided directory from a given set.
func (d *DB) RemoveDirEntry(setID string, path string, deleteFromDiscoverBucket bool) error {
	err := d.removeEntry(setID, path, dirBucket)
	if err != nil {
		return err
	}

	if deleteFromDiscoverBucket {
		return d.removeEntry(setID, path, discoveredFoldersBucket)
	}

	return nil
}

// GetFilesInDir returns all file paths from inside the given directory (and all
// nested inside) for the given set using the db.
func (d *DBRO) GetFilesInDir(setID string, dirpath string) ([]string, error) {
	return d.getPathsWithPrefix(setID, discoveredBucket, dirpath)
}

// GetFoldersInDir returns all folder paths from inside the given directory (and all
// nested inside) for the given set using the db.
func (d *DBRO) GetFoldersInDir(setID string, dirpath string) ([]string, error) {
	return d.getPathsWithPrefix(setID, discoveredFoldersBucket, dirpath)
}

// getPathsWithPrefix returns all the filepaths for the given set from the given sub
// bucket prefix, that have the given prefix.
func (d *DBRO) getPathsWithPrefix(setID, bucketName, prefix string) ([]string, error) {
	var entries []string

	err := d.db.View(func(tx *bolt.Tx) error {
		subBucketName := []byte(bucketName + separator + setID)
		setsBucket := tx.Bucket([]byte(setsBucket))

		entriesBucket := setsBucket.Bucket(subBucketName)
		if entriesBucket == nil {
			return nil
		}

		c := entriesBucket.Cursor()

		prefix := []byte(prefix)

		for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
			entries = append(entries, string(k))
		}

		return nil
	})

	return entries, err
}

// MergeFileEntries sets the file paths for the given backup set. Only supply
// absolute paths to files. It keeps the existing entries in the set and
// handles duplicates by updating the existing entries. If the file is not
// already in the set, it will be added with the status Registered.
func (d *DB) MergeFileEntries(setID string, paths []string) error {
	entries := make([]*Dirent, len(paths))

	for n, path := range paths {
		entries[n] = &Dirent{
			Path: path,
		}
	}

	return d.mergeEntries(setID, entries, fileBucket, Registered)
}

// mergeEntries sets the paths for the given backup set in a sub bucket with the
// given prefix. Only supply absolute paths. It keeps the existing entries in the set and
// handles duplicates by updating the existing entries.
// If the file is not already in the set, it will be added with the given status.
func (d *DB) mergeEntries(setID string, dirents []*Dirent, bucketName string, initialStatus EntryStatus) error {
	return d.db.Update(func(tx *bolt.Tx) error {
		b, existing, err := d.getExistingEntries(tx, bucketName, setID)
		if err != nil {
			return err
		}

		// this sort is critical to database write speed.
		sort.Slice(dirents, func(i, j int) bool {
			return strings.Compare(dirents[i].Path, dirents[j].Path) == -1
		})

		ec, err := newEntryCreator(d, tx, b, existing, setID, initialStatus)
		if err != nil {
			return err
		}

		return ec.UpdateOrCreateEntries(dirents)
	})
}

// SetRemoveRequests writes a list of remove requests into the database.
// Directory paths will be put into the database with a trailing slash.
func (d *DB) SetRemoveRequests(sid string, removeReqs []RemoveReq) error {
	return d.db.Update(func(tx *bolt.Tx) error {
		sfsb, err := d.newSetFileBucket(tx, removedBucket, sid)
		if err != nil {
			return err
		}

		return d.putRemoveRequestsInBucket(removeReqs, sfsb.Bucket)
	})
}

func (d *DB) putRemoveRequestsInBucket(remReqs []RemoveReq, b *bolt.Bucket) error {
	for _, remReq := range remReqs {
		if remReq.IsDir {
			remReq.Path += "/"
		}

		err := b.Put([]byte(remReq.Path), d.encodeToBytes(remReq))
		if err != nil {
			return err
		}
	}

	return nil
}

// UpdateRemoveRequest replaces the given removeReq in the db.
func (d *DB) UpdateRemoveRequest(removeReq RemoveReq) error {
	return d.db.Update(func(tx *bolt.Tx) error {
		b := getSubBucket(tx, removeReq.Set.ID(), removedBucket)
		if b == nil {
			return nil
		}

		return d.putRemoveRequestsInBucket([]RemoveReq{removeReq}, b)
	})
}

// deleteObjectFromSubBucket deletes the object with the given key from the db.
func (d *DB) deleteObjectFromSubBucket(key, setID, subBucket string) error {
	return d.db.Update(func(tx *bolt.Tx) error {
		b := getSubBucket(tx, setID, subBucket)
		if b == nil {
			return nil
		}

		return b.Delete([]byte(key))
	})
}

// getSubBucket returns a given subBucket for a given set.
func getSubBucket(tx *bolt.Tx, setID, subBucket string) *bolt.Bucket {
	subBucketName := []byte(subBucket + separator + setID)
	setsBucket := tx.Bucket([]byte(setsBucket))

	return setsBucket.Bucket(subBucketName)
}

// GetIncompleteRemoveRequests returns all incomplete removeReqs from every set.
func (d *DBRO) GetIncompleteRemoveRequests() ([]RemoveReq, error) {
	var allRemReqs []RemoveReq

	sets, err := d.GetAll()
	if err != nil {
		return nil, err
	}

	for _, set := range sets {
		remReqs, err := d.GetRemoveRequests(set.ID())
		if err != nil {
			return nil, err
		}

		for _, remReq := range remReqs {
			if !remReq.IsComplete {
				allRemReqs = append(allRemReqs, remReq)
			}
		}
	}

	return allRemReqs, nil
}

// GetRemoveRequests returns all objects from the remove bucket in the database.
func (d *DBRO) GetRemoveRequests(sid string) ([]RemoveReq, error) {
	var remReqs []RemoveReq

	err := d.db.View(func(tx *bolt.Tx) error {
		b := getSubBucket(tx, sid, removedBucket)
		if b == nil {
			return nil
		}

		return b.ForEach(func(_, v []byte) error {
			remReq := d.decodeRemoveRequest(v)

			remReqs = append(remReqs, remReq)

			return nil
		})
	})

	return remReqs, err
}

func (d *DBRO) decodeRemoveRequest(v []byte) RemoveReq {
	dec := codec.NewDecoderBytes(v, d.ch)

	var remReq RemoveReq

	dec.MustDecode(&remReq)

	return remReq
}

// GetExcludedPaths returns the paths for all files and dirs that should not be
// included in any discoveries on the given set.
func (d *DB) GetExcludedPaths(setID string) ([]string, error) {
	remReqs, err := d.GetRemoveRequests(setID)
	if err != nil {
		return nil, err
	}

	paths := make([]string, len(remReqs))

	for i, remReq := range remReqs {
		paths[i] = remReq.Path
	}

	return paths, nil
}

// OptimiseRemoveBucket removes all redundant entries from the remove bucket for
// a given set.
func (d *DB) OptimiseRemoveBucket(setID string) error {
	remReqs, err := d.GetRemoveRequests(setID)
	if err != nil {
		return err
	}

	curDir := "this is an impossible path prefix"

	for _, remReq := range remReqs {
		curDir, err = d.removeEntryIfRedundant(remReq, curDir)
		if err != nil {
			return err
		}
	}

	return nil
}

func (d *DB) removeEntryIfRedundant(remReq RemoveReq, curDir string) (string, error) {
	if !remReq.IsComplete {
		return curDir, nil
	}

	if strings.HasPrefix(remReq.Path, curDir) {
		err := d.deleteObjectFromSubBucket(remReq.Path, remReq.Set.ID(), removedBucket)
		if err != nil {
			return "", err
		}

		return curDir, nil
	}

	if strings.HasSuffix(remReq.Path, "/") {
		return remReq.Path, nil
	}

	return curDir, nil
}

// RemoveFromRemovedBucket removes the given path from the removed bucket of the given set.
func (d *DB) RemoveFromRemovedBucket(path, sid string) error {
	return d.deleteObjectFromSubBucket(path, sid, removedBucket)
}

// GetExistingDirs returns all existing dir paths.
func (d *DB) GetExistingDirs(setID string) (map[string]struct{}, error) {
	existing, err := d.GetDirEntries(setID)
	if err != nil {
		return nil, err
	}

	existingMap := make(map[string]struct{}, len(existing))

	for _, entry := range existing {
		if entry.Status != Registered {
			existingMap[entry.Path] = struct{}{}
		}
	}

	return existingMap, err
}

// getExistingEntries returns all existing entries in the given sub bucket of
// the setsBucket. If an entry has recently been added (has the status
// 'Registered'), it will not be returned.
func (d *DB) getExistingEntries(tx *bolt.Tx, subBucketName string, setID string) (*bolt.Bucket,
	map[string][]byte, error,
) {
	existing := make(map[string][]byte)

	sfsb, err := d.newSetFileBucket(tx, subBucketName, setID)
	if err != nil {
		return nil, existing, err
	}

	err = sfsb.Bucket.ForEach(func(k, v []byte) error {
		path := string(k)
		entry := d.decodeEntry(v)

		if entry.Status != Registered {
			existing[path] = bytes.Clone(v)
		}

		return nil
	})
	if err != nil {
		return sfsb.Bucket, existing, err
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
func newDirentFromPath(path string) *Dirent {
	dirent := &Dirent{
		Path: path,
		Mode: os.ModeIrregular,
	}

	info, err := os.Lstat(path)
	if err != nil {
		return dirent
	}

	dirent.Mode = info.Mode().Type()

	statt, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return dirent
	}

	dirent.Inode = statt.Ino

	return dirent
}

// MergeDirEntries sets the directory paths for the given backup set. Only supply
// absolute paths to directories. It keeps the existing entries in the set and
// handles duplicates by updating the existing entries. If the dir is not
// already in the set, it will be added with the status Registered.
func (d *DB) MergeDirEntries(setID string, entries []*Dirent) error {
	return d.mergeEntries(setID, entries, dirBucket, Registered)
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
// the files and a list of the dirs discovered in those directories.
type DiscoverCallback func([]*Entry) ([]*Dirent, []*Dirent, error)

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

		set.reset()

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

	var fileEntries []*Dirent

	var dirEntries []*Dirent

	if cb != nil {
		fileEntries, dirEntries, err = cb(entries)
	}

	err = errors.Join(err, <-errCh)
	if err != nil {
		return nil, err
	}

	return d.setDiscoveredEntries(setID, fileEntries, dirEntries)
}

func (d *DB) statPureFileEntries(setID string) error {
	return d.db.Update(func(tx *bolt.Tx) error {
		sfsb, err := d.newSetFileBucket(tx, fileBucket, setID)
		if err != nil {
			return err
		}

		direntCh := make(chan *Dirent)
		numEntries := 0

		sfsb.Bucket.ForEach(func(k, v []byte) error { //nolint:errcheck
			numEntries++
			path := string(k)

			d.filePool.Submit(func() {
				direntCh <- newDirentFromPath(path)
			})

			return nil
		})

		return d.handleFilePoolResults(tx, sfsb, setID, direntCh, numEntries)
	})
}

func (d *DB) handleFilePoolResults(tx *bolt.Tx, sfsb *setFileSubBucket, setID string,
	direntCh chan *Dirent, numEntries int,
) error {
	dirents := make([]*Dirent, numEntries)

	_, existing, err := d.getExistingEntries(tx, fileBucket, setID)
	if err != nil {
		return err
	}

	for n := range dirents {
		dirent := <-direntCh
		dirents[n] = dirent
	}

	sort.Slice(dirents, func(i, j int) bool {
		return strings.Compare(dirents[i].Path, dirents[j].Path) == -1
	})

	ec, err := newEntryCreator(d, tx, sfsb.Bucket, existing, setID, Pending)
	if err != nil {
		return err
	}

	return ec.UpdateOrCreateEntries(dirents)
}

// setDiscoveredEntries adds discovered file paths for the given backup set's
// directory entries. Only supply absolute paths to files.
//
// It also updates LastDiscovery, sets NumFiles and sets status to
// PendingUpload unless the set contains no files, in which case it sets status
// to Complete.
//
// Returns the updated set and an error if the setID isn't in the database.
func (d *DB) setDiscoveredEntries(setID string, fileDirents, dirDirents []*Dirent) (*Set, error) {
	if err := d.mergeEntries(setID, fileDirents, discoveredBucket, Pending); err != nil {
		return nil, err
	}

	if err := d.mergeEntries(setID, dirDirents, discoveredFoldersBucket, Pending); err != nil {
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

		set.DiscoveryCompleted(d.countAllFilesInSet(tx, setID))

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
func (d *DB) SetEntryStatus(r *transfer.Request) (*Entry, error) {
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
		} else if entry.isDir {
			return nil
		}

		erru := d.updateSetBasedOnEntry(got, entry)
		if erru != nil {
			return erru
		}

		return b.Put(bid, d.encodeToBytes(got))
	})

	return entry, err
}

// requestToSetID returns a setID for the Request. Returns an error if the
// Request doesn't have a Requester and Set defined.
func requestToSetID(r *transfer.Request) (string, error) {
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
func (d *DB) updateFileEntry(tx *bolt.Tx, setID string, r *transfer.Request,
	setDiscoveryTime time.Time) (*Entry, error) {
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

	if entry.Status == Pending || entry.Status == Failed {
		entry.Attempts++
		entry.newSize = entry.Attempts == 1
	}

	requestStatusToEntryStatus(r, entry)

	if entry.Status != Orphaned {
		entry.Size = r.UploadedSize()
	}

	if err = d.updateFailedLookup(tx, setID, r.Local, entry); err != nil {
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
	entry.isDir = kind == dirBucket || kind == discoveredFoldersBucket

	return entry, b
}

// requestStatusToEntryStatus converts Request.Status and stores it as a Status
// on the entry. Also sets entry.Attempts, unFailed and newFail as appropriate.
func requestStatusToEntryStatus(r *transfer.Request, entry *Entry) { //nolint:gocyclo,funlen,cyclop
	entry.newFail = false
	entry.unFailed = false

	switch r.Status { //nolint:exhaustive
	case transfer.RequestStatusUploading:
		entry.Status = UploadingEntry

		if r.Stuck != nil {
			entry.LastError = r.Stuck.String()
		}
	case transfer.RequestStatusUploaded:
		entry.Status = Uploaded
		entry.unFailed = entry.Attempts > 1
		entry.LastError = ""
	case transfer.RequestStatusReplaced:
		entry.Status = Replaced
		entry.unFailed = entry.Attempts > 1
		entry.LastError = ""
	case transfer.RequestStatusUnmodified:
		entry.Status = Skipped
		entry.LastError = ""
	case transfer.RequestStatusFailed:
		entry.Status = Failed

		if r.Error != "" {
			entry.LastError = r.Error
		}

		entry.newFail = entry.Attempts == 1
	case transfer.RequestStatusMissing:
		entry.Status = Missing
		entry.unFailed = entry.Attempts > 1
	case transfer.RequestStatusOrphaned:
		entry.Status = Orphaned
		entry.unFailed = entry.Attempts > 1
	case transfer.RequestStatusPending:
		entry.Status = Pending
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

// RemovePathFromFailedBucket removes the entry with the given setID and path
// from the failed bucket.
func (d *DB) RemovePathFromFailedBucket(setID, path string) error {
	return d.db.Update(func(tx *bolt.Tx) error {
		return d.removeFailedLookup(tx, setID, path)
	})
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
func (d *DB) updateSetBasedOnEntry(set *Set, entry *Entry) error {
	return set.UpdateBasedOnEntry(entry, d.GetFileEntries)
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

	set := struct {
		Set
		SizeFiles *uint64
	}{}

	set.SizeFiles = &set.SizeTotal

	dec.MustDecode(&set)

	set.LogChangesToSlack(d.slacker)

	return &set.Set
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

// GetDiscoveredFileEntries returns all the discovered file entries for the given set (only
// SetDiscoveredEntries, not SetFileEntries).
func (d *DBRO) GetDiscoveredFileEntries(setID string) ([]*Entry, error) {
	return d.getEntries(setID, discoveredBucket)
}

// GetDirEntries returns all the dir entries for the given set.
func (d *DBRO) GetDirEntries(setID string) ([]*Entry, error) {
	return d.getEntries(setID, dirBucket)
}

// GetDirEntries returns all the dir entries for the given set.
func (d *DBRO) GetAllDirEntries(setID string) ([]*Entry, error) {
	entries, err := d.getEntries(setID, dirBucket)
	if err != nil {
		return nil, err
	}

	entries2, err := d.getEntries(setID, discoveredFoldersBucket)
	if err != nil {
		return nil, err
	}

	return append(entries, entries2...), nil
}

// SetError updates a set with the given error message. Returns an error if the
// setID isn't in the database.
func (d *DB) SetError(setID, errMsg string) error {
	return d.updateSetProperties(setID, func(got *Set) {
		got.SetError(errMsg)
	})
}

// UpdateBasedOnRemovedEntry updates set counts based on the given entry that's
// been removed.
func (d *DB) UpdateBasedOnRemovedEntry(setID string, entry *Entry) error {
	return d.updateSetProperties(setID, func(got *Set) {
		got.SizeRemoved += entry.Size
		got.SizeTotal -= entry.Size
		got.NumFiles--
		got.NumObjectsRemoved++

		got.removedEntryToSetCounts(entry)
	})
}

// IncrementNumObjectRemoved increments the number of objects removed for the
// given set.
func (d *DB) IncrementNumObjectRemoved(setID string) error {
	return d.updateSetProperties(setID, func(got *Set) {
		got.NumObjectsRemoved++
	})
}

// UpdateSetTotalToRemove sets num of objects to be removed to provided value
// and resets num of objects removed if the previous removal was successful
// otherwise just increases number to be removed with provided value.
func (d *DB) UpdateSetTotalToRemove(setID string, num uint64) error {
	return d.updateSetProperties(setID, func(got *Set) {
		if got.NumObjectsToBeRemoved == got.NumObjectsRemoved {
			got.NumObjectsToBeRemoved = num
			got.NumObjectsRemoved = 0

			return
		}

		got.NumObjectsToBeRemoved += num
	})
}

// IncrementSetTotalRemoved increments the number of objects removed for the
// set.
func (d *DB) IncrementSetTotalRemoved(setID string) error {
	return d.updateSetProperties(setID, func(got *Set) {
		got.NumObjectsRemoved++
	})
}

// ResetRemoveSize resets the size removed for the set.
func (d *DB) ResetRemoveSize(setID string) error {
	return d.updateSetProperties(setID, func(got *Set) {
		if got.NumObjectsToBeRemoved == got.NumObjectsRemoved {
			got.SizeRemoved = 0
		}
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
		got.SetWarning(warnMsg)
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

	putter, err := transfer.New(d.remoteBackupHandler, []*transfer.Request{{
		Local:  d.backupPath,
		Remote: d.remoteBackupPath,
		Meta:   transfer.NewMeta(),
	}})
	if err != nil {
		return err
	}

	defer func() {
		putter.Cleanup()
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

func drainChannel(ch chan *transfer.Request) {
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
func (d *DB) EnableRemoteBackups(remotePath string, handler transfer.Handler) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.remoteBackupPath = remotePath
	d.remoteBackupHandler = handler
}

func (d *DBRO) LogSetChangesToSlack(slacker Slacker) {
	d.slacker = slacker
}

// MakeSetWritable sets ReadOnly to false on a given set.
// This is the only way to change the ReadOnly set.
func (d *DB) MakeSetWritable(sid string) error {
	return d.updateSetProperties(sid, func(s *Set) {
		s.ReadOnly = false
	})
}
