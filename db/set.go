package db

import "time"

type Status int

const (
	// PendingDiscovery is a Set status meaning the set's entries are pending
	// existence, size and directory content discovery.
	PendingDiscovery Status = iota

	// PendingUpload is a Set status meaning discovery has completed, but no
	// entries have been uploaded since then.
	PendingUpload

	// Uploading is a Set status meaning discovery has completed and upload of
	// entries has started.
	Uploading

	// Failing is a Set status meaning at least 1 of the entries has failed to
	// upload after 3 attempts. Other uploads are ongoing.
	Failing

	// Complete is a Set status meaning all entries have had an upload attempt
	// since the last discovery. (Some uploads may have failed, but they had
	// 3 retries.)
	Complete
)

// Set describes a backup set; a list of files and directories to backup, plus
// some metadata. All properties are required unless otherwise noted.
type Set struct {
	id int64

	// An arbitrary (short) name for this backup set.
	Name string

	// The username of the person requesting this backup.
	Requester string

	// The method of transforming local Entries paths in to remote paths, to
	// determine the upload location. "humgen" to use the put.HumgenTransformer,
	// or "prefix=local:remote" to use the put.PrefixTransformer.
	Transformer string

	// Monitor the files and directories and re-upload them whenever they
	// change, checking for changes after the given amount of time. Optional,
	// defaults to unmonitored (a one time upload of Entries).
	MonitorTime time.Duration

	// Tells the monitor if it should remove any files from the set that have
	// been locally deleted.
	MonitorRemovals bool

	// An optional longer free-text description of this backup set.
	Description string

	// Optional additional metadata which will be applied to every file in the
	// set.
	Metadata map[string]string

	// Delete local paths after successful upload. Optional, defaults to no
	// deletions (ie. do a backup, not a move).
	DeleteLocal bool

	// Delete remote paths if removed from the set. Optional, defaults to no
	// deletions (ie. keep all uploads and ignore removed Entries).
	// DeleteRemote bool

	// Receive an optional notification after this date if DeleteRemote is true
	// and there are still Entries in this set.
	// Expiry time.Time

	// StartedDiscovery provides the last time that discovery started. This is a
	// read-only value.
	StartedDiscovery time.Time

	// LastDiscovery provides the last time that discovery completed. This is a
	// read-only value.
	LastDiscovery time.Time

	// NumFiles provides the total number of set and discovered files in this
	// set, as of the last discovery. This is a read-only value.
	NumFiles uint64

	// SizeTotal provides the total size (bytes) of set and discovered files in
	// this set, as of the last discovery. This is a read-only value.
	SizeTotal uint64

	// Uploaded provides the total number of set and discovered files in this
	// set that have, for the first time, been uploaded or confirmed uploaded
	// since the last discovery. This is a read-only value.
	Uploaded uint64

	// Replaced is like Uploaded, but for files that had previously been
	// uploaded to iRODS, and now uploaded again because the file on local disk
	// was newer.
	Replaced uint64

	// Skipped is like Uploaded, but for files that had previously been
	// uploaded to iRODS, and were not uploaded again because the file on local
	// disk was the same age.
	Skipped uint64

	// Failed provides the total number of set and discovered files in this set
	// that have failed their upload since the last discovery. This is a
	// read-only value.
	Failed uint64

	// Missing provides the total number of set and discovered files in this set
	// that no longer exist locally since the last discovery. This is a
	// read-only value.
	Missing uint64

	// Abnormal provides the total number of set files in this set that were
	// neither regular files, nor Symlinks (ie. they were fifo or socket files
	// etc).
	Abnormal uint64

	// Symlinks provides the total number of set and discovered files in this
	// set that are symlinks. This is a read-only value.
	Symlinks uint64

	// Hardlinks provides the total number of set and discovered files in this
	// set that are hardlinks and skipped. This is a read-only value.
	Hardlinks uint64

	// Status provides the current status for the set since the last discovery.
	// This is a read-only value.
	Status Status

	// LastCompleted provides the last time that all uploads completed
	// (regardless of success or failure). This is a read-only value.
	LastCompleted time.Time

	// LastCompletedCount provides the count of files on the last upload attempt
	// (those successfully uploaded, those which failed, but not those which
	// were missing locally). This is a read-only value.
	LastCompletedCount uint64

	// LastCompletedSize provides the size of files (bytes) counted in
	// LastCompletedCount. This is a read-only value.
	LastCompletedSize uint64

	// SizeUploaded provides the size of files (bytes) actually uploaded (not
	// skipped) since the last discovery. This is a read-only value.
	SizeUploaded uint64

	// SizeRemoved provides the size of files (bytes) part of the most recent
	// remove. This is a read-only value.
	SizeRemoved uint64

	// NumObjectsToBeRemoved provides the number of objects to be removed in the
	// current remove process.
	NumObjectsToBeRemoved uint64

	// numObjectsRemoved provides the number of objects already removed in the
	// current remove process.
	NumObjectsRemoved uint64

	// Error holds any error that applies to the whole set, such as an issue
	// with the Transformer. This is a read-only value.
	Error string

	// Warning contains errors that do not stop progression. This is a read-only
	// value.
	Warning string

	readonly bool
}

func (d *DB) CreateSet(set *Set) error {
	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	res, err := tx.Exec(createTransformer, set.Transformer)
	if err != nil {
		return err
	}

	tID, err := res.LastInsertId()
	if err != nil {
		return err
	}

	if res, err = tx.Exec(createSet, set.Name, set.Requester, tID, set.MonitorTime, set.Description); err != nil {
		return err
	}

	set.id, err = res.LastInsertId()
	if err != nil {
		return err
	}

	return tx.Commit()
}

func (d *DBRO) GetSet(name, requester string) (*Set, error) {
	return scanSet(d.db.QueryRow(getSetByNameRequester, name, requester))
}

func scanSet(scanner scanner) (*Set, error) {
	set := new(Set)

	if err := scanner.Scan(
		&set.id,
		&set.Name,
		&set.Requester,
		&set.Description,
		&set.MonitorTime,
		&set.NumFiles,
		&set.SizeTotal,
		&set.StartedDiscovery,
		&set.LastDiscovery,
		&set.Error,
		&set.Status,
		&set.LastCompletedCount,
		&set.LastCompletedSize,
		&set.Error,
		&set.Warning,
		&set.readonly,
		&set.Transformer,
	); err != nil {
		return nil, err
	}

	return set, nil
}

func (d *DBRO) GetSetsByRequester(requester string) *IterErr[*Set] {
	return iterRows(d, scanSet, getSetsByRequester, requester)
}

func (d *DBRO) GetAllSets(requester string) *IterErr[*Set] {
	return iterRows(d, scanSet, getAllSets, requester)
}

func (d *DB) SetSetWarning(set *Set) error {
	return d.exec(updateSetError, set.Warning, set.id)
}

func (d *DB) SetSetError(set *Set) error {
	return d.exec(updateSetError, set.Error, set.id)
}

func (d *DB) SetSetDicoveryStarted(set *Set) error {
	return d.exec(updateDiscoveryStarted, set.id, set.StartedDiscovery)
}

func (d *DB) SetSetDicoveryCompleted(set *Set) error {
	return d.exec(updateDiscoveryStarted,
		set.LastCompleted,
		set.LastCompletedCount,
		set.LastCompletedSize,
		set.id,
	)
}

func (d *DB) DeleteSet(set *Set) error {
	return d.exec(deleteSet, set.id)
}
