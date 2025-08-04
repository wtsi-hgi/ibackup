/*******************************************************************************
 * Copyright (c) 2025 Genome Research Ltd.
 *
 * Author: Michael Woolnough <mw31@sanger.ac.uk>
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

package db

import (
	"bytes"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

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
	Transformer *Transformer

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
	Metadata Metadata

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

	// Orphaned provides the total number of set and discovered files in this
	// set that no longer exist locally since the last discovery but do exist
	// remotely. This is a read-only value.
	Orphaned uint64

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

	Reason     string
	ReviewDate time.Time
	DeleteDate time.Time

	// Error holds any error that applies to the whole set, such as an issue
	// with the Transformer. This is a read-only value.
	Error string

	// Warning contains errors that do not stop progression. This is a read-only
	// value.
	Warning string

	modifiable bool

	Hidden bool
}

type Metadata map[string]string

func (m *Metadata) Scan(src any) error {
	if data, ok := src.([]byte); ok {
		*m = make(Metadata)

		return json.NewDecoder(bytes.NewReader(data)).Decode(m)
	} else if str, ok := src.(string); ok {
		*m = make(Metadata)

		return json.NewDecoder(strings.NewReader(str)).Decode(m)
	}

	return ErrInvalidMetadata
}

func (m Metadata) Value() (driver.Value, error) { //nolint:unparam
	var sb strings.Builder

	json.NewEncoder(&sb).Encode(m) //nolint:errcheck,errchkjson

	return sb.String(), nil
}

func (s *Set) IsReadonly() bool {
	return !s.modifiable
}

var (
	ErrReadonlySet          = errors.New("cannot modify readonly set")
	ErrInvalidSetName       = errors.New("invalid set name")
	ErrInvalidRequesterName = errors.New("invalid requester name")
	ErrInvalidMetadata      = errors.New("invalid metadata")
)

func (d *DB) CreateSet(set *Set) error { //nolint:gocyclo
	if strings.HasPrefix(set.Name, "\x00") || set.Name == "" { //nolint:gocritic,nestif
		return ErrInvalidSetName
	} else if strings.HasPrefix(set.Requester, "\x00") || set.Requester == "" {
		return ErrInvalidRequesterName
	} else if !set.Transformer.isValid() {
		return ErrInvalidTransformer
	}

	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	if err := d.createSet(tx, set); err != nil {
		return err
	}

	return tx.Commit()
}

func (d *DB) createSet(tx *sql.Tx, set *Set) error {
	tID, err := d.execReturningRowID(tx, createTransformer, set.Transformer.name,
		set.Transformer.re.String(), set.Transformer.replace)
	if err != nil {
		return err
	}

	res, err := tx.Exec(createSet, set.Name, set.Requester, tID, set.MonitorTime, set.MonitorRemovals,
		set.Description, set.Metadata, set.Reason, timeOrNull(set.ReviewDate), timeOrNull(set.DeleteDate))
	if err != nil {
		return err
	}

	if set.id, err = res.LastInsertId(); err != nil {
		return err
	}

	set.modifiable = true

	return nil
}

func timeOrNull(t time.Time) sql.NullTime {
	return sql.NullTime{
		Time:  t,
		Valid: !t.IsZero(),
	}
}

func execReturningRowID(tx *sql.Tx, sql string, params ...any) (int64, error) {
	res, err := tx.Exec(sql, params...)
	if err != nil {
		return 0, err
	}

	return res.LastInsertId()
}

func (d *DBRO) GetSet(name, requester string) (*Set, error) {
	if strings.HasPrefix(name, "\x00") {
		return nil, ErrInvalidSetName
	} else if strings.HasPrefix(requester, "\x00") {
		return nil, ErrInvalidRequesterName
	}

	return scanSet(d.db.QueryRow(getSetByNameRequester, name, requester))
}

func scanSet(scanner scanner) (*Set, error) { //nolint:funlen
	set := new(Set)

	var (
		reviewDate, deleteDate                                 sql.NullTime
		transformerName, transformerRegexp, transformerReplace string
	)

	if err := scanner.Scan(
		&set.id,
		&set.Name,
		&set.Requester,
		&set.Description,
		&set.MonitorTime,
		&set.MonitorRemovals,
		&set.Metadata,
		&set.Reason,
		&reviewDate,
		&deleteDate,
		&set.NumFiles,
		&set.SizeTotal,
		&set.Uploaded,
		&set.Replaced,
		&set.Skipped,
		&set.Failed,
		&set.Missing,
		&set.Orphaned,
		&set.Abnormal,
		&set.Hardlinks,
		&set.Symlinks,
		&set.SizeUploaded,
		&set.NumObjectsRemoved,
		&set.SizeRemoved,
		&set.NumObjectsToBeRemoved,
		&set.StartedDiscovery,
		&set.LastDiscovery,
		&set.Status,
		&set.LastCompletedCount,
		&set.LastCompletedSize,
		&set.Error,
		&set.Warning,
		&set.modifiable,
		&set.Hidden,
		&transformerName,
		&transformerRegexp,
		&transformerReplace,
	); err != nil {
		return nil, err
	}

	set.ReviewDate = reviewDate.Time
	set.DeleteDate = deleteDate.Time

	transformer, err := NewTransformer(transformerName, transformerRegexp, transformerReplace)
	if err != nil {
		return nil, err
	}

	set.Transformer = transformer

	return set, nil
}

func (d *DBRO) GetSetsByRequester(requester string) *IterErr[*Set] {
	if strings.HasPrefix(requester, "\x00") {
		return iterErr[*Set](ErrInvalidRequesterName)
	}

	return iterRows(d, scanSet, getSetsByRequester, requester)
}

func (d *DBRO) GetAllSets() *IterErr[*Set] {
	return iterRows(d, scanSet, getAllSets)
}

func (d *DB) SetSetWarning(set *Set) error {
	if !set.modifiable {
		return ErrReadonlySet
	}

	return d.exec(updateSetError, set.Warning, set.id)
}

func (d *DB) SetSetError(set *Set) error {
	if !set.modifiable {
		return ErrReadonlySet
	}

	return d.exec(updateSetError, set.Error, set.id)
}

func (d *DB) SetSetDicoveryStarted(set *Set) error {
	if !set.modifiable {
		return ErrReadonlySet
	}

	return d.exec(updateDiscoveryStarted, set.id)
}

func (d *DB) SetSetDicoveryCompleted(set *Set) error {
	if !set.modifiable {
		return ErrReadonlySet
	}

	return d.exec(updateLastDiscoveryCompleted, set.id)
}

func (d *DB) DeleteSet(set *Set) error {
	if !set.modifiable {
		return ErrReadonlySet
	}

	return d.exec(deleteSet, set.id)
}

func (d *DB) SetSetReadonly(set *Set) error {
	if !set.modifiable {
		return ErrReadonlySet
	}

	if err := d.exec(updateSetReadonly, set.id); err != nil {
		return err
	}

	set.modifiable = false

	return nil
}

func (d *DB) SetSetModifiable(set *Set) error {
	if err := d.exec(updateSetModifiable, set.id); err != nil {
		return err
	}

	set.modifiable = true

	return nil
}

func (d *DB) SetSetHidden(set *Set) error {
	if !set.modifiable {
		return ErrReadonlySet
	}

	if err := d.exec(updateSetHidden, set.id); err != nil {
		return err
	}

	set.Hidden = true

	return nil
}

func (d *DB) SetSetVisible(set *Set) error {
	if !set.modifiable {
		return ErrReadonlySet
	}

	if err := d.exec(updateSetVisible, set.id); err != nil {
		return err
	}

	set.Hidden = false

	return nil
}

func (d *DB) SetSetMonitored(set *Set) error {
	if !set.modifiable {
		return ErrReadonlySet
	}

	return d.exec(updateSetMonitored, set.MonitorTime, set.MonitorRemovals, set.id)
}

func (d *DB) SetSetTransformer(set *Set) error {
	if !set.modifiable {
		return ErrReadonlySet
	} else if !set.Transformer.isValid() {
		return ErrInvalidTransformer
	}

	if err := d.GetSetDiscovery(set).ForEach(func(disc *Discover) error {
		switch disc.Type { //nolint:exhaustive
		case DiscoverFile, DiscoverDirectory:
			if !set.Transformer.Match(disc.Path) {
				return fmt.Errorf("path: %s: %w", disc.Path, ErrInvalidTransformPath)
			}
		}

		return nil
	}); err != nil {
		return err
	}

	return d.changeTransformer(set)
}

func (d *DB) changeTransformer(set *Set) error { //nolint:gocyclo
	eset, err := scanSet(d.db.QueryRow(getSetByID, set.id))
	if err != nil {
		return err
	}

	eset.Transformer = set.Transformer

	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	if _, err = tx.Exec(shiftSetRequester, set.id); err != nil { //nolint:nestif
		return err
	} else if err = d.createSet(tx, eset); err != nil {
		return err
	} else if err = d.moveDiscovery(tx, set.id, eset.id); err != nil {
		return err
	} else if err = d.moveSetFiles(tx, set.id, eset); err != nil {
		return err
	} else if err = d.removeSetFiles(tx, set.id); err != nil {
		return err
	}

	set.id = eset.id

	return tx.Commit()
}

func (d *DB) moveDiscovery(tx *sql.Tx, from, to int64) error {
	_, err := tx.Exec(updateDiscoverySet, to, from)

	return err
}

func (d *DB) moveSetFiles(tx *sql.Tx, from int64, to *Set) (err error) {
	files := iterRows(&d.DBRO, scanFile, getSetsFiles, from)

	for file := range files.Iter {
		if err := d.addSetFile(tx, to, file); err != nil {
			return err
		}
	}

	return files.Error
}

func (d *DB) removeSetFiles(tx *sql.Tx, setID int64) error {
	_, err := tx.Exec(createQueuedRemovalForSet, setID)

	return err
}
