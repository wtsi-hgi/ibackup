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
	"database/sql"
	"errors"
	"iter"
	"strings"
	"time"
)

var (
	ErrTransformerConflict = errors.New("remote file already exists with different transformer")
)

type FileType uint8

const (
	// Unknown is an Entry type meaning the local file is either missing or
	// there was an error trying to get its type.
	Unknown FileType = iota

	Regular
	Hardlink
	Symlink
	Directory
	Abnormal // fifos and sockets etc. that we shouldn't upload
)

type FileStatus uint8

const (
	StatusNone FileStatus = iota
	StatusMissing
	StatusOrphaned
	StatusUploaded
	StatusReplaced
	StatusSkipped
)

type File struct {
	id                int64
	setID             int64
	LocalPath         string
	RemotePath        string
	Size, Inode       uint64
	MountPount        string
	Btime, Mtime      int64
	Type              FileType
	Status            FileStatus
	Owner             string
	Group             string
	SymlinkDest       string
	LastUpload        time.Time
	LastError         string
	LastFailedAttempt time.Time
	Attempts          int
}

func (d *DB) CompleteDiscovery(set *Set, toAdd, toRemove iter.Seq[*File]) error { //nolint:gocyclo
	if !set.modifiable {
		return ErrReadonlySet
	}

	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	for file := range toAdd {
		if err := d.addSetFile(tx, set, file); err != nil {
			return err
		}
	}

	if _, err := tx.Exec(deleteChangedInodes); err != nil { //nolint:nestif
		return err
	} else if err = d.removeFiles(tx, set.id, toRemove, false); err != nil {
		return err
	} else if _, err = tx.Exec(updateLastDiscovery, set.id); err != nil {
		return err
	} else if _, err = tx.Exec(deleteHeldDiscovery, set.id); err != nil {
		return err
	}

	return tx.Commit()
}

func (d *DB) addSetFile(tx *sql.Tx, set *Set, file *File) error {
	var err error

	file.RemotePath, err = set.Transformer.Transform(file.LocalPath)
	if err != nil {
		return err
	}

	hlID, err := d.execReturningRowID(tx, createHardlink, file.Inode, file.MountPount,
		file.Btime, file.Mtime, file.Size, file.Type, file.Owner,
		file.Group, file.SymlinkDest, file.RemotePath)
	if err != nil {
		return err
	}

	rfID, err := d.execReturningRowID(tx, createRemoteFile, file.RemotePath, hlID, set.Transformer.id)
	if err != nil {
		if d.isForeignKeyError(err) {
			return ErrTransformerConflict
		}

		return err
	}

	file.id, err = d.execReturningRowID(tx, createSetFile, file.LocalPath, set.id, rfID, file.Status, rfID)

	return err
}

func (d *DB) RemoveSetFiles(set *Set, toRemove iter.Seq[*File]) error {
	if !set.modifiable {
		return ErrReadonlySet
	}

	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	if err := d.removeFiles(tx, set.id, toRemove, true); err != nil {
		return err
	}

	return tx.Commit()
}

//nolint:gocognit,gocyclo
func (d *DB) removeFiles(tx *sql.Tx, setID int64, toRemove iter.Seq[*File], updateDiscovery bool) error {
	trashSetID, err := d.getTrashSetID(tx, setID)
	if err != nil {
		return err
	}

	trash := trashSetID != setID

	for file := range toRemove {
		if trash {
			_, err = tx.Exec(createTrashFile, trashSetID, file.id)
			if err != nil {
				return err
			}
		}

		if updateDiscovery {
			if _, err := tx.Exec(createDiscoverRemoveFromFile, file.id); err != nil {
				return err
			}
		}

		if _, err := tx.Exec(createQueuedRemoval, file.id); err != nil {
			return err
		}
	}

	return nil
}

func (d *DB) getTrashSetID(tx *sql.Tx, setID int64) (int64, error) {
	var trashSetID int64

	err := tx.QueryRow(getTrashSetID, setID).Scan(&trashSetID)
	if errors.Is(err, sql.ErrNoRows) {
		res, errr := tx.Exec(createTrashSet, setID)
		if errr != nil {
			return 0, errr
		}

		return res.LastInsertId()
	}

	return trashSetID, err
}

func (d *DB) RemoveSetFilesInDir(set *Set, dir string) error { //nolint:gocyclo
	if !set.modifiable {
		return ErrReadonlySet
	} else if !strings.HasSuffix(dir, "/") {
		dir += "/"
	}

	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	files := iterRows(&d.DBRO, scanFileID(set.id), getSetsFilesWithPrefix, set.id, dir)

	if err = d.removeFiles(tx, set.id, files.Iter, false); err != nil {
		return err
	}

	if files.Error != nil {
		return err
	}

	if _, err = tx.Exec(deleteRedundantDiscovers, set.id, dir); err != nil {
		return err
	}

	if _, err = tx.Exec(createDiscover, set.id, dir, DiscoverRemovedDirectory); err != nil {
		return err
	}

	return tx.Commit()
}

func scanFileID(setID int64) func(scanner) (*File, error) {
	return func(scanner scanner) (*File, error) {
		file := new(File)

		if err := scanner.Scan(&file.id); err != nil {
			return nil, err
		}

		file.setID = setID

		return file, nil
	}
}

func (d *DBRO) GetSetFiles(set *Set) *IterErr[*File] {
	if set.modifiable {
		return iterRows(d, scanModifiableFile(set.id), getSetsFilesWithErrors, set.id)
	}

	return iterRows(d, scanFile(set.id), getSetsFiles, set.id)
}

func scanFile(setID int64) func(scanner) (*File, error) {
	return func(scanner scanner) (*File, error) {
		file := new(File)

		if err := scanner.Scan(
			&file.id,
			&file.LocalPath,
			&file.LastUpload,
			&file.Status,
			&file.RemotePath,
			&file.Size,
			&file.Type,
			&file.Owner,
			&file.Group,
			&file.Inode,
			&file.MountPount,
			&file.Btime,
			&file.Mtime,
			&file.SymlinkDest,
		); err != nil {
			return nil, err
		}

		file.setID = setID

		return file, nil
	}
}

func scanModifiableFile(setID int64) func(scanner) (*File, error) { //nolint:funlen
	return func(scanner scanner) (*File, error) {
		file := new(File)

		var lastFailedAttempt sql.NullTime

		if err := scanner.Scan(
			&file.id,
			&file.LocalPath,
			&file.LastUpload,
			&file.Status,
			&file.RemotePath,
			&file.Size,
			&file.Type,
			&file.Owner,
			&file.Group,
			&file.Inode,
			&file.MountPount,
			&file.Btime,
			&file.Mtime,
			&file.SymlinkDest,
			&file.LastError,
			&lastFailedAttempt,
			&file.Attempts,
		); err != nil {
			return nil, err
		}

		file.LastFailedAttempt = lastFailedAttempt.Time
		file.setID = setID

		return file, nil
	}
}
