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
	"iter"
	"strings"
	"time"
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
	LocalPath         string
	RemotePath        string
	Size, Inode       uint64
	MountPount        string
	InodeRemote       string
	Btime, Mtime      int64
	Type              FileType
	Status            FileStatus
	Owner             string
	SymlinkDest       string
	LastUpload        time.Time
	LastError         string
	LastFailedAttempt time.Time
	Attempts          int
	modifiable        bool
}

func (d *DB) SetSetFiles(set *Set, toAdd, toRemove iter.Seq[*File]) error {
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

	if err := d.removeFiles(tx, toRemove, false); err != nil {
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
		file.Btime, file.InodeRemote, file.Mtime, file.Size, file.Type, file.Owner,
		file.SymlinkDest, file.RemotePath)
	if err != nil {
		return err
	}

	rfID, err := d.execReturningRowID(tx, createRemoteFile, file.RemotePath, hlID)
	if err != nil {
		return err
	}

	file.id, err = d.execReturningRowID(tx, createSetFile, file.LocalPath, set.id, rfID, file.Status, rfID)

	return err
}

func (d *DB) RemoveSetFiles(toRemove iter.Seq[*File]) error {
	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	if err := d.removeFiles(tx, toRemove, true); err != nil {
		return err
	}

	return tx.Commit()
}

func (d *DB) removeFiles(tx *sql.Tx, toRemove iter.Seq[*File], updateDiscovery bool) error { //nolint:gocognit
	for file := range toRemove {
		if !file.modifiable {
			return ErrReadonlySet
		}

		if err := d.trashFile(tx, file); err != nil {
			return err
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

	files := iterRows(&d.DBRO, scanFileID, getSetsFilesWithPrefix, set.id, dir)

	if err = d.removeFiles(tx, files.Iter, false); err != nil {
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

func scanFileID(scanner scanner) (*File, error) {
	file := new(File)

	if err := scanner.Scan(&file.id); err != nil {
		return nil, err
	}

	file.modifiable = true

	return file, nil
}

func (d *DB) trashFile(tx *sql.Tx, file *File) error {
	trashSetID, err := d.execReturningRowID(tx, createTrashSetForFile, file.id)
	if err != nil {
		return err
	}

	if trashSetID == 0 {
		return nil
	}

	trashID, err := d.execReturningRowID(tx, createTrashFile, trashSetID, file.id)
	if err != nil {
		return err
	}

	_, err = tx.Exec(disableTrashFileRemoveTask, trashID)

	return err
}

func (d *DBRO) GetSetFiles(set *Set) *IterErr[*File] {
	if set.modifiable {
		return iterRows(d, scanModifiableFile, getSetsFilesWithErrors, set.id)
	}

	return iterRows(d, scanFile, getSetsFiles, set.id)
}

func scanFile(scanner scanner) (*File, error) {
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
		&file.Inode,
		&file.MountPount,
		&file.Btime,
		&file.Mtime,
		&file.InodeRemote,
		&file.SymlinkDest,
	); err != nil {
		return nil, err
	}

	return file, nil
}

func scanModifiableFile(scanner scanner) (*File, error) {
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
		&file.Inode,
		&file.MountPount,
		&file.Btime,
		&file.Mtime,
		&file.InodeRemote,
		&file.SymlinkDest,
		&file.LastError,
		&lastFailedAttempt,
		&file.Attempts,
	); err != nil {
		return nil, err
	}

	file.LastFailedAttempt = lastFailedAttempt.Time
	file.modifiable = true

	return file, nil
}

func (d *DBRO) CountRemoteFileRefs(file *File) (int64, error) {
	var count int64

	err := d.db.QueryRow(getRemoteFileRefs, file.id).Scan(&count)

	return count, err
}
