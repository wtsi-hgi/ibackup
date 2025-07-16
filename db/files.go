package db

import (
	"database/sql"
	"iter"
	"time"
)

type FileType uint8

const (
	Regular FileType = iota
	Hardlink
	Symlink
	Directory
	Abnormal // fifos and sockets etc. that we shouldn't upload

	// Unknown is an Entry type meaning the local file is either missing or
	// there was an error trying to get its type.
	Unknown = -1
)

type File struct {
	id           int64
	LocalPath    string
	RemotePath   string
	Size, Inode  int64
	MountPount   string
	InodeRemote  string
	Btime, Mtime int64
	Type         FileType
	Owner        string
	SymlinkDest  string
	LastUpload   time.Time
	modifiable   bool
}

func (d *DB) AddSetFiles(set *Set, toAdd iter.Seq[*File]) error {
	if !set.modifiable {
		return ErrReadonlySet
	}

	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	for file := range toAdd {
		if err := d.addSetFile(tx, set.id, file); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (d *DB) addSetFile(tx *sql.Tx, setID int64, file *File) error {
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

	file.id, err = d.execReturningRowID(tx, createSetFile, file.LocalPath, setID, rfID)

	return err
}

func (d *DB) RemoveSetFiles(toRemove iter.Seq[*File]) error {
	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	for file := range toRemove {
		if !file.modifiable {
			return ErrReadonlySet
		}

		if err := d.trashFile(tx, file); err != nil {
			return err
		}

		if _, err := tx.Exec(createQueuedRemoval, file.id); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (d *DB) trashFile(tx *sql.Tx, file *File) error {
	trashID, err := d.execReturningRowID(tx, createTrashSetForFile, file.id)
	if err != nil {
		return err
	}

	if trashID == 0 {
		return nil
	}

	if _, err = tx.Exec(createTrashFile, trashID, file.id); err != nil {
		return err
	}

	_, err = tx.Exec(disableTrashFileRemoveTask, trashID)

	return err
}

func (d *DBRO) GetSetFiles(set *Set) *IterErr[*File] {
	scanner := scanFile

	if set.modifiable {
		scanner = scanModifiableFile
	}

	return iterRows(d, scanner, getSetsFiles, set.id)
}

func scanFile(scanner scanner) (*File, error) {
	file := new(File)

	if err := scanner.Scan(
		&file.id,
		&file.LocalPath,
		&file.RemotePath,
		&file.LastUpload,
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
	file, err := scanFile(scanner)
	if err != nil {
		return nil, err
	}

	file.modifiable = true

	return file, nil
}

func (d *DBRO) CountRemoteFileRefs(file *File) (int64, error) {
	var count int64

	err := d.db.QueryRow(getRemoteFileRefs, file.id).Scan(&count)

	return count, err
}
