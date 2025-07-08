package db

import (
	"database/sql"
	"iter"
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
	SymlinkDest  string
}

func (d *DB) SetSetFiles(set *Set, toAdd, toRemove iter.Seq[*File]) error {
	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	for file := range toRemove {
		if err := d.removeSetFile(tx, set.id, file); err != nil {
			return err
		}
	}

	for file := range toAdd {
		if err := d.addSetFile(tx, set.id, file); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (d *DB) addSetFile(tx *sql.Tx, setID int64, file *File) error {
	hlID, err := d.execReturningRowID(tx, createHardlink, file.Inode, file.MountPount,
		file.Btime, file.InodeRemote, file.Mtime, file.Size, file.Type, file.SymlinkDest,
		file.InodeRemote, file.Mtime, file.SymlinkDest, file.Size)
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

func (d *DB) removeSetFile(tx *sql.Tx, setID int64, file *File) error {
	_, err := tx.Exec(deleteSetFile, file.id)

	return err
}

func (d *DBRO) GetSetFiles(set *Set) *IterErr[*File] {
	return iterRows(d, scanFile, getSetsFiles, set.id)
}

func scanFile(scanner scanner) (*File, error) {
	file := new(File)

	if err := scanner.Scan(
		&file.id,
		&file.LocalPath,
		&file.RemotePath,
		&file.Size,
		&file.Type,
		&file.SymlinkDest,
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

func (d *DB) ClearSetFiles(set *Set) error {
	return d.exec(deleteSetFiles, set.id)
}
