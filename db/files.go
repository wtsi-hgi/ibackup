package db

import (
	"database/sql"
	"iter"
)

type File struct {
	id                    int64
	LocalPath, RemotePath string
	Size                  int64
	Inode                 int64
	MountPount            string
	InodeRemote           string
	Btime, Mtime          int64
	Type                  uint8
	SymlinkDest           string
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

	return nil
}

func (d *DB) addSetFile(tx *sql.Tx, setID int64, file *File) error {
	hlID, err := d.execReturningRowID(tx, createHardlink, file.Inode, file.MountPount, file.MountPount,
		file.InodeRemote, file.Btime, file.Mtime, file.Size, file.Type, file.SymlinkDest,
		file.Mtime, file.SymlinkDest)
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
	_, err := tx.Exec(deleteSetFile, setID, file.id)

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
		&file.InodeRemote,
	); err != nil {
		return nil, err
	}

	return file, nil
}

func (d *DB) ClearSetFiles(set *Set) error {
	return d.exec(deleteSetFiles, set.id)
}
