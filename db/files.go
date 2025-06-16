package db

import "iter"

type File struct {
	id                    uint64
	LocalPath, RemotePath string
	Size                  int64
	Inode                 int64
	MountPount            string
	InodeRemote           string
	Btime, Mtime          int64
	Type                  uint8
	SymlinkDest           string
}

func (d *DB) SetSetFiles(set *Set, toAdd, toRemove iter.Seq[File]) error {
	for file := range toRemove {
		if err := d.removeSetFile(set.id, file); err != nil {
			return err
		}
	}

	for file := range toAdd {
		if err := d.addSetFile(set.id, file); err != nil {
			return err
		}
	}

	return nil
}

func (d *DB) addSetFile(setID int64, file File) error {
	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	res, err := tx.Exec(createHardlink, file.Inode, file.MountPount, file.InodeRemote,
		file.Btime, file.Mtime, file.Size, file.Type, file.SymlinkDest, file.Mtime, file.SymlinkDest)
	if err != nil {
		return err
	}

	hlID, err := res.LastInsertId()
	if err != nil {
		return err
	}

	if res, err = tx.Exec(createRemoteFile, file.RemotePath, hlID); err != nil {
		return err
	}

	rfID, err := res.LastInsertId()
	if err != nil {
		return err
	}

	if _, err = tx.Exec(createSetFile, file.LocalPath, setID, rfID); err != nil {
		return err
	}

	return tx.Commit()
}

func (d *DB) removeSetFile(setID int64, file File) error {
	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	if _, err = tx.Exec(deleteSetFile, setID, file.id); err != nil {
		return err
	}

	return tx.Commit()
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
