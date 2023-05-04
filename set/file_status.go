package set

import (
	"os"
	"strconv"
	"strings"
	"syscall"

	"github.com/ugorji/go/codec"
	"github.com/wtsi-ssg/wrstat/v4/walk"
	bolt "go.etcd.io/bbolt"
)

// statAndUpdatePureFileEntry checks if the given path exists. If it exists and
// is a link the entry is updated to reflect if it's a hard or symlink.
func (d *DB) statAndUpdatePureFileEntry(tx *bolt.Tx, entry *Entry) error {
	info, err := os.Lstat(entry.Path)
	if err != nil {
		return err
	}

	statt, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return nil
	}

	de := &walk.Dirent{
		Path:  entry.Path,
		Type:  info.Mode().Type(),
		Inode: statt.Ino,
	}

	entry.Inode = statt.Ino

	return d.updateEntryWithTypeDetails(tx, de, entry)
}

func (d *DB) updateEntryWithTypeDetails(tx *bolt.Tx, de *walk.Dirent, entry *Entry) error {
	if de.IsDir() {
		entry.Type = Directory

		return nil
	}

	eType, err := d.direntToEntryType(tx, de)
	if err != nil {
		return err
	}

	entry.Type = eType
	entry.Dest = ""

	if eType == Symlink {
		entry.Dest, err = os.Readlink(de.Path)
	}

	return err
}

// direntToEntryType returns missing, symlink or hardlink status if the
// Dirent is one of those (default Pending).
func (d *DB) direntToEntryType(tx *bolt.Tx, de *walk.Dirent) (EntryType, error) {
	eType := Regular

	switch {
	case de.Inode == 0:
		eType = Unknown
	case de.IsSymlink():
		eType = Symlink
	default:
		isHardLink, err := d.handleInode(tx, de)
		if err != nil {
			return eType, err
		}

		if isHardLink {
			eType = Hardlink
		}
	}

	return eType, nil
}

// handleInode recordes the inode of the given Dirent in the database, and
// returns if it is a hardlink (we've seen the inode before).
func (d *DB) handleInode(tx *bolt.Tx, de *walk.Dirent) (bool, error) {
	found := false
	key := d.inodeMountPointKey(de)

	b := tx.Bucket([]byte(inodeBucket))

	var files []string

	if existing := b.Get(key); existing != nil {
		files = d.decodeIMPValue(existing)
		found = true

		for n, existing := range files {
			if de.Path == existing {
				found = n > 0

				break
			}
		}
	}

	files = append(files, de.Path)

	err := b.Put(key, d.encodeToBytes(files))

	return found, err
}

// inodeMountPointKey returns the inodeBucket key for the Dirent's inode and
// the Dirent's mount point for its path.
func (d *DB) inodeMountPointKey(de *walk.Dirent) []byte {
	return append(strconv.AppendUint([]byte{}, de.Inode, hexBase), d.getMountPointFromPath(de.Path)...)
}

// getMountPointFromPath determines the mount point for the given path based on
// the mount points available on the system when the server started. If nothing
// matches, returns /.
func (d *DB) getMountPointFromPath(path string) string {
	for _, mp := range d.mountList {
		if strings.HasPrefix(path, mp) {
			return mp
		}
	}

	return "/"
}

// decodeIMPValue takes a byte slice representation of an InodeMountPoint value
// (a []string) as stored in the db by AddInodeMountPoint(), and converts it
// back in to []string.
func (d *DB) decodeIMPValue(v []byte) []string {
	dec := codec.NewDecoderBytes(v, d.ch)

	var files []string

	dec.MustDecode(&files)

	return files
}
