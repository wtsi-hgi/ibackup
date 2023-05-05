package set

import (
	"sort"
	"strconv"
	"strings"

	"github.com/moby/sys/mountinfo"
	"github.com/ugorji/go/codec"
	"github.com/wtsi-ssg/wrstat/v4/walk"
	bolt "go.etcd.io/bbolt"
)

// getMountPoints retrieves a list of mount point paths to be used when
// determining hardlinks. The list is sorted longest first and stored on the
// server object.
func (d *DB) getMountPoints() error {
	mounts, err := mountinfo.GetMounts(func(info *mountinfo.Info) (bool, bool) {
		switch info.FSType {
		case "devpts", "devtmpfs", "cgroup", "rpc_pipefs", "fusectl",
			"binfmt_misc", "sysfs", "debugfs", "tracefs", "proc", "securityfs",
			"pstore", "mqueue", "hugetlbfs", "configfs":
			return true, false
		}

		return false, false
	})
	if err != nil {
		return err
	}

	d.mountList = make([]string, len(mounts))

	for n, mp := range mounts {
		d.mountList[n] = mp.Mountpoint
	}

	sort.Slice(d.mountList, func(i, j int) bool {
		return len(d.mountList[i]) > len(d.mountList[j])
	})

	return nil
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
