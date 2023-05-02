package set

import (
	"os"
	"sort"
	"strings"
	"syscall"

	"github.com/moby/sys/mountinfo"
	"github.com/wtsi-ssg/wrstat/v4/walk"
	bolt "go.etcd.io/bbolt"
)

// updateNonRegularEntries checks if the given path exists, and if not returns
// true and updates the corresponding entry for that path in the given set with
// a missing status. Symlinks are treated as if they are missing.
func (d *DB) updateNonRegularEntries(tx *bolt.Tx, path string) (EntryStatus, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return Missing, err
	}

	statt, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return Pending, nil
	}

	de := &walk.Dirent{
		Path:  path,
		Type:  info.Mode().Type(),
		Inode: statt.Ino,
	}

	return d.entryTypeToRequestStatus(tx, de)
}

func (d *DB) getEntryStatus(tx *bolt.Tx, de *walk.Dirent) (EntryStatus, error) {
	if de.IsDir() {
		return Pending, nil
	}

	return d.entryTypeToRequestStatus(tx, de)
}

// entryTypeToRequestStatus returns missing, symlink or hardlink status if the
// Dirent is one of those. Returns blank string if not.
func (d *DB) entryTypeToRequestStatus(tx *bolt.Tx, de *walk.Dirent) (EntryStatus, error) {
	var status EntryStatus

	switch {
	case de.Inode == 0:
		status = Missing
	case de.IsSymlink():
		status = SymLink
	default:
		isHardLink, err := d.addInodeMountPoint(tx, de.Path, de.Inode, d.getMountPointFromPath(de.Path))
		if err != nil {
			return status, err
		}

		if isHardLink {
			status = HardLink
		}
	}

	return status, nil
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
