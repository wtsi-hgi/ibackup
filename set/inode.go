/*******************************************************************************
 * Copyright (c) 2023 Genome Research Ltd.
 *
 * Authors: Michael Woolnough <mw31@sanger.ac.uk>
 *          Sendu Bala <sb10@sanger.ac.uk>
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

package set

import (
	"os"
	"sort"
	"strconv"
	"strings"
	"syscall"

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

const transformerInodeSeparator = ":"

// handleInode records the inode of the given Dirent in the database, and
// returns if it is a hardlink (we've seen the inode before).
func (d *DB) handleInode(tx *bolt.Tx, de *walk.Dirent, transformerID string) (string, error) {
	key := d.inodeMountPointKeyFromDirent(de)

	b := tx.Bucket([]byte(inodeBucket))

	transformerPath := transformerID + transformerInodeSeparator + de.Path

	v := b.Get(key)
	if v == nil {
		return "", b.Put(key, d.encodeToBytes([]string{transformerPath}))
	}

	files := d.decodeIMPValue(v, de.Inode)

	if len(files) == 0 {
		return "", b.Put(key, d.encodeToBytes([]string{transformerPath}))
	}

	_, hardlinkDest, err := splitTransformerPath(files[0])
	if err != nil {
		return "", err
	}

	isExistingPath, isOriginalPath := alreadyInFiles(transformerPath, files)

	if isOriginalPath {
		return "", nil
	}

	if isExistingPath {
		return hardlinkDest, nil
	}

	return hardlinkDest, b.Put(key, d.encodeToBytes(append(files, transformerPath)))
}

func splitTransformerPath(tp string) (string, string, error) {
	transformerID, hardlinkDest, ok := strings.Cut(tp, transformerInodeSeparator)
	if !ok {
		return "", "", &Error{msg: ErrInvalidTransformerPath}
	}

	return transformerID, hardlinkDest, nil
}

// alreadyInFiles checks if path is in existing and returns true if so.
// Additionally returns true if it's the first entry in files, meaning it's the
// original and not considered a hardlink.
func alreadyInFiles(path string, existing []string) (bool, bool) {
	if path == existing[0] {
		return true, true
	}

	for _, existing := range existing[1:] {
		if path == existing {
			return true, false
		}
	}

	return false, false
}

// inodeMountPointKeyFromDirent returns the inodeBucket key for the Dirent's
// inode and the Dirent's mount point for its path.
func (d *DB) inodeMountPointKeyFromDirent(de *walk.Dirent) []byte {
	return append(strconv.AppendUint([]byte{}, de.Inode, hexBase), d.GetMountPointFromPath(de.Path)...)
}

func (d *DB) inodeMountPointKeyFromEntry(e *Entry) []byte {
	return append(strconv.AppendUint([]byte{}, e.Inode, hexBase), d.GetMountPointFromPath(e.Path)...)
}

// GetMountPointFromPath determines the mount point for the given path based on
// the mount points available on the system when the server started. If nothing
// matches, returns /.
func (d *DB) GetMountPointFromPath(path string) string {
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
//
// Before returning the slice, checks that at least one path still exists and
// has the given inode; if not, will return an empty slice.
func (d *DB) decodeIMPValue(v []byte, inode uint64) []string {
	dec := codec.NewDecoderBytes(v, d.ch)

	var files []string

	dec.MustDecode(&files)

	existingFiles := make([]string, 0, len(files))

	var found bool

	for _, file := range files {
		if !found {
			if valid := impFileIsValid(file, inode); !valid {
				continue
			}
		}

		found = true

		existingFiles = append(existingFiles, file)
	}

	return existingFiles
}

func impFileIsValid(file string, inode uint64) bool {
	_, path, err := splitTransformerPath(file)
	if err != nil {
		return false
	}

	info, err := os.Lstat(path)
	if err != nil {
		return false
	}

	ino := info.Sys().(*syscall.Stat_t).Ino //nolint:forcetypeassert

	return ino == inode
}

// HardlinkPaths returns all known hardlink paths that share the same mountpoint
// and inode as the entry provided.
func (d *DB) HardlinkPaths(e *Entry) ([]string, error) {
	var transformerPaths []string

	if err := d.db.View(func(tx *bolt.Tx) error {
		transformerPaths = d.getTransformerPaths(tx, e)

		return nil
	}); err != nil {
		return nil, err
	}

	files := make([]string, 0, len(transformerPaths))

	for _, transformerPath := range transformerPaths {
		_, path, err := splitTransformerPath(transformerPath)
		if err != nil {
			return nil, err
		}

		if path == e.Path {
			continue
		}

		files = append(files, path)
	}

	return files, nil
}

func (d *DB) getTransformerPaths(tx *bolt.Tx, e *Entry) []string {
	ib := tx.Bucket([]byte(inodeBucket))

	key := d.inodeMountPointKeyFromEntry(e)

	v := ib.Get(key)
	if v == nil {
		return nil
	}

	transformerPaths := d.decodeIMPValue(v, e.Inode)

	if len(transformerPaths) == 0 {
		return nil
	}

	return transformerPaths
}

func (d *DB) HardlinkRemote(e *Entry) (string, error) {
	var remotePath string

	err := d.db.View(func(tx *bolt.Tx) error {
		transformerPaths := d.getTransformerPaths(tx, e)

		if len(transformerPaths) == 0 {
			return nil
		}

		transformerID, path, err := splitTransformerPath(transformerPaths[0])
		if err != nil {
			return err
		}

		remotePath, err = getRemotePath(tx, transformerID, path)

		return err
	})

	return remotePath, err
}

func getRemotePath(tx *bolt.Tx, transformerID, path string) (string, error) {
	tb := tx.Bucket([]byte(transformerFromIDBucket))

	v := tb.Get([]byte(transformerID))
	if v == nil {
		return "", &Error{msg: ErrInvalidTransformerPath}
	}

	s := &Set{Transformer: string(v)}

	t, err := s.MakeTransformer()
	if err != nil {
		return "", err
	}

	return t(path)
}
