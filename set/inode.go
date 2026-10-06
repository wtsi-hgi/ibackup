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
	"bytes"
	"errors"
	"io/fs"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
	"syscall"

	"github.com/moby/sys/mountinfo"
	"github.com/ugorji/go/codec"
	"github.com/wtsi-hgi/ibackup/errs"
	"github.com/wtsi-hgi/ibackup/statter"
	bolt "go.etcd.io/bbolt"
)

const transformerInodeSeparator = ":"

const ErrElementNotInSlice = "element not in slice"

var errSetsBucketMissing = errors.New("sets bucket missing")

// setsWithPath appends to sets the IDs of sets not yet seen whose sub-bucket
// of the given kind has the given path.
func setsWithPath(b *bolt.Bucket, kind string, path []byte, seen map[string]struct{}, sets []string) []string {
	c := b.Cursor()
	p := []byte(kind)

	for k, _ := c.Seek(p); k != nil && bytes.HasPrefix(k, p); k, _ = c.Next() {
		sb := b.Bucket(k)
		if sb == nil || sb.Get(path) == nil {
			continue
		}

		_, setID, ok := strings.Cut(string(k), separator)
		if !ok {
			continue
		}

		if _, already := seen[setID]; already {
			continue
		}

		seen[setID] = struct{}{}
		sets = append(sets, setID)
	}

	return sets
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

// handleInode records the inode of the given Dirent in the database, and
// returns the path to the first local file with that inode if we've seen if
// before.
func (d *DB) handleInode(tx *bolt.Tx, de *Dirent, transformerID string) (string, error) {
	key := d.inodeMountPointKeyFromDirent(de)
	b := tx.Bucket([]byte(inodeBucket))
	transformerPath := transformerID + transformerInodeSeparator + de.Path

	v := b.Get(key)
	if v == nil {
		return "", b.Put(key, d.encodeToBytes([]string{transformerPath}))
	}

	existingFiles, allFiles := d.decodeIMPValue(v, de.Inode)
	if len(existingFiles) == 0 {
		return "", b.Put(key, d.encodeToBytes([]string{transformerPath}))
	}

	_, hardlinkDest, err := splitTransformerPath(getFirstNonBlankValue(allFiles))
	if err != nil {
		return "", err
	}

	isExistingPath, isOriginalPath := alreadyInFiles(transformerPath, allFiles)

	if isOriginalPath {
		return "", nil
	}

	if isExistingPath {
		return hardlinkDest, nil
	}

	return hardlinkDest, b.Put(key, d.encodeToBytes(append(allFiles, transformerPath)))
}

func getFirstNonBlankValue(arr []string) string {
	for _, str := range arr {
		if str != "" {
			return str
		}
	}

	return ""
}

// GetFilesFromInode returns all the paths that share the provided inode on the
// given mount point. It returns none if there's no record for the inode, which
// a file's entry can still have (see removeFileFromInode()).
func (d *DB) GetFilesFromInode(inode uint64, mountPoint string) ([]string, error) {
	de := &Dirent{Inode: inode, Path: mountPoint}
	key := d.inodeMountPointKeyFromDirent(de)

	var files []string

	err := d.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(inodeBucket))

		v := b.Get(key)
		if v == nil {
			return nil
		}

		_, files = d.decodeIMPValue(v, de.Inode)

		return nil
	})

	for i, file := range files {
		if file == "" {
			continue
		}

		_, files[i], err = splitTransformerPath(file)
		if err != nil {
			return nil, err
		}
	}

	return files, err
}

// RemoveFileFromInode removes entry for the given path from inode bucket if it
// is the last file with that inode. Otherwise just removes itself from the list
// (if the path is the original file 'removal' is setting it to be blank). If
// there's no record for the inode, there's nothing to remove.
func (d *DB) RemoveFileFromInode(path string, inode uint64) error {
	return d.db.Update(func(tx *bolt.Tx) error {
		return d.removeFileFromInode(tx, path, inode)
	})
}

// removeFileFromInode does RemoveFileFromInode()'s work in the given
// transaction. A file's entry can keep an inode that no longer has a record:
// once its local file is deleted, a new file reusing the inode replaces the
// record, which goes when that file is removed; and the record's key changes
// if the path's mount point does (eg. an automount mounted at server start).
func (d *DB) removeFileFromInode(tx *bolt.Tx, path string, inode uint64) error {
	de := newDirentFromPath(path)
	de.Inode = inode
	key := d.inodeMountPointKeyFromDirent(de)
	b := tx.Bucket([]byte(inodeBucket))

	v := b.Get(key)
	if v == nil {
		return nil
	}

	_, files := d.decodeIMPValue(v, de.Inode)

	return d.updateInodeEntryBasedOnFiles(b, key, path, files)
}

// removeInodeIfUnused removes the given removed file entry from our inode
// records, unless it isn't a file with an inode, or another set still has it.
// Entries whose local file is gone (missing or orphaned) have inode 0 and so
// have no inode record.
func (d *DB) removeInodeIfUnused(tx *bolt.Tx, entry *Entry) error {
	if entry.Type == Symlink || entry.Type == Abnormal || entry.Inode == 0 {
		return nil
	}

	setsWithFile, err := d.setsForFile(tx, entry.Path)
	if err != nil || len(setsWithFile) > 0 {
		return err
	}

	return d.removeFileFromInode(tx, entry.Path, entry.Inode)
}

// GetAllSetsForFile returns a slice of setIDs for sets that contain the given
// file.
func (d *DBRO) GetAllSetsForFile(path string) ([]string, error) {
	var sets []string

	err := d.db.View(func(tx *bolt.Tx) error {
		var err error

		sets, err = d.setsForFile(tx, path)

		return err
	})

	return sets, err
}

func isPathInTransformerPaths(path string, files []string) (bool, error) {
	for _, file := range files {
		if file == "" {
			continue
		}

		_, pathFromSplit, err := splitTransformerPath(file)
		if err != nil {
			return false, err
		}

		if pathFromSplit == path {
			return true, nil
		}
	}

	return false, nil
}

func (d *DBRO) setsForFile(tx *bolt.Tx, path string) ([]string, error) {
	b := tx.Bucket([]byte(setsBucket))
	if b == nil {
		return nil, errSetsBucketMissing
	}

	seen := make(map[string]struct{})
	sets := setsWithPath(b, fileBucket, []byte(path), seen, nil)

	return setsWithPath(b, discoveredBucket, []byte(path), seen, sets), nil
}

func (d *DB) updateInodeEntryBasedOnFiles(b *bolt.Bucket, key []byte, path string, files []string) error {
	isInFiles, err := isPathInTransformerPaths(path, files)
	if err != nil {
		return err
	}

	if !isInFiles {
		return nil
	}

	if len(files) == 1 || (len(files) == 2 && files[0] == "") {
		return b.Delete(key)
	}

	files, err = removePathFromInodeFiles(path, files)
	if err != nil {
		return err
	}

	return b.Put(key, d.encodeToBytes(files))
}

// removePathFromInodeFiles matches path against each file's path, not against
// files[0]'s transformer, since files[0] is blanked once the original is
// removed, and other sets' hardlinks may have other transformers.
func removePathFromInodeFiles(path string, files []string) ([]string, error) {
	index := slices.IndexFunc(files, func(file string) bool {
		_, filePath, err := splitTransformerPath(file)

		return err == nil && filePath == path
	})

	switch index {
	case -1:
		return nil, errs.PathError{Msg: ErrElementNotInSlice, Path: path}
	case 0:
		files[0] = ""

		return files, nil
	default:
		return slices.Delete(files, index, index+1), nil
	}
}

// RemoveElementFromSlice returns the given slice without the given element.
func RemoveElementFromSlice(slice []string, element string) ([]string, error) {
	index := slices.Index(slice, element)
	if index < 0 {
		return nil, errs.PathError{Msg: ErrElementNotInSlice, Path: element}
	}

	return slices.Delete(slice, index, index+1), nil
}

func splitTransformerPath(tp string) (string, string, error) {
	transformerID, hardlinkDest, ok := strings.Cut(tp, transformerInodeSeparator)
	if !ok {
		return "", "", &Error{Msg: ErrInvalidTransformerPath}
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
func (d *DB) inodeMountPointKeyFromDirent(de *Dirent) []byte {
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
// Before returning the slice of existingFiles, checks that at least one path
// still exists and has the given inode; if not, will return an empty slice.
// Also returns a slice of all decoded files.
func (d *DB) decodeIMPValue(v []byte, inode uint64) ([]string, []string) {
	dec := codec.NewDecoderBytes(v, d.ch)

	var files []string

	dec.MustDecode(&files)

	existingFiles := make([]string, 0, len(files))

	var found bool

	for _, file := range files {
		if !found {
			if valid := d.impFileIsValid(file, inode); !valid {
				continue
			}
		}

		found = true

		existingFiles = append(existingFiles, file)
	}

	return existingFiles, files
}

func (d *DB) impFileIsValid(file string, inode uint64) bool {
	_, path, err := splitTransformerPath(file)
	if err != nil {
		return false
	}

	fi, err := d.stat(path)
	if err != nil {
		return false
	}

	return fi.Sys().(*syscall.Stat_t).Ino == inode //nolint:errcheck,forcetypeassert
}

func (d *DB) stat(path string) (fs.FileInfo, error) {
	for range 3 {
		ino, err := statter.Stat(path)
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			return ino, err
		}
	}

	return nil, os.ErrDeadlineExceeded
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

	transformerPaths, _ := d.decodeIMPValue(v, e.Inode)

	if len(transformerPaths) == 0 {
		return nil
	}

	return transformerPaths
}

// HardlinkRemote gets the remote path of the first hardlink we uploaded that
// shares the given entry's inode and mountpoint.
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
		return "", &Error{Msg: ErrInvalidTransformerPath}
	}

	s := &Set{Transformer: string(v)}

	return s.TransformPath(path)
}
