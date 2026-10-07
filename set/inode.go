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
	"github.com/wtsi-hgi/ibackup/statter"
	bolt "go.etcd.io/bbolt"
)

const transformerInodeSeparator = ":"

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

// checkInodeFiles returns an error if any of the given files from an inode
// record, other than a blank (removed) original, isn't a valid transformer path.
func checkInodeFiles(files []string) error {
	for _, file := range files {
		if file == "" {
			continue
		}

		if _, _, err := splitTransformerPath(file); err != nil {
			return err
		}
	}

	return nil
}

// inodeFileHasPath returns true if the given file from an inode record is for
// the given path, whatever transformer added it. A blank (removed) original
// never is.
func inodeFileHasPath(file, path string) bool {
	_, filePath, err := splitTransformerPath(file)

	return err == nil && filePath == path
}

// removeFromInodeFiles returns files without those that remove returns true
// for, given their index, blanking the original (files[0]) instead of
// removing it.
func removeFromInodeFiles(files []string, remove func(int, string) bool) []string {
	kept := make([]string, 0, len(files))

	for i, file := range files {
		switch {
		case !remove(i, file):
			kept = append(kept, file)
		case i == 0:
			kept = append(kept, "")
		}
	}

	return kept
}

// entryHardlinkDest returns the given entry's Dest if it's a hardlink with the
// given inode, otherwise blank.
func entryHardlinkDest(entry *Entry, inode uint64) string {
	if entry.Type == Hardlink && entry.Inode == inode {
		return entry.Dest
	}

	return ""
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
//
// The first file (the original) is uploaded as a regular file, so it isn't a
// hardlink of itself when a set with another transformer adds it. Each other
// file is recorded once per transformer, since each is a separate remote object
// that points to the one remote inode file, and removal counts them to know
// when that file is unused (see removeInodeIfUnused()).
//
// Once the original is removed, hardlinks keep its path (see hardlinkDest()).
// If no set's entry knows it, the given file becomes the original, uploaded as
// a regular file. Re-adding the removed original's own path therefore makes it
// a hardlink of itself, reusing its existing remote inode file rather than
// uploading its data again.
//
// The given stored entry is the discovering set's existing encoded entry for
// the Dirent's path, if any. It's only trusted if the record already holds this
// file, since otherwise it may be for an earlier file that had the same inode.
func (d *DB) handleInode(tx *bolt.Tx, de *Dirent, transformerID string, stored []byte) (string, error) {
	key := d.inodeMountPointKeyFromDirent(de)
	b := tx.Bucket([]byte(inodeBucket))
	transformerPath := transformerID + transformerInodeSeparator + de.Path

	allFiles := d.currentInodeFiles(b.Get(key), de.Inode)
	if allFiles == nil {
		return "", b.Put(key, d.encodeToBytes([]string{transformerPath}))
	}

	if inodeFileHasPath(allFiles[0], de.Path) {
		return "", nil
	}

	recorded := slices.Contains(allFiles[1:], transformerPath)
	if !recorded {
		stored = nil
	}

	hardlinkDest, err := d.hardlinkDest(tx, allFiles, de.Inode, stored)
	if err != nil {
		return "", err
	}

	return hardlinkDest, d.putInodeFile(b, key, allFiles, de.Path, transformerPath, hardlinkDest, recorded)
}

// currentInodeFiles returns the files of the given encoded inode record for the
// given inode, or nil if there's no record or none of its files still have the
// inode locally (eg. the inode was reused by a new file).
func (d *DB) currentInodeFiles(v []byte, inode uint64) []string {
	if v == nil {
		return nil
	}

	existingFiles, allFiles := d.decodeIMPValue(v, inode)
	if len(existingFiles) == 0 {
		return nil
	}

	return allFiles
}

// putInodeFile adds the given transformer path, for the given path, to the
// given files of the inode record with the given key, as a hardlink of the
// given original path unless the files are already recorded to have it, or as
// the original if that's blank. A new original replaces every other file for
// its path, whatever transformer added it, since it's uploaded as a regular
// file for all of them.
func (d *DB) putInodeFile(b *bolt.Bucket, key []byte, files []string, path, transformerPath, original string,
	recorded bool,
) error {
	if original == "" {
		others := slices.DeleteFunc(files[1:], func(file string) bool { return inodeFileHasPath(file, path) })

		return b.Put(key, d.encodeToBytes(append([]string{transformerPath}, others...)))
	}

	if recorded {
		return nil
	}

	return b.Put(key, d.encodeToBytes(append(files, transformerPath)))
}

// hardlinkDest returns the path of the original (first) file of the given
// inode record files, which hardlinks store their data under (see
// Entry.InodeStoragePath()). Once the original is removed its slot is blank, so
// the path is taken from a stored hardlink entry with the given inode instead:
// the links then keep using the original's existing remote inode file, rather
// than being uploaded again to a new one. Returns blank if no entry has it, eg.
// for a stale record whose files are in no set.
//
// The given stored entry (the discovering set's own encoded entry for the file
// being discovered, or nil) is checked first. Only if that doesn't have the path
// are the sets holding each of the other files' paths scanned, costing time
// linear in the number of sets times the number of hardlinks whose original was
// removed.
func (d *DB) hardlinkDest(tx *bolt.Tx, files []string, inode uint64, stored []byte) (string, error) {
	if files[0] != "" {
		_, dest, err := splitTransformerPath(files[0])

		return dest, err
	}

	if stored != nil {
		if dest := entryHardlinkDest(d.decodeEntry(stored), inode); dest != "" {
			return dest, nil
		}
	}

	return d.scannedHardlinkDest(tx, files[1:], inode)
}

// scannedHardlinkDest returns the Dest of the first stored entry, in any set,
// for one of the given inode record files' paths that is a hardlink with the
// given inode, or blank if there isn't one.
func (d *DB) scannedHardlinkDest(tx *bolt.Tx, files []string, inode uint64) (string, error) {
	for _, file := range files {
		_, path, err := splitTransformerPath(file)
		if err != nil {
			return "", err
		}

		dest, err := d.storedHardlinkDest(tx, path, inode)
		if err != nil || dest != "" {
			return dest, err
		}
	}

	return "", nil
}

// storedHardlinkDest returns the Dest of the first set's stored entry for the
// given path that is a hardlink with the given inode, or blank if there isn't
// one.
func (d *DB) storedHardlinkDest(tx *bolt.Tx, path string, inode uint64) (string, error) {
	setIDs, err := d.setsForFile(tx, path)
	if err != nil {
		return "", err
	}

	for _, setID := range setIDs {
		entry, _, err := d.getEntry(tx, setID, path)
		if err != nil {
			return "", err
		}

		if dest := entryHardlinkDest(entry, inode); dest != "" {
			return dest, nil
		}
	}

	return "", nil
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

// RemoveFileFromInode removes every entry for the given path, whatever
// transformer added it, from the inode bucket (if the path is the original file
// 'removal' is setting it to be blank), and removes the inode's record once it
// has no other files. If there's no record for the inode, there's nothing to
// remove.
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
	return d.updateInodeFiles(tx, path, inode, func(files []string) []string {
		return removeFromInodeFiles(files, func(_ int, file string) bool {
			return inodeFileHasPath(file, path)
		})
	})
}

// updateInodeFiles replaces the files of the record for the given path's inode
// with the result of update, deleting the record if no files remain.
func (d *DB) updateInodeFiles(tx *bolt.Tx, path string, inode uint64, update func([]string) []string) error {
	de := newDirentFromPath(path)
	de.Inode = inode
	key := d.inodeMountPointKeyFromDirent(de)
	b := tx.Bucket([]byte(inodeBucket))

	v := b.Get(key)
	if v == nil {
		return nil
	}

	_, files := d.decodeIMPValue(v, de.Inode)

	if err := checkInodeFiles(files); err != nil {
		return err
	}

	kept := update(files)
	if slices.Equal(kept, files) {
		return nil
	}

	if !slices.ContainsFunc(kept, func(file string) bool { return file != "" }) {
		return b.Delete(key)
	}

	return b.Put(key, d.encodeToBytes(kept))
}

// removeInodeIfUnused removes the given removed file entry, of a set with the
// given transformer, from our inode records, unless it isn't a file with an
// inode, or another set still uses it. Entries whose local file is gone
// (missing or orphaned) have inode 0 and so have no inode record.
//
// The records' hardlink entries are a count of the remote objects that point to
// the remote inode file (see handleInode()), so a hardlink entry goes once no
// set with its transformer has its path, even while sets with other
// transformers do; otherwise removing their remote objects later would never
// see the inode file as unused. The original, uploaded as a regular file by
// sets with any transformer, stays until no set has it.
func (d *DB) removeInodeIfUnused(tx *bolt.Tx, entry *Entry, transformer string) error {
	if entry.Type == Symlink || entry.Type == Abnormal || entry.Inode == 0 {
		return nil
	}

	setsWithFile, err := d.setsForFile(tx, entry.Path)
	if err != nil {
		return err
	}

	if len(setsWithFile) == 0 {
		return d.removeFileFromInode(tx, entry.Path, entry.Inode)
	}

	return d.removeHardlinkFromInodeIfUnused(tx, entry, transformer, setsWithFile)
}

// removeHardlinkFromInodeIfUnused removes the given entry's hardlink entry for
// the given transformer from its inode's record, leaving the original as is,
// unless one of the given sets, that still have the entry's path, has that
// transformer.
func (d *DB) removeHardlinkFromInodeIfUnused(tx *bolt.Tx, entry *Entry, transformer string,
	setsWithFile []string,
) error {
	used, err := d.anySetHasTransformer(tx, setsWithFile, transformer)
	if err != nil || used {
		return err
	}

	transformerID := tx.Bucket([]byte(transformerToIDBucket)).Get([]byte(transformer))
	if transformerID == nil {
		return nil
	}

	transformerPath := string(transformerID) + transformerInodeSeparator + entry.Path

	return d.updateInodeFiles(tx, entry.Path, entry.Inode, func(files []string) []string {
		return removeFromInodeFiles(files, func(i int, file string) bool {
			return i > 0 && file == transformerPath
		})
	})
}

// anySetHasTransformer returns true if any of the given sets has the given
// transformer.
func (d *DB) anySetHasTransformer(tx *bolt.Tx, setIDs []string, transformer string) (bool, error) {
	for _, setID := range setIDs {
		s, _, _, err := d.getSetByID(tx, setID)
		if err != nil {
			return false, err
		}

		if s.Transformer == transformer {
			return true, nil
		}
	}

	return false, nil
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

func (d *DBRO) setsForFile(tx *bolt.Tx, path string) ([]string, error) {
	b := tx.Bucket([]byte(setsBucket))
	if b == nil {
		return nil, errSetsBucketMissing
	}

	seen := make(map[string]struct{})
	sets := setsWithPath(b, fileBucket, []byte(path), seen, nil)

	return setsWithPath(b, discoveredBucket, []byte(path), seen, sets), nil
}

func splitTransformerPath(tp string) (string, string, error) {
	transformerID, hardlinkDest, ok := strings.Cut(tp, transformerInodeSeparator)
	if !ok {
		return "", "", &Error{Msg: ErrInvalidTransformerPath}
	}

	return transformerID, hardlinkDest, nil
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
