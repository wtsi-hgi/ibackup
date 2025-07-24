/*******************************************************************************
 * Copyright (c) 2025 Genome Research Ltd.
 *
 * Author: Rosie Kern <rk18@sanger.ac.uk>
 * Author: Iaroslav Popov <ip13@sanger.ac.uk>
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

// package remove is used to remove from remote storage.

package remove

import (
	"errors"
	"maps"
	"path/filepath"
	"strings"

	"github.com/wtsi-hgi/ibackup/errs"
	"github.com/wtsi-hgi/ibackup/transfer"
)

type Handler interface {
	// RemoveMeta deletes the given metadata from the given object.
	RemoveMeta(path string, meta map[string]string) error

	// AddMeta adds the given metadata to the given object. Given metadata keys
	// should already have been removed with RemoveMeta() from the remote
	// object.
	AddMeta(path string, meta map[string]string) error

	// GetMeta returns the meta for a given remote path.
	GetMeta(path string) (map[string]string, error)

	// RemoveDir deletes a given empty folder. If folder is not empty, returns
	// error of type DirNotEmptyError.
	RemoveDir(path string) error

	// RemoveFile deletes a given file. If file is not found in remote storage,
	// returns an error containing substring "file does not exist".
	RemoveFile(path string) error

	// QueryMeta return paths to all objects with given metadata inside the
	// provided scope.
	QueryMeta(dirToSearch string, meta map[string]string) ([]string, error)

	// Cleanup stops any connections created earlier and does any other cleanup
	// needed.
	Cleanup()
}

// UpdateSetsAndRequestersOnRemoteFile updates the given file's metadata in
// remote storage for the path with the given sets and requesters.
func UpdateSetsAndRequestersOnRemoteFile(handler Handler, path string,
	sets, requesters []string, meta map[string]string) error {
	metaToRemove := map[string]string{
		transfer.MetaKeySets:      meta[transfer.MetaKeySets],
		transfer.MetaKeyRequester: meta[transfer.MetaKeyRequester],
	}

	newMeta := map[string]string{
		transfer.MetaKeySets:      strings.Join(sets, ","),
		transfer.MetaKeyRequester: strings.Join(requesters, ","),
	}

	if maps.Equal(metaToRemove, newMeta) {
		return nil
	}

	err := handler.RemoveMeta(path, metaToRemove)
	if err != nil {
		return err
	}

	return handler.AddMeta(path, newMeta)
}

// RemoveFileAndParentFoldersIfEmpty removes the provided path from iRODS and
// any now empty parent folders.
func RemoveFileAndParentFoldersIfEmpty(handler Handler, path string) error { //nolint:revive
	err := handler.RemoveFile(path)
	if err != nil {
		return err
	}

	return removeEmptyFoldersRecursively(handler, filepath.Dir(path))
}

func removeEmptyFoldersRecursively(handler Handler, path string) error {
	err := handler.RemoveDir(path)
	if err != nil {
		var dirNotEmpty errs.DirNotEmptyError
		if errors.As(err, &dirNotEmpty) {
			return nil
		}

		return errs.NewDirError(err.Error(), path)
	}

	return removeEmptyFoldersRecursively(handler, filepath.Dir(path))
}

// RemoveRemoteDir removes the remote path of a given directory from the remote
// storage.
func RemoveRemoteDir(handler Handler, path string, transformer transfer.PathTransformer) error { //nolint:revive
	rpath, err := transformer(path)
	if err != nil {
		return err
	}

	return handler.RemoveDir(rpath)
}

// FindHardlinksWithInode returns paths to all hardlinks that point to the
// provided inode path.
func FindHardlinksWithInode(rInodePath string, transformer transfer.PathTransformer,
	handler Handler) ([]string, error) {
	dir, err := transformer("/")
	if err != nil {
		return nil, err
	}

	return handler.QueryMeta(dir, map[string]string{transfer.MetaKeyRemoteHardlink: rInodePath})
}
