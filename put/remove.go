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

package put

import (
	"reflect"
	"strings"
)

// RemovePathFromSetInIRODS removes the given path from iRODS if the path is not
// associated with any other sets. Otherwise it updates the iRODS metadata for
// the path to not include the given set.
func RemovePathFromSetInIRODS(handler Handler, transformer PathTransformer, path string,
	sets, requesters []string, meta map[string]string) error {
	if len(sets) == 0 {
		return handleHardlinkAndRemoveFromIRODS(handler, path, transformer, meta)
	}

	metaToRemove := map[string]string{
		MetaKeySets:      meta[MetaKeySets],
		MetaKeyRequester: meta[MetaKeyRequester],
	}

	newMeta := map[string]string{
		MetaKeySets:      strings.Join(sets, ","),
		MetaKeyRequester: strings.Join(requesters, ","),
	}

	if reflect.DeepEqual(metaToRemove, newMeta) {
		return nil
	}

	err := handler.RemoveMeta(path, metaToRemove)
	if err != nil {
		return err
	}

	return handler.AddMeta(path, newMeta)
}

// handleHardLinkAndRemoveFromIRODS removes the given path from iRODS. If the
// path is found to be a hardlink, it checks if there are other hardlinks to the
// same file, if not, it removes the file.
func handleHardlinkAndRemoveFromIRODS(handler Handler, path string, transformer PathTransformer,
	meta map[string]string) error {
	err := handler.removeFile(path)
	if err != nil {
		return err
	}

	if meta[MetaKeyHardlink] == "" {
		return nil
	}

	dirToSearch, err := transformer("/")
	if err != nil {
		return err
	}

	items, err := handler.queryMeta(dirToSearch, map[string]string{MetaKeyRemoteHardlink: meta[MetaKeyRemoteHardlink]})
	if err != nil {
		return err
	}

	if len(items) != 0 {
		return nil
	}

	return handler.removeFile(meta[MetaKeyRemoteHardlink])
}

// RemoveDirFromIRODS removes the remote path of a given directory from iRODS.
func RemoveDirFromIRODS(handler Handler, path string, transformer PathTransformer) error {
	rpath, err := transformer(path)
	if err != nil {
		return err
	}

	err = handler.RemoveDir(rpath)
	if err != nil {
		return err
	}

	return nil
}
