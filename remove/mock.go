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

package remove

import (
	"os"
	"strings"

	"github.com/wtsi-hgi/ibackup/put"
)

type LocalHandler struct {
	put.LocalHandler
}

// GetLocalHandler returns a Handler that doesn't actually interact with iRODS,
// but instead simply treats "Remote" as local paths and copies from Local to
// Remote for any Put()s. For use during tests.
func GetLocalHandler() *LocalHandler {
	return &LocalHandler{
		LocalHandler: put.LocalHandler{
			Meta: make(map[string]map[string]string),
		},
	}
}

func (l *LocalHandler) RemoveDir(path string) error {
	return os.Remove(path)
}

func (l *LocalHandler) RemoveFile(path string) error {
	delete(l.Meta, path)

	return os.Remove(path)
}

func (l *LocalHandler) QueryMeta(dirToSearch string, meta map[string]string) ([]string, error) {
	var objects []string

	for path, pathMeta := range l.Meta {
		if !strings.HasPrefix(path, dirToSearch) {
			continue
		}

		if doesMetaContainMeta(pathMeta, meta) {
			objects = append(objects, path)
		}
	}

	return objects, nil
}

func doesMetaContainMeta(sourceMeta, targetMeta map[string]string) bool {
	valid := true

	for k, v := range targetMeta {
		if sourceMeta[k] != v {
			valid = false

			break
		}
	}

	return valid
}
