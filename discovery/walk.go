/*******************************************************************************
 * Copyright (c) 2025 Genome Research Ltd.
 *
 * Author: Michael Woolnough <mw31@sanger.ac.uk>
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

package discovery

import (
	"errors"
	"io/fs"

	"github.com/wtsi-hgi/ibackup/db"
	"github.com/wtsi-ssg/wrstat/v6/walk"
)

func walkDirectories(dirs []string, filter StateMachine[bool], monitorRemovals bool, statter *Statter) error {
	for _, dir := range dirs {
		err := walkDirectory(dir, filter, monitorRemovals, statter)
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
	}

	return nil
}

func walkDirectory(dir string, filter StateMachine[bool], monitorRemovals bool, statter *Statter) error {
	return walk.New(func(entry *walk.Dirent) error {
		path := entry.Bytes()

		if match := filter.Match(path); match == nil || !*match { //nolint:nestif
			if monitorRemovals {
				statter.PassFile(&db.File{LocalPath: toString(path), Status: db.StatusMissing})
			}
		} else {
			statter.StatFile(toString(path))
		}

		return nil
	}, true, false).Walk(dir, func(string, error) {})
}
