/*******************************************************************************
 * Copyright (c) 2026 Genome Research Ltd.
 *
 * Author: Sendu Bala <sb10@sanger.ac.uk>
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

package testutil

import (
	"fmt"
	"path/filepath"
	"strings"
)

// irodsCollectionCommands runs the iCommands that resetIRODSCollection needs.
type irodsCollectionCommands interface {
	ILS(args ...string) ([]byte, error)
	IRM(args ...string) ([]byte, error)
	IMKDIR(args ...string) ([]byte, error)
}

func resetIRODSCollection(icmd irodsCollectionCommands, collection string) error {
	out, err := icmd.ILS(collection)
	if err != nil {
		return recreateIRODSCollection(icmd, collection)
	}

	entries := irodsCollectionEntries(collection, string(out))
	if len(entries) == 0 {
		return nil
	}

	if out, err := icmd.IRM(append([]string{"-rf"}, entries...)...); err != nil {
		return fmt.Errorf("irm -rf failed: %w; output: %s", err, string(out))
	}

	return nil
}

// irodsCollectionEntries returns the paths of the data objects and
// subcollections in the given `ils` output for the collection.
func irodsCollectionEntries(collection, ilsOutput string) []string {
	var entries []string

	for line := range strings.Lines(ilsOutput) {
		entry, isEntry := strings.CutPrefix(strings.TrimRight(line, "\n"), "  ")
		if !isEntry {
			continue
		}

		if subCollection, isColl := strings.CutPrefix(entry, "C- "); isColl {
			entries = append(entries, subCollection)

			continue
		}

		entries = append(entries, filepath.Join(collection, entry))
	}

	return entries
}

func recreateIRODSCollection(icmd irodsCollectionCommands, collection string) error {
	if out, err := icmd.IRM("-rf", collection); err != nil {
		return fmt.Errorf("irm -rf failed: %w; output: %s", err, string(out))
	}

	if out, err := icmd.IMKDIR("-p", collection); err != nil {
		return fmt.Errorf("imkdir -p failed: %w; output: %s", err, string(out))
	}

	return nil
}

// ResetCollection empties the given collection. It removes the collection's
// contents rather than recreating it, because creating a collection takes
// several seconds, but recreates it if it can't be listed.
func (cmd *ICommander) ResetCollection(collection string) error {
	return resetIRODSCollection(cmd, collection)
}
