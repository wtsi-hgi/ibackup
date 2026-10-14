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
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"
)

// staleCollectionAge is how old a test collection must be before a test process
// removes it as left behind by a killed or timed-out run, whose t.Cleanup never
// ran. It is far beyond the longest test run, so no collection still in use by
// a concurrent run is removed.
const staleCollectionAge = 24 * time.Hour

const iquestNoRows = "CAT_NO_ROWS_FOUND"

var (
	sweepOnce sync.Once //nolint:gochecknoglobals

	errBadIquestLine    = errors.New("unexpected iquest output line")
	errQuoteInIRODSPath = errors.New("iRODS path contains a quote")
)

// testCollection is a collection and when it was created.
type testCollection struct {
	path    string
	created time.Time
}

// sweepStaleCollections removes the collections listed in base that are named
// like test collections and were created more than staleCollectionAge before
// now.
func sweepStaleCollections(
	base string,
	list func(string) ([]testCollection, error),
	remove func(string),
	now time.Time,
) error {
	collections, err := list(base)
	if err != nil {
		return err
	}

	var stale []string

	for _, c := range collections {
		if strings.HasPrefix(path.Base(c.path), collectionPrefix) && now.Sub(c.created) > staleCollectionAge {
			stale = append(stale, c.path)
		}
	}

	removeCollections(remove, stale)

	return nil
}

// listIRODSSubcollections returns the collections directly in base, with their
// creation times from the iCAT.
func listIRODSSubcollections(base string) ([]testCollection, error) {
	if strings.Contains(base, "'") {
		return nil, fmt.Errorf("%w: %s", errQuoteInIRODSPath, base)
	}

	ctx, cancel := context.WithTimeout(context.Background(), irodsCommandTimeout)
	defer cancel()

	query := fmt.Sprintf("select COLL_CREATE_TIME, COLL_NAME where COLL_PARENT_NAME = '%s'", base)

	out, err := exec.CommandContext(ctx, "iquest", "--no-page", "%s %s", query).CombinedOutput() //nolint:gosec

	// For no rows, iquest prints iquestNoRows and exits 0 before iRODS 4.3, but
	// exits 1 from 4.3.
	if err != nil && !strings.HasPrefix(string(out), iquestNoRows) {
		return nil, fmt.Errorf("iquest failed: %w: %s", err, strings.TrimSpace(string(out)))
	}

	return parseIquestCollections(string(out))
}

func parseIquestCollections(out string) ([]testCollection, error) {
	if strings.HasPrefix(out, iquestNoRows) {
		return nil, nil
	}

	var collections []testCollection

	for line := range strings.Lines(out) {
		line = strings.TrimRight(line, "\n")

		created, name, ok := strings.Cut(line, " ")

		seconds, err := strconv.ParseInt(created, 10, 64)
		if !ok || err != nil {
			return nil, fmt.Errorf("%w: %q", errBadIquestLine, line)
		}

		collections = append(collections, testCollection{path: name, created: time.Unix(seconds, 0)})
	}

	return collections, nil
}

// sweepStaleCollectionsOnce removes, the first time it is called in this
// process, the test collections directly in base that are older than
// staleCollectionAge. Failures are logged, since they should not fail tests.
func sweepStaleCollectionsOnce(base string) {
	sweepOnce.Do(func() {
		err := sweepStaleCollections(base, listIRODSSubcollections, removeIRODSCollection, time.Now())
		if err != nil {
			slog.Warn("failed to remove stale iRODS test collections", "base", base, "err", err)
		}
	})
}
