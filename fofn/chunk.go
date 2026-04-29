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

package fofn

import (
	"bufio"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"github.com/wtsi-hgi/ibackup/transfer"
	"github.com/wtsi-hgi/ibackup/transformer"
	"vimagination.zapto.org/byteio"
)

const (
	chunkNameFormat  = "chunk.%06d"
	unmodified       = "unmodified"
	unmodifiedReport = unmodified + ".report"
)

// TargetChunks is the ideal number of chunks to split a fofn into.
const TargetChunks = 100

var (
	ErrMinChunkTooSmall = errors.New("minChunk must be >= 1")
	ErrMaxChunkTooSmall = errors.New("maxChunk must be >= 1")
	ErrMinExceedsMax    = errors.New("minChunk must be <= maxChunk")
)

// chunkDeck implements a shuffle-bag approach for distributing entries evenly
// across chunks. It maintains a shuffled deck of chunk indices [0..n-1]; when
// the deck is exhausted it refills and reshuffles.
type chunkDeck struct {
	rng  *rand.Rand
	n    int
	deck []int
	pos  int
}

func newChunkDeck(n int, rng *rand.Rand) *chunkDeck {
	deck := make([]int, n)
	for i := range n {
		deck[i] = i
	}

	d := &chunkDeck{
		rng:  rng,
		n:    n,
		deck: deck,
		pos:  n, // force refill on first call
	}

	return d
}

func (d *chunkDeck) next() int {
	if d.pos >= d.n {
		d.rng.Shuffle(d.n, func(i, j int) {
			d.deck[i], d.deck[j] = d.deck[j], d.deck[i]
		})

		d.pos = 0
	}

	idx := d.deck[d.pos]
	d.pos++

	return idx
}

type bufWriter struct {
	path string
	*bufio.Writer
	io.Closer
}

func newBufWriter(path string) (*bufWriter, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	return &bufWriter{
		path:   path,
		Writer: bufio.NewWriter(f),
		Closer: f,
	}, nil
}

func (b *bufWriter) Close() error {
	if b == nil {
		return nil
	}

	return errors.Join(b.Flush(), b.Closer.Close())
}

type chunkWriters struct {
	unchangeWriter *bufWriter
	chunks         []*bufWriter
	deck           *chunkDeck
}

func newChunkWriters(dir string, numChunks int, hasUnchanged bool, randSeed int64) (*chunkWriters, error) {
	c := &chunkWriters{
		chunks: make([]*bufWriter, numChunks),
	}

	for i := range numChunks {
		w, err := newBufWriter(filepath.Join(dir, fmt.Sprintf(chunkNameFormat, i)))
		if err != nil {
			c.Close()

			return nil, fmt.Errorf("failed to create chunk file: %w", err)
		}

		c.chunks[i] = w
	}

	if hasUnchanged {
		w, err := newBufWriter(filepath.Join(dir, unmodifiedReport))
		if err != nil {
			return nil, fmt.Errorf("failed to create unchanged report file: %w", err)
		}

		c.unchangeWriter = w
	}

	c.deck = newChunkDeck(numChunks, rand.New(rand.NewSource(randSeed))) //nolint:gosec

	return c, nil
}

func (c *chunkWriters) writeChunkPath(local, tx string) error {
	remote, err := transformer.Transform(tx, local)
	if err != nil {
		return err
	}

	local64 := base64.StdEncoding.EncodeToString([]byte(local))
	remote64 := base64.StdEncoding.EncodeToString([]byte(remote))

	_, err = fmt.Fprintf(c.chunks[c.deck.next()], "%s\t%s\n", local64, remote64)

	return err
}

func (c *chunkWriters) writeUnchangedPath(local, tx string) error {
	remote, err := transformer.Transform(tx, local)
	if err != nil {
		return err
	}

	_, err = fmt.Fprintln(c.unchangeWriter, formatReportLine(ReportEntry{
		Local:  local,
		Remote: remote,
		Status: transfer.RequestStatusUnmodified,
	}))

	return err
}

func (c *chunkWriters) paths() []string {
	paths := make([]string, len(c.chunks))

	for n, p := range c.chunks {
		paths[n] = p.path
	}

	return paths
}

func (c *chunkWriters) Close() error {
	err := c.unchangeWriter.Close()

	for _, chunk := range c.chunks {
		err = errors.Join(err, chunk.Close())
	}

	return err
}

// writeShuffledChunks reads a null-terminated fofn file and writes the entries
// into shuffled chunk files. Each entry is transformed using the provided
// function and written as a base64-encoded local/remote pair separated by a
// tab.
//
// The function uses a two-pass approach: first counting entries via
// scanner.CountNullTerminated, then streaming them into randomly assigned chunk
// files. The random assignment is deterministic for a given randSeed. If
// randSeed is 0, time.Now().UnixNano() is used for non-deterministic shuffling.
//
// Chunk files are named chunk.000000, chunk.000001, etc. Returns the paths of
// the created chunk files, or nil if the fofn is empty.
func writeShuffledChunks(
	subDir subDir, transformer, dir string,
	minChunk, maxChunk int,
	randSeed, lastRunTime int64,
) ([]string, error) {
	count, hasUnchanged, err := countEntries(subDir, lastRunTime)
	if err != nil {
		return nil, err
	}

	if count == 0 {
		return nil, nil
	}

	numChunks := calculateChunks(count, minChunk, maxChunk)

	if randSeed == 0 {
		randSeed = time.Now().UnixNano()
	}

	return streamToChunks(subDir, transformer, dir, numChunks, randSeed, lastRunTime, hasUnchanged)
}

func validateChunkBounds(minChunk, maxChunk int) error {
	if minChunk < 1 {
		return fmt.Errorf("%w: got %d", ErrMinChunkTooSmall, minChunk)
	}

	if maxChunk < 1 {
		return fmt.Errorf("%w: got %d", ErrMaxChunkTooSmall, maxChunk)
	}

	if minChunk > maxChunk {
		return fmt.Errorf("%w: %d > %d", ErrMinExceedsMax, minChunk, maxChunk)
	}

	return nil
}

func countEntries(subDir subDir, lastRunTime int64) (count int, hasUnchanged bool, err error) { //nolint:gocognit,gocyclo,lll
	f, err := os.Open(subDir.FOFNPath())
	if err != nil {
		return 0, false, err
	}
	defer f.Close()

	lr := byteio.StickyLittleEndianReader{Reader: bufio.NewReader(f)}

	for {
		mtime := lr.ReadInt64()

		if errors.Is(lr.Err, io.EOF) {
			return count, hasUnchanged, nil
		}

		local := lr.ReadString0()

		if mtime > 0 && mtime < lastRunTime && !subDir.Status[local].uploadFailed() {
			hasUnchanged = true
		} else {
			count++
		}
	}
}

// calculateChunks returns the optimal number of chunks for n entries given
// minimum and maximum files-per-chunk constraints. It assumes valid inputs:
// minChunk >= 1, maxChunk >= 1, minChunk <= maxChunk. Returns 0 for n == 0.
func calculateChunks(n, minChunk, maxChunk int) int {
	if n == 0 {
		return 0
	}

	ideal := (n + TargetChunks - 1) / TargetChunks

	perChunk := ideal
	if perChunk < minChunk {
		perChunk = minChunk
	}

	if perChunk > maxChunk {
		perChunk = maxChunk
	}

	return (n + perChunk - 1) / perChunk
}

func streamToChunks(
	subDir subDir, transformer, dir string,
	numChunks int,
	randSeed, lastRunTime int64,
	hasUnchanged bool,
) ([]string, error) {
	w, err := newChunkWriters(dir, numChunks, hasUnchanged, randSeed)
	if err != nil {
		return nil, err
	}

	defer func() {
		if cerr := w.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("error closing chunk files: %w", cerr)
		}
	}()

	if err := distributeEntries(subDir, transformer, w, lastRunTime); err != nil {
		return nil, err
	}

	return w.paths(), nil
}

func distributeEntries(subDir subDir, transformer string, w *chunkWriters, lastRunTime int64) error {
	f, err := os.Open(subDir.FOFNPath())
	if err != nil {
		return err
	}
	defer f.Close()

	lr := byteio.StickyLittleEndianReader{Reader: bufio.NewReader(f)}

	for {
		mtime := lr.ReadInt64()

		if lr.Err != nil {
			if errors.Is(lr.Err, io.EOF) {
				return nil
			}

			return lr.Err
		}

		if err := writePath(subDir, w, lastRunTime, mtime, lr.ReadString0(), transformer); err != nil {
			return err
		}
	}
}

func writePath(subDir subDir, w *chunkWriters, lastRunTime, mtime int64, path, transformer string) error {
	if mtime > 0 && mtime < lastRunTime && !subDir.Status[path].uploadFailed() {
		return w.writeUnchangedPath(path, transformer)
	}

	return w.writeChunkPath(path, transformer)
}
