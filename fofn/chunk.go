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
	"fmt"
	"math/rand"
	"os"
	"path/filepath"

	"github.com/wtsi-hgi/ibackup/internal/scanner"
)

const chunkNameFormat = "chunk.%06d"

// WriteShuffledChunks reads a null-terminated fofn file
// and writes the entries into shuffled chunk files. Each
// entry is transformed using the provided function and
// written as a base64-encoded local/remote pair separated
// by a tab.
//
// The function uses a two-pass approach: first counting
// entries via scanner.ScanNullTerminated, then streaming
// them into randomly assigned chunk files. The random
// assignment is deterministic for a given randSeed.
//
// Chunk files are named chunk.000000, chunk.000001, etc.
// Returns the paths of the created chunk files, or nil
// if the fofn is empty.
func WriteShuffledChunks(
	fofnPath string,
	transform func(string) (string, error),
	dir string,
	chunkSize int,
	randSeed int64,
) ([]string, error) {
	count, err := countEntries(fofnPath)
	if err != nil {
		return nil, err
	}

	if count == 0 {
		return nil, nil
	}

	numChunks := (count + chunkSize - 1) / chunkSize

	return streamToChunks(
		fofnPath, transform, dir, numChunks, randSeed,
	)
}

func countEntries(fofnPath string) (int, error) {
	var count int

	err := scanner.ScanNullTerminated(
		fofnPath, func(_ string) error {
			count++

			return nil
		},
	)

	return count, err
}

func streamToChunks(
	fofnPath string,
	transform func(string) (string, error),
	dir string,
	numChunks int,
	randSeed int64,
) ([]string, error) {
	files, paths, err := createChunkFiles(dir, numChunks)
	if err != nil {
		return nil, err
	}

	defer closeFiles(files)

	writers := createWriters(files)

	rng := rand.New(rand.NewSource(randSeed)) //nolint:gosec

	err = scanner.ScanNullTerminated(
		fofnPath, func(entry string) error {
			return writeEntry(
				writers, rng, numChunks,
				entry, transform,
			)
		},
	)

	if err != nil {
		return nil, err
	}

	if err := flushWriters(writers); err != nil {
		return nil, err
	}

	return paths, nil
}

func createChunkFiles(
	dir string, numChunks int,
) ([]*os.File, []string, error) {
	files := make([]*os.File, numChunks)
	paths := make([]string, numChunks)

	for i := range numChunks {
		name := fmt.Sprintf(chunkNameFormat, i)
		p := filepath.Join(dir, name)

		f, err := os.Create(p)
		if err != nil {
			closeFiles(files[:i])

			return nil, nil, fmt.Errorf(
				"create chunk file: %w", err,
			)
		}

		files[i] = f
		paths[i] = p
	}

	return files, paths, nil
}

func createWriters(files []*os.File) []*bufio.Writer {
	writers := make([]*bufio.Writer, len(files))

	for i, f := range files {
		writers[i] = bufio.NewWriter(f)
	}

	return writers
}

func writeEntry(
	writers []*bufio.Writer,
	rng *rand.Rand,
	numChunks int,
	entry string,
	transform func(string) (string, error),
) error {
	remote, err := transform(entry)
	if err != nil {
		return err
	}

	chunk := rng.Intn(numChunks)

	local64 := base64.StdEncoding.EncodeToString(
		[]byte(entry),
	)
	remote64 := base64.StdEncoding.EncodeToString(
		[]byte(remote),
	)

	_, err = fmt.Fprintf(
		writers[chunk], "%s\t%s\n", local64, remote64,
	)

	return err
}

func flushWriters(writers []*bufio.Writer) error {
	for _, w := range writers {
		if err := w.Flush(); err != nil {
			return fmt.Errorf("flush chunk writer: %w", err)
		}
	}

	return nil
}

func closeFiles(files []*os.File) {
	for _, f := range files {
		if f != nil {
			f.Close()
		}
	}
}
