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
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestChunk(t *testing.T) {
	Convey("WriteShuffledChunks", t, func() {
		dir := t.TempDir()

		transform := func(local string) (string, error) {
			return "/remote" + local, nil
		}

		Convey("splits 25 paths into 3 chunks with chunkSize 10", func() {
			fofnPath := writeFofn(dir, generatePaths(25))
			outDir := filepath.Join(dir, "out1")
			So(os.MkdirAll(outDir, 0750), ShouldBeNil)

			paths, err := WriteShuffledChunks(fofnPath, transform, outDir, 10, 1)
			So(err, ShouldBeNil)
			So(paths, ShouldHaveLength, 3)

			for i, p := range paths {
				expected := filepath.Join(outDir, fmt.Sprintf("chunk.%06d", i))
				So(p, ShouldEqual, expected)
			}

			totalLines := countChunkLines(paths)
			So(totalLines, ShouldEqual, 25)

			verifyBase64Format(paths)
		})

		Convey("puts 10 paths into 1 chunk with chunkSize 10", func() {
			fofnPath := writeFofn(dir, generatePaths(10))
			outDir := filepath.Join(dir, "out2")
			So(os.MkdirAll(outDir, 0750), ShouldBeNil)

			paths, err := WriteShuffledChunks(fofnPath, transform, outDir, 10, 1)
			So(err, ShouldBeNil)
			So(paths, ShouldHaveLength, 1)

			totalLines := countChunkLines(paths)
			So(totalLines, ShouldEqual, 10)
		})

		Convey("returns empty slice for empty fofn", func() {
			fofnPath := writeFofn(dir, nil)
			outDir := filepath.Join(dir, "out3")
			So(os.MkdirAll(outDir, 0750), ShouldBeNil)

			paths, err := WriteShuffledChunks(fofnPath, transform, outDir, 10, 1)
			So(err, ShouldBeNil)
			So(paths, ShouldBeEmpty)
		})

		Convey("produces valid base64-encoded path pairs", func() {
			inputPaths := generatePaths(15)
			fofnPath := writeFofn(dir, inputPaths)
			outDir := filepath.Join(dir, "out4")
			So(os.MkdirAll(outDir, 0750), ShouldBeNil)

			paths, err := WriteShuffledChunks(fofnPath, transform, outDir, 10, 1)
			So(err, ShouldBeNil)

			allLocals, allRemotes := decodeAllChunks(paths)

			inputSet := make(map[string]bool, len(inputPaths))
			for _, p := range inputPaths {
				inputSet[p] = true
			}

			for _, local := range allLocals {
				So(inputSet[local], ShouldBeTrue)
			}

			for _, remote := range allRemotes {
				So(strings.HasPrefix(remote, "/remote/"), ShouldBeTrue)
			}

			So(allLocals, ShouldHaveLength, len(inputPaths))
		})

		Convey("produces deterministic output with same seed", func() {
			inputPaths := generatePaths(1000)
			fofnPath := writeFofn(dir, inputPaths)

			outDir1 := filepath.Join(dir, "det1")
			So(os.MkdirAll(outDir1, 0750), ShouldBeNil)

			paths1, err := WriteShuffledChunks(fofnPath, transform, outDir1, 10, 42)
			So(err, ShouldBeNil)

			outDir2 := filepath.Join(dir, "det2")
			So(os.MkdirAll(outDir2, 0750), ShouldBeNil)

			paths2, err := WriteShuffledChunks(fofnPath, transform, outDir2, 10, 42)
			So(err, ShouldBeNil)

			So(len(paths1), ShouldEqual, len(paths2))

			for i := range paths1 {
				content1 := readFileContent(paths1[i])
				content2 := readFileContent(paths2[i])
				So(content1, ShouldEqual, content2)
			}
		})

		Convey("assigns paths to specific chunks for known seeds", func() {
			inputPaths := make([]string, 10)
			for i := range 10 {
				inputPaths[i] = fmt.Sprintf("/f%d", i)
			}

			fofnPath := writeFofn(dir, inputPaths)

			Convey("seed 1", func() {
				outDir := filepath.Join(dir, "seed1")
				So(os.MkdirAll(outDir, 0750), ShouldBeNil)

				paths, err := WriteShuffledChunks(fofnPath, transform, outDir, 3, 1)
				So(err, ShouldBeNil)
				So(paths, ShouldHaveLength, 4)

				c0 := decodeChunkLocals(paths[0])
				c1 := decodeChunkLocals(paths[1])
				c2 := decodeChunkLocals(paths[2])
				c3 := decodeChunkLocals(paths[3])

				So(c0, ShouldResemble,
					[]string{"/f7", "/f8", "/f9"})
				So(c1, ShouldResemble,
					[]string{"/f0", "/f4", "/f6"})
				So(c2, ShouldResemble,
					[]string{"/f5"})
				So(c3, ShouldResemble,
					[]string{"/f1", "/f2", "/f3"})
			})

			Convey("seed 2", func() {
				outDir := filepath.Join(dir, "seed2")
				So(os.MkdirAll(outDir, 0750), ShouldBeNil)

				paths, err := WriteShuffledChunks(fofnPath, transform, outDir, 3, 2)
				So(err, ShouldBeNil)
				So(paths, ShouldHaveLength, 4)

				c0 := decodeChunkLocals(paths[0])
				c1 := decodeChunkLocals(paths[1])
				c2 := decodeChunkLocals(paths[2])
				c3 := decodeChunkLocals(paths[3])

				So(c0, ShouldResemble,
					[]string{"/f2", "/f3", "/f4", "/f7", "/f9"})
				So(c1, ShouldBeEmpty)
				So(c2, ShouldResemble,
					[]string{"/f0", "/f1", "/f5", "/f6"})
				So(c3, ShouldResemble,
					[]string{"/f8"})
			})
		})

		Convey("streams without excessive memory", func() {
			const (
				numEntries = 1_000_000
				entryLen   = 100
				chunkSz    = 10_000
			)

			fofnPath := writeLargeFofn(dir, numEntries, entryLen)
			outDir := filepath.Join(dir, "mem")
			So(os.MkdirAll(outDir, 0750), ShouldBeNil)

			runtime.GC()

			var before runtime.MemStats
			runtime.ReadMemStats(&before)

			_, err := WriteShuffledChunks(fofnPath, transform, outDir, chunkSz, 1)
			So(err, ShouldBeNil)

			runtime.GC()

			var after runtime.MemStats
			runtime.ReadMemStats(&after)

			var growth uint64
			if after.HeapInuse > before.HeapInuse {
				growth = after.HeapInuse - before.HeapInuse
			}

			So(growth, ShouldBeLessThan, 20*1024*1024)
		})
	})
}

// writeFofn creates a null-terminated fofn file containing
// the given paths and returns its path.
func writeFofn(dir string, paths []string) string {
	p := filepath.Join(dir, "fofn")

	f, err := os.Create(p)
	So(err, ShouldBeNil)

	for _, path := range paths {
		_, err = f.WriteString(path + "\x00")
		So(err, ShouldBeNil)
	}

	err = f.Close()
	So(err, ShouldBeNil)

	return p
}

// writeLargeFofn creates a fofn file with n entries, each
// of the specified byte length, and returns its path.
func writeLargeFofn(dir string, n, entryLen int) string {
	p := filepath.Join(dir, "large.fofn")

	f, err := os.Create(p)
	So(err, ShouldBeNil)

	entry := "/" + strings.Repeat("x", entryLen-1)

	writeErrors := 0

	for range n {
		_, werr := f.WriteString(entry + "\x00")
		if werr != nil {
			writeErrors++
		}
	}

	So(writeErrors, ShouldEqual, 0)

	err = f.Close()
	So(err, ShouldBeNil)

	return p
}

// generatePaths creates n paths of the form /path/000000.
func generatePaths(n int) []string {
	paths := make([]string, n)

	for i := range n {
		paths[i] = fmt.Sprintf("/path/%06d", i)
	}

	return paths
}

// countChunkLines counts the total number of non-empty
// lines across all chunk files.
func countChunkLines(paths []string) int {
	total := 0

	for _, p := range paths {
		lines := readNonEmptyLines(p)
		total += len(lines)
	}

	return total
}

// readNonEmptyLines reads all non-empty lines from a file.
func readNonEmptyLines(path string) []string {
	data, err := os.ReadFile(path)
	So(err, ShouldBeNil)

	if len(data) == 0 {
		return nil
	}

	raw := strings.Split(strings.TrimRight(string(data), "\n"), "\n")

	var lines []string

	for _, line := range raw {
		if line != "" {
			lines = append(lines, line)
		}
	}

	return lines
}

// readFileContent reads the full contents of a file.
func readFileContent(path string) string {
	data, err := os.ReadFile(path)
	So(err, ShouldBeNil)

	return string(data)
}

// verifyBase64Format checks that every line in every chunk
// file has exactly 2 tab-separated base64-encoded fields.
func verifyBase64Format(paths []string) {
	for _, p := range paths {
		lines := readNonEmptyLines(p)

		for _, line := range lines {
			parts := strings.Split(line, "\t")
			So(parts, ShouldHaveLength, 2)

			_, err := base64.StdEncoding.DecodeString(parts[0])
			So(err, ShouldBeNil)

			_, err = base64.StdEncoding.DecodeString(parts[1])
			So(err, ShouldBeNil)
		}
	}
}

// decodeAllChunks reads all chunk files and returns the
// decoded local and remote paths.
func decodeAllChunks(
	paths []string,
) (locals, remotes []string) {
	for _, p := range paths {
		l, r := decodeChunk(p)
		locals = append(locals, l...)
		remotes = append(remotes, r...)
	}

	return locals, remotes
}

// decodeChunk reads a chunk file and returns the decoded
// local and remote paths.
func decodeChunk(path string) (locals, remotes []string) {
	lines := readNonEmptyLines(path)

	for _, line := range lines {
		parts := strings.Split(line, "\t")
		So(parts, ShouldHaveLength, 2)

		localBytes, err := base64.StdEncoding.DecodeString(parts[0])
		So(err, ShouldBeNil)

		remoteBytes, err := base64.StdEncoding.DecodeString(parts[1])
		So(err, ShouldBeNil)

		locals = append(locals, string(localBytes))
		remotes = append(remotes, string(remoteBytes))
	}

	return locals, remotes
}

// decodeChunkLocals reads a chunk file and returns only the
// decoded local paths.
func decodeChunkLocals(path string) []string {
	locals, _ := decodeChunk(path)

	return locals
}
