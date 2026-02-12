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

func TestCalculateChunks(t *testing.T) {
	Convey("CalculateChunks returns the optimal number of chunks", t, func() {
		Convey("returns 0 for n=0", func() {
			So(CalculateChunks(0, 250, 10000), ShouldEqual, 0)
		})

		Convey("returns 1 for n=1", func() {
			So(CalculateChunks(1, 250, 10000), ShouldEqual, 1)
		})

		Convey("returns 1 for n=250 (exactly minChunk)", func() {
			So(CalculateChunks(250, 250, 10000), ShouldEqual, 1)
		})

		Convey("returns 2 for n=251 (just over minChunk)", func() {
			So(CalculateChunks(251, 250, 10000), ShouldEqual, 2)
		})

		Convey("returns 2 for n=500", func() {
			So(CalculateChunks(500, 250, 10000), ShouldEqual, 2)
		})

		Convey("returns 40 for n=10000", func() {
			So(CalculateChunks(10000, 250, 10000), ShouldEqual, 40)
		})

		Convey("returns 100 for n=25000 (ideal hits TargetChunks)", func() {
			So(CalculateChunks(25000, 250, 10000), ShouldEqual, 100)
		})

		Convey("returns 100 for n=50000", func() {
			So(CalculateChunks(50000, 250, 10000), ShouldEqual, 100)
		})

		Convey("returns 100 for n=100000", func() {
			So(CalculateChunks(100000, 250, 10000), ShouldEqual, 100)
		})

		Convey("returns 100 for n=1000000 (at maxChunk boundary)", func() {
			So(CalculateChunks(1000000, 250, 10000), ShouldEqual, 100)
		})

		Convey("returns 101 for n=1000001 (just over maxChunk boundary)", func() {
			So(CalculateChunks(1000001, 250, 10000), ShouldEqual, 101)
		})

		Convey("returns 200 for n=2000000", func() {
			So(CalculateChunks(2000000, 250, 10000), ShouldEqual, 200)
		})

		Convey("returns 1000 for n=10000000", func() {
			So(CalculateChunks(10000000, 250, 10000), ShouldEqual, 1000)
		})

		Convey("returns 1 for n=100 (below minChunk)", func() {
			So(CalculateChunks(100, 250, 10000), ShouldEqual, 1)
		})

		Convey("returns 1 for n=249 (just below minChunk)", func() {
			So(CalculateChunks(249, 250, 10000), ShouldEqual, 1)
		})

		Convey("returns 100 for n=999999 (just below maxChunk boundary)", func() {
			So(CalculateChunks(999999, 250, 10000), ShouldEqual, 100)
		})

		Convey("returns 100 for n=25001", func() {
			So(CalculateChunks(25001, 250, 10000), ShouldEqual, 100)
		})

		Convey("returns 5 for n=50 with min=10 max=20", func() {
			So(CalculateChunks(50, 10, 20), ShouldEqual, 5)
		})

		Convey("returns 125 for n=2500 with min=10 max=20", func() {
			So(CalculateChunks(2500, 10, 20), ShouldEqual, 125)
		})

		Convey("returns 100 for n=1500 with min=10 max=20", func() {
			So(CalculateChunks(1500, 10, 20), ShouldEqual, 100)
		})

		Convey("returns 3 for n=25 with degenerate min=max=10", func() {
			So(CalculateChunks(25, 10, 10), ShouldEqual, 3)
		})

		Convey("returns 10 for n=100 with degenerate min=max=10", func() {
			So(CalculateChunks(100, 10, 10), ShouldEqual, 10)
		})

		Convey("returns 50 for n=50 with min=1 max=100", func() {
			So(CalculateChunks(50, 1, 100), ShouldEqual, 50)
		})

		Convey("returns 10000 for n=100000000", func() {
			So(CalculateChunks(100000000, 250, 10000), ShouldEqual, 10000)
		})

		Convey("returns 3 for n=501", func() {
			So(CalculateChunks(501, 250, 10000), ShouldEqual, 3)
		})

		Convey("table-driven comprehensive test", func() {
			cases := []struct {
				n, minChunk, maxChunk, expected int
			}{
				{0, 250, 10000, 0},
				{1, 250, 10000, 1},
				{100, 250, 10000, 1},
				{249, 250, 10000, 1},
				{250, 250, 10000, 1},
				{251, 250, 10000, 2},
				{500, 250, 10000, 2},
				{501, 250, 10000, 3},
				{1000, 250, 10000, 4},
				{10000, 250, 10000, 40},
				{24999, 250, 10000, 100},
				{25000, 250, 10000, 100},
				{25001, 250, 10000, 100},
				{50000, 250, 10000, 100},
				{100000, 250, 10000, 100},
				{500000, 250, 10000, 100},
				{999999, 250, 10000, 100},
				{1000000, 250, 10000, 100},
				{1000001, 250, 10000, 101},
				{2000000, 250, 10000, 200},
				{10000000, 250, 10000, 1000},
				{100000000, 250, 10000, 10000},
			}

			failures := 0

			for _, tc := range cases {
				got := CalculateChunks(tc.n, tc.minChunk, tc.maxChunk)
				if got != tc.expected {
					failures++
				}
			}

			So(failures, ShouldEqual, 0)
		})
	})
}

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

			paths, err := WriteShuffledChunks(fofnPath, transform, outDir, 10, 10, 1)
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

			paths, err := WriteShuffledChunks(fofnPath, transform, outDir, 10, 10, 1)
			So(err, ShouldBeNil)
			So(paths, ShouldHaveLength, 1)

			totalLines := countChunkLines(paths)
			So(totalLines, ShouldEqual, 10)
		})

		Convey("returns empty slice for empty fofn", func() {
			fofnPath := writeFofn(dir, nil)
			outDir := filepath.Join(dir, "out3")
			So(os.MkdirAll(outDir, 0750), ShouldBeNil)

			paths, err := WriteShuffledChunks(fofnPath, transform, outDir, 10, 10, 1)
			So(err, ShouldBeNil)
			So(paths, ShouldBeEmpty)
		})

		Convey("produces valid base64-encoded path pairs", func() {
			inputPaths := generatePaths(15)
			fofnPath := writeFofn(dir, inputPaths)
			outDir := filepath.Join(dir, "out4")
			So(os.MkdirAll(outDir, 0750), ShouldBeNil)

			paths, err := WriteShuffledChunks(fofnPath, transform, outDir, 10, 10, 1)
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

			paths1, err := WriteShuffledChunks(fofnPath, transform, outDir1, 10, 10, 42)
			So(err, ShouldBeNil)

			outDir2 := filepath.Join(dir, "det2")
			So(os.MkdirAll(outDir2, 0750), ShouldBeNil)

			paths2, err := WriteShuffledChunks(fofnPath, transform, outDir2, 10, 10, 42)
			So(err, ShouldBeNil)

			So(len(paths1), ShouldEqual, len(paths2))

			mismatches := 0

			for i := range paths1 {
				content1 := readFileContent(paths1[i])
				content2 := readFileContent(paths2[i])

				if content1 != content2 {
					mismatches++
				}
			}

			So(mismatches, ShouldEqual, 0)
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

				paths, err := WriteShuffledChunks(fofnPath, transform, outDir, 3, 3, 1)
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

				paths, err := WriteShuffledChunks(fofnPath, transform, outDir, 3, 3, 2)
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

			_, err := WriteShuffledChunks(fofnPath, transform, outDir, chunkSz, chunkSz, 1)
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

		Convey("VC2-1: 25 paths min=max=10 gives 3 chunks", func() {
			fofnPath := writeFofn(dir, generatePaths(25))
			outDir := filepath.Join(dir, "vc2_1")
			So(os.MkdirAll(outDir, 0750), ShouldBeNil)

			paths, err := WriteShuffledChunks(
				fofnPath, transform, outDir, 10, 10, 1,
			)
			So(err, ShouldBeNil)
			So(paths, ShouldHaveLength, 3)

			totalLines := countChunkLines(paths)
			So(totalLines, ShouldEqual, 25)
		})

		Convey("VC2-2: 50000 paths min=250 max=10000 gives 100 chunks", func() {
			fofnPath := writeLargeFofn(dir, 50000, 13)
			outDir := filepath.Join(dir, "vc2_2")
			So(os.MkdirAll(outDir, 0750), ShouldBeNil)

			paths, err := WriteShuffledChunks(
				fofnPath, transform, outDir, 250, 10000, 1,
			)
			So(err, ShouldBeNil)
			So(paths, ShouldHaveLength, 100)

			totalLines := countChunkLines(paths)
			So(totalLines, ShouldEqual, 50000)
		})

		Convey("VC2-3: 100 paths min=250 max=10000 gives 1 chunk", func() {
			fofnPath := writeFofn(dir, generatePaths(100))
			outDir := filepath.Join(dir, "vc2_3")
			So(os.MkdirAll(outDir, 0750), ShouldBeNil)

			paths, err := WriteShuffledChunks(
				fofnPath, transform, outDir, 250, 10000, 1,
			)
			So(err, ShouldBeNil)
			So(paths, ShouldHaveLength, 1)

			totalLines := countChunkLines(paths)
			So(totalLines, ShouldEqual, 100)
		})

		Convey("VC2-4: 201 paths min=max=2 gives 101 chunks", func() {
			fofnPath := writeFofn(dir, generatePaths(201))
			outDir := filepath.Join(dir, "vc2_4")
			So(os.MkdirAll(outDir, 0750), ShouldBeNil)

			paths, err := WriteShuffledChunks(
				fofnPath, transform, outDir, 2, 2, 1,
			)
			So(err, ShouldBeNil)
			So(paths, ShouldHaveLength, 101)

			totalLines := countChunkLines(paths)
			So(totalLines, ShouldEqual, 201)
		})

		Convey("VC2-5: empty fofn gives 0 chunks and nil", func() {
			fofnPath := writeFofn(dir, nil)
			outDir := filepath.Join(dir, "vc2_5")
			So(os.MkdirAll(outDir, 0750), ShouldBeNil)

			paths, err := WriteShuffledChunks(
				fofnPath, transform, outDir, 250, 10000, 1,
			)
			So(err, ShouldBeNil)
			So(paths, ShouldBeNil)
		})

		Convey("VC2-6: 1 path min=250 max=10000 gives 1 chunk with 1 line", func() {
			fofnPath := writeFofn(dir, generatePaths(1))
			outDir := filepath.Join(dir, "vc2_6")
			So(os.MkdirAll(outDir, 0750), ShouldBeNil)

			paths, err := WriteShuffledChunks(
				fofnPath, transform, outDir, 250, 10000, 1,
			)
			So(err, ShouldBeNil)
			So(paths, ShouldHaveLength, 1)

			totalLines := countChunkLines(paths)
			So(totalLines, ShouldEqual, 1)
		})

		Convey("VC2-7: minChunk=0 returns error, no chunks", func() {
			fofnPath := writeFofn(dir, generatePaths(10))
			outDir := filepath.Join(dir, "vc2_7")
			So(os.MkdirAll(outDir, 0750), ShouldBeNil)

			paths, err := WriteShuffledChunks(
				fofnPath, transform, outDir, 0, 10, 1,
			)
			So(err, ShouldNotBeNil)
			So(paths, ShouldBeNil)

			entries := dirEntries(outDir)
			So(entries, ShouldBeEmpty)
		})

		Convey("VC2-8: maxChunk=0 returns error, no chunks", func() {
			fofnPath := writeFofn(dir, generatePaths(10))
			outDir := filepath.Join(dir, "vc2_8")
			So(os.MkdirAll(outDir, 0750), ShouldBeNil)

			paths, err := WriteShuffledChunks(
				fofnPath, transform, outDir, 10, 0, 1,
			)
			So(err, ShouldNotBeNil)
			So(paths, ShouldBeNil)

			entries := dirEntries(outDir)
			So(entries, ShouldBeEmpty)
		})

		Convey("VC2-9: minChunk > maxChunk returns error, no chunks", func() {
			fofnPath := writeFofn(dir, generatePaths(10))
			outDir := filepath.Join(dir, "vc2_9")
			So(os.MkdirAll(outDir, 0750), ShouldBeNil)

			paths, err := WriteShuffledChunks(
				fofnPath, transform, outDir, 500, 100, 1,
			)
			So(err, ShouldNotBeNil)
			So(paths, ShouldBeNil)

			entries := dirEntries(outDir)
			So(entries, ShouldBeEmpty)
		})

		Convey("VC2-10: 1M entries streams without excessive memory", func() {
			const (
				numEntries = 1_000_000
				entryLen   = 100
			)

			fofnPath := writeLargeFofn(dir, numEntries, entryLen)
			outDir := filepath.Join(dir, "vc2_10")
			So(os.MkdirAll(outDir, 0750), ShouldBeNil)

			runtime.GC()

			var before runtime.MemStats
			runtime.ReadMemStats(&before)

			_, err := WriteShuffledChunks(
				fofnPath, transform, outDir, 250, 10000, 1,
			)
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

	writeErrors := 0

	for _, path := range paths {
		if _, wErr := f.WriteString(path + "\x00"); wErr != nil {
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

// verifyBase64Format checks that every line in every chunk
// file has exactly 2 tab-separated base64-encoded fields.
func verifyBase64Format(paths []string) {
	formatErrors := 0
	decodeErrors := 0

	for _, p := range paths {
		lines := readNonEmptyLines(p)

		for _, line := range lines {
			parts := strings.Split(line, "\t")
			if len(parts) != 2 {
				formatErrors++

				continue
			}

			if _, err := base64.StdEncoding.DecodeString(parts[0]); err != nil {
				decodeErrors++
			}

			if _, err := base64.StdEncoding.DecodeString(parts[1]); err != nil {
				decodeErrors++
			}
		}
	}

	So(formatErrors, ShouldEqual, 0)
	So(decodeErrors, ShouldEqual, 0)
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

// readFileContent reads the full contents of a file.
func readFileContent(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}

	return string(data)
}

// decodeChunkLocals reads a chunk file and returns only the
// decoded local paths.
func decodeChunkLocals(path string) []string {
	locals, _ := decodeChunk(path)

	return locals
}

// decodeChunk reads a chunk file and returns the decoded
// local and remote paths.
func decodeChunk(path string) (locals, remotes []string) {
	lines := readNonEmptyLines(path)

	formatErrors := 0
	decodeErrors := 0

	for _, line := range lines {
		parts := strings.Split(line, "\t")
		if len(parts) != 2 {
			formatErrors++

			continue
		}

		localBytes, err := base64.StdEncoding.DecodeString(parts[0])
		if err != nil {
			decodeErrors++

			continue
		}

		remoteBytes, err := base64.StdEncoding.DecodeString(parts[1])
		if err != nil {
			decodeErrors++

			continue
		}

		locals = append(locals, string(localBytes))
		remotes = append(remotes, string(remoteBytes))
	}

	So(formatErrors, ShouldEqual, 0)
	So(decodeErrors, ShouldEqual, 0)

	return locals, remotes
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

// dirEntries returns the names of entries in a directory.
func dirEntries(dir string) []string {
	entries, err := os.ReadDir(dir)
	So(err, ShouldBeNil)

	names := make([]string, len(entries))

	for i, e := range entries {
		names[i] = e.Name()
	}

	return names
}
