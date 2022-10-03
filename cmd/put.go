/*******************************************************************************
 * Copyright (c) 2022 Genome Research Ltd.
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

package cmd

import (
	"bufio"
	"io"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/wtsi-hgi/ibackup/put"
)

const (
	// maxScanTokenSize defines the size of bufio scan's buffer, enabling us to
	// parse very long lines - longer than the max length of of 2 file paths,
	// plus lots extra for metadata.
	maxScanTokenSize = 4096 * 1024

	putFileCols    = 3
	putFileMinCols = 2
	putMetaParts   = 2
)

// options for this cmd.
var putFile string
var putMeta string

// putCmd represents the put command.
var putCmd = &cobra.Command{
	Use:   "put",
	Short: "Safely copy files from a local filesystem to iRODS, with metadata",
	Long: `Safely copy files from a local filesystem to iRODS, with metadata.

Use the -f arg (defaults to STDIN) to provide a tab-delimited file with these 4
columns:
/absolute/path/of/local/file /remote/irods/path/of/file key1:val1;key2:val2

(where keys and vals are the metadata you'd like to add)
Column 3 is optional, and you can use the -m arg to apply the same metadata to
lines lacking column 3 instead.

put will then efficiently copy all column 1 paths to column 2 locations in
iRODS, using a single connection, sequentially. Another connection in parallel
will apply the column 3 metadata. (This makes it orders of magnitude faster than
running 'iput' for each of many small files.)

put will always forcable overwrite existing iRODS locations, and calculate and
register a checksum on the server side and verify against a locally-calculated
checksum.

Collections for your iRODS paths in column 2 will be automatically created if
necessary.

You need to have the baton commands in your PATH for this to work.

You also need to have your iRODS environment set up and must be authenticated
with iRODS (eg. run 'iinit') before running this command. If 'iput' works for
you, so should this.`,
	Run: func(cmd *cobra.Command, args []string) {
		requests := parsePutFile(putFile, putMeta)

		handler, err := put.GetBatonHandler()
		if err != nil {
			die("%s", err)
		}

		p, err := put.New(handler, requests)
		if err != nil {
			die("%s", err)
		}

		err = p.CreateCollections()
		if err != nil {
			die("%s", err)
		}

		results := p.Put()

		fails, replaced, uploads, skipped := 0, 0, 0, 0

		for r := range results {
			switch r.Status {
			case put.RequestStatusFailed:
				warn("%s failed: %s", r.Local, r.Error)
				fails++
			case put.RequestStatusMissing:
				warn("%s missing: %s", r.Local, r.Error)
				fails++
			case put.RequestStatusReplaced:
				replaced++
			case put.RequestStatusUnmodified:
				skipped++
			case put.RequestStatusUploaded:
				uploads++
			}

		}

		info("%d uploaded (%d overwrites); %d skipped; %d failed", uploads+replaced, replaced, skipped, fails)

		if fails > 0 {
			os.Exit(1)
		}
	},
}

func init() {
	RootCmd.AddCommand(putCmd)

	// flags specific to this sub-command
	putCmd.Flags().StringVarP(&putFile, "file", "f", "-",
		"tab-delimited /local/path /irods/path key:val;key:val file (- means STDIN)")
	putCmd.Flags().StringVarP(&putMeta, "meta", "m", "",
		"key:val;key:val default metadata to apply to -f rows lacking column 3")
}

func parsePutFile(path string, meta string) []*put.Request {
	defaultMeta := parseMetaString(meta)
	scanner, df := createScannerForFile(path)

	defer df()

	var prs []*put.Request //nolint:prealloc

	lineNum := 0
	for scanner.Scan() {
		lineNum++

		pr := parsePutFileLine(scanner.Text(), lineNum, defaultMeta)
		if pr == nil {
			continue
		}

		prs = append(prs, pr)
	}

	serr := scanner.Err()
	if serr != nil {
		die("failed to read whole file: %s", serr.Error())
	}

	return prs
}

func parseMetaString(meta string) map[string]string {
	kvs := strings.Split(meta, ";")
	mm := make(map[string]string, len(kvs))

	for _, kv := range kvs {
		parts := strings.Split(kv, ":")
		if len(parts) == putMetaParts {
			mm[parts[0]] = parts[1]
		}
	}

	if len(mm) != len(kvs) {
		die("invalid meta: %s", meta)
	}

	return mm
}

func createScannerForFile(path string) (*bufio.Scanner, func()) {
	var reader io.Reader

	var dfunc func()

	if path == "-" {
		reader = os.Stdin
		dfunc = func() {}
	} else {
		reader, dfunc = openFile(path)
	}

	scanner := bufio.NewScanner(reader)
	buf := make([]byte, maxScanTokenSize)
	scanner.Buffer(buf, maxScanTokenSize)

	return scanner, dfunc
}

// openFile opens the given path, and returns it as an io.Reader along with a
// function you should defer (to close the file).
func openFile(path string) (io.Reader, func()) {
	file, err := os.Open(path)
	if err != nil {
		die("could not open file '%s': %s", path, err)
	}

	return file, func() {
		file.Close()
	}
}

func parsePutFileLine(line string, lineNum int, defaultMeta map[string]string) *put.Request {
	cols := strings.Split(line, "\t")
	colsn := len(cols)

	if colsn < putFileMinCols || cols[0] == "" {
		return nil
	}

	checkPutFileCols(colsn, lineNum)

	meta := defaultMeta

	if colsn == putFileCols && cols[2] != "" {
		meta = parseMetaString(cols[2])
	}

	return &put.Request{
		Local:  cols[0],
		Remote: cols[1],
		Meta:   meta,
	}
}

func checkPutFileCols(cols int, lineNum int) {
	if cols > putFileCols {
		die("line %d has too many columns; check `ibackup put -h`", lineNum)
	}
}
