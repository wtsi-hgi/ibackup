/*******************************************************************************
 * Copyright (c) 2022, 2023 Genome Research Ltd.
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
	"bytes"
	b64 "encoding/base64"
	"io"
	"os"
	"os/user"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cobra"
	"github.com/wtsi-hgi/ibackup/put"
	"github.com/wtsi-hgi/ibackup/server"
)

const (
	// maxScanTokenSize defines the size of bufio scan's buffer, enabling us to
	// parse very long lines - longer than the max length of of 2 file paths,
	// plus lots extra for metadata.
	maxScanTokenSize = 4096 * 1024

	putFileCols    = 3
	putFileMinCols = 2
	putMetaParts   = 2
	putSet         = "manual"

	// maxMBToHandle is the total file size of requests we'll deal with in
	// server mode, after which we'll exit so another client can use fresh
	// iRODS connections and so our job doesn't last too long and get killed for
	// using too much time.
	maxMBToHandle = 10000

	// minMBperSecondUploadSpeed is the slowest MB/s we think an upload should
	// take; if it drops below this and is still uploading, we'll report to the
	// server the upload might be stuck.
	minMBperSecondUploadSpeed = 5

	// minTimeForUpload is the minimum time an upload carries on for before
	// we'll consider it stuck, regardless of the MB/s.
	minTimeForUpload = 1 * time.Minute
)

// options for this cmd.
var putFile string
var putMeta string
var putVerbose bool
var putBase64 bool
var putServerMode bool
var putLog string

// putCmd represents the put command.
var putCmd = &cobra.Command{
	Use:   "put",
	Short: "Safely copy files from a local filesystem to iRODS, with metadata",
	Long: `Safely copy files from a local filesystem to iRODS, with metadata.

Use the -f arg (defaults to STDIN) to provide a tab-delimited file with these 3
columns:
/absolute/path/of/local/file /remote/irods/path/of/file key1:val1;key2:val2

(where keys and vals are the metadata you'd like to add)
Column 3 is optional, and you can use the -m arg to apply the same metadata to
lines lacking column 3 instead.

Some 'ibackup:' prefixed metadata will always be added:
  "mtime"      : mtime of source file, 1sec truncated UTC RFC 3339
  "owner"      : a username
  "group"      : a unix group name
  "date"       : date upload initiated, 1sec truncated UTC RFC 3339
  "requesters" : a comma sep list of usernames of people who reqested the backup
  "sets"       : a comma sep list of backup set names this file belongs to

(when using this command, the "set" is automatically named "manual")

Because local and remote paths could contain tabs and newlines in their names,
you can base64 encode them and use the --base64 option.

put will then efficiently copy all column 1 paths to column 2 locations in
iRODS, using a single connection, sequentially. Another connection in parallel
will apply the metadata. (This makes it orders of magnitude faster than running
'iput' for each of many small files.)

put will always calculate and register a checksum on the server side and verify
against a locally-calculated checksum.

It will overwrite outdated iRODS files, but it will skip files if they already
exist in iRODS with ibackup:mtime metadata matching the mtime of the local file.

If local file paths are missing, warnings about them will be logged, but this
cmd will still exit 0. It only exits non-zero on failure to upload an existing
local file.

Collections for your iRODS paths in column 2 will be automatically created if
necessary.

(The ibackup server also calls this command with the --from_server and --url
options instead of a 3 column -f file, which makes this command work on upload
requests stored in the server.)

You need to have the baton commands in your PATH for this to work.

You also need to have your iRODS environment set up and must be authenticated
with iRODS (eg. run 'iinit') before running this command. If 'iput' works for
you, so should this.`,
	Run: func(cmd *cobra.Command, args []string) {
		if putLog != "" {
			putVerbose = true
			logToFile(putLog)
		}

		if putVerbose {
			host, errh := os.Hostname()
			if errh != nil {
				die("%s", errh)
			}

			info("client starting on host %s, pid %d", host, os.Getpid())
		}

		handler, err := put.GetBatonHandler()
		if err != nil {
			die("%s", err)
		}

		if putServerMode && serverURL != "" {
			client, err := newServerClient(serverURL, serverCert)
			if err != nil {
				die(err.Error())
			}

			doServerPut(client, handler, putVerbose)
		} else {
			doLocalPut(handler, putFile, putMeta, putBase64, putVerbose)
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
	putCmd.Flags().BoolVarP(&putVerbose, "verbose", "v", false,
		"report upload status of every file")
	putCmd.Flags().BoolVarP(&putBase64, "base64", "b", false,
		"input paths are base64 encoded")
	putCmd.Flags().BoolVarP(&putServerMode, "server", "s", false,
		"pull requests from the server instead of --file; only usable by the user who started the server")
	putCmd.Flags().StringVarP(&putLog, "log", "l", "",
		"log to the given file (implies --verbose)")
}

func doServerPut(client *server.Client, handler put.Handler, verbose bool) {
	p, err := put.New(handler)
	if err != nil {
		die("%s", err)
	}

	defer doCleanup(p)

	count, err := client.UploadRequests(p, maxMBToHandle, minMBperSecondUploadSpeed, minTimeForUpload, appLogger)
	if err != nil {
		die("got %d requests before error: %s", count, err)
	}

	if verbose {
		info("handled %d requests, now exiting", count)
	}
}

func doCleanup(p *put.Putter) {
	if errc := p.Cleanup(); errc != nil {
		warn("cleanup gave errors: %s", errc)
	}
}

func doLocalPut(handler put.Handler, file, meta string, base64Encoded, verbose bool) {
	requests, err := getRequestsFromFile(file, meta, base64Encoded)
	if err != nil {
		die("%s", err)
	}

	if len(requests) == 0 {
		warn("no requests to work on")

		return
	}

	if verbose {
		info("got %d requests to work on", len(requests))
	}

	p, err := put.New(handler)
	if err != nil {
		die("%s", err)
	}

	defer doCleanup(p)

	r := handlePutting(p, requests, verbose)

	if r.fails > 0 {
		defer os.Exit(1)
	}
}

// getRequestsFromFile reads our 3 column file format from a file or STDIN and
// turns the info in to put Requests.
func getRequestsFromFile(file, meta string, base64Encoded bool) ([]*put.Request, error) {
	user, err := user.Current()
	if err != nil {
		return nil, err
	}

	requests := parsePutFile(file, meta, user.Username, fofnLineSplitter(false), base64Encoded)

	return requests, nil
}

func parsePutFile(path, meta, requester string, splitter bufio.SplitFunc, base64Encoded bool) []*put.Request {
	defaultMeta := parseMetaString(meta)
	scanner, df := createScannerForFile(path, splitter)

	defer df()

	var prs []*put.Request //nolint:prealloc

	lineNum := 0
	for scanner.Scan() {
		lineNum++

		pr := parsePutFileLine(scanner.Text(), base64Encoded, lineNum, defaultMeta, requester)
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
	if meta == "" {
		return nil
	}

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

// fofnLineSplitter returns a bufio.SplitFunc that splits on \n be default, or
// null if the given bool is true.
func fofnLineSplitter(onNull bool) bufio.SplitFunc {
	if onNull {
		return scanNulls
	}

	return bufio.ScanLines
}

// scanNulls is a bufio.SplitFunc like bufio.ScanLines, but it splits on null
// characters.
func scanNulls(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}

	if i := bytes.IndexByte(data, '\000'); i >= 0 {
		return i + 1, data[0:i], nil
	}

	if atEOF {
		return len(data), data, nil
	}

	return 0, nil, nil
}

// createScannerForFile creates a bufio.Scanner that will scan the given file,
// splitting lines using the given splitter (eg. output of fofnLineSplitter()).
func createScannerForFile(path string, splitter bufio.SplitFunc) (*bufio.Scanner, func()) {
	var reader io.Reader

	var dfunc func()

	if path == "-" {
		reader = os.Stdin
		dfunc = func() {}
	} else {
		reader, dfunc = openFile(path)
	}

	scanner := bufio.NewScanner(reader)

	scanner.Split(splitter)

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

func parsePutFileLine(line string, base64Encoded bool, lineNum int,
	defaultMeta map[string]string, requester string) *put.Request {
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
		Local:     decodeBase64(cols[0], base64Encoded),
		Remote:    decodeBase64(cols[1], base64Encoded),
		Requester: requester,
		Set:       putSet,
		Meta:      meta,
	}
}

func checkPutFileCols(cols int, lineNum int) {
	if cols > putFileCols {
		die("line %d has too many columns; check `ibackup put -h`", lineNum)
	}
}

// decodeBase64 returns path as-is if isEncoded is false, or after base64
// decoding it if true.
func decodeBase64(path string, isEncoded bool) string {
	if !isEncoded {
		return path
	}

	b, err := b64.StdEncoding.DecodeString(path)
	if err != nil {
		die("%s", err)
	}

	return string(b)
}

type results struct {
	total    int
	verbose  bool
	i        int
	fails    int
	missing  int
	replaced int
	skipped  int
	uploads  int
	mu       sync.Mutex
}

func (r *results) update(req *put.Request) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.i++

	warnIfBad(req, r.i, r.total, r.verbose)

	switch req.Status {
	case put.RequestStatusFailed, put.RequestStatusUploading:
		r.fails++
	case put.RequestStatusMissing:
		r.missing++
	case put.RequestStatusReplaced:
		r.replaced++
	case put.RequestStatusUnmodified:
		r.skipped++
	case put.RequestStatusUploaded:
		r.uploads++
	}
}

// warnIfBad warns if this Request failed or is missing. If verbose, logs at
// info level the Request details.
func warnIfBad(r *put.Request, i, total int, verbose bool) {
	switch r.Status {
	case put.RequestStatusFailed, put.RequestStatusMissing:
		warn("[%d/%d] %s %s: %s", i, total, r.Local, r.Status, r.Error)
	default:
		if verbose {
			info("[%d/%d] %s %s", i, total, r.Local, r.Status)
		}
	}
}

// handlePutting does non-server putting of the given requests, printing results
// to STDOUT.
func handlePutting(p *put.Putter, requests []*put.Request, verbose bool) *results {
	if verbose {
		info("will create collections")
	}

	err := p.CreateCollections(requests)
	if err != nil {
		warn("collection creation failed: %s", err)
	} else if verbose {
		info("collections created")
	}

	r := &results{total: len(requests), verbose: verbose}

	for _, request := range requests {
		shouldPut, _ := p.Validate(request)

		if shouldPut {
			p.Put(request)
		}

		r.update(request)
	}

	info("%d uploaded (%d replaced); %d skipped; %d failed; %d missing",
		r.uploads+r.replaced, r.replaced, r.skipped, r.fails, r.missing)

	return r
}
