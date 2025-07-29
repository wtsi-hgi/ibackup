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
	"fmt"
	"io"
	"os"
	"os/user"
	"strings"
	"sync"
	"time"

	"github.com/inconshreveable/log15"
	"github.com/spf13/cobra"
	"github.com/wtsi-hgi/ibackup/baton"
	"github.com/wtsi-hgi/ibackup/server"
	"github.com/wtsi-hgi/ibackup/transfer"
)

const (
	// maxScanTokenSize defines the size of bufio scan's buffer, enabling us to
	// parse very long lines - longer than the max length of of 2 file paths,
	// plus lots extra for metadata.
	maxScanTokenSize = 4096 * 1024

	putFileCols    = 3
	putFileMinCols = 2
	putSet         = "manual"

	numIRODSConnections = 2
	heartbeatFreq       = 1 * time.Minute

	// minMBperSecondUploadSpeed is the slowest MB/s we think an upload should
	// take; if it drops below this and is still uploading, we'll report to the
	// server the upload might be stuck.
	minMBperSecondUploadSpeed = 5

	// minTimeForUpload is the minimum time an upload carries on for before
	// we'll consider it stuck, regardless of the MB/s.
	minTimeForUpload = 1 * time.Minute

	// defaultMaxStuckTime is the default maximum time we'll wait for an upload
	// we think is stuck to complete, before giving up with an error.
	defaultMaxStuckTime = 1 * time.Hour

	// runFor is the minimum time we'll try and run for in server mode,
	// getting more requests if we finish early.
	runFor = 30 * time.Minute
)

// options for this cmd.
var (
	putFile       string
	putMeta       string
	putVerbose    bool
	putBase64     bool
	putServerMode bool
	putLog        string
	maxStuckTime  time.Duration
)

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

(The ibackup server also calls this command with the --server and --url options
instead of a 3 column -f file, which makes this command work on upload requests
stored in the server.)

You need to have the baton commands in your PATH for this to work.

You also need to have your iRODS environment set up and must be authenticated
with iRODS (eg. run 'iinit') before running this command. If 'iput' works for
you, so should this.`,
	RunE: func(_ *cobra.Command, _ []string) error {
		if putLog != "" {
			putVerbose = true
			logToFile(putLog)
		}

		if serverMode() {
			return handlePutServerMode(time.Now())
		}

		return handlePutManualMode()
	},
}

func init() {
	RootCmd.AddCommand(putCmd)

	// flags specific to this sub-command
	putCmd.Flags().StringVarP(&putFile, "file", "f", "-",
		"tab-delimited /local/path /irods/path key:val;key:val file (- means STDIN)")
	putCmd.Flags().StringVarP(&putMeta, "meta", "m", "",
		"key=val;key=val default metadata to apply to -f rows lacking column 3")
	putCmd.Flags().BoolVarP(&putVerbose, "verbose", "v", false,
		"report upload status of every file")
	putCmd.Flags().BoolVarP(&putBase64, "base64", "b", false,
		"input paths are base64 encoded")
	putCmd.Flags().BoolVarP(&putServerMode, "server", "s", false,
		"pull requests from the server instead of --file; only usable by the user who started the server")
	putCmd.Flags().StringVarP(&putLog, "log", "l", "",
		"log to the given file (implies --verbose)")
	putCmd.Flags().DurationVar(&maxStuckTime, "stuck-timeout", defaultMaxStuckTime,
		"override the default stuck wait time (1h)")
}

func serverMode() bool {
	return putServerMode && serverURL != ""
}

func handlePutServerMode(started time.Time) error {
	return handleServerMode(started, (*server.Client).GetSomeUploadRequests,
		handlePut, (*server.Client).SendPutResultsToServer)
}

func handleServerMode( //nolint:gocognit,gocyclo,funlen
	started time.Time,
	getReq func(*server.Client) ([]*transfer.Request, error),
	handleReq func(*server.Client, []*transfer.Request) (chan *transfer.Request, chan *transfer.Request,
		chan *transfer.Request, func(), error),
	sendReq func(*server.Client, chan *transfer.Request, chan *transfer.Request, chan *transfer.Request,
		float64, time.Duration, time.Duration, log15.Logger) error,
) error {
	for time.Since(started) < runFor {
		client, err := newServerClient(serverURL, serverCert)
		if err != nil {
			return err
		}

		requests, err := getReq(client)
		if err != nil {
			warn("%s", err)

			return nil
		}

		err = client.MakingIRODSConnections(numIRODSConnections, heartbeatFreq)
		if err != nil {
			return err
		}

		uploadStarts, uploadResults, skipResults, dfunc, err := handleReq(client, requests)
		if err != nil {
			return err
		} else if dfunc == nil {
			return nil
		}

		err = sendReq(client, uploadStarts, uploadResults, skipResults,
			minMBperSecondUploadSpeed, minTimeForUpload, maxStuckTime, appLogger)

		dfunc()

		errm := client.ClosedIRODSConnections()
		if errm != nil {
			return errm
		}

		if err != nil {
			warn("%s", err)

			return nil
		}
	}

	if putVerbose {
		info("all done, exiting")
	}

	return nil
}

func handleGetPut(
	requests []*transfer.Request,
	get func([]*transfer.Request) (*transfer.Putter, error),
) (chan *transfer.Request, chan *transfer.Request, chan *transfer.Request, func(), error) {
	if putVerbose {
		host, errh := os.Hostname()
		if errh != nil {
			return nil, nil, nil, nil, errh
		}

		info("client starting on host %s, pid %d", host, os.Getpid())
	}

	if len(requests) == 0 {
		warn("no requests to work on")

		return nil, nil, nil, nil, nil
	}

	if putVerbose {
		info("got %d requests to work on", len(requests))
	}

	p, err := get(requests)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	transferStarts, transferResults, skipResults := p.Put()

	return transferStarts, transferResults, skipResults, p.Cleanup, nil
}

func handlePut(client *server.Client, requests []*transfer.Request) (chan *transfer.Request,
	chan *transfer.Request, chan *transfer.Request, func(), error,
) {
	return handleGetPut(requests, func(requests []*transfer.Request) (*transfer.Putter, error) {
		p, err := getPutter(requests)
		if err != nil {
			return nil, err
		}

		handleCollections(client, p)

		return p, nil
	})
}

func getPutter(requests []*transfer.Request) (*transfer.Putter, error) {
	handler, err := baton.GetBatonHandler()
	if err != nil {
		return nil, err
	}

	p, err := transfer.New(handler, requests)
	if err != nil {
		return nil, err
	}

	return p, nil
}

func handleCollections(client *server.Client, p *transfer.Putter) {
	if putVerbose {
		info("will create collections")
	}

	if client == nil {
		createCollection(p)

		return
	}

	err := client.StartingToCreateCollections()
	if err != nil {
		warn("telling server we started to create collections failed: %s", err)
	}

	createCollection(p)

	if err = client.FinishedCreatingCollections(); err != nil {
		warn("telling server we finished creating collections failed: %s", err)
	}
}

func createCollection(p *transfer.Putter) {
	err := p.CreateCollections()
	if err != nil {
		warn("collection creation failed: %s", err)
	} else if putVerbose {
		info("collections created")
	}
}

func handlePutManualMode() error {
	requests, err := getRequestsFromFile(putFile, putMeta, putBase64)
	if err != nil {
		return err
	}

	_, uploadResults, skipResults, dfunc, err := handlePut(nil, requests)
	if err != nil {
		return err
	} else if dfunc == nil {
		return nil
	}

	defer dfunc()

	printResults("up", uploadResults, skipResults, len(requests), putVerbose)

	return nil
}

// getRequestsFromFile reads our 3 column file format from a file or STDIN and
// turns the info in to put Requests.
func getRequestsFromFile(file, meta string, base64Encoded bool) ([]*transfer.Request, error) {
	user, err := user.Current()
	if err != nil {
		return nil, err
	}

	return parsePutFile(file, meta, user.Username, fofnLineSplitter(false), base64Encoded)
}

func parsePutFile(path, meta, requester string, splitter bufio.SplitFunc, //nolint:funlen
	base64Encoded bool) ([]*transfer.Request, error) {
	defaultMeta, err := transfer.ParseMetaString(meta, nil)
	if err != nil {
		return nil, err
	}

	scanner, df, err := createScannerForFile(path, splitter)
	if err != nil {
		return nil, err
	}

	defer df()

	var prs []*transfer.Request //nolint:prealloc

	lineNum := 0
	for scanner.Scan() {
		lineNum++

		pr, err := parsePutFileLine(scanner.Text(), base64Encoded, lineNum, defaultMeta, requester)
		if err != nil {
			return nil, err
		} else if pr == nil {
			continue
		}

		prs = append(prs, pr)
	}

	serr := scanner.Err()
	if serr != nil {
		return nil, fmt.Errorf("failed to read whole file: %w", serr)
	}

	return prs, nil
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
func createScannerForFile(path string, splitter bufio.SplitFunc) (*bufio.Scanner, func(), error) {
	var reader io.Reader

	var dfunc func()

	if path == "-" {
		reader = os.Stdin
		dfunc = func() {}
	} else {
		var err error

		reader, dfunc, err = openFile(path)
		if err != nil {
			return nil, nil, err
		}
	}

	scanner := bufio.NewScanner(reader)

	scanner.Split(splitter)

	buf := make([]byte, maxScanTokenSize)
	scanner.Buffer(buf, maxScanTokenSize)

	return scanner, dfunc, nil
}

// openFile opens the given path, and returns it as an io.Reader along with a
// function you should defer (to close the file).
func openFile(path string) (io.Reader, func(), error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, fmt.Errorf("could not open file '%s': %w", path, err)
	}

	return file, func() {
		file.Close()
	}, nil
}

func parsePutFileLine(line string, base64Encoded bool, lineNum int,
	defaultMeta *transfer.Meta, requester string,
) (*transfer.Request, error) {
	cols := strings.Split(line, "\t")
	colsn := len(cols)

	if colsn < putFileMinCols || cols[0] == "" {
		return nil, nil //nolint:nilnil
	}

	if err := checkPutFileCols(colsn, lineNum); err != nil {
		return nil, err
	}

	var meta *transfer.Meta

	if colsn == putFileCols && cols[2] != "" {
		parsedMeta, err := transfer.ParseMetaString(cols[2], nil)
		if err != nil {
			return nil, fmt.Errorf("metadata error: %w", err)
		}

		meta = parsedMeta
	} else {
		meta = defaultMeta.Clone()
	}

	local, err := decodeBase64(cols[0], base64Encoded)
	if err != nil {
		return nil, err
	}

	remote, err := decodeBase64(cols[1], base64Encoded)
	if err != nil {
		return nil, err
	}

	return &transfer.Request{
		Local:     local,
		Remote:    remote,
		Requester: requester,
		Set:       putSet,
		Meta:      meta,
	}, nil
}

func checkPutFileCols(cols int, lineNum int) error {
	if cols > putFileCols {
		return fmt.Errorf("line %d has too many columns; check `ibackup put -h`", lineNum)
	}

	return nil
}

// decodeBase64 returns path as-is if isEncoded is false, or after base64
// decoding it if true.
func decodeBase64(path string, isEncoded bool) (string, error) {
	if !isEncoded {
		return path, nil
	}

	b, err := b64.StdEncoding.DecodeString(path)
	if err != nil {
		return "", err
	}

	return string(b), nil
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

func (r *results) update(req *transfer.Request) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.i++

	warnIfBad(req, r.i, r.total, r.verbose)

	switch req.Status { //nolint:exhaustive
	case transfer.RequestStatusFailed, transfer.RequestStatusUploading:
		r.fails++
	case transfer.RequestStatusMissing, transfer.RequestStatusOrphaned:
		r.missing++
	case transfer.RequestStatusReplaced:
		r.replaced++
	case transfer.RequestStatusUnmodified, transfer.RequestStatusHardlinkSkipped:
		r.skipped++
	case transfer.RequestStatusUploaded, transfer.RequestStatusWarning:
		r.uploads++
	}
}

// printResults reads from the given channels, outputs info about them to STDOUT
// and STDERR, then emits summary numbers. Supply the total number of requests
// made. Exits 1 if there were upload errors.
func printResults(upDown string, transferResults, skipResults chan *transfer.Request, total int, verbose bool) {
	r := &results{total: total, verbose: verbose}

	var wg sync.WaitGroup

	for _, ch := range []chan *transfer.Request{skipResults, transferResults} {
		wg.Add(1)

		go func(ch chan *transfer.Request) {
			defer wg.Done()

			for req := range ch {
				r.update(req)
			}
		}(ch)
	}

	wg.Wait()

	info("%d %sloaded (%d replaced); %d skipped; %d failed; %d missing",
		r.uploads+r.replaced, upDown, r.replaced, r.skipped, r.fails, r.missing)

	if r.fails > 0 {
		exitCode = 1
	}
}

// warnIfBad warns if this Request failed or is missing. If verbose, logs at
// info level the Request details.
func warnIfBad(r *transfer.Request, i, total int, verbose bool) {
	switch r.Status {
	case transfer.RequestStatusFailed, transfer.RequestStatusMissing, transfer.RequestStatusOrphaned,
		transfer.RequestStatusWarning:
		warn("[%d/%d] %s %s: %s", i, total, r.Local, r.Status, r.Error)
	case transfer.RequestStatusHardlinkSkipped:
		warn("[%d/%d] Hardlink skipped: %s\t%s", i, total, r.Local, r.Hardlink)
	default:
		if verbose {
			info("[%d/%d] %s %s", i, total, r.Local, r.Status)
		}
	}
}
