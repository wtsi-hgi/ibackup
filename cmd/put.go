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
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/spf13/cobra"
	ex "github.com/wtsi-npg/extendo/v2"
	logs "github.com/wtsi-npg/logshim"
	"github.com/wtsi-npg/logshim-zerolog/zlog"
	"golang.org/x/term"
)

type Error string

func (e Error) Error() string { return string(e) }

const (
	errPutFailure = Error("one or more put operations failed")

	// maxScanTokenSize defines the size of bufio scan's buffer, enabling us to
	// parse very long lines - longer than the max length of of 2 file paths,
	// plus lots extra for metadata.
	maxScanTokenSize = 4096 * 1024

	putFileCols      = 3
	putFileMinCols   = 2
	putMetaParts     = 2
	numBatonClients  = 2
	minDirsForUnique = 2
)

const logLevel = logs.ErrorLevel

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

		err := put(requests)
		if err != nil {
			die("%s", err)
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

type PutRequest struct {
	Local  string
	Remote string
	Meta   map[string]string
}

func (p *PutRequest) ToRodsItem() *ex.RodsItem {
	avus := make([]ex.AVU, len(p.Meta))

	i := 0

	for k, v := range p.Meta {
		avus[i].Attr = k
		avus[i].Value = v
		i++
	}

	return &ex.RodsItem{
		IDirectory: filepath.Dir(p.Local),
		IFile:      filepath.Base(p.Local),
		IPath:      filepath.Dir(p.Remote),
		IName:      filepath.Base(p.Remote),
		IAVUs:      avus,
	}
}

func parsePutFile(path string, meta string) []*PutRequest {
	defaultMeta := parseMetaString(meta)
	scanner, df := createScannerForFile(path)

	defer df()

	var prs []*PutRequest //nolint:prealloc

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
	} else {
		reader, dfunc = openPutFile(path)
	}

	scanner := bufio.NewScanner(reader)
	buf := make([]byte, maxScanTokenSize)
	scanner.Buffer(buf, maxScanTokenSize)

	return scanner, dfunc
}

// openPutFile opens the given path, and returns it as an io.Reader along with a
// function you should defer (to close the file).
func openPutFile(path string) (io.Reader, func()) {
	file, err := os.Open(path)
	if err != nil {
		die("could not open file '%s': %s", path, err)
	}

	return file, func() {
		file.Close()
	}
}

func parsePutFileLine(line string, lineNum int, defaultMeta map[string]string) *PutRequest {
	cols := strings.Split(line, "\t")
	colsn := len(cols)

	if colsn < putFileMinCols || cols[0] == "" {
		return nil
	}

	checkPutFileCols(colsn, lineNum)

	local := makePutFilePathAbsolute(cols[0], lineNum)

	checkPutFilePathAbsolute(cols[1], lineNum)

	meta := defaultMeta

	if colsn == putFileCols && cols[2] != "" {
		meta = parseMetaString(cols[2])
	}

	return &PutRequest{local, cols[1], meta}
}

func checkPutFileCols(cols int, lineNum int) {
	if cols > putFileCols {
		die("line %d has too many columns; check `ibackup put -h`", lineNum)
	}
}

func makePutFilePathAbsolute(path string, lineNum int) string {
	local, err := filepath.Abs(path)
	if err != nil {
		die("line %d has an invalid local path '%s': %s", lineNum, path, err)
	}

	return local
}

func checkPutFilePathAbsolute(path string, lineNum int) {
	if !filepath.IsAbs(path) {
		die("line %d has non-absolute iRODS path '%s'", lineNum, path)
	}
}

func put(requests []*PutRequest) error {
	clients, df, err := getBatonClients(numBatonClients)
	if err != nil {
		return err
	}

	defer df()

	err = createCollections(clients[0], requests)
	if err != nil {
		return err
	}

	errCh := putFilesInIRODS(clients, requests)

	var hadErrors bool

	for err := range errCh {
		warn("error: %s", err)

		hadErrors = true
	}

	if hadErrors {
		return errPutFailure
	}

	return nil
}

func setupLogger() logs.Logger {
	var writer io.Writer
	if term.IsTerminal(int(os.Stdout.Fd())) {
		writer = zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}
	} else {
		writer = os.Stderr
	}

	logger := zlog.New(zerolog.SyncWriter(writer), logLevel)

	return logs.InstallLogger(logger)
}

func getBatonClients(n int) ([]*ex.Client, func(), error) {
	setupLogger()

	pool := ex.NewClientPool(ex.DefaultClientPoolParams, "")

	clientCh, err := getClientsFromPoolConcurrently(pool, n)

	clients := make([]*ex.Client, n)

	i := 0

	for client := range clientCh {
		clients[i] = client
		i++
	}

	return clients, stopBatonClientsAndClosePool(pool, clients), err
}

func getClientsFromPoolConcurrently(pool *ex.ClientPool, n int) (chan *ex.Client, error) {
	clientCh := make(chan *ex.Client, n)
	errCh := make(chan error, n)

	var wg sync.WaitGroup

	for i := 0; i < n; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			client, err := pool.Get()
			if err != nil {
				errCh <- err
			}

			clientCh <- client
		}()
	}

	wg.Wait()
	close(errCh)
	close(clientCh)

	return clientCh, <-errCh
}

// stopBatonClientsAndClosePool returns a function you can defer that will stop
// the given clients and close the pool.
func stopBatonClientsAndClosePool(pool *ex.ClientPool, clients []*ex.Client) func() {
	return func() {
		for _, client := range clients {
			errs := client.Stop()
			if errs != nil {
				warn("client stop failure: %s", errs)
			}
		}

		pool.Close()
	}
}

// createCollections creates all the unique leaf collections in the Remotes of
// the given requests. Double-checks they really got created, because it doesn't
// always seem to work.
func createCollections(client *ex.Client, requests []*PutRequest) error {
	dirs := getUniqueRequestLeafCollections(requests)
	args := ex.Args{}

	for _, dir := range dirs {
		ri := ex.RodsItem{IPath: dir}

		if _, err := client.ListItem(args, ri); err == nil {
			continue
		}

		if _, err := client.MkDir(args, ri); err != nil {
			return err
		}

		if _, err := client.ListItem(args, ri); err != nil {
			return err
		}
	}

	return nil
}

func getUniqueRequestLeafCollections(requests []*PutRequest) []string {
	dirs := getSortedRequestCollections(requests)
	if len(dirs) < minDirsForUnique {
		return dirs
	}

	var uniqueLeafs []string //nolint:prealloc

	previous, dirs := dirs[0], dirs[1:]

	for _, dir := range dirs {
		if dir == previous || strings.HasPrefix(dir, previous+"/") {
			previous = dir

			continue
		}

		uniqueLeafs = append(uniqueLeafs, previous)
		previous = dir
	}

	if noLeavesOrNewLeaf(uniqueLeafs, previous) {
		uniqueLeafs = append(uniqueLeafs, previous)
	}

	return uniqueLeafs
}

func noLeavesOrNewLeaf(uniqueLeafs []string, last string) bool {
	return len(uniqueLeafs) == 0 || last != uniqueLeafs[len(uniqueLeafs)-1]
}

func getSortedRequestCollections(requests []*PutRequest) []string {
	dirs := make([]string, len(requests))

	for i, request := range requests {
		dirs[i] = filepath.Dir(request.Remote)
	}

	sort.Strings(dirs)

	return dirs
}

func putFilesInIRODS(clients []*ex.Client, requests []*PutRequest) chan error {
	errCh := make(chan error, len(requests))
	metaCh := make(chan ex.RodsItem, len(requests))

	metaDoneCh := applyMetadataConcurrently(clients[1], metaCh, errCh)

	for _, pr := range requests {
		items, errp := clients[0].Put(
			ex.Args{
				Force:    true,
				Checksum: true,
			},
			*pr.ToRodsItem(),
		)
		if errp != nil {
			errCh <- errp

			continue
		}

		info("%s", items[0].IChecksum)

		metaCh <- items[0]
	}

	close(metaCh)
	<-metaDoneCh
	close(errCh)

	return errCh
}

func applyMetadataConcurrently(client *ex.Client, metaCh chan ex.RodsItem, errCh chan error) chan struct{} {
	doneCh := make(chan struct{})

	go func() {
		for item := range metaCh {
			errm := replaceMetadata(client, &item, item.IAVUs)
			if errm != nil {
				errCh <- errm
			}
		}

		close(doneCh)
	}()

	return doneCh
}

func replaceMetadata(client *ex.Client, item *ex.RodsItem, avus []ex.AVU) error {
	it, err := client.ListItem(ex.Args{AVU: true}, *item)
	if err != nil {
		return err
	}

	item.IAVUs = it.IAVUs
	currentAVUs := item.IAVUs

	toKeep := ex.SetIntersectAVUs(avus, currentAVUs)

	if err = removeUnneededAVUs(client, item, avus, currentAVUs, toKeep); err != nil {
		return err
	}

	toAdd := ex.SetDiffAVUs(avus, toKeep)

	if len(toAdd) > 0 {
		add := ex.CopyRodsItem(*item)
		add.IAVUs = toAdd

		if _, err := client.MetaAdd(ex.Args{}, add); err != nil {
			return err
		}
	}

	return nil
}

func removeUnneededAVUs(client *ex.Client, item *ex.RodsItem, newAVUs, currentAVUs, toKeep []ex.AVU) error {
	repAttrs := make(map[string]struct{})
	for _, avu := range newAVUs {
		repAttrs[avu.Attr] = struct{}{}
	}

	var toRemove []ex.AVU

	for _, avu := range currentAVUs {
		if _, ok := repAttrs[avu.Attr]; ok && !ex.SearchAVU(avu, toKeep) {
			toRemove = append(toRemove, avu)
		}
	}

	rem := ex.CopyRodsItem(*item)
	rem.IAVUs = toRemove

	if len(toRemove) > 0 {
		if _, err := client.MetaRem(ex.Args{}, rem); err != nil {
			return err
		}
	}

	return nil
}
