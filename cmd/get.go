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
	"os"
	"time"

	"github.com/spf13/cobra"
	"github.com/wtsi-hgi/ibackup/baton"
	"github.com/wtsi-hgi/ibackup/transfer"
)

var (
	overwrite       bool
	hardlinksNormal bool
)

// putCmd represents the put command.
var getCmd = &cobra.Command{
	Use:   "get",
	Short: "Safely copy files from iRODS to a local filesystem",
	Long: `Safely copy files from iRODS to a local filesystem.

Use the -f arg (defaults to STDIN) to provide a tab-delimited file with these 2
columns:
/absolute/path/of/local/file /remote/irods/path/of/file

Because local and remote paths could contain tabs and newlines in their names,
you can base64 encode them and use the --base64 option.

get will then efficiently copy all column 2 paths in iRODS to column 1
locations, using a single connection, sequentially. (This makes it orders of
magnitude faster than running 'iget' for each of many small files.)

get will always confirm the checksum stored on the server side in flight, but
will not confirm the data stored on disk.

It will not overwrite existing local files unless the -o/--overwrite flag is
provided. Remote files with the same mtime as a local file will always be
skipped so it should be safe to run get multiple times without redownloading
files many times.

Remote files that are denoted as hardlinks will be skipped with a warning
containing the intended local path and the remote path that contains the actual
hardlink data.

If remote file paths are missing, warnings about them will be logged, but this
cmd will still exit 0. It only exits non-zero on failure to download an existing
remote file.

Directories for your local paths in column 1 will be automatically created if
necessary.

(The ibackup server also calls this command with the --server and --url options
instead of a 2 column -f file, which makes this command work on restore requests
stored in the server.)

You need to have the baton commands in your PATH for this to work.

You also need to have your iRODS environment set up and must be authenticated
with iRODS (eg. run 'iinit') before running this command. If 'iget' works for
you, so should this.`,
	Run: func(_ *cobra.Command, _ []string) {
		if putLog != "" {
			putVerbose = true

			logToFile(putLog)
		}

		if serverMode() {
			handleGetServerMode(time.Now())
		} else {
			handleGetManualMode()
		}
	},
}

func init() {
	RootCmd.AddCommand(getCmd)

	// flags specific to this sub-command
	getCmd.Flags().StringVarP(&putFile, "file", "f", "-",
		"tab-delimited /local/path /irods/path file (- means STDIN)")
	getCmd.Flags().BoolVarP(&putVerbose, "verbose", "v", false,
		"report download status of every file")
	getCmd.Flags().BoolVarP(&putBase64, "base64", "b", false,
		"input paths are base64 encoded")
	getCmd.Flags().BoolVarP(&putServerMode, "server", "s", false,
		"pull requests from the server instead of --file; only usable by the user who started the server")
	getCmd.Flags().StringVarP(&putLog, "log", "l", "",
		"log to the given file (implies --verbose)")
	getCmd.Flags().DurationVar(&maxStuckTime, "stuck-timeout", defaultMaxStuckTime,
		"override the default stuck wait time (1h)")
	getCmd.Flags().BoolVarP(&overwrite, "overwrite", "o", false,
		"overwrite local files")
	getCmd.Flags().BoolVar(&hardlinksNormal, "hardlinks_as_normal", false,
		"download hardlinked files") //nolint:misspell
}

func handleGetServerMode(_ time.Time) {
	// handleServerMode(started, (*server.Client).GetSomeDownloadRequests,
	// handleGet, (*server.Client).SendGetResultsToServer)
	dief("server mode is not currently implemented")
}

func getGetter(requests []*transfer.Request) *transfer.Putter {
	handler, err := baton.GetBatonHandler()
	if err != nil {
		die(err)
	}

	p, err := transfer.NewGetter(handler, requests, overwrite, hardlinksNormal)
	if err != nil {
		die(err)
	}

	return p
}

func handleGetManualMode() {
	requests, err := getRequestsFromFile(putFile, "", putBase64)
	if err != nil {
		die(err)
	}

	_, downloadResults, skipResults, dfunc := handleGet(requests)

	exitCode := printResults(
		"down", downloadResults, skipResults,
		len(requests), putVerbose, nil,
	)

	dfunc()

	if exitCode != 0 {
		os.Exit(1)
	}
}

func handleGet(requests []*transfer.Request) (chan *transfer.Request, chan *transfer.Request,
	chan *transfer.Request, func()) {
	return handleGetPut(requests, getGetter)
}
