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
	"fmt"
	"strings"
	"time"

	"github.com/dustin/go-humanize" //nolint:misspell
	"github.com/spf13/cobra"
	"github.com/wtsi-hgi/ibackup/server"
	"github.com/wtsi-hgi/ibackup/set"
)

const dateShort = "06/01/02"
const bytesInMiB = 1024 * 1024
const hundredForPercentCalc float64 = 100

// options for this cmd.
var statusUser string
var statusName string
var statusDetails bool
var statusIncomplete bool

// statusCmd represents the status command.
var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Get the status of your backup sets",
	Long: `Get the status of your backup sets.

Having used 'ibackup add' to add the details of one or more backup sets, use
this command to get the current backup status of your sets. Provide --name to
get the status of just that set, and --details to get the individual backup
status of every file in the set (only possible with a --name).

When not using --name, provide --incomplete to only see currently incomplete
sets. This will include sets with failures but everything else uploaded (shown
with a "complete" status, see below), but exclude sets where everything is
missing or uploaded.

You need to supply the ibackup server's URL in the form domain:port (using the
IBACKUP_SERVER_URL environment variable, or overriding that with the --url
argument) and if necessary, the certificate (using the IBACKUP_SERVER_CERT
environment variable, or overriding that with the --cert argument).

Once a backup set has been added, it goes through these statuses:
pending discovery: the server will check which files in the set exist, and
  discover the contents of directories in the set.
pending upload: the server completed discovery, and has queued your files to be
  backed up, but none have been backed up yet. Most likely because other files
  from other sets are being backed up first.
uploading: at least one file in your set has been uploaded to iRODS, but not all
  of them have.
failing: at least one file in your set has failed to upload to iRODS after
  multiple retries, and the server has given up on it while it continues to try
  to upload other files in your set.
complete: all files in your backup set were either missing, successfully
  uploaded, or failed.

With --details, you'll see tab-separated columns of Path, Status, Size, Date
and Error, with one file per line.

Without --details, you'll still see these details for files that failed their
upload.

If you are the user who started the ibackup server, you can use the --user
option to get the status of a given requestor's backup sets, instead of your
own.
`,
	Run: func(cmd *cobra.Command, args []string) {
		ensureURLandCert()

		if statusDetails && statusName == "" {
			die("--details can only be used with --name")
		}

		if statusName != "" && statusIncomplete {
			die("--incomplete and --name can't be used together")
		}

		client, err := newServerClient(serverURL, serverCert)
		if err != nil {
			die(err.Error())
		}

		status(client, statusIncomplete, statusUser, statusName, statusDetails)
	},
}

func init() {
	RootCmd.AddCommand(statusCmd)

	// flags specific to this sub-command
	statusCmd.Flags().StringVar(&statusUser, "user", currentUsername(),
		"pretend to be this user (only works if you started the server)")
	statusCmd.Flags().StringVarP(&statusName, "name", "n", "",
		"get status for just the set with this name")
	statusCmd.Flags().BoolVarP(&statusDetails, "details", "d", false,
		"in combination with --name, show the status of every file in the set")
	statusCmd.Flags().BoolVarP(&statusIncomplete, "incomplete", "i", false,
		"only show currently incomplete sets")
}

// status does the main job of getting backup set status from the server.
func status(client *server.Client, incomplete bool, user, name string, details bool) {
	qs, err := client.GetQueueStatus()
	if err != nil {
		die("unable to get server queue status: %s", err)
	}

	displayQueueStatus(qs)

	var sets []*set.Set

	if name != "" {
		sets = getSetByName(client, user, name)
	} else {
		sets = getSets(client, incomplete, user)
	}

	if len(sets) == 0 {
		warn("no backup sets")

		return
	}

	displaySets(client, sets, details)
}

// displayQueueStatus prints out QStatus in a nice way. If user is admin, also
// shows details of stuck requests.
func displayQueueStatus(qs *server.QStatus) {
	info("Global put queue status: %d queued; %d reserved to be worked on; %d failed",
		qs.Total, qs.Reserved, qs.Failed)
	info("Global put client status (/%d): %d creating collections; %d currently uploading",
		numPutClients, qs.CreatingCollections, qs.Uploading)

	if qs.Stuck != nil {
		_, err := getPasswordFromServerTokenFile()
		if err == nil {
			for _, r := range qs.Stuck {
				warn("set '%s' for %s [%s => %s] %s", r.Set, r.Requester, r.Local, r.Remote, r.Stuck)
			}
		}
	}
}

// getSetByName gets a set with the given name owned by the given user. Dies
// on error.
func getSetByName(client *server.Client, user, name string) []*set.Set {
	got, err := client.GetSetByName(user, name)
	if err != nil {
		die(err.Error())
	}

	return []*set.Set{got}
}

// getSets gets all or incomplete sets belonging to the given user. Dies on
// error.
func getSets(client *server.Client, incomplete bool, user string) []*set.Set {
	sets, err := client.GetSets(user)
	if err != nil {
		die(err.Error())
	}

	if incomplete {
		sets = incompleteSets(sets)
	}

	return sets
}

// incompleteSets returns the incomplete sets from amongst the given sets.
// "incomplete" includes sets with status complete, but failures.
func incompleteSets(sets []*set.Set) []*set.Set {
	var incomplete []*set.Set

	for _, s := range sets {
		if s.Status != set.Complete || s.Failed > 0 {
			incomplete = append(incomplete, s)
		}
	}

	return incomplete
}

// displaySets prints info about the given sets to STDOUT. Failed entry details
// will also be printed, and optionally non-failed.
func displaySets(client *server.Client, sets []*set.Set, showNonFailedEntries bool) {
	l := len(sets)

	for i, forDisplay := range sets {
		cliPrint("\n")
		displaySet(forDisplay)

		displayDirs(getDirs(client, forDisplay.ID()))

		if showNonFailedEntries {
			displayAllEntries(client, forDisplay)
		} else {
			displayFailedEntries(client, forDisplay)
		}

		if i != l-1 {
			cliPrint("\n-----\n")
		}
	}
}

// displaySet prints info about the given set to STDOUT.
func displaySet(s *set.Set) {
	cliPrint("Name: %s\n", s.Name)
	cliPrint("Transformer: %s\n", s.Transformer)
	cliPrint("Monitored: %v; Archive: %v\n", s.Monitor, s.DeleteLocal)

	if s.Description != "" {
		cliPrint("Description: %s\n", s.Description)
	}

	if s.Error != "" {
		cliPrint("Status: unable to proceed\n")
		cliPrint("Error: %s\n", s.Error)
	} else {
		cliPrint("Status: %s\n", s.Status)
	}

	cliPrint("Discovery: %s\n", s.Discovered())
	cliPrint("Num files: %s; Size files: %s\n", s.Count(), s.Size())
	cliPrint("Uploaded: %d; Failed: %d; Missing: %d\n", s.Uploaded, s.Failed, s.Missing)

	switch s.Status {
	case set.Complete:
		cliPrint("Completed in: %s\n", s.LastCompleted.Sub(s.StartedDiscovery))
	case set.Uploading:
		displayETA(s)
	default:
	}
}

// displayETA prints info about ETA for the given currently uploading set to
// STDOUT.
func displayETA(s *set.Set) {
	var (
		basedOn   string
		total     uint64
		done      uint64
		remaining float64
		speed     float64
		unit      string
		timeUnit  time.Duration
	)

	if s.LastCompletedSize > 0 {
		basedOn, unit, total, done, remaining, speed, timeUnit = determineETADetailsFromSize(s)
	} else {
		basedOn, unit, total, done, remaining, speed, timeUnit = determineETADetailsFromCount(s)
	}

	if done == 0 {
		return
	}

	percentComplete := (hundredForPercentCalc / float64(total)) * float64(done)
	eta := time.Duration((remaining / speed) * float64(timeUnit))

	cliPrint("%.2f%% complete (based on %s); %.2f %s; ETA: %s\n",
		percentComplete, basedOn, speed, unit, eta.Round(time.Second))
}

func determineETADetailsFromSize(s *set.Set) (basedOn, unit string, total, done uint64, //nolint:unparam
	remaining, speed float64, timeUnit time.Duration) {
	basedOn = "last completed size"
	total = s.LastCompletedSize
	done = s.SizeFiles
	remaining = bytesToMB(total - done)

	if done == 0 {
		return
	}

	speed = bytesToMB(done) / time.Since(s.LastDiscovery).Seconds()
	unit = "MB/s"
	timeUnit = time.Second

	return
}

func determineETADetailsFromCount(s *set.Set) (basedOn, unit string, total, done uint64, //nolint:unparam
	remaining, speed float64, timeUnit time.Duration) {
	basedOn = "number of files"
	total = s.NumFiles
	done = s.Uploaded
	remaining = float64(total - done)

	if done == 0 {
		return
	}

	speed = float64(done) / time.Since(s.LastDiscovery).Hours()
	unit = "files/hr"
	timeUnit = time.Hour

	return
}

// bytesToMB converts bytes to number of MB.
func bytesToMB(bytes uint64) float64 {
	return float64(bytes) / bytesInMiB
}

// getDirs gets the dir entries for a set and returns their paths. If the dir is
// missing, the path is appended with some text mentioning that.
func getDirs(client *server.Client, setID string) []string {
	got, err := client.GetDirs(setID)
	if err != nil {
		die(err.Error())
	}

	paths := make([]string, len(got))

	for i, entry := range got {
		paths[i] = entry.Path

		if entry.Status == set.Missing {
			paths[i] += " (missing)"
		}
	}

	return paths
}

// displayDirs prints out directories one per line with a header, if dirs is not
// empty.
func displayDirs(dirs []string) {
	if len(dirs) > 0 {
		cliPrint("Directories:\n")

		for _, dir := range dirs {
			cliPrint("  %s\n", dir)
		}
	}
}

// displayFailedEntries prints out details about up to 10 failed entries in the
// given set.
func displayFailedEntries(client *server.Client, given *set.Set) {
	failed, skipped, err := client.GetFailedFiles(given.ID())
	if err != nil {
		die(err.Error())
	}

	displayEntries(failed)

	if skipped > 0 {
		cliPrint("[... and %d others]\n", skipped)
	}
}

// displayAllEntries prints out details about all entries in the given
// set.
func displayAllEntries(client *server.Client, given *set.Set) {
	all, err := client.GetFiles(given.ID())
	if err != nil {
		die(err.Error())
	}

	displayEntries(all)
}

// displayEntries prints info about the given file entries to STDOUT.
func displayEntries(entries []*set.Entry) {
	if len(entries) == 0 {
		return
	}

	printEntriesHeader()

	for _, entry := range entries {
		var date string

		if entry.LastAttempt.IsZero() {
			date = "-"
		} else {
			date = entry.LastAttempt.Format(dateShort)
		}

		cols := []string{
			entry.Path,
			entry.Status.String(),
			humanize.IBytes(entry.Size),
			fmt.Sprintf("%d", entry.Attempts),
			date,
			entry.LastError,
		}

		cliPrintRaw(strings.Join(cols, "\t"))
		cliPrintRaw("\n")
	}
}

// printEntriesHeader prints a header for a subsequent 6 column output of entry
// details.
func printEntriesHeader() {
	cliPrint("\n")
	cliPrint(strings.Join([]string{"Path", "Status", "Size", "Attempts", "Date", "Error"}, "\t"))
	cliPrint("\n")
}
