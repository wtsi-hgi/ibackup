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
	"strings"

	"github.com/dustin/go-humanize" //nolint:misspell
	"github.com/spf13/cobra"
	"github.com/wtsi-hgi/ibackup/server"
	"github.com/wtsi-hgi/ibackup/set"
)

const dateShort = "06/01/02"

// options for this cmd.
var statusName string
var statusDetails bool
var statusUser string

// statusCmd represents the status command.
var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Get the status of your backup sets",
	Long: `Get the status of your backup sets.

Having used 'ibackup add' to add the details of one or more backup sets, use
this command to get the current backup status of your sets. Provide --name to
get the status of just that set, and --details to get the individual backup
status of every file in the set (only possible with a --name).

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
  uploaded, or permanently failed.

With --details, you'll see tab-separated columns of Path, Status, Size, Date
and Error, with one file per line, and those with errors appearing first.

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

		client, err := newServerClient(serverURL, serverCert)
		if err != nil {
			die(err.Error())
		}

		status(client, statusUser, statusName, statusDetails)
	},
}

func init() {
	RootCmd.AddCommand(statusCmd)

	// flags specific to this sub-command
	statusCmd.Flags().StringVarP(&statusName, "name", "n", "",
		"get status for just the set with this name")
	statusCmd.Flags().BoolVarP(&statusDetails, "details", "d", false,
		"in combination with --name, show the status of every file in the set")
	statusCmd.Flags().StringVar(&statusUser, "user", "",
		"pretend to be this user (only works if you started the server)")
}

// status does the main job of getting backup set status from the server.
func status(client *server.Client, user, name string, details bool) {
	if user == "" {
		user = currentUsername()
	}

	var sets []*set.Set

	if name != "" {
		sets = getSetByName(client, user, name)
	} else {
		sets = getSets(client, user)
	}

	if len(sets) == 0 {
		warn("no backup sets")

		return
	}

	displaySets(client, sets, details)
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

// getSets gets all the sets belonging to the given user. Dies on error.
func getSets(client *server.Client, user string) []*set.Set {
	sets, err := client.GetSets(user)
	if err != nil {
		die(err.Error())
	}

	return sets
}

// displaySets prints info about the given sets to STDOUT. Failed entry details
// will also be printed, and optionally non-failed.
func displaySets(client *server.Client, sets []*set.Set, showNonFailedEntries bool) {
	l := len(sets)

	for i, forDisplay := range sets {
		displaySet(forDisplay)

		displayDirs(getDirs(client, forDisplay.ID()))

		displayEntriesIfFailed(client, forDisplay, showNonFailedEntries)

		if i != l-1 {
			cliPrint("\n-----\n\n")
		}
	}
}

// displaySet prints info about the given set to STDOUT.
func displaySet(s *set.Set) {
	cliPrint("Name: %s\n", s.Name)
	cliPrint("Transformer: %s\n", s.Transformer)
	cliPrint("Monitored: %v\n", s.Monitor)

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
	cliPrint("Num files: %s\n", s.Count())
	cliPrint("Size files: %s\n", s.Size())
	cliPrint("Uploaded: %d\n", s.Uploaded)
	cliPrint("Failed: %d\n", s.Failed)
	cliPrint("Missing: %d\n", s.Missing)
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

// displayEntriesIfFailed prints out details about failed entries in the given
// set. Also prints other details if showNonFailed is true.
func displayEntriesIfFailed(client *server.Client, given *set.Set, showNonFailed bool) {
	if given.Error == "" && !showNonFailed {
		return
	}

	failed, nonFailed := getEntries(client, given.ID())

	printed := printEntriesHeader(failed)
	displayEntries(failed)

	if showNonFailed {
		if !printed {
			printEntriesHeader(nonFailed)
		}

		displayEntries(nonFailed)
	}
}

// getEntries gets the file entries for a set. It returns ones that have errors,
// and then all the others.
func getEntries(client *server.Client, setID string) ([]*set.Entry, []*set.Entry) {
	got, err := client.GetFiles(setID)
	if err != nil {
		die(err.Error())
	}

	var failed, others []*set.Entry

	for _, entry := range got {
		if entry.Status != set.Uploaded && entry.LastError != "" {
			failed = append(failed, entry)
		} else {
			others = append(others, entry)
		}
	}

	return failed, others
}

// printEntriesHeader prints a header for a subsequent 5 column output of entry
// details, but only if there are more than 0 entries. Returns true if it
// printed the header.
func printEntriesHeader(entries []*set.Entry) bool {
	if len(entries) == 0 {
		return false
	}

	cliPrint("\n")
	cliPrint(strings.Join([]string{"Path", "Status", "Size", "Date", "Error"}, "\t"))
	cliPrint("\n")

	return true
}

// displayEntries prints info about the given file entries to STDOUT.
func displayEntries(entries []*set.Entry) {
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
			date,
			entry.LastError,
		}

		cliPrint(strings.Join(cols, "\t"))
		cliPrint("\n")
	}
}
