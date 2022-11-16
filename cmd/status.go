/*******************************************************************************
 * Copyright (c) 2022 Genome Research Ltd.
 *
 * Authors:
 *	- Sendu Bala <sb10@sanger.ac.uk>
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

	"github.com/dustin/go-humanize" //nolint:misspell
	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"
	"github.com/wtsi-hgi/ibackup/server"
	"github.com/wtsi-hgi/ibackup/set"
)

const dateShort = "06/01/02"

// options for this cmd.
var statusName string
var statusDetails bool

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
  uploaded, or permanently failed. Details of any failures will be given even
  without the --details option.
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

		status(client, statusName, statusDetails)
	},
}

func init() {
	RootCmd.AddCommand(statusCmd)

	// flags specific to this sub-command
	statusCmd.Flags().StringVarP(&statusName, "name", "n", "", "get status for just the set with this name")
	statusCmd.Flags().BoolVarP(&statusDetails, "details", "d", false,
		"in combination with --name, show the status of every file in the set")
}

// status does the main job of getting backup set status from the server.
func status(client *server.Client, name string, details bool) {
	user := currentUsername()

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

		displayDirs(getDirs(client, sets[i].ID()))

		failed, nonFailed := getEntries(client, sets[i].ID())

		displayFailedEntries(failed)

		if showNonFailedEntries {
			displayEntries(nonFailed)
		}

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
		cliPrint("Error: %s\n", s.Error)
	}

	cliPrint("Status: %s\n", s.Status)
	cliPrint("Discovery: %s\n", s.Discovered())
	cliPrint("Num files: %s\n", s.Count())
	cliPrint("Size files: %s\n", s.Size())
	cliPrint("Uploaded: %d\n", s.Uploaded)
	cliPrint("Failed: %d\n", s.Failed)
	cliPrint("Missing: %d\n", s.Missing)
}

// getDirs gets the dir entries for a set and returns their paths.
func getDirs(client *server.Client, setID string) []string {
	got, err := client.GetDirs(setID)
	if err != nil {
		die(err.Error())
	}

	paths := make([]string, len(got))

	for i, entry := range got {
		paths[i] = entry.Path
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

// displayFailedEntries prints info about the given failing file entries to
// STDOUT.
func displayFailedEntries(entries []*set.Entry) {
	if len(entries) == 0 {
		return
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Path", "Status", "Size", "Date", "Error"})

	appendEntryTableRows(table, entries, true)

	table.Render()
}

// appendEntryTableRows adds a row to the table for each entry, with an optional
// column for error details.
func appendEntryTableRows(table *tablewriter.Table, entries []*set.Entry, errorCol bool) {
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
			humanize.Bytes(entry.Size),
			date,
		}

		if errorCol {
			cols = append(cols, entry.LastError)
		}

		table.Append(cols)
	}
}

// displayEntries prints info about the given file entries with no errors to
// STDOUT.
func displayEntries(entries []*set.Entry) {
	if len(entries) == 0 {
		return
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Path", "Status", "Size", "Date"})

	appendEntryTableRows(table, entries, false)

	table.Render()
}
