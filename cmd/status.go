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
	"github.com/spf13/cobra"
	"github.com/wtsi-hgi/ibackup/server"
	"github.com/wtsi-hgi/ibackup/set"
)

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
PendingDiscovery: the server will check which files in the set exist, and
  discover the contents of directories in the set.
PendingUpload: the server completed discovery, and has queued your files to be
  backed up, but none have been backed up yet. Most likely because other files
  from other sets are being backed up first.
Uploading: at least one file in your set has been uploaded to iRODS, but not all
  of them have.
Failing: at least one file in your set has failed to upload to iRODS after
  multiple retries, and the server has given up on it while it continues to try
  to upload other files in your set.
Complete: all files in your backup set were either missing, successfully
  uploaded, or permanently failed. Details of any failures will be given even
  without the --details option.
`,
	Run: func(cmd *cobra.Command, args []string) {
		ensureURLandCert()

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
		warn("no sets")

		return
	}

	for _, set := range sets {
		displaySet(set, details)
	}

	return
}

// getSetByName
func getSetByName(client *server.Client, user, name string) []*set.Set {
	got, err := client.GetSetByName(user, name)
	if err != nil {
		die(err.Error())
	}

	return []*set.Set{got}
}

func getSets(client *server.Client, user string) []*set.Set {
	sets, err := client.GetSets(user)
	if err != nil {
		die(err.Error())
	}

	return sets
}

// displaySet prints info about the given set to STDOUT, optionally with details
// on the status of every file in the set.
func displaySet(set *set.Set, details bool) {

}
