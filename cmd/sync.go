/*******************************************************************************
 * Copyright (c) 2025 Genome Research Ltd.
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
	"github.com/spf13/cobra"
)

var syncUser string
var syncSetName string
var syncDelete bool

// syncCmd represents the sync command.
var syncCmd = &cobra.Command{
	Use:   "sync",
	Short: "Sync local files with iRODS",
	Long: `Sync local files with iRODS.

Having used 'ibackup add' to add the details of a backup sets, use this command
to check for local changes and do any needed uploads in that set. Provide the
required --name to choose the set.

This re-triggers discovery of files in any directories specified as part of your
set. It's like starting over from the beginning, though any files already
uploaded to iRODS and unchanged locally will not be uploaded again. This is also
the same as what happens if you had set the --monitor option and the time period
had elapsed.

If the --delete option is given, then any files that are no longer present
locally will be deleted from iRODS. This is the same as what happens if you had
set the --monitor option along with --monitor-removals.

You need to supply the ibackup server's URL in the form domain:port (using the
IBACKUP_SERVER_URL environment variable, or overriding that with the --url
argument) and if necessary, the certificate (using the IBACKUP_SERVER_CERT
environment variable, or overriding that with the --cert argument).

If you are the user who started the ibackup server, you can use the --user
option to retry the given requestor's backup sets, instead of your own.	
`,
	RunE: func(_ *cobra.Command, _ []string) error {
		ensureURLandCert()

		client, err := newServerClient(serverURL, serverCert)
		if err != nil {
			return err
		}

		rSet, err := getRequestedSet(client, syncUser, syncSetName)
		if err != nil {
			return err
		}

		return client.TriggerDiscovery(rSet.ID(), syncDelete)
	},
}

func init() {
	RootCmd.AddCommand(syncCmd)

	syncCmd.Flags().StringVar(&syncUser, "user", currentUsername(), helpTextuser)
	syncCmd.Flags().StringVarP(&syncSetName, "name", "n", "", helpTextName)
	syncCmd.Flags().BoolVar(&syncDelete, "delete", false,
		"delete extraneous files from iRODS")

	if err := syncCmd.MarkFlagRequired("name"); err != nil {
		die(err)
	}
}
