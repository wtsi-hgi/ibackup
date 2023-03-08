/*******************************************************************************
 * Copyright (c) 2023 Genome Research Ltd.
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
)

// options for this cmd.
var retryUser string
var retrySet string
var retryAll bool
var retryFailed bool

// retryCmd represents the retry command.
var retryCmd = &cobra.Command{
	Use:   "retry",
	Short: "Retry file uploads in a set",
	Long: `Retry file uploads in a set.

Having used 'ibackup add' to add the details of a backup sets, use this command
to retry file uploads in that set. Provide the required --name to choose the
set.

You can retry --all uploads in the set. This re-triggers discovery of files in
any directories specified as part of your set. It's like starting over from the
beginning, though any files already uploaded to iRODS and unchanged locally will
not be uploaded again.

Alternatively, you can retry just --failed uploads in the set.

You need to supply the ibackup server's URL in the form domain:port (using the
IBACKUP_SERVER_URL environment variable, or overriding that with the --url
argument) and if necessary, the certificate (using the IBACKUP_SERVER_CERT
environment variable, or overriding that with the --cert argument).

If you are the user who started the ibackup server, you can use the --user
option to retry the given requestor's backup sets, instead of your own.
`,
	Run: func(cmd *cobra.Command, args []string) {
		ensureURLandCert()

		if retrySet == "" {
			die("--name is required")
		}

		if retryAll && retryFailed {
			die("--all and --failed are mutually exclusive")
		}

		if !retryAll && !retryFailed {
			die("at least one of --all and --failed are required")
		}

		client, err := newServerClient(serverURL, serverCert)
		if err != nil {
			die(err.Error())
		}

		retrySetUploads(client, retryUser, retrySet, retryAll)
	},
}

func init() {
	RootCmd.AddCommand(retryCmd)

	// flags specific to this sub-command
	retryCmd.Flags().StringVarP(&retryUser, "user", "u", currentUsername(), "set belongs to this user")
	retryCmd.Flags().StringVarP(&retrySet, "name", "n", "", "name of set to retry")
	retryCmd.Flags().BoolVarP(&retryAll, "all", "a", false,
		"retry all uploads in the --name'd set")
	retryCmd.Flags().BoolVarP(&retryFailed, "failed", "f", false,
		"retry only failed uploads in the --name'd set")
}

func retrySetUploads(client *server.Client, requester, setName string, all bool) {
	set, err := client.GetSetByName(requester, setName)
	if err != nil {
		die(err.Error())
	}

	if all {
		if errt := client.TriggerDiscovery(set.ID()); err != nil {
			die(errt.Error())
		}

		info("initiated retry of set %s", setName)

		return
	}

	retried, err := client.RetryFailedSetUploads(set.ID())
	if err != nil {
		die(err.Error())
	}

	if retried == 0 {
		warn("no failed uploads to retry in set '%s'", setName)
	} else {
		info("initated retry of %d failed entries", retried)
	}
}
