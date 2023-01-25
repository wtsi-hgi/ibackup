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
	"github.com/wtsi-hgi/ibackup/put"
	"github.com/wtsi-hgi/ibackup/server"
)

// options for this cmd.
var queueAll bool
var queueUser string
var queueSet string
var queuePath string
var queueKick bool
var queueDelete bool

// queueCmd represents the queue command.
var queueCmd = &cobra.Command{
	Use:   "queue",
	Short: "Administer the server queue",
	Long: `Administer the server queue.

The user who started the server can use this sub-command to retry or remove
buried items in the server's global put queue.

You need to supply the ibackup server's URL in the form domain:port (using the
IBACKUP_SERVER_URL environment variable, or overriding that with the --url
argument) and if necessary, the certificate (using the IBACKUP_SERVER_CERT
environment variable, or overriding that with the --cert argument).

You can --retry or --delete --all currently buried requests, or specify which
ones by setting --user and --name (apply to all buried requests in the set with
that name belonging to that user). You can also add a local --path to limit to a
single file in that set.

Specifying nothing displays details about all currently buried requests in the
queue.
`,
	Run: func(cmd *cobra.Command, args []string) {
		ensureURLandCert()

		_, err := getPasswordFromServerTokenFile()
		if err != nil {
			die("Only the user who started the server can use this sub-command.")
		}

		if queueDelete && queueKick {
			die("-d and -r are mutually exclusive")
		}

		client, err := newServerClient(serverURL, serverCert)
		if err != nil {
			die(err.Error())
		}

		if queueDelete || queueKick {
			if queuePath != "" && (queueUser == "" || queueSet == "") {
				die("--path needs --user and --name")
			}

			if queueSet != "" && queueUser == "" {
				die("--name needs --user")
			}

			if queueAll && queueUser != "" {
				die("--all is mutually exclusive from --user, --name and --path")
			}

			if queueUser == "" && !queueAll {
				die("at least one of --all or --user must be provided")
			}

			bf := &server.BuriedFilter{
				User: queueUser,
				Set:  queueSet,
				Path: queuePath,
			}

			handleBuried(client, queueDelete, queueKick, bf)
		} else {
			rs, err := client.BuriedRequests()
			if err != nil {
				die("unable to get buried requests: %s", err)
			}

			displayBuriedRequests(rs)
		}
	},
}

func init() {
	RootCmd.AddCommand(queueCmd)

	// flags specific to this sub-command
	queueCmd.Flags().StringVarP(&queueUser, "user", "u", "", "limit -d/-r to sets of this user")
	queueCmd.Flags().StringVarP(&queueSet, "name", "n", "", "limit -d/-r to this backup set")
	queueCmd.Flags().StringVarP(&queuePath, "path", "p", "", "limit -d/-r to this local file in the set")
	queueCmd.Flags().BoolVarP(&queueAll, "all", "a", false,
		"apply -d/-r to all buried items in the queue")
	queueCmd.Flags().BoolVarP(&queueDelete, "delete", "d", false,
		"delete certain buried items in the queue")
	queueCmd.Flags().BoolVarP(&queueKick, "retry", "r", false,
		"retry certain buried items in the queue")
}

func displayBuriedRequests(rs []*put.Request) {
	if len(rs) == 0 {
		warn("no buried requests")

		return
	}

	cliPrint("Requester\tSet\tPath\tError\n")

	for _, r := range rs {
		cliPrint("%s\t%s\t%s\t%s\n", r.Requester, r.Set, r.Local, r.Error)
	}
}

func handleBuried(client *server.Client, remove, retry bool, bf *server.BuriedFilter) {
	var (
		n      int
		err    error
		action string
	)

	if remove {
		n, err = client.RemoveBuried(bf)
		action = "removed"
	} else if retry {
		n, err = client.RetryBuried(bf)
		action = "retried"
	}

	if err != nil {
		die("unable to process buried requests: %s", err)
	}

	info("%d requests %s", n, action)
}
