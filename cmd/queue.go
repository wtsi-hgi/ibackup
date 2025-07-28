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
	"errors"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/wtsi-hgi/ibackup/server"
	"github.com/wtsi-hgi/ibackup/transfer"
)

// options for this cmd.
var queueAll bool
var queueUser string
var queueSet string
var queuePath string
var queueKick bool
var queueDelete bool
var queueUploading bool

var ErrAdminOnly = errors.New("only the user who started the server can use this sub-command.")

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
queue. Specifying --uploading instead shows details about requests that are
currently uploading. Specifying just --all shows details about all requests in
the queue.
`,
	PreRun: func(cmd *cobra.Command, _ []string) {
		kick, _ := cmd.Flags().GetBool("kick")     //nolint:errcheck
		remove, _ := cmd.Flags().GetBool("delete") //nolint:errcheck

		if kick || remove {
			if path, _ := cmd.Flags().GetString("path"); path != "" { //nolint:errcheck
				must(cmd.MarkFlagRequired("user"))
				must(cmd.MarkFlagRequired("name"))
			} else if name, _ := cmd.Flags().GetString("name"); name != "" { //nolint:errcheck
				must(cmd.MarkFlagRequired("user"))
			}

			cmd.MarkFlagsMutuallyExclusive("all", "user")
			cmd.MarkFlagsMutuallyExclusive("all", "name")
			cmd.MarkFlagsMutuallyExclusive("all", "path")

			cmd.MarkFlagsOneRequired("all", "user")
		}
	},
	RunE: func(_ *cobra.Command, _ []string) error {
		gclient, err := gasClientCLI(serverURL, serverCert)
		if err != nil {
			return err
		}

		if !gclient.CanReadServerToken() {
			return ErrAdminOnly
		}

		client, err := newServerClient(serverURL, serverCert)
		if err != nil {
			return err
		}

		switch {
		case queueDelete || queueKick:
			bf := &server.BuriedFilter{
				User: queueUser,
				Set:  queueSet,
				Path: queuePath,
			}

			return handleBuried(client, queueDelete, queueKick, bf)
		case queueUploading:
			rs, err := client.UploadingRequests()
			if err != nil {
				return fmt.Errorf("unable to get uploading requests: %w", err)
			}

			displayRequests(rs, "uploading")
		case queueAll:
			rs, err := client.AllRequests()
			if err != nil {
				return fmt.Errorf("unable to get all requests: %w", err)
			}

			displayRequests(rs, "(0)")
		default:
			rs, err := client.BuriedRequests()
			if err != nil {
				return fmt.Errorf("unable to get buried requests: %w", err)
			}

			displayRequests(rs, "buried")
		}

		return nil
	},
}

func init() {
	RootCmd.AddCommand(queueCmd)

	// flags specific to this sub-command
	queueCmd.Flags().StringVarP(&queueUser, "user", "u", "", "limit -d/-r to sets of this user")
	queueCmd.Flags().StringVarP(&queueSet, "name", "n", "", "limit -d/-r to this backup set")
	queueCmd.Flags().StringVarP(&queuePath, "path", "p", "", "limit -d/-r to this local file in the set")
	queueCmd.Flags().BoolVarP(&queueAll, "all", "a", false,
		"apply -d/-r to all buried items in the queue, or show all items in the queue")
	queueCmd.Flags().BoolVarP(&queueDelete, "delete", "d", false,
		"delete certain buried items in the queue")
	queueCmd.Flags().BoolVarP(&queueKick, "retry", "r", false,
		"retry certain buried items in the queue")
	queueCmd.Flags().BoolVar(&queueUploading, "uploading", false, "show uploading items in the queue")

	queueCmd.MarkFlagsMutuallyExclusive("retry", "delete")
}

// displayRequests prints out details of each request. If there are none, warns
// that there aren't the given kind of request.
func displayRequests(rs []*transfer.Request, kind string) {
	if len(rs) == 0 {
		warn("no %s requests", kind)

		return
	}

	cliPrint("Requester\tSet\tLocal\tRemote\tStatus\tError\n")

	for _, r := range rs {
		cliPrintf("%s\t%s\t%s\t%s\t%s\t%s\n", r.Requester, r.Set, r.Local, r.Remote, r.Status, r.Error)
	}
}

func handleBuried(client *server.Client, remove, retry bool, bf *server.BuriedFilter) error {
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
		return fmt.Errorf("unable to process buried requests: %w", err)
	}

	info("%d requests %s", n, action)

	return nil
}
