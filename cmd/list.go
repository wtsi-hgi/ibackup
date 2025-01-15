/*******************************************************************************
 * Copyright (c) 2024 Genome Research Ltd.
 *
 * Author: Rosie Kern <rk18@sanger.ac.uk>
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
	"github.com/wtsi-hgi/ibackup/set"
)

// options for this cmd.
var lstName string
var lstUser string
var lstLocal bool
var lstRemote bool

// listCmd represents the list command.
var listCmd = &cobra.Command{
	Use:   "list",
	Short: "Get paths for a set.",
	Long: `Get paths for a set.
 
Having used 'ibackup add' to add the details of one or more backup sets, use
this command to see the paths for every file in a set. This command
requires --name to be supplied.

Provide --local or --remote to see all the local/remote file paths for the set 
(these flags are mutually exclusive). If neither --local nor --remote is 
provided, each line will contain the local path and the corresponding remote 
path, tab separated.

You need to supply the ibackup server's URL in the form domain:port (using the
IBACKUP_SERVER_URL environment variable, or overriding that with the --url
argument) and if necessary, the certificate (using the IBACKUP_SERVER_CERT
environment variable, or overriding that with the --cert argument).

If you are the user who started the ibackup server, you can use the --user
option to get the status of a given requestor's backup sets, instead of your
own. You can specify the user as "all" to see all user's sets.
`,
	Run: func(_ *cobra.Command, _ []string) {
		if lstName == "" {
			die("--name must be set")
		}

		if lstLocal && lstRemote {
			die("--local and --remote are mutually exclusive")
		}

		client, err := newServerClient(serverURL, serverCert)
		if err != nil {
			die("%s", err.Error())
		}

		getRemote(client, lstLocal, lstRemote, lstUser, lstName)
	},
}

func init() {
	RootCmd.AddCommand(listCmd)

	// flags specific to this sub-command
	listCmd.Flags().StringVar(&lstUser, "user", currentUsername(),
		"pretend to be this user (only works if you started the server)")
	listCmd.Flags().StringVarP(&lstName, "name", "n", "",
		"get local and remote paths for the --name'd set")
	listCmd.Flags().BoolVarP(&lstLocal, "local", "l", false,
		"only get local paths for the --name'd set")
	listCmd.Flags().BoolVarP(&lstRemote, "remote", "r", false,
		"only get remote paths for the --name'd set")
}

// getRemote gets the set from the provided name and displays its remote paths.
func getRemote(client *server.Client, local, remote bool, user, name string) {
	sets := getSetByName(client, user, name)
	if len(sets) == 0 {
		warn("no backup sets")

		return
	}

	displayPaths(client, sets[0], local, remote)
}

func displayPaths(client *server.Client, given *set.Set, local, remote bool) {
	entries, err := client.GetFiles(given.ID())
	if err != nil {
		die("%s", err.Error())
	}

	if local {
		displayLocalPaths(entries)

		return
	}

	transformer := getSetTransformer(given)

	if remote {
		displayRemotePaths(entries, transformer)

		return
	}

	displayLocalAndRemotePaths(entries, transformer)
}

func displayLocalPaths(entries []*set.Entry) {
	for _, entry := range entries {
		cliPrint("%s\n", entry.Path)
	}
}

func displayRemotePaths(entries []*set.Entry, transformer put.PathTransformer) {
	for _, entry := range entries {
		remotePath := getRemotePath(entry.Path, transformer)

		cliPrint("%s\n", remotePath)
	}
}

func displayLocalAndRemotePaths(entries []*set.Entry, transformer put.PathTransformer) {
	for _, entry := range entries {
		remotePath := getRemotePath(entry.Path, transformer)

		cliPrint("%s\t%s\n", entry.Path, remotePath)
	}
}
