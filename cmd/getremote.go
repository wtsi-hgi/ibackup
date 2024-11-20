/*******************************************************************************
 * Copyright (c) 2022 Genome Research Ltd.
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
	"github.com/wtsi-hgi/ibackup/server"
	"github.com/wtsi-hgi/ibackup/set"
)

// options for this cmd.
var grName string
var grUser string

// getremoteCmd represents the getremote command.
var getremoteCmd = &cobra.Command{
	Use:   "getremote",
	Short: "Get remote paths for a set.",
	Long: `Get remote paths for a set.
 
Having used 'ibackup add' to add the details of one or more backup sets, use
this command to see the remote paths for every file in a set. This command
requires --name to be supplied, this is to specify which paths will be shown.

You need to supply the ibackup server's URL in the form domain:port (using the
IBACKUP_SERVER_URL environment variable, or overriding that with the --url
argument) and if necessary, the certificate (using the IBACKUP_SERVER_CERT
environment variable, or overriding that with the --cert argument).

If you are the user who started the ibackup server, you can use the --user
option to get the status of a given requestor's backup sets, instead of your
own. You can specify the user as "all" to see all user's sets.
`,
	Run: func(_ *cobra.Command, _ []string) {
		if grName == "" {
			die("--name must be set")
		}

		client, err := newServerClient(serverURL, serverCert)
		if err != nil {
			die(err.Error())
		}

		getRemote(client, grUser, grName)
	},
}

func init() {
	RootCmd.AddCommand(getremoteCmd)

	// flags specific to this sub-command
	getremoteCmd.Flags().StringVar(&grUser, "user", currentUsername(),
		"pretend to be this user (only works if you started the server)")
	getremoteCmd.Flags().StringVarP(&grName, "name", "n", "",
		"get remote paths for the set with this name")
}

func getRemote(client *server.Client, user, name string) {
	sets := getSetByName(client, user, name)
	if len(sets) == 0 {
		warn("no backup sets")

		return
	}

	displayRemotePaths(client, sets[0])
}

func displayRemotePaths(client *server.Client, given *set.Set) {
	transformer := getSetTransformer(given)

	entries, err := client.GetFiles(given.ID())
	if err != nil {
		die(err.Error())
	}

	var remotePath string

	warnedAboutTransformer := false

	for _, entry := range entries {
		remotePath, warnedAboutTransformer = getRemotePath(entry.Path, transformer, warnedAboutTransformer)

		if remotePath != "" {
			cliPrint(remotePath)
			cliPrint("\n")
		}
	}
}
