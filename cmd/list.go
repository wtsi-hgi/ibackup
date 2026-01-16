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
	"os"

	"github.com/spf13/cobra"
	"github.com/wtsi-hgi/ibackup/server"
	"github.com/wtsi-hgi/ibackup/set"
	"github.com/wtsi-hgi/ibackup/transformer"
)

// options for this cmd.
var (
	lstName      string
	lstUser      string
	lstLocal     bool
	lstRemote    bool
	lstAll       bool
	lstDB        string
	lstUploaded  bool
	lstSize      bool
	lstBase64    bool
	lstTrash     bool
	lstLastState bool
)

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

Provide --uploaded to only show paths that were successfully uploaded.
If you provide --size, the size of each file in bytes will be shown in an extra
tab separated column at the end.

Provide --deleted to show only files that no longer exist at the local path;
this can be combined with the --uploaded flag to show only files that exist
remotely and not locally.

To avoid issues with paths having tabs and newlines in them, you can use the
--base64 option to output the paths base64 encoded.

You need to supply the ibackup server's URL in the form domain:port (using the
IBACKUP_SERVER_URL environment variable, or overriding that with the --url
argument) and if necessary, the certificate (using the IBACKUP_SERVER_CERT
environment variable, or overriding that with the --cert argument).

If you are the user who started the ibackup server, you can use the --user
option to get the status of a given requestor's backup sets, instead of your
own. You can specify the user as "all" to see all user's sets.

Alternatively, the user who started the server can use this sub-command to list
all successfully uploaded paths for all sets for all users by specifying both
--all and the --database option which should be the path to the local backup of
the ibackup database, defaulting to the value of the
IBACKUP_LOCAL_DB_BACKUP_PATH environmental variable.
`,
	Run: func(_ *cobra.Command, _ []string) {
		if lstAll && lstDB == "" {
			dief("--all requires --database to be set")
		}

		if lstAll && lstName != "" {
			dief("--name and --all are mutually exclusive")
		}

		if lstName == "" && !lstAll {
			dief("--name must be set")
		}

		if lstLocal && lstRemote {
			dief("--local and --remote are mutually exclusive")
		}

		if lstAll {
			getAllSetsFromDBAndDisplayPaths(lstDB, lstLocal, lstRemote, lstUploaded,
				lstLastState, lstSize, lstBase64)

			return
		}

		client, err := newServerClient(serverURL, serverCert)
		if err != nil {
			die(err)
		}

		if lstTrash {
			lstName = set.TrashPrefix + lstName
		}

		getSetFromServerAndDisplayPaths(client, lstLocal, lstRemote, lstUploaded,
			lstLastState, lstSize, lstBase64, lstUser, lstName)
	},
}

func init() {
	RootCmd.AddCommand(listCmd)

	// flags specific to this sub-command
	listCmd.Flags().StringVar(&lstUser, "user", currentUsername(), helpTextuser)
	listCmd.Flags().StringVarP(&lstName, "name", "n", "",
		"get local and remote paths for the --name'd set")
	listCmd.Flags().BoolVarP(&lstLocal, "local", "l", false,
		"only get local paths for the --name'd set")
	listCmd.Flags().BoolVarP(&lstRemote, "remote", "r", false,
		"only get remote paths for the --name'd set")
	listCmd.Flags().BoolVarP(&lstAll, "all", "a", false,
		"get all paths for all sets for all users, requires --database and only works if you started the server")
	listCmd.Flags().StringVarP(&lstDB, "database", "d",
		os.Getenv("IBACKUP_LOCAL_DB_BACKUP_PATH"), "path to ibackup database file, required with --all")
	listCmd.Flags().BoolVarP(&lstUploaded, "uploaded", "u", false,
		"only show paths that were successfully uploaded")
	listCmd.Flags().BoolVarP(&lstSize, "size", "s", false,
		"show the size of each file in bytes")
	listCmd.Flags().BoolVarP(&lstBase64, "base64", "b", false,
		"output paths base64 encoded")
	listCmd.Flags().BoolVar(&lstLastState, "last-state", false,
		"show uploaded files excluding orphaned ones")
	listCmd.Flags().BoolVar(&lstTrash, "trash", false,
		"get paths from the trash version of the set")

	if isAdmin() {
		return
	}

	for _, flag := range []string{"user", "trash"} {
		if err := listCmd.Flags().MarkHidden(flag); err != nil {
			die(err)
		}
	}
}

func getAllSetsFromDBAndDisplayPaths(dbPath string, local, remote, uploaded, //nolint:funlen,gocyclo
	lastState, size, encode bool,
) {
	db, err := set.NewRO(dbPath)
	if err != nil {
		die(err)
	}

	sets, err := db.GetAll()
	if err != nil {
		die(err)
	}

	if len(sets) == 0 {
		warn("no backup sets")

		return
	}

	var filter set.EntryFilter

	switch {
	case lastState:
		filter = set.FileEntryFilterLastState
	case uploaded:
		filter = set.FileEntryFilterUploaded
	}

	for _, s := range sets {
		info("getting paths for set %s.%s", s.Requester, s.Name)

		entries, err := db.GetFileEntries(s.ID(), filter)
		if err != nil {
			die(err)
		}

		transformer := getSetTransformerIfNeeded(s, !local)

		displayEntryPaths(entries, transformer, local, remote, size, encode)
	}
}

func getSetTransformerIfNeeded(s *set.Set, needed bool) transformer.PathTransformer {
	if !needed {
		return nil
	}

	return getSetTransformer(s)
}

func displayEntryPaths(entries []*set.Entry, transformer transformer.PathTransformer,
	local, remote, size, encode bool,
) {
	format := "%[1]s\t%[2]s"

	if local {
		format = "%[1]s"
	} else if remote {
		format = "%[2]s"
	}

	if size {
		format += "\t%[3]d"
	}

	format += "\n"

	for _, entry := range entries {
		remotePath := getRemotePath(entry.Path, transformer, !local)
		localPath := encodeBase64(entry.Path, encode)
		remotePath = encodeBase64(remotePath, encode)

		cliPrintf(format, localPath, remotePath, entry.Size)
	}
}

func getSetFromServerAndDisplayPaths(client *server.Client,
	local, remote, uploaded, lastState, size, encode bool, user, name string,
) {
	set := getSetByName(client, user, name)

	getFiles := client.GetFiles

	switch {
	case lastState:
		getFiles = client.GetLastStateFiles
	case uploaded:
		getFiles = client.GetUploadedFiles
	}

	entries, err := getFiles(set.ID())
	if err != nil {
		die(err)
	}

	transformer := getSetTransformerIfNeeded(set, !local)

	displayEntryPaths(entries, transformer, local, remote, size, encode)
}
