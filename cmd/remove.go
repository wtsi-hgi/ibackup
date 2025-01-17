/*******************************************************************************
 * Copyright (c) 2025 Genome Research Ltd.
 *
 * Authors:
 *	- Rosie Kern <rk18@sanger.ac.uk>
 *  - Iaroslav Popov <ip13@sanger.ac.uk>
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
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/wtsi-hgi/ibackup/server"
)

// options for this cmd.
var removeUser string
var removeName string
var removeItems string
var removePath string

// removeCmd represents the add command.
var removeCmd = &cobra.Command{
	Use:   "remove",
	Short: "Remove files from backed up set",
	Long: `Remove files from backed up set.

 Remove files from a backed up set by providing the files or directories to be
 removed. This will remove files from the set and from iRODS if it is not found
 in any other sets.
 
 `,
	Run: func(cmd *cobra.Command, args []string) {
		ensureURLandCert()

		if (removeItems == "") == (removePath == "") {
			die("only one of --items or --path can be provided")
		}

		var files []string
		var dirs []string

		if removeItems != "" {
			filesAndDirs := readPaths(removeItems, fofnLineSplitter(setNull))
			files, dirs = categorisePaths(filesAndDirs, files, dirs)
		}

		if removePath != "" {
			removePath, err := filepath.Abs(removePath)
			if err != nil {
				die("%s", err.Error())
			}

			if pathIsDir(removePath) {
				dirs = append(dirs, removePath)
			} else {
				files = append(files, removePath)
			}
		}

		client, err := newServerClient(serverURL, serverCert)
		if err != nil {
			die("%s", err.Error())
		}

		handleRemove(client, removeUser, removeName, files, dirs)
	},
}

func init() {
	RootCmd.AddCommand(removeCmd)

	// flags specific to this sub-command
	removeCmd.Flags().StringVar(&removeUser, "user", currentUsername(),
		"pretend to be this user (only works if you started the server)")
	removeCmd.Flags().StringVarP(&removeName, "name", "n", "", "remove files from the set with this name")
	removeCmd.Flags().StringVarP(&removeItems, "items", "i", "",
		"path to file with one absolute local directory or file path per line")
	removeCmd.Flags().StringVarP(&removePath, "path", "p", "",
		"path to a single file or directory you wish to remove")

	if err := removeCmd.MarkFlagRequired("name"); err != nil {
		die("%s", err.Error())
	}
}

func handleRemove(client *server.Client, user, name string, files, dirs []string) {
	sets := getSetByName(client, user, name)
	if len(sets) == 0 {
		warn("No backup sets found with name %s", name)

		return
	}

	err := client.RemoveFiles(sets[0].ID(), files)
	if err != nil {
		die("%s", err.Error())
	}

	err = client.RemoveDirs(sets[0].ID(), dirs)
	if err != nil {
		die("%s", err.Error())
	}

}
