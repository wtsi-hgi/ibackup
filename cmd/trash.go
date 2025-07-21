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
	"errors"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/wtsi-hgi/ibackup/server"
	"github.com/wtsi-hgi/ibackup/set"
)

// options for this cmd.
var trashUser string
var trashRemove bool
var trashName string
var trashItems string
var trashPath string
var trashExpired bool
var trashAllExpired bool
var trashNull bool

var ErrTrashRemove = errors.New("you must provide --remove")
var ErrTrashName = errors.New("exactly one of --name or --all-expired must be provided")
var ErrTrashItems = errors.New("exactly one of --items, --path or --expired must be provided")
var ErrTrashAllExpired = errors.New("--all-expired does not take any other flags")

// removeCmd represents the add command.
var trashCmd = &cobra.Command{
	Use:   "trash",
	Short: "Restore or remove objects from trash set",
	Long: `Restore or remove objects from trash set
 
  Removes or restores objects from a trash set by providing the files and/or 
  directories. You need to provide a base set name (not trash set name).
  
  If you provide --remove, this will remove objects from the trash set and 
  from iRODS if they are not found in any other sets.
  
  You also need to supply the ibackup server's URL in the form domain:port (using
  the IBACKUP_SERVER_URL environment variable, or overriding that with the --url
  argument) and if necessary, the certificate (using the IBACKUP_SERVER_CERT
  environment variable, or overriding that with the --cert argument).
 
  --name is a required flag used to describe which set you want to remove files 
  from.
 
  You must also provide at least one of:
  --items: the path to a file containing the paths of files/directories you want
		   to remove from the set. Each path should be on its own line. Because 
		   filenames can contain new line characters in them, it's safer to 
		   null-terminate them instead and use the optional --null argument.
  --path: if you want to remove a single file or directory, provide its absolute
		  path.
  `,
	PreRunE: func(_ *cobra.Command, _ []string) error {
		ensureURLandCert()

		if !trashRemove {
			return ErrTrashRemove
		}

		if (trashName == "") == !trashAllExpired {
			return ErrTrashName
		}

		setCount := 0
		if trashItems != "" {
			setCount++
		}
		if trashPath != "" {
			setCount++
		}
		if trashExpired {
			setCount++
		}

		if !trashAllExpired && setCount != 1 {
			return ErrTrashItems
		}

		if trashAllExpired && setCount != 0 {
			return ErrTrashAllExpired
		}

		return nil
	},
	RunE: func(_ *cobra.Command, _ []string) error {

		var paths []string

		client, err := newServerClient(serverURL, serverCert)
		if err != nil {
			return err
		}

		if trashAllExpired || trashExpired {
			return handleTrashExpired(client, trashUser, trashName)
		}

		if trashItems != "" {
			paths = append(paths, readPaths(trashItems, fofnLineSplitter(trashNull))...)
		}

		if trashPath != "" {
			trashPath, err = filepath.Abs(trashPath)
			if err != nil {
				return err
			}

			paths = append(paths, trashPath)
		}

		return handleTrash(client, trashUser, trashName, paths)
	},
}

func init() {
	RootCmd.AddCommand(trashCmd)

	// flags specific to this sub-command
	trashCmd.Flags().StringVar(&trashUser, "user", currentUsername(), helpTextuser)
	trashCmd.Flags().BoolVarP(&trashRemove, "remove", "r", false, "remove objects permanently")
	trashCmd.Flags().StringVarP(&trashName, "name", "n", "", "remove objects from the set with this name")
	trashCmd.Flags().StringVarP(&trashItems, "items", "i", "", helpTextItems)
	trashCmd.Flags().StringVarP(&trashPath, "path", "p", "", helpTextPath)
	trashCmd.Flags().BoolVarP(&trashExpired, "expired", "e", false, "remove all expired objects for the set")
	trashCmd.Flags().BoolVar(&trashAllExpired, "all-expired", false, "remove all expired objects")
	trashCmd.Flags().BoolVarP(&trashNull, "null", "0", false, helpTextNull)
}

// handleRemove does the main job of sending the set, files and dirs to the server.
func handleTrash(client *server.Client, user, name string, paths []string) error {
	trashSetName := set.TrashPrefix + name

	sets := getSetByName(client, user, trashSetName)

	return client.RemoveFilesAndDirs(sets[0].ID(), paths)
}

func handleTrashExpired(client *server.Client, user, name string) error {
	if name == "" {
		return client.RemoveAllExpiredEntries()
	}

	trashSetName := set.TrashPrefix + name

	sets := getSetByName(client, user, trashSetName)

	return client.RemoveExpiredEntriesForSet(sets[0].ID())
}
