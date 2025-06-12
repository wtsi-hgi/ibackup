/*******************************************************************************
 * Copyright (c) 2025 Genome Research Ltd.
 *
 * Authors:
 *	- Iaroslav Popov <ip13@sanger.ac.uk>
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

	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/wtsi-hgi/ibackup/server"
	"github.com/wtsi-hgi/ibackup/set"
)

// options for this cmd.
var (
	editSetName             string
	editUser                string
	editAddPath             string
	editStopMonitor         bool
	editStopMonitorRemovals bool
	editStopArchive         bool
	editMakeReadOnly        bool
	editMakeWritable        bool
)

var ErrInvalidEdit = errors.New("you can either make a set read-only or writable, not both")

// editCmd represents the edit command.
var editCmd = &cobra.Command{
	Use:   "edit",
	Short: "Edit a backup set",
	Long: `Edit a backup set.
 
Edit an existing backup set.`,
	PreRunE: func(_ *cobra.Command, _ []string) error {
		if editMakeReadOnly && editMakeWritable {
			return ErrInvalidEdit
		}

		return nil
	},
	RunE: func(_ *cobra.Command, _ []string) error {
		ensureURLandCert()

		client, err := newServerClient(serverURL, serverCert)
		if err != nil {
			return err
		}

		userSet, err := client.GetSetByName(editUser, editSetName)
		if err != nil {
			return err
		}

		if editStopMonitor {
			userSet.MonitorTime = 0
		}

		if editStopMonitorRemovals {
			userSet.MonitorRemovals = false
		}

		if editStopArchive {
			userSet.DeleteLocal = false
		}

		if editMakeReadOnly {
			userSet.ReadOnly = true
		}

		err = editSet(client, userSet, editMakeWritable)
		if err != nil {
			return err
		}

		if editAddPath != "" {
			err = updateSet(client, userSet.ID(), editAddPath)
		}

		return err
	},
}

func init() {
	RootCmd.AddCommand(editCmd)

	editCmd.Flags().StringVarP(&editSetName, "name", "n", "", "a name of the backup set you want to edit")
	editCmd.Flags().StringVar(&editUser, "user", currentUsername(),
		"pretend to be this user (only works if you started the server)")
	editCmd.Flags().StringVar(&editAddPath, "add", "", "path to a file or directory to add to the set")
	editCmd.Flags().BoolVar(&editStopMonitor, "stop-monitor", false, "stop monitoring the set for changes")
	editCmd.Flags().BoolVar(&editStopMonitorRemovals, "stop-monitor-removals", false,
		"stop monitoring the set for locally removed files")
	editCmd.Flags().BoolVar(&editStopArchive, "stop-archiving", false, "disable archive mode")
	editCmd.Flags().BoolVar(&editMakeReadOnly, "make-readonly", false,
		"make the set read-only (backup set will be preserved at the current state)")
	editCmd.Flags().BoolVar(&editMakeWritable, "disable-readonly", false,
		"disable read-only mode (only admins can do this)")

	if err := editCmd.MarkFlagRequired("name"); err != nil {
		die(err)
	}
}

// editSet updates properties of the given set.
func editSet(client *server.Client, givenSet *set.Set, makeWritable bool) error {
	if makeWritable {
		return client.AddOrUpdateSetMakingWritable(givenSet)
	}

	return client.AddOrUpdateSet(givenSet)
}

// updateSet updates the files in the set.
func updateSet(client *server.Client, sid string, path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}

	absPath, err := filepath.Abs(path)
	if err != nil {
		return err
	}

	if info.IsDir() {
		err = nil
	} else {
		files, err := client.GetFiles(sid)
		if err != nil {
			return err
		}

		for _, file := range files {
			print(file)
		}
		err = client.SetFiles(sid, []string{absPath})
	}

	return err
}
