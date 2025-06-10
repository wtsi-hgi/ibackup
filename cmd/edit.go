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

	"github.com/spf13/cobra"
	"github.com/wtsi-hgi/ibackup/server"
	"github.com/wtsi-hgi/ibackup/set"
)

// options for this cmd.
var (
	editSetName             string
	editUser                string
	editStopMonitor         bool
	editStopMonitorRemovals bool
	editStopArchive         bool
	editMakeReadOnly        bool
	editMakeWritable        bool
)

var ErrInvalidEdit = errors.New("you can either make a set read-only or writable, not both")
var ErrSetIsNotWritable = errors.New("the set is read-only, you cannot change it")
var ErrNotAdmin = errors.New("only admin can do that")

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

		requireAdmin := false
		if editMakeWritable {
			userSet.ReadOnly = false
			requireAdmin = true
		}

		if userSet.ReadOnly {
			return ErrSetIsNotWritable
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

		return edit(client, userSet, requireAdmin)
	},
}

func init() {
	RootCmd.AddCommand(editCmd)

	editCmd.Flags().StringVarP(&editSetName, "name", "n", "", "a name of the backup set you want to edit")
	editCmd.Flags().StringVar(&editUser, "user", currentUsername(),
		"pretend to be this user (only works if you started the server)")
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

func edit(client *server.Client, givenSet *set.Set, requireAdmin bool) error {
	var err error

	if requireAdmin {
		err = client.AddOrUpdateSetRequireAdmin(givenSet)
		if errors.Is(err, server.ErrBadRequester) {
			err = ErrNotAdmin
		}

		return err
	}

	return client.AddOrUpdateSet(givenSet)
}
