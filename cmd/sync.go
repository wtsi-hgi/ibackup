/*******************************************************************************
 * Copyright (c) 2025 Genome Research Ltd.
 *
 * Author: Sendu Bala <sb10@sanger.ac.uk>
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

var syncUser string
var syncSetName string
var syncDelete bool

var (
	ErrSyncNoName = errors.New("you must specify --name")
)

// syncCmd represents the sync command.
var syncCmd = &cobra.Command{
	Use:   "sync",
	Short: "Perform one-time sync operations on a set",
	Long:  `Perform one-time sync operations on a set.`,
	PreRunE: func(_ *cobra.Command, _ []string) error {
		if syncSetName == "" {
			return ErrSyncNoName
		}

		return nil
	},
	RunE: func(_ *cobra.Command, _ []string) error {
		ensureURLandCert()

		client, err := newServerClient(serverURL, serverCert)
		if err != nil {
			return err
		}

		userSet, err := client.GetSetByName(syncUser, syncSetName)
		if err != nil {
			return err
		}

		if syncDelete {
			return syncWithDeletion(client, userSet)
		}

		return nil
	},
}

func init() { //nolint:funlen
	RootCmd.AddCommand(syncCmd)

	syncCmd.Flags().StringVar(&syncUser, "user", currentUsername(), helpTextuser)
	syncCmd.Flags().StringVarP(&syncSetName, "name", "n", "", helpTextName)
	syncCmd.Flags().BoolVar(&syncDelete, "delete", false,
		"delete extraneous files from iRODS")

	if err := syncCmd.MarkFlagRequired("name"); err != nil {
		die(err)
	}
}

func syncWithDeletion(client *server.Client, set *set.Set) error {
	return client.SyncWithDeletion(set.ID())
}
