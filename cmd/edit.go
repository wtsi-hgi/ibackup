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
	"time"

	"github.com/spf13/cobra"
	"github.com/wtsi-hgi/ibackup/server"
	"github.com/wtsi-hgi/ibackup/set"
	"github.com/wtsi-hgi/ibackup/transfer"
)

// options for this cmd.
var (
	editSetName             string
	editUser                string
	editDescription         string
	editMetaData            string
	editReason              transfer.Reason
	editReview              string
	editRemovalDate         string
	editTransformer         string
	editAddPath             string
	editStopMonitor         bool
	editStopMonitorRemovals bool
	editStopArchive         bool
	editArchive             bool
	editMakeReadOnly        bool
	editMakeWritable        bool
	editHide                bool
	editUnHide              bool

	ErrInvalidEditRO      = errors.New("you can either make a set read-only or writable, not both")
	ErrInvalidEditArchive = errors.New("you can either archive a set or stop archiving, not both")
	ErrInvalidEditHide    = errors.New("you can either hide or unhide a set, not both")
)

// editCmd represents the edit command.
var editCmd = &cobra.Command{
	Use:   "edit",
	Short: "Edit a backup set",
	Long: `Edit a backup set.
 
Edit an existing backup set. You cannot edit readonly sets and you cannot use 
--add on sets that are in the process of a removal.

To edit the backup set you must provide --name, which should be the name of a 
preexisting backup set.`,
	PreRunE: func(_ *cobra.Command, _ []string) error {
		if editMakeReadOnly && editMakeWritable {
			return ErrInvalidEditRO
		}

		if editArchive && editStopArchive {
			return ErrInvalidEditArchive
		}

		if editHide && editUnHide {
			return ErrInvalidEditHide
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

		err = editSetMetaData(userSet, editMetaData, editReason, editReview, editRemovalDate)
		if err != nil {
			return err
		}

		if editDescription != "" {
			userSet.Description = editDescription
		}

		if editStopMonitor {
			userSet.MonitorTime = 0
		}

		if editStopMonitorRemovals {
			userSet.MonitorRemovals = false
		}

		if editArchive {
			userSet.DeleteLocal = true
		} else if editStopArchive {
			userSet.DeleteLocal = false
		}

		if editMakeReadOnly {
			userSet.ReadOnly = true
		}

		if editHide {
			userSet.Hide = true
		} else if editUnHide {
			userSet.Hide = false
		}

		if editTransformer != "" {
			userSet.Transformer = editTransformer
		}

		err = editSet(client, userSet, editMakeWritable)
		if err != nil {
			return err
		}

		if editAddPath == "" {
			return nil
		}

		return updateSet(client, userSet.ID(), editAddPath)
	},
}

func init() {
	RootCmd.AddCommand(editCmd)

	editCmd.Flags().StringVarP(&editSetName, "name", "n", "", helpTextName)
	editCmd.Flags().StringVar(&editUser, "user", currentUsername(), helpTextuser)
	editCmd.Flags().StringVar(&editDescription, "description", "", helpTextDescription)
	editCmd.Flags().StringVar(&editMetaData, "metadata", "", helpTextMetaData)
	editCmd.Flags().Var(&editReason, "reason", helpTextReason)
	editCmd.Flags().StringVar(&editReview, "review", "", helpTextReview)
	editCmd.Flags().StringVar(&editRemovalDate, "removal-date", "", helpTextRemoval)
	editCmd.Flags().StringVar(&editTransformer, "transformer", "", helpTextTransformer)
	editCmd.Flags().StringVar(&editAddPath, "add", "", helpTextPath)
	editCmd.Flags().BoolVar(&editStopMonitor, "stop-monitor", false, "stop monitoring the set for changes")
	editCmd.Flags().BoolVar(&editStopMonitorRemovals, "stop-monitor-removals", false,
		"stop monitoring the set for locally removed files")
	editCmd.Flags().BoolVarP(&editArchive, "archive", "a", false, helpTextArchive)
	editCmd.Flags().BoolVar(&editStopArchive, "stop-archiving", false, "disable archive mode")
	editCmd.Flags().BoolVar(&editMakeReadOnly, "make-readonly", false,
		"make the set read-only (backup set will be preserved at the current state)")
	editCmd.Flags().BoolVar(&editMakeWritable, "disable-readonly", false,
		"disable read-only mode (only admins can do this)")
	editCmd.Flags().BoolVar(&editHide, "hide", false,
		"hide set when viewing status")
	editCmd.Flags().BoolVar(&editUnHide, "unhide", false,
		"unhide set when viewing status")

	if err := editCmd.MarkFlagRequired("name"); err != nil {
		die(err)
	}
}

func editSetMetaData(userSet *set.Set, metaData string, //nolint:gocyclo,funlen
	reason transfer.Reason, reviewDate, removalDate string) error {
	if reason == transfer.Unset && reviewDate == "" && removalDate == "" && metaData == "" {
		return nil
	}

	existingMeta := userSet.Metadata

	reason, err := givenOrExistingReason(reason, existingMeta)
	if err != nil {
		return err
	}

	reviewDate, err = givenOrExistingReviewDate(reviewDate, existingMeta)
	if err != nil {
		return err
	}

	removalDate, err = givenOrExistingRemovalDate(removalDate, existingMeta)
	if err != nil {
		return err
	}

	metaData = givenOrExistingMetaData(metaData, userSet)

	meta, err := transfer.HandleMeta(metaData, reason, reviewDate, removalDate, existingMeta)
	if err != nil {
		return err
	}

	userSet.Metadata = meta.Metadata()

	return nil
}

func givenOrExistingReason(reason transfer.Reason, existingMeta map[string]string) (transfer.Reason, error) {
	if reason != transfer.Unset {
		return reason, nil
	}

	err := reason.Set(existingMeta[transfer.MetaKeyReason])

	return reason, err
}

func givenOrExistingReviewDate(reviewDate string, existingMeta map[string]string) (string, error) {
	return givenOrExistingDate(reviewDate, existingMeta[transfer.MetaKeyReview])
}

func givenOrExistingRemovalDate(removalDate string, existingMeta map[string]string) (string, error) {
	return givenOrExistingDate(removalDate, existingMeta[transfer.MetaKeyRemoval])
}

func givenOrExistingMetaData(metaData string, userSet *set.Set) string {
	if metaData != "" {
		return metaData
	}

	return userSet.UserMetadata()
}

func givenOrExistingDate(input, existing string) (string, error) {
	if input != "" {
		return input, nil
	}

	t := time.Time{}

	err := t.UnmarshalText([]byte(existing))

	return t.Format(time.DateOnly), err
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
		err = client.MergeDirs(sid, []string{absPath})
	} else {
		err = client.MergeFiles(sid, []string{absPath})
	}

	if err != nil {
		return err
	}

	return client.TriggerDiscovery(sid)
}
