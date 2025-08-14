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

	"github.com/spf13/cobra"
	"github.com/wtsi-hgi/ibackup/server"
	"github.com/wtsi-hgi/ibackup/set"
)

var (
	ErrSetNoName = errors.New("you must specify --name")
)

// options for this cmd.
var retryUser string
var retrySet string

// retryCmd represents the retry command.
var retryCmd = &cobra.Command{
	Use:   "retry",
	Short: "Retry file uploads in a set",
	Long: `Retry file uploads in a set.

Having used 'ibackup add' to add the details of a backup sets, use this command
to retry failed file uploads in that set. Provide the required --name to choose
the set.

You need to supply the ibackup server's URL in the form domain:port (using the
IBACKUP_SERVER_URL environment variable, or overriding that with the --url
argument) and if necessary, the certificate (using the IBACKUP_SERVER_CERT
environment variable, or overriding that with the --cert argument).

If you are the user who started the ibackup server, you can use the --user
option to retry the given requestor's backup sets, instead of your own.
`,
	PreRunE: func(_ *cobra.Command, _ []string) error {
		if retrySet == "" {
			return ErrSetNoName
		}

		return nil
	},
	RunE: func(_ *cobra.Command, _ []string) error {
		ensureURLandCert()

		client, err := newServerClient(serverURL, serverCert)
		if err != nil {
			return err
		}

		rSet, err := getRequestedSet(client, retryUser, retrySet)
		if err != nil {
			return err
		}

		return retryFailedSetUploads(client, rSet.ID())
	},
}

func init() {
	RootCmd.AddCommand(retryCmd)

	// flags specific to this sub-command
	retryCmd.Flags().StringVarP(&retryUser, "user", "u", currentUsername(), "set belongs to this user")
	retryCmd.Flags().StringVarP(&retrySet, "name", "n", "", "name of set to retry")

	if err := retryCmd.MarkFlagRequired("name"); err != nil {
		die(err)
	}
}

func getRequestedSet(client *server.Client, requester, setName string) (*set.Set, error) {
	requestedSet, err := client.GetSetByName(requester, setName)
	if err != nil {
		return nil, err
	}

	if requestedSet.ReadOnly {
		return nil, set.Error{Msg: set.ErrSetIsNotWritable}
	}

	return requestedSet, nil
}

func retryFailedSetUploads(client *server.Client, sid string) error {
	retried, err := client.RetryFailedSetUploads(sid)
	if err != nil {
		return err
	}

	if retried == 0 {
		warn("no failed uploads to retry in set '%s'", setName)
	} else {
		info("initated retry of %d failed entries", retried)
	}

	return nil
}
