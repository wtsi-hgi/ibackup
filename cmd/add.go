/*******************************************************************************
 * Copyright (c) 2022 Genome Research Ltd.
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
	"github.com/spf13/cobra"
)

// type Error string

// func (e Error) Error() string { return string(e) }

// options for this cmd.

// addCmd represents the add command.
var addCmd = &cobra.Command{
	Use:   "add",
	Short: "Add or update a backup set",
	Long: `Add or update a backup set.

Add a backup set to the ibackup server by providing its URL in the form
domain:port (using the IBACKUP_SERVER environment variable, or overriding that
with a command line argument), and details about the set.
`,
	Run: func(cmd *cobra.Command, args []string) {
		ensureURLandCert()

		err := add(serverURL, serverCert)
		if err != nil {
			die(err.Error())
		}
	},
}

func init() {
	RootCmd.AddCommand(addCmd)

	// flags specific to these sub-commands
}

// add does the main job of sending the backup set to the server.
func add(url, cert string) error {
	_, err := getJWT(url, cert)
	if err != nil {
		return err
	}

	// body, dss, err := server.GetWhereDataIs(url, cert, token, dir, groups, users, types, splits)
	// if err != nil {
	// 	return err
	// }

	return nil
}
