/*******************************************************************************
 * Copyright (c) 2022 Genome Research Ltd.
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
	"fmt"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
)

// options for this cmd.
var arFile string
var arPrefix string
var localPrefix string
var remotePrefix string
var arHumgen bool

const arPrefixParts = 2

// addremoteCmd represents the addremote command.
var addremoteCmd = &cobra.Command{
	Use:   "addremote",
	Short: "Add a remote path column to a local path fofn",
	Long: `Add a remote path column to a local path fofn.

The 'put' subcommand takes a file that has at least 2 columns: local path, and
remote iRODS path, defining your source files and where you would like to put
them.

If you have a fofn of local paths, and want to put them in iRODS at similar
paths, just with different root directories, you can use this subcommand to add
the remote path column.

Provide the fofn to -f, or pipe it in. Results are output to STDOUT.

--prefix "/local:/remote" lets you define a local path prefix before the colon
that you'd like to replace with the remote path prefix after the colon.

So given local path '/mnt/diska/project1/file.txt', you can say you want to put
this in iRODS as '/zone/project1/file.txt' by using:
--prefix '/mnt/diska:/zone'
This will generate the tab-delimeted 2 column file:
/mnt/diska/project1/file.txt /zone/project1/file.txt
Which you can pipe to the 'put' subcommand.

(If the local prefix isn't present, the local path will be assumed to be
relative to the local prefix, and will end up relative to the remote prefix.)

Specific to the "humgen" group at the Sanger Institute, you can use the --humgen
option to do a more complex transformation from local "lustre" paths to the
"canonical" iRODS path in the humgen zone.
`,
	Run: func(cmd *cobra.Command, args []string) {
		if arHumgen && arPrefix != "" {
			die("--humgen and --prefix are mutually exclusive")
		}

		if !arHumgen && arPrefix == "" {
			die("you must specify one of --prefix and --humgen")
		}

		tf := humgenTransform
		if arPrefix != "" {
			preparePrefix(arPrefix)
			tf = prefixTransform
		}

		transformARFile(arFile, tf)
	},
}

func init() {
	RootCmd.AddCommand(addremoteCmd)

	// flags specific to this sub-command
	addremoteCmd.Flags().StringVarP(&arFile, "file", "f", "-",
		"path to file with one local path per line (- means read from STDIN)")
	addremoteCmd.Flags().StringVarP(&arPrefix, "prefix", "p", "",
		"'/local/prefix:/remote/prefix' string to replace local prefix with remote")
	addremoteCmd.Flags().BoolVar(&arHumgen, "humgen", false,
		"generate the humgen zone canonical path for lustre paths")
}

func humgenTransform(local string) string {
	parts := strings.Split(local, "/")
	ptPart := -1

	for i, part := range parts {
		if part == "projects" || part == "teams" || part == "users" {
			ptPart = i

			break
		}
	}

	if parts[1] != "lustre" || ptPart < 4 || ptPart+2 > len(parts)-1 {
		die("'%s' is not a valid humgen lustre path", local)
	}

	return fmt.Sprintf("/humgen/%s/%s/%s/%s", parts[ptPart], parts[ptPart+1], parts[2],
		strings.Join(parts[ptPart+2:], "/"))
}

func preparePrefix(def string) {
	parts := strings.Split(def, ":")
	if len(parts) != arPrefixParts {
		die("'%s' wrong format, must be like '/local/prefix:/remote/prefix'", def)
	}

	localPrefix = parts[0]
	remotePrefix = parts[1]
}

func prefixTransform(local string) string {
	remote := strings.Replace(local, localPrefix, remotePrefix, 1)

	if remote == local {
		remote = filepath.Join(remotePrefix, local)
	}

	return remote
}

type transformFunc func(string) string

func transformARFile(path string, tf transformFunc) {
	scanner, df := createScannerForFile(path)
	defer df()

	for scanner.Scan() {
		local := scanner.Text()
		remote := tf(local)
		fmt.Printf("%s\t%s\n", local, remote)
	}

	serr := scanner.Err()
	if serr != nil {
		die("failed to read whole file: %s", serr.Error())
	}
}
