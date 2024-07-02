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
	"bufio"
	b64 "encoding/base64"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"github.com/wtsi-hgi/ibackup/put"
)

// options for this cmd.
var arFile string
var arPrefix string
var arHumgen bool
var arGengen bool
var arNull bool
var arBase64 bool

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

Because local path file names can have tabs and newlines in them, it is
recommended you that you pass in null-terminated paths, and output
base64-encoded paths. To do that, use both these options: -0 --base64. You'll
also need to use the --base64 option in 'ibackup put' in that case.

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

Specific to the "humgen" and "genge" groups at the Sanger Institute, you can use
the --humgen or --gengen options to do a more complex transformation from local
"lustre" paths to the "canonical" iRODS path in the humgen zone.
`,
	Run: func(cmd *cobra.Command, args []string) {
		if arHumgen && arPrefix != "" {
			die("--humgen and --prefix are mutually exclusive")
		}

		if arGengen && arPrefix != "" {
			die("--gengen and --prefix are mutually exclusive")
		}

		if arHumgen && arGengen {
			die("--humgen and --gengen are mutually exclusive")
		}

		if !arHumgen && !arGengen && arPrefix == "" {
			die("you must specify one of --prefix, --humgen or --gengen")
		}

		pt := put.HumgenTransformer
		if arPrefix != "" {
			pt = makePrefixTransformer(arPrefix)
		} else if arGengen {
			pt = put.GengenTransformer
		}

		transformARFile(arFile, pt, fofnLineSplitter(arNull), arBase64)
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
		"generate the humgen zone canonical path for humgen lustre paths")
	addremoteCmd.Flags().BoolVar(&arGengen, "gengen", false,
		"generate the humgen zone canonical path for gengen lustre paths")
	addremoteCmd.Flags().BoolVarP(&arNull, "null", "0", false,
		"input paths are terminated by a null character instead of a new line")
	addremoteCmd.Flags().BoolVarP(&arBase64, "base64", "b", false,
		"output paths base64 encoded")
}

func makePrefixTransformer(def string) put.PathTransformer {
	parts := strings.Split(def, ":")
	if len(parts) != arPrefixParts {
		die("'%s' wrong format, must be like '/local/prefix:/remote/prefix'", def)
	}

	return put.PrefixTransformer(parts[0], parts[1])
}

func transformARFile(path string, pt put.PathTransformer, splitter bufio.SplitFunc, encode bool) {
	scanner, df := createScannerForFile(path, splitter)
	defer df()

	for scanner.Scan() {
		local := scanner.Text()

		r, err := put.NewRequestWithTransformedLocal(local, pt)
		if err != nil {
			die("%s", err)
		}

		err = r.ValidatePaths()
		if err != nil {
			die("%s", err)
		}

		fmt.Printf("%s\t%s\n", encodeBase64(r.Local, encode), encodeBase64(r.Remote, encode))
	}

	serr := scanner.Err()
	if serr != nil {
		die("failed to read whole file: %s", serr.Error())
	}
}

// encodeBase64 returns path as-is if encode is false, or after base64 encoding
// it if true.
func encodeBase64(path string, isEncoded bool) string {
	if !isEncoded {
		return path
	}

	return b64.StdEncoding.EncodeToString([]byte(path))
}
