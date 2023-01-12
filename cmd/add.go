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
	"bufio"
	"os"

	"github.com/spf13/cobra"
	"github.com/wtsi-hgi/ibackup/server"
	"github.com/wtsi-hgi/ibackup/set"
)

// options for this cmd.
var setName string
var setTransformer string
var setDescription string
var setFiles string
var setDirs string
var setPath string
var setNull bool
var setMonitor bool
var setArchive bool
var setUser string

// addCmd represents the add command.
var addCmd = &cobra.Command{
	Use:   "add",
	Short: "Add or update a backup set",
	Long: `Add or update a backup set.

Add a backup set to the ibackup server by providing details about what you want
backed up.

You also need to supply the ibackup server's URL in the form domain:port (using
the IBACKUP_SERVER_URL environment variable, or overriding that with the --url
argument) and if necessary, the certificate (using the IBACKUP_SERVER_CERT
environment variable, or overriding that with the --cert argument).

To describe the backup set you must provide:
--name : a short unique name for this backup set.
--transformer : define where your local files should be backed up to by defining
  a conversion of local path to a remote iRODS path:
    'humgen' : for files stored on the Sanger Institute's lustre filesystem in a
      Human Genetics project or team folder, use this transformer to backup
	  files to the canonical path in the iRODS humgen zone.
    'prefix=local:remote': replace 'local' with a local path prefix, and 'remote'
      with a remote one, eg. 'prefix=/mnt/diska=/zone1' would backup
	  /mnt/diska/subdir/file.txt to /zone1/subdir/file.txt.

You must also provide at least one of:
--files : the path to a file containing the paths of files you want to have
          backed up as part of this backup set. Each path should be on its own
		  line. Because filenames can contain new line characters in them, it's
		  safer to null-terminate them instead and use the optional --null
		  argument.
--dirs : like --files, but the file contains directories you want to back up.
         Directories will be recursed and all files inside will be backed up.
--path : if you just want to backup a single file or directory, provide its
         absolute path.

You can also provide:
--description : an longer description of the backup set, to describe its
                purpose.
--monitor : recheck the saved file and directory paths every day, and backup any
            new or altered files in the set.
--archive : delete local files after successfully uploading them. (The actual
            deletion is not yet implemented, but you can at least record the
		    fact you wanted deletion now, so they can be deleted in the future.)

Having added a set, you can use 'ibackup status' to monitor the backup progress
of your sets. If you add a set with the same --name again, you will overwrite
its properties. Eg. if you provide a different list of files to a monitored set,
it will backup and monitor the new list of files in future.

If you are the user who started the ibackup server, you can use the --user
option to add sets on behalf of other users.
`,
	Run: func(cmd *cobra.Command, args []string) {
		ensureURLandCert()

		if setFiles == "" && setDirs == "" && setPath == "" {
			die("at least one of --files or --dirs or --path must be set")
		}

		client, err := newServerClient(serverURL, serverCert)
		if err != nil {
			die(err.Error())
		}

		files := readPaths(setFiles, fofnLineSplitter(setNull))
		dirs := readPaths(setDirs, fofnLineSplitter(setNull))

		if setPath != "" {
			info, errs := os.Stat(setPath)
			if errs != nil {
				die(errs.Error())
			}

			if info.IsDir() {
				dirs = append(dirs, setPath)
			} else {
				files = append(files, setPath)
			}
		}

		err = add(client, setName, setUser, setTransformer, setDescription, setMonitor, setArchive, files, dirs)
		if err != nil {
			die(err.Error())
		}

		info("your backup set has been saved and will now be processed")
	},
}

func init() {
	RootCmd.AddCommand(addCmd)

	// flags specific to this sub-command
	addCmd.Flags().StringVarP(&setName, "name", "n", "", "a short name for this backup set")
	addCmd.Flags().StringVarP(&setTransformer, "transformer", "t", "humgen", "'humgen' | 'prefix=local:remote'")
	addCmd.Flags().StringVarP(&setFiles, "files", "f", "",
		"path to file with one absolute local file path per line")
	addCmd.Flags().StringVarP(&setDirs, "dirs", "d", "",
		"path to file with one absolute local directory path per line")
	addCmd.Flags().StringVarP(&setPath, "path", "p", "",
		"path to a single file or directory you wish to backup")
	addCmd.Flags().BoolVarP(&setNull, "null", "0", false,
		"input paths are terminated by a null character instead of a new line")
	addCmd.Flags().StringVar(&setDescription, "description", "", "a long description of this backup set")
	addCmd.Flags().BoolVarP(&setMonitor, "monitor", "m", false,
		"monitor the paths daily for changes and new files to upload")
	addCmd.Flags().BoolVarP(&setArchive, "archive", "a", false,
		"delete local files after successfully uploading them (deletions not yet implemented)")
	addCmd.Flags().StringVar(&setUser, "user", currentUsername(),
		"pretend to be the this user (only works if you started the server)")

	if err := addCmd.MarkFlagRequired("name"); err != nil {
		die(err.Error())
	}

	if err := addCmd.MarkFlagRequired("transformer"); err != nil {
		die(err.Error())
	}
}

// newServerClient tries to get a jwt for the given server url, and returns a
// client that can interact with it.
func newServerClient(url, cert string) (*server.Client, error) {
	token, err := getJWT(url, cert)
	if err != nil {
		return nil, err
	}

	return server.NewClient(url, cert, token), nil
}

// readPaths turns the line content (split as per splitter) of the given file.
// If file is blank, returns nil.
func readPaths(file string, splitter bufio.SplitFunc) []string {
	if file == "" {
		return nil
	}

	scanner, df := createScannerForFile(file, splitter)
	defer df()

	var paths []string //nolint:prealloc

	for scanner.Scan() {
		paths = append(paths, scanner.Text())
	}

	serr := scanner.Err()
	if serr != nil {
		die("failed to read whole file: %s", serr.Error())
	}

	return paths
}

// add does the main job of sending the backup set details to the server.
func add(client *server.Client, name, requester, transformer, description string,
	monitor, archive bool, files, dirs []string) error {
	set := &set.Set{
		Name:        name,
		Requester:   requester,
		Transformer: transformer,
		Description: description,
		Monitor:     monitor,
		DeleteLocal: archive,
	}

	if err := client.AddOrUpdateSet(set); err != nil {
		return err
	}

	if err := client.SetFiles(set.ID(), files); err != nil {
		return err
	}

	if err := client.SetDirs(set.ID(), dirs); err != nil {
		return err
	}

	return client.TriggerDiscovery(set.ID())
}
