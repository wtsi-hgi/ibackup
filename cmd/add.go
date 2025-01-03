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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"time"

	"github.com/spf13/cobra"
	"github.com/wtsi-hgi/ibackup/put"
	"github.com/wtsi-hgi/ibackup/server"
	"github.com/wtsi-hgi/ibackup/set"
)

const (
	hoursInDay      = 24
	hoursInWeek     = hoursInDay * 7
	numOfBackupMeta = 3
)

// options for this cmd.
var setName string
var setTransformer string
var setDescription string
var setFiles string
var setDirs string
var setItems string
var setPath string
var setNull bool
var setMonitor string
var setArchive bool
var setUser string
var setMetadata string
var setReason put.Reason
var setReview string
var setRemoval string

var ErrCancel = errors.New("cancelled add")

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
    'gengen' : like 'humgen', but for Generative Genomics data.
    'prefix=local:remote' : replace 'local' with a local path prefix, and
	  'remote' with a remote one, eg. 'prefix=/mnt/diska:/zone1' would backup
	  /mnt/diska/subdir/file.txt to /zone1/subdir/file.txt.
  This defaults to the environment variable IBACKUP_TRANSFORMER.

You must also provide at least one of:
--files : the path to a file containing the paths of files you want to have
          backed up as part of this backup set. Each path should be on its own
		  line. Because filenames can contain new line characters in them, it's
		  safer to null-terminate them instead and use the optional --null
		  argument.
--dirs : like --files, but the file contains directories you want to back up.
         Directories will be recursed and all files inside will be backed up.
--items: like --files and --dirs, but the file can contain either a file or 
		 directory on each line. Should only be used for a smaller number of 
		 entries.
--path : if you just want to backup a single file or directory, provide its
         absolute path.

For a quicker add, use --files or --dirs. If you have a smaller, simpler set,
--items can be used.

You can also provide:
--description : an longer description of the backup set, to describe its
                purpose.
--monitor : recheck the saved file and directory paths after the given time
            period (minimum 1hr) after last completion, and backup any new or
            altered files in the set.
--archive : delete local files after successfully uploading them. (The actual
            deletion is not yet implemented, but you can at least record the
		    fact you wanted deletion now, so they can be deleted in the future.)
--reason  : the reason you are storing the set, which can be 'backup', 'archive' 
			or 'quarantine'. The default is 'backup' which will set review date 
			to 6 months and removal date to 1 year. 'archive' sets review date 
			to 1 year and removal date to 2 years, while 'quarantine' sets 
			review date to 2 months and removal date to 3 months.
--review  : the time until the set should be reviewed, provided in months or
			years. This duration must be shorter than the time until removal.
--remove  : the time until the set should be removed, provided in months or 
			years. This duration must be longer than the time until review.

Having added a set, you can use 'ibackup status' to monitor the backup progress
of your sets. If you add a set with the same --name again, you will overwrite
its properties. Eg. if you provide a different list of files to a monitored set,
it will backup and monitor the new list of files in future.

If you are the user who started the ibackup server, you can use the --user
option to add sets on behalf of other users.
`,
	Run: func(cmd *cobra.Command, args []string) {
		ensureURLandCert()

		if setFiles == "" && setDirs == "" && setPath == "" && setItems == "" {
			die("at least one of --files or --dirs or --items or --path must be provided")
		}

		if setTransformer == "" {
			die("-t must be provided")
		}

		var monitorDuration time.Duration
		if setMonitor != "" {
			var err error

			monitorDuration, err = parseDuration(setMonitor)
			if err != nil {
				die("invalid monitor duration: %s", err)
			}

			if monitorDuration < 1*time.Hour {
				die("monitor duration must be 1h or more, not %s", monitorDuration)
			}
		}

		client, err := newServerClient(serverURL, serverCert)
		if err != nil {
			die(err.Error())
		}

		files := readPaths(setFiles, fofnLineSplitter(setNull))
		dirs := readPaths(setDirs, fofnLineSplitter(setNull))

		if setItems != "" {
			filesAndDirs := readPaths(setItems, fofnLineSplitter(setNull))
			files, dirs = categorisePaths(filesAndDirs, files, dirs)
		}

		if setPath != "" {
			setPath, err = filepath.Abs(setPath)
			if err != nil {
				die(err.Error())
			}

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

		meta, err := put.HandleMeta(setMetadata, setReason, setReview, setRemoval)
		if err != nil {
			die(err.Error())
		}

		err = add(client, setName, setUser, setTransformer, setDescription, monitorDuration, setArchive, files, dirs, meta)
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
	addCmd.Flags().StringVarP(&setTransformer, "transformer", "t",
		os.Getenv("IBACKUP_TRANSFORMER"), "'humgen' | 'gengen' | 'prefix=local:remote'")
	addCmd.Flags().StringVarP(&setFiles, "files", "f", "",
		"path to file with one absolute local file path per line")
	addCmd.Flags().StringVarP(&setDirs, "dirs", "d", "",
		"path to file with one absolute local directory path per line")
	addCmd.Flags().StringVarP(&setItems, "items", "i", "",
		"path to file with one absolute local directory or file path per line")
	addCmd.Flags().StringVarP(&setPath, "path", "p", "",
		"path to a single file or directory you wish to backup")
	addCmd.Flags().BoolVarP(&setNull, "null", "0", false,
		"input paths are terminated by a null character instead of a new line")
	addCmd.Flags().StringVar(&setDescription, "description", "", "a long description of this backup set")
	addCmd.Flags().StringVarP(&setMonitor, "monitor", "m", "",
		"monitor the paths for changes and new files to upload the given time period "+
			"(eg. 1d for 1 day, or 2w for 2 weeks, min 1h) after completion")
	addCmd.Flags().BoolVarP(&setArchive, "archive", "a", false,
		"delete local files after successfully uploading them (deletions not yet implemented)")
	addCmd.Flags().StringVar(&setUser, "user", currentUsername(),
		"pretend to be the this user (only works if you started the server)")
	addCmd.Flags().StringVar(&setMetadata, "metadata", "",
		"key=val;key=val metadata to apply to all files in the set")
	addCmd.Flags().Var(&setReason, "reason",
		"storage reason: 'backup' | 'archive' | 'quarantine'")
	addCmd.Flags().StringVar(&setReview, "review", "",
		"months/years until review date, provided in format: <number><unit>, e.g. 1y for 1 year")
	addCmd.Flags().StringVar(&setRemoval, "remove", "",
		"months/years until removal date, provided in format: <number><unit>, e.g. 1y for 1 year")

	if err := addCmd.MarkFlagRequired("name"); err != nil {
		die(err.Error())
	}
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

// categorisePaths categorises each path in a given slice into either a
// directory or file and appends the path to the corresponding slice.
func categorisePaths(filesAndDirs, files, dirs []string) ([]string, []string) {
	dirSet := make(map[string]bool)

	for _, path := range filesAndDirs {
		if pathIsDir(path) {
			dirs = append(dirs, path)
			dirSet[path] = true
		}
	}

	for _, path := range filesAndDirs {
		if !dirSet[path] && !fileDirIsInDirs(path, dirSet) {
			files = append(files, path)
		}
	}

	return files, dirs
}

func pathIsDir(path string) bool {
	info, err := os.Stat(path)
	if err != nil {
		warn("Invalid path: %s", err)

		return false
	}

	return info.IsDir()
}

// fileDirIsInDirs returns true if the directory of the provided file (or any of
// its parents) is present in the provided map of directories.
func fileDirIsInDirs(file string, dirSet map[string]bool) bool {
	fileDir := filepath.Dir(file)

	for fileDir != filepath.Dir(fileDir) {
		if dirSet[fileDir] {
			return true
		}

		fileDir = filepath.Dir(fileDir)
	}

	return false
}

// add does the main job of sending the backup set details to the server.
func add(client *server.Client, name, requester, transformer, description string,
	monitor time.Duration, archive bool, files, dirs []string, meta *put.Meta) error {
	if err := checkExistingSet(client, name, requester); err != nil {
		return err
	}

	set := &set.Set{
		Name:        name,
		Requester:   requester,
		Transformer: transformer,
		Description: description,
		MonitorTime: monitor,
		DeleteLocal: archive,
		Metadata:    meta.Metadata(),
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

func checkExistingSet(client *server.Client, name, requester string) error {
	_, err := client.GetSetByName(requester, name)
	if errors.Is(err, server.ErrBadSet) {
		return nil
	} else if err != nil {
		return err
	}

	resp, err := askYesNo(fmt.Sprintf("Set with name %s already exists, are you sure you wish to overwrite (y/N)? ", name))
	if err != nil {
		return err
	}

	if !resp {
		return ErrCancel
	}

	return nil
}

func askYesNo(prompt string) (bool, error) {
	b := bufio.NewReader(os.Stdin)

	cliPrint(prompt)

	input, _, err := b.ReadLine()
	if err != nil {
		return false, err
	}

	switch string(input) {
	case "y", "Y":
		return true, nil
	default:
		return false, nil
	}
}

func parseDuration(s string) (time.Duration, error) {
	durationRegex := regexp.MustCompile("[0-9]+[dw]")

	if durationRegex.MatchString(s) {
		s = durationRegex.ReplaceAllStringFunc(s, func(d string) string {
			num, err := strconv.ParseInt(d[:len(d)-1], 10, 64)
			if err != nil {
				return d
			}

			switch d[len(d)-1] {
			case 'd':
				num *= hoursInDay
			case 'w':
				num *= hoursInWeek
			}

			return strconv.FormatInt(num, 10) + "h"
		})
	}

	return time.ParseDuration(s)
}
