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
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/wtsi-hgi/ibackup/internal/scanner"
	"github.com/wtsi-hgi/ibackup/server"
	"github.com/wtsi-hgi/ibackup/set"
	"github.com/wtsi-hgi/ibackup/transfer"
)

const (
	hoursInDay          = 24
	hoursInWeek         = hoursInDay * 7
	helpTextName        = "a short name for this backup set"
	helpTextTransformer = "'prefix=local:remote'"
	helpTextDescription = "a long description of this backup set"
	helpTextFiles       = "path to file with one absolute local file path per line"
	helpTextDirs        = "path to file with one absolute local directory path per line"
	helpTextItems       = "path to file with one absolute local directory or file path per line"
	helpTextPath        = "path to a single file or directory you wish to backup"
	helpTextNull        = "input paths are terminated by a null character instead of a new line"
	helpTextMonitor     = "monitor the paths for changes and new files to upload the given time period " +
		"(eg. 1d for 1 day, or 2w for 2 weeks, min 1h) after completion"
	helpTextMonitorRemovals = "also monitor and update the set for files removed locally"
	helpTextArchive         = "delete local files after successfully uploading them (deletions not yet implemented)"
	helpTextuser            = "pretend to be the this user (only works if you started the server)"
	helpTextMetaData        = "key=val;key=val metadata to apply to all files in the set"
	helpTextReason          = "storage reason: 'backup' | 'archive' | 'quarantine'"
	helpTextReview          = "time until review date (<number><y|m>, eg. 1y for 1 year), " +
		"or exact review date in the format YYYY-MM-DD"
	helpTextRemoval = "time until removal date (<number><y|m>, eg. 1y for 1 year), " +
		"or exact removal date in the format YYYY-MM-DD"
)

// options for this cmd.
var (
	setName            string
	setTransformer     string
	setDescription     string
	setFiles           string
	setDirs            string
	setItems           string
	setPath            string
	setNull            bool
	setMonitor         string
	setMonitorRemovals bool
	setArchive         bool
	setUser            string
	setMetadata        string
	setReason          transfer.Reason
	setReview          string
	setRemoval         string
)

var (
	ErrCancel       = errors.New("cancelled add")
	ErrDuplicateSet = errors.New(
		"set with this name already exists, please choose a different name " +
			"or use ibackup edit to update it",
	)
)

type InvalidMonitorDurationError struct {
	Err error
}

func (e *InvalidMonitorDurationError) Error() string {
	return fmt.Sprintf("invalid monitor duration: %v", e.Err)
}

type MonitorDurationTooShortError struct {
	Duration    time.Duration
	MinDuration time.Duration
}

func (e *MonitorDurationTooShortError) Error() string {
	return fmt.Sprintf("monitor duration must be %s or more, not %s", e.MinDuration, e.Duration)
}

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
--name : a short unique name for this backup set; must not contain comma.
--transformer : define where your local files should be backed up to by defining
  a conversion of local path to a remote iRODS path:` + "\u200b" + `
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
--monitor-removals : after providing --monitor, you can provide this to also
					 monitor if file and directory paths have been removed
					 locally. If any paths are found to be removed, the monitor
					 will remove them from the set as well as from iRODS. This
					 option means your set is no longer a proper back up, and
					 you cannot restore locally removed files.
--archive : delete local files after successfully uploading them. (The actual
            deletion is not yet implemented, but you can at least record the
		    fact you wanted deletion now, so they can be deleted in the future.)
--reason  : the reason you are storing the set, which can be 'backup', 'archive' 
			or 'quarantine'. The default is 'backup' which will default review 
			date to 6 months and removal date to 1 year. 'archive' defaults 
			review date to 1 year and removal date to 2 years, while 
			'quarantine' defaults review date to 2 months and removal date to 3 
			months.
--review  : the date when the set should be reviewed, provided as a duration in 
			months or years or the date itself. E.g. '6m' for a review date 6 
			months from now, '2y' for a review date 2 years from now, 
			'2030-04-21' for the review date to be as provided. This date must 
			be before the removal date.
--remove  : the date when the set should be removed, provided as a duration in 
			months or years or the date itself. Input format is the same as for 
			--review. This date must be after the review date.

Having added a set, you can use 'ibackup status' to monitor the backup progress
of your sets. If you add a set with the same --name again, you will overwrite
its properties. Eg. if you provide a different list of files to a monitored set,
it will backup and monitor the new list of files in future.

If you are the user who started the ibackup server, you can use the --user
option to add sets on behalf of other users.

` + configSubHelp,
	Run: func(cmd *cobra.Command, args []string) {
		ensureURL()

		if setFiles == "" && setDirs == "" && setPath == "" && setItems == "" {
			dief("at least one of --files or --dirs or --items or --path must be provided")
		}

		if setMonitor == "" && setMonitorRemovals {
			dief("cannot use --monitor-removals without --monitor")
		}

		if setTransformer == "" {
			dief("-t must be provided")
		}

		var monitorDuration time.Duration
		if setMonitor != "" {
			var err error

			monitorDuration, err = parseDuration(setMonitor, 1*time.Hour)
			if err != nil {
				die(err)
			}
		}

		client, err := newServerClient(serverURL, serverCert)
		if err != nil {
			die(err)
		}

		files := readPaths(setFiles, scanner.FofnLineSplitter(setNull))
		dirs := readPaths(setDirs, scanner.FofnLineSplitter(setNull))

		filesAndDirs, err := getPathsFromInput(setItems, setPath, setNull)
		if err != nil {
			die(err)
		}

		files, dirs = categorisePaths(filesAndDirs, files, dirs)

		set, err := checkExistingSet(client, setName, setUser)
		if err != nil {
			die(err)
		}

		var prevMeta map[string]string
		if set != nil {
			prevMeta = set.Metadata
		}

		meta, err := transfer.HandleMeta(setMetadata, setReason, setReview, setRemoval, prevMeta)
		if err != nil {
			dief("metadata error: %s", err)
		}

		err = add(client, setName, setUser, setTransformer, setDescription, monitorDuration,
			setMonitorRemovals, setArchive, files, dirs, meta)
		if err != nil {
			die(err)
		}

		info("your backup set has been saved and will now be processed")
	},
}

func init() {
	RootCmd.AddCommand(addCmd)

	// flags specific to this sub-command
	addCmd.Flags().StringVarP(&setName, "name", "n", "", helpTextName)
	addCmd.Flags().StringVarP(&setTransformer, "transformer", "t",
		os.Getenv("IBACKUP_TRANSFORMER"), helpTextTransformer)
	addCmd.Flags().StringVarP(&setFiles, "files", "f", "", helpTextFiles)
	addCmd.Flags().StringVarP(&setDirs, "dirs", "d", "", helpTextDirs)
	addCmd.Flags().StringVarP(&setItems, "items", "i", "", helpTextItems)
	addCmd.Flags().StringVarP(&setPath, "path", "p", "", helpTextPath)
	addCmd.Flags().BoolVarP(&setNull, "null", "0", false, helpTextNull)
	addCmd.Flags().StringVar(&setDescription, "description", "", helpTextDescription)
	addCmd.Flags().StringVarP(&setMonitor, "monitor", "m", "", helpTextMonitor)
	addCmd.Flags().BoolVar(&setMonitorRemovals, "monitor-removals", false, helpTextMonitorRemovals)
	addCmd.Flags().BoolVarP(&setArchive, "archive", "a", false, helpTextArchive)
	addCmd.Flags().StringVar(&setUser, "user", currentUsername(), helpTextuser)
	addCmd.Flags().StringVar(&setMetadata, "metadata", "", helpTextMetaData)
	addCmd.Flags().Var(&setReason, "reason", helpTextReason)
	addCmd.Flags().StringVar(&setReview, "review", "", helpTextReview)
	addCmd.Flags().StringVar(&setRemoval, "remove", "", helpTextRemoval)

	if err := addCmd.MarkFlagRequired("name"); err != nil {
		die(err)
	}
}

func updateAddDescFlag() {
	var (
		txLongDesc, txFlagDesc string
		maxLen                 = 0
		keys                   = make([]string, 0, len(Config.Transformers))
	)

	for name := range Config.Transformers {
		maxLen = max(maxLen, len(name))

		keys = append(keys, name)
	}

	slices.Sort(keys)

	for _, name := range keys {
		txFlagDesc += "'" + name + "' | "                                       //nolint:perfsprint
		txLongDesc += "\n    " + name + strings.Repeat(" ", maxLen-len(name)) + //nolint:perfsprint
			": " + Config.Transformers[name].Description
	}

	if len(Config.Transformers) > 0 {
		addCmd.Long = strings.ReplaceAll(addCmd.Long, "\u200b", txLongDesc)
		addCmd.Flags().Lookup("transformer").Usage = txFlagDesc + helpTextTransformer
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
		dief("failed to read whole file: %s", serr.Error())
	}

	return paths
}

func getPathsFromInput(items, path string, null bool) ([]string, error) {
	var paths []string

	if items != "" {
		itemPaths := readPaths(items, scanner.FofnLineSplitter(null))

		paths = append(paths, itemPaths...)
	}

	if path != "" {
		absPath, err := filepath.Abs(path)
		if err != nil {
			return nil, err
		}

		paths = append(paths, absPath)
	}

	return paths, nil
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
		dief("Invalid path: %s", err)

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
	monitor time.Duration, monitorRemovals, archive bool, files, dirs []string, meta *transfer.Meta) error {
	set := &set.Set{
		Name:            name,
		Requester:       requester,
		Transformer:     transformer,
		Description:     description,
		MonitorTime:     monitor,
		MonitorRemovals: monitorRemovals,
		DeleteLocal:     archive,
		Metadata:        meta.Metadata(),
	}

	if err := client.AddOrUpdateSet(set); err != nil {
		return err
	}

	if err := client.MergeFiles(set.ID(), files); err != nil {
		return err
	}

	if err := client.MergeDirs(set.ID(), dirs); err != nil {
		return err
	}

	return client.TriggerDiscovery(set.ID(), false)
}

func checkExistingSet(client *server.Client, name, requester string) (*set.Set, error) {
	set, err := client.GetSetByName(requester, name)
	if errors.Is(err, server.ErrBadSet) {
		return set, nil
	} else if err != nil {
		return nil, err
	}

	return set, ErrDuplicateSet
}

func parseDuration(s string, minDuration time.Duration) (time.Duration, error) {
	s = convertDurationString(s)

	monitorDuration, err := time.ParseDuration(s)
	if err != nil {
		return 0, &InvalidMonitorDurationError{
			Err: err,
		}
	}

	if monitorDuration < minDuration {
		return 0, &MonitorDurationTooShortError{
			Duration:    monitorDuration,
			MinDuration: minDuration,
		}
	}

	return monitorDuration, nil
}

func convertDurationString(s string) string {
	durationRegex := regexp.MustCompile("[0-9]+[dw]")

	return durationRegex.ReplaceAllStringFunc(s, func(d string) string {
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
