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
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
	"github.com/wtsi-hgi/ibackup/db"
	"github.com/wtsi-hgi/ibackup/discovery"
	"github.com/wtsi-hgi/ibackup/metadata"
)

const (
	helpTextName        = "a short name for this backup set"
	helpTextTransformer = "'humgen' | 'gengen' | 'prefix=local:remote'"
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
//
//nolint:gochecknoglobals
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
	setMetadata        string
	setReason          metadata.Reason
	setReview          string
	setRemoval         string

	setUser = currentUsername()
)

var (
	ErrCancel       = errors.New("cancelled add")
	ErrDuplicateSet = errors.New(
		"set with this name already exists, please choose a different name " +
			"or use ibackup edit to update it",
	)
	ErrFileNotTransformable = errors.New("could not transform file")
)

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
`,
	PreRun: func(cmd *cobra.Command, _ []string) {
		if mr, _ := cmd.Flags().GetBool("monitor-removals"); mr { //nolint:errcheck
			must(cmd.MarkFlagRequired("monitor"))
		}

		must(RootCmd.MarkPersistentFlagRequired("config"))
	},
	RunE: func(_ *cobra.Command, _ []string) error {
		config, err := newConfig(serverConfig)
		if err != nil {
			return err
		}

		client, err := db.Init(dbDriver, config.DBURI)
		if err != nil {
			return err
		}

		transformer, err := config.Transformers.Compile(setTransformer)
		if err != nil {
			return err
		}

		var monitorDuration time.Duration

		if setMonitor != "" {
			monitorDuration, err = metadata.ParseDuration(setMonitor, 1*time.Hour)
			if err != nil {
				return err
			}
		}

		discoveryItems, err := processDiscovery(transformer, setFiles, setDirs, setItems, setPath, setNull)
		if err != nil {
			return err
		}

		setReviewDate, setRemovalDate, err := metadata.ParseMetadataDates(setReason.AsReason(), setReview, setRemoval)
		if err != nil {
			return err
		}

		metadata, err := metadata.ParseMetaString(setMetadata)
		if err != nil {
			return err
		}

		set := &db.Set{
			Name:            setName,
			Requester:       setUser,
			Transformer:     transformer,
			MonitorTime:     monitorDuration,
			MonitorRemovals: setMonitorRemovals,
			Description:     setDescription,
			Metadata:        metadata,
			DeleteLocal:     setArchive,
			Reason:          setReason.AsReason(),
			ReviewDate:      setReviewDate,
			DeleteDate:      setRemovalDate,
		}

		if err := client.CreateSet(set); err != nil {
			return err
		}

		for _, discovery := range discoveryItems {
			if err := client.AddSetDiscovery(set, discovery); err != nil {
				return err
			}
		}

		return discover(client, set)
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
	addCmd.Flags().StringVar(&setMetadata, "metadata", "", helpTextMetaData)
	addCmd.Flags().Var(&setReason, "reason", helpTextReason)
	addCmd.Flags().StringVar(&setReview, "review", "", helpTextReview)
	addCmd.Flags().StringVar(&setRemoval, "remove", "", helpTextRemoval)

	if isExeOwner() {
		addCmd.Flags().StringVar(&setUser, "user", "", helpTextuser)
	}

	must(addCmd.MarkFlagRequired("name"))
	must(addCmd.MarkFlagRequired("transformer"))

	addCmd.MarkFlagsOneRequired("files", "dirs", "items", "path")
}

func processDiscovery(transformer *db.Transformer, setFiles, setDirs, setItems, setPath string, nullSep bool) ([]*db.Discover, error) { //nolint:gocognit,gocyclo,lll,funlen
	var (
		discoveryItems []*db.Discover
		err            error
	)

	if setFiles != "" {
		if discoveryItems, err = ReadFon(discoveryItems, transformer, setFiles, false, nullSep); err != nil {
			return nil, err
		}
	}

	if setDirs != "" {
		if discoveryItems, err = ReadFon(discoveryItems, transformer, setDirs, true, nullSep); err != nil {
			return nil, err
		}
	}

	if setItems != "" {
		if discoveryItems, err = ReadFoi(discoveryItems, transformer, setItems, nullSep); err != nil {
			return nil, err
		}
	}

	if setPath != "" { //nolint:nestif
		if !transformer.Match(setPath) {
			return nil, fmt.Errorf("path: %s: %w", setPath, db.ErrInvalidTransformPath)
		}

		discoveryItems, err = AddDiscoveryFromFile(discoveryItems, setPath)
		if err != nil {
			return nil, err
		}
	}

	return discoveryItems, nil
}

func ReadFon(existing []*db.Discover, transformer *db.Transformer,
	path string, isDir, nullSep bool) ([]*db.Discover, error) {
	fofnType := db.DiscoverFOFN
	endType := db.DiscoverFile

	if isDir { //nolint:nestif
		fofnType = db.DiscoverFODN
		endType = db.DiscoverDirectory

		if nullSep {
			fofnType = db.DiscoverFODNNull
		}
	} else if nullSep {
		fofnType = db.DiscoverFOFNNull
	}

	fofn, err := discovery.ReadFon(transformer, &db.Discover{
		Type: fofnType,
		Path: path,
	})
	if err != nil {
		return nil, err
	}

	for _, file := range fofn {
		existing = append(existing, &db.Discover{
			Type: endType,
			Path: file,
		})
	}

	return existing, nil
}

func ReadFoi(existing []*db.Discover, transformer *db.Transformer, path string, nullSep bool) ([]*db.Discover, error) {
	fofnType := db.DiscoverFOFN

	if nullSep {
		fofnType = db.DiscoverFOFNNull
	}

	fofn, err := discovery.ReadFon(transformer, &db.Discover{
		Type: fofnType,
		Path: path,
	})
	if err != nil {
		return nil, err
	}

	for _, file := range fofn {
		if existing, err = AddDiscoveryFromFile(existing, file); err != nil {
			return nil, err
		}
	}

	return existing, nil
}

func AddDiscoveryFromFile(existing []*db.Discover, path string) ([]*db.Discover, error) {
	stat, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}

	endType := db.DiscoverFile

	if stat.IsDir() {
		endType = db.DiscoverDirectory
	}

	return append(existing, &db.Discover{
		Type: endType,
		Path: path,
	}), nil
}

func discover(client *db.DB, set *db.Set) error {
	info("Discovery Started")

	var files, missing, symlinks, abnormal int

	if err := discovery.Discover(client, set, func(f *db.File) {
		switch f.Type {
		case db.Abnormal:
			abnormal++
		case db.Symlink:
			symlinks++
		default:
			if f.Status == db.StatusMissing {
				missing++
			} else {
				files++
			}
		}

		printDiscoveryStatus(files, missing, symlinks, abnormal)
	}); err != nil {
		return err
	}

	fmt.Println() //nolint:forbidigo
	info("Discovery Complete")

	return nil
}

func printDiscoveryStatus(files, missing, symlinks, abnormal int) {
	fmt.Printf("\033[2K\rFiles: %d\tSymlinks: %d\tOther: %d\tMissing: %d", files, symlinks, abnormal, missing) //nolint:forbidigo,lll
}
