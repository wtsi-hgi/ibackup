package cmd

import (
	"crypto/md5" //nolint:gosec
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/dustin/go-humanize" //nolint:misspell
	"github.com/spf13/cobra"
	"github.com/wtsi-hgi/ibackup/baton"
	"github.com/wtsi-hgi/ibackup/internal"
	"github.com/wtsi-hgi/ibackup/set"
	"github.com/wtsi-hgi/ibackup/transfer"
	"github.com/wtsi-npg/extendo/v2"
)

// options for this cmd.
var filestatusIrods bool
var filestatusDB string

// statusCmd represents the status command.
var filestatusCmd = &cobra.Command{
	Use:   "filestatus",
	Short: "Get the status of a file in the database",
	Long: `Get the status of a file in the database.

The user who started the server can use this sub-command to print out a the
status of the given file for each set the file appears in.

The --database option should be the path to the local backup of the ibackup
database, defaulting to the value of the IBACKUP_LOCAL_DB_BACKUP_PATH
environmental variable.

The --irods options will gather additional information about the file, such as
the local and remote checksums.
`,
	Run: func(_ *cobra.Command, args []string) {
		if len(args) < 1 {
			dief("you must supply the file to be checked")
		}

		err := fileSummary(filestatusDB, args[0], filestatusIrods)
		if err != nil {
			die(err)
		}
	},
}

func init() {
	RootCmd.AddCommand(filestatusCmd)

	filestatusCmd.Flags().StringVarP(&filestatusDB, "database", "d",
		os.Getenv("IBACKUP_LOCAL_DB_BACKUP_PATH"), "path to ibackup database file")
	filestatusCmd.Flags().BoolVarP(&filestatusIrods, "irods", "i", false,
		"do additional checking in iRods")
}

func fileSummary(dbPath, filePath string, useIrods bool) error {
	db, err := set.NewRO(dbPath)
	if err != nil {
		return err
	}

	sets, err := db.GetAll()
	if err != nil {
		return err
	}

	fsg := newFSG(db, filePath, useIrods)

	if fsg.baton != nil {
		defer fsg.baton.StopIgnoreError()
	}

	if err := fsg.printFileStatuses(sets); err != nil {
		return err
	}

	if !fsg.found {
		cliPrint("file not found in any registered set\n")
	}

	return nil
}

type fileStatusGetter struct {
	db       *set.DBRO
	filePath string
	baton    *extendo.Client
	useIRods bool
	found    bool

	md5once sync.Once
	md5sum  string
}

func newFSG(db *set.DBRO, filePath string, useIRods bool) *fileStatusGetter {
	fsg := &fileStatusGetter{
		db:       db,
		filePath: filePath,
		useIRods: useIRods,
	}

	if !useIRods {
		return fsg
	}

	baton.GetBatonHandler() //nolint:errcheck

	if client, err := extendo.FindAndStart("--unbuffered", "--no-error"); err != nil {
		warn("error occurred invoking baton; disabling irods mode: %s", err)

		fsg.useIRods = false
	} else {
		fsg.baton = client
	}

	return fsg
}

func (fsg *fileStatusGetter) printFileStatuses(sets []*set.Set) error {
	for _, set := range sets {
		if err := fsg.printFileStatusIfInSet(set); err != nil {
			return err
		}
	}

	return nil
}

func (fsg *fileStatusGetter) printFileStatusIfInSet(s *set.Set) error {
	entry, err := fsg.db.GetFileEntryForSet(s.ID(), fsg.filePath)
	if err != nil {
		var errr set.Error

		if ok := errors.As(err, &errr); ok && errr.Msg == set.ErrInvalidEntry {
			return nil
		}

		return err
	}

	if entry != nil {
		fsg.found = true

		if err := fsg.printFileStatus(s, entry); err != nil {
			return err
		}
	}

	return nil
}

func (fsg *fileStatusGetter) printFileStatus(set *set.Set, f *set.Entry) error {
	dest, err := set.TransformPath(f.Path)
	if err != nil {
		return err
	}

	lastAttemptTime, err := internal.TimeToMeta(f.LastAttempt)
	if err != nil {
		return err
	}

	cliPrintf("file found in set: %s\n", set.Name)
	cliPrintf("           status: %s\n", f.Status)
	cliPrintf("             size: %s\n", humanize.IBytes(f.Size)) //nolint:misspell
	cliPrintf("      destination: %s\n", dest)
	cliPrintf("   last attempted: %s\n", lastAttemptTime)

	if f.LastError != "" {
		cliPrintf("       last error: %s\n", f.LastError)
	}

	if fsg.useIRods {
		return fsg.printIRodsStatus(f.Path, dest)
	}

	return nil
}

func (fsg *fileStatusGetter) printIRodsStatus(local, remote string) error {
	file, err := fsg.baton.ListItem(extendo.Args{AVU: true, Checksum: true}, extendo.RodsItem{
		IPath: filepath.Dir(remote),
		IName: filepath.Base(remote),
	})
	if err != nil {
		return err
	}

	uploadDate, remoteMTime := fsg.getIrodsTimesFromAVUs(file.IAVUs)

	if uploadDate != "" {
		cliPrintf("iRods upload date: %s\n", uploadDate)
	}

	if remoteMTime != "" {
		localTime, err := getMTime(local)
		if err != nil {
			return err
		}

		cliPrintf("      local mTime: %s\n", localTime)
		cliPrintf("      iRods mTime: %s\n", remoteMTime)
	}

	cliPrintf("   iRods checksum: %s\n", file.IChecksum)
	cliPrintf("   local checksum: %s\n", fsg.calcMD5Sum(local))

	return nil
}

func (fsg *fileStatusGetter) getIrodsTimesFromAVUs(avus []extendo.AVU) (string, string) {
	var uploadDate, remoteMTime string

	for _, avu := range avus {
		if avu.Attr == transfer.MetaKeyDate {
			uploadDate = avu.Value
		}

		if avu.Attr == transfer.MetaKeyMtime {
			remoteMTime = avu.Value
		}
	}

	return uploadDate, remoteMTime
}

func getMTime(local string) (string, error) {
	stat, err := os.Stat(local)
	if err != nil {
		return "", err
	}

	return internal.TimeToMeta(stat.ModTime())
}

func (fsg *fileStatusGetter) calcMD5Sum(path string) string {
	fsg.md5once.Do(func() {
		f, err := os.Open(path)
		if err != nil {
			fsg.md5sum = err.Error()

			return
		}

		m := md5.New() //nolint:gosec

		_, err = io.Copy(m, f)
		f.Close()

		if err != nil {
			fsg.md5sum = err.Error()
		} else {
			fsg.md5sum = hex.EncodeToString(m.Sum(nil))
		}
	})

	return fsg.md5sum
}
