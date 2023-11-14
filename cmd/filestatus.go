package cmd

import (
	"bytes"
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"
	"github.com/wtsi-hgi/ibackup/set"
)

// options for this cmd.
var filestatusIrods bool
var filestatusDB string

// statusCmd represents the status command.
var filestatusCmd = &cobra.Command{
	Use:   "filestatus",
	Short: "Get the status of a file in the database",
	Long: `Get the status of a file in the database.
`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) < 1 {
			die("you must supply the file to be checked")
		}

		err := filesummary(filestatusDB, args[0])
		if err != nil {
			die(err.Error())
		}
	},
}

func init() {
	RootCmd.AddCommand(filestatusCmd)

	filestatusCmd.Flags().StringVarP(&filestatusDB, "database", "d",
		os.Getenv("IBACKUP_DATABASE_PATH"), "path to iBackup database file")
	filestatusCmd.Flags().BoolVarP(&filestatusIrods, "irods", "i", false,
		"do additional checking in iRods")
}

type fileStatus struct {
	SetName       string
	Status        string
	Size          uint64
	Destination   string
	Uploaded      time.Time
	IRodsChecksum string
	LocalChecksum string
}

type fileStatusGetter struct {
	ch       chan fileStatus
	db       *set.DBRO
	filePath string
}

func filesummary(dbPath, filePath string) error {
	db, err := set.NewRO(dbPath)
	if err != nil {
		return err
	}

	sets, err := db.GetAll()
	if err != nil {
		return err
	}

	ch := make(chan fileStatus)

	go func() {
		fsg := fileStatusGetter{
			ch:       ch,
			db:       db,
			filePath: filePath,
		}

		if err := fsg.getFileStatuses(sets); err != nil {
			die(err.Error())
		}

		close(ch)
	}()

	printFileStatuses(ch)

	return nil
}

func (fsg *fileStatusGetter) getFileStatuses(sets []*set.Set) error {
	for _, set := range sets {
		if err := fsg.handleSet(set); err != nil {
			return err
		}
	}

	return nil
}

func (fsg *fileStatusGetter) handleSet(s *set.Set) error {
	entry, err := fsg.db.GetFileEntryForSet(s.ID(), fsg.filePath)
	if err != nil {
		var errr set.Error

		if ok := errors.As(err, &errr); ok && errr.Msg == set.ErrInvalidEntry {
			return nil
		}

		return err
	}

	if entry != nil {
		if err := fsg.sendFileStatus(s, entry); err != nil {
			return err
		}
	}

	return nil
}

func (fsg *fileStatusGetter) sendFileStatus(set *set.Set, f *set.Entry) error {
	transformer, err := set.MakeTransformer()
	if err != nil {
		return err
	}

	dest, err := transformer(f.Path)
	if err != nil {
		return err
	}

	fs := fileStatus{
		SetName:     set.Name,
		Status:      f.Status.String(),
		Size:        f.Size,
		Destination: dest,
		Uploaded:    f.LastAttempt,
	}

	if filestatusIrods {
		if err := fs.getIrodsStatus(dest); err != nil {
			return err
		}

		fs.LocalChecksum = calcMD5Sum(f.Path)
	}

	fsg.ch <- fs

	return nil
}

type batonRequest struct {
	Collection string `json:"collection"`
	DataObject string `json:"data_object"`
}

type batonResponse struct {
	Checksum   string `json:"checksum"`
	Timestamps []struct {
		Created    time.Time `json:"created"`
		Modified   time.Time `json:"modified"`
		Replicates int       `json:"replicates"`
	} `json:"timestamps"`
	Message string
}

func (fs *fileStatus) getIrodsStatus(path string) error {
	req, err := json.Marshal(batonRequest{
		Collection: filepath.Dir(path),
		DataObject: filepath.Base(path),
	})
	if err != nil {
		return err
	}

	resp, err := getBatonResponse(req)
	if err != nil {
		return err
	}

	if resp.Message != "" {
		return fmt.Errorf("Error getting irods info: %s", resp.Message) //nolint:goerr113
	}

	fs.IRodsChecksum = resp.Checksum

	return nil
}

func getBatonResponse(req []byte) (*batonResponse, error) {
	var buf bytes.Buffer

	cmd := exec.Command("baton-list", "--checksum", "--timestamp")
	cmd.Stdin = bytes.NewBuffer(req)
	cmd.Stdout = &buf

	if err := cmd.Run(); err != nil {
		return nil, err
	}

	var resp batonResponse

	if err := json.Unmarshal(buf.Bytes(), &resp); err != nil {
		return nil, err
	}

	return &resp, nil
}

func calcMD5Sum(path string) string {
	f, err := os.Open(path)
	if err != nil {
		return err.Error()
	}

	m := md5.New() //nolint:gosec

	_, err = io.Copy(m, f)
	f.Close()

	if err != nil {
		return err.Error()
	}

	return fmt.Sprintf("%x", m.Sum(nil))
}

func printFileStatuses(ch chan fileStatus) {
	for fs := range ch {
		cliPrint("file found in set: %s\n", fs.SetName)
		cliPrint("           status: %s\n", fs.Status)
		cliPrint("     size (bytes): %d\n", fs.Size)
		cliPrint("      destination: %s\n", fs.Destination)
		cliPrint("         uploaded: %s\n", fs.Uploaded)

		if fs.IRodsChecksum != "" {
			cliPrint("   iRods checksum: %s\n", fs.IRodsChecksum)
			cliPrint("   local checksum: %s\n", fs.LocalChecksum)
		}
	}
}
