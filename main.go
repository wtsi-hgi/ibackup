package main

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/rs/zerolog"
	ex "github.com/wtsi-npg/extendo/v2"
	logs "github.com/wtsi-npg/logshim"
	"github.com/wtsi-npg/logshim-zerolog/zlog"
	"golang.org/x/crypto/ssh/terminal"
)

const logLevel = logs.ErrorLevel

type checksummedFile struct {
	path string
	md5  string
}

func main() {
	basenames := os.Args[1:]
	idir := "/lustre/scratch123/hgi/teams/hgi/sb10/irods"
	ipath := "/humgen/test/irods_test"

	setupLogger()

	pool := ex.NewClientPool(ex.DefaultClientPoolParams, "")
	defer pool.Close()

	client, err := pool.Get()
	if err != nil {
		fmt.Printf("start up failure: %s\n", err)
		os.Exit(1)
	}

	defer client.Stop()

	_, err = client.MkDir(ex.Args{}, ex.RodsItem{IPath: ipath})
	if err != nil {
		fmt.Printf("MkDir failure: %s\n", err)
		os.Exit(1)
	}

	cfCh := make(chan checksummedFile, len(basenames))
	errCh := make(chan error, len(basenames))
	var wg sync.WaitGroup

	for _, basename := range basenames {
		source := filepath.Join(idir, basename)
		wg.Add(1)

		go func(path string) {
			defer wg.Done()

			md5, err := calculateMD5ChecksumOfFile(path)
			if err != nil {
				errCh <- err
			} else {
				cfCh <- checksummedFile{path, md5}
			}
		}(source)
	}

	var wg2 sync.WaitGroup
	wg2.Add(1)

	go func() {
		defer wg2.Done()

		for cf := range cfCh {
			_, err = ex.ArchiveDataObject(
				client,
				cf.path,
				filepath.Join(ipath, filepath.Base(cf.path)),
				cf.md5,
				[]ex.AVU{
					{Attr: "archive-date", Value: "2022-09-27"},
					{Attr: "archived-by", Value: "ibackup-v0"},
				},
			)

			if err != nil {
				errCh <- err
			}
		}
	}()

	wg.Wait()
	close(cfCh)
	wg2.Wait()
	close(errCh)

	for err := range errCh {
		fmt.Printf("error: %s\n", err)
	}
}

func setupLogger() logs.Logger {
	var writer io.Writer
	if terminal.IsTerminal(int(os.Stdout.Fd())) {
		writer = zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}
	} else {
		writer = os.Stderr
	}

	logger := zlog.New(zerolog.SyncWriter(writer), logLevel)

	return logs.InstallLogger(logger)
}

func calculateMD5ChecksumOfFile(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}

	defer file.Close()

	hash := md5.New()
	_, err = io.Copy(hash, file)

	hashInBytes := hash.Sum(nil)[:16]

	return hex.EncodeToString(hashInBytes), err
}
