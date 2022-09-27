package main

import (
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/rs/zerolog"
	ex "github.com/wtsi-npg/extendo/v2"
	logs "github.com/wtsi-npg/logshim"
	"github.com/wtsi-npg/logshim-zerolog/zlog"
	"golang.org/x/crypto/ssh/terminal"
)

const logLevel = logs.ErrorLevel

func main() {
	basenames := os.Args[1:]
	idir := "/lustre/scratch123/hgi/teams/hgi/sb10/irods"
	ipath := "/humgen/test/irods_test"

	setupLogger()

	pool := ex.NewClientPool(ex.DefaultClientPoolParams, "")

	putClient, err := pool.Get()
	if err != nil {
		fmt.Printf("start up failure: %s\n", err)
		os.Exit(1)
	}

	_, err = putClient.MkDir(ex.Args{}, ex.RodsItem{IPath: ipath})
	if err != nil {
		fmt.Printf("MkDir failure: %s\n", err)
		os.Exit(1)
	}

	errCh := make(chan error, len(basenames))
	metaCh := make(chan ex.RodsItem, len(basenames))
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()

		metaClient, errg := pool.Get()
		if errg != nil {
			fmt.Printf("start up failure: %s\n", errg)
			os.Exit(1)
		}

		defer metaClient.Stop()

		for item := range metaCh {
			item.IAVUs = []ex.AVU{
				{Attr: "archive-date", Value: "2022-09-27"},
				{Attr: "archived-by", Value: "ibackup-v0"},
			}

			_, errm := metaClient.MetaAdd(ex.Args{}, item)
			if errm != nil {
				errCh <- errm
			}
		}
	}()

	for _, basename := range basenames {
		items, errp := putClient.Put(ex.Args{}, ex.RodsItem{
			IDirectory: idir,
			IFile:      basename,
			IPath:      ipath,
			IName:      basename,
		})
		if errp != nil {
			errCh <- errp

			continue
		}

		checksum, errl := putClient.ListChecksum(items[0])
		fmt.Printf("%s (%s)\n", checksum, errl)

		metaCh <- items[0]
	}

	close(metaCh)
	wg.Wait()
	close(errCh)

	for err := range errCh {
		fmt.Printf("error: %s\n", err)
	}

	putClient.Stop()
	pool.Close()
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
