/*******************************************************************************
 * Copyright (c) 2026 Genome Research Ltd.
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
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/wtsi-hgi/ibackup/fofn"
)

const (
	defaultWatchInterval = 5 * time.Minute
	defaultWatchMinChunk = 250
	defaultWatchMaxChunk = 10000
	defaultWatchRAM      = 1024
	defaultWatchTime     = 8 * time.Hour
	defaultWatchRetries  = 3
)

var (
	errDirRequired    = errors.New("--dir is required")
	errDirNotADir     = errors.New("--dir is not a directory")
	errConfigRequired = fmt.Errorf("%s environment variable must be set", ConfigKey)
	errMinChunkSmall  = errors.New("--min-chunk must be >= 1")
	errMaxChunkSmall  = errors.New("--max-chunk must be >= 1")
	errMinExceedsMax  = errors.New("--min-chunk must be <= --max-chunk")
)

// command-line options for watchfofns.
var (
	watchDir          string
	watchInterval     time.Duration
	watchMinChunk     int
	watchMaxChunk     int
	watchRAM          int
	watchTime         time.Duration
	watchRetries      int
	watchLimitGroup   string
	watchWRDeployment string
)

// watchCtxFunc creates the context used by watchfofns.
// Tests override this to control shutdown.
var watchCtxFunc = defaultWatchCtx //nolint:gochecknoglobals

// watchfofnsCmd represents the watchfofns command.
var watchfofnsCmd = &cobra.Command{
	Use:   "watchfofns",
	Short: "Watch a directory for fofn changes and submit backup jobs",
	Long: `Watch a directory for fofn changes and submit backup jobs.

The watchfofns command monitors a watch directory for subdirectories containing
fofn files. When a new or updated fofn is detected, it creates chunk files and
submits backup jobs via wr.

Each subdirectory must contain a config.yml specifying the transformer to use
and optional metadata.

The IBACKUP_CONFIG environment variable must be set to the ibackup configuration
file (for named transformers).`,
	RunE: func(_ *cobra.Command, _ []string) error {
		return runWatchFofns()
	},
}

// SetWatchCtxFunc overrides the context factory for
// testing. Pass nil to restore the default.
func SetWatchCtxFunc(
	fn func() (context.Context, context.CancelFunc),
) {
	if fn == nil {
		watchCtxFunc = defaultWatchCtx

		return
	}

	watchCtxFunc = fn
}

func defaultWatchCtx() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
}

func init() {
	RootCmd.AddCommand(watchfofnsCmd)
	registerWatchFofnsFlags()
}

func registerWatchFofnsFlags() {
	f := watchfofnsCmd.Flags()

	f.StringVar(&watchDir, "dir", "", "watch directory (required)")
	f.DurationVar(&watchInterval, "interval", defaultWatchInterval, "poll interval")
	f.IntVar(&watchMinChunk, "min-chunk", defaultWatchMinChunk, "minimum files per chunk")
	f.IntVar(&watchMaxChunk, "max-chunk", defaultWatchMaxChunk, "maximum files per chunk")
	f.StringVar(&watchWRDeployment, "wr-deployment", "production", "wr deployment name")

	registerWatchFofnsJobFlags(f)
}

func registerWatchFofnsJobFlags(f *pflag.FlagSet) {
	f.IntVar(&watchRAM, "ram", defaultWatchRAM, "RAM (MB) per put job")
	f.DurationVar(&watchTime, "time", defaultWatchTime, "time limit per put job")
	f.IntVar(&watchRetries, "retries", defaultWatchRetries, "wr job retries")
	f.StringVar(&watchLimitGroup, "limit-group", "irods", "wr limit group")
}

// runWatchFofns validates flags, creates a watcher, and
// runs the polling loop until a signal is received.
func runWatchFofns() error {
	if err := validateWatchFlags(); err != nil {
		return err
	}

	submitter, err := fofn.NewWRSubmitter(watchWRDeployment, appLogger)
	if err != nil {
		return err
	}

	defer func() {
		if derr := submitter.Disconnect(); derr != nil {
			warn("watchfofns: disconnect failed: %s", derr)
		}
	}()

	watcher := createWatcher(submitter)

	ctx, cancel := watchCtxFunc()
	defer cancel()

	info("watchfofns: polling %s every %s",
		watchDir, watchInterval)

	return watcher.Run(ctx, watchInterval)
}

// validateWatchFlags checks all required flags and
// environment variables before starting the watcher.
func validateWatchFlags() error {
	if err := validateWatchDir(); err != nil {
		return err
	}

	if err := validateWatchConfig(); err != nil {
		return err
	}

	return validateChunkFlags()
}

// validateWatchDir checks that --dir was provided and
// points to an existing directory.
func validateWatchDir() error {
	if watchDir == "" {
		return errDirRequired
	}

	fi, err := os.Stat(watchDir)
	if err != nil {
		return fmt.Errorf("--dir: %w", err)
	}

	if !fi.IsDir() {
		return errDirNotADir
	}

	return nil
}

// validateWatchConfig checks that IBACKUP_CONFIG is set.
func validateWatchConfig() error {
	if os.Getenv(ConfigKey) == "" {
		return errConfigRequired
	}

	return nil
}

// validateChunkFlags checks that --min-chunk and
// --max-chunk form a valid range.
func validateChunkFlags() error {
	if watchMinChunk < 1 {
		return fmt.Errorf("%w: got %d", errMinChunkSmall, watchMinChunk)
	}

	if watchMaxChunk < 1 {
		return fmt.Errorf("%w: got %d", errMaxChunkSmall, watchMaxChunk)
	}

	if watchMinChunk > watchMaxChunk {
		return fmt.Errorf("%w: %d > %d", errMinExceedsMax, watchMinChunk, watchMaxChunk)
	}

	return nil
}

// createWatcher builds a Watcher with the configured
// options and the given submitter.
func createWatcher(submitter fofn.JobSubmitter) *fofn.Watcher {
	cfg := fofn.ProcessSubDirConfig{
		MinChunk: watchMinChunk,
		MaxChunk: watchMaxChunk,
		RunConfig: fofn.RunConfig{
			RAM:         watchRAM,
			Time:        watchTime,
			Retries:     uint8(watchRetries), //nolint:gosec
			LimitGroups: []string{watchLimitGroup},
		},
	}

	return fofn.NewWatcher(watchDir, submitter, cfg)
}
