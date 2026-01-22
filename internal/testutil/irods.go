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

package testutil

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	irodsRandomHexBytes   = 4
	irodsCommandTimeout   = 2 * time.Minute
	irodsRetryMaxAttempts = 8
	irodsRetryBackoff     = 250 * time.Millisecond
)

var serialMu sync.Mutex //nolint:gochecknoglobals

var (
	errIRODSCmdNil           = errors.New("irods command runner is nil")
	errIRODSRetriesExhausted = errors.New("exhausted iRODS retries")
)

type irodsLogger interface {
	Logf(format string, args ...any)
}

// IRODSCmd runs iCommands with retry behaviour for transient failures.
type IRODSCmd struct {
	logger      irodsLogger
	timeout     time.Duration
	maxAttempts int
	backoff     time.Duration
}

// RequireIRODSTestCollection returns a unique iRODS test collection path
// or skips the test when required config or commands are missing.
func RequireIRODSTestCollection(tb testing.TB, commands ...string) string {
	tb.Helper()

	base := irodsBase(tb)
	if base == "" {
		return ""
	}

	if len(commands) == 0 {
		commands = defaultIRODSCommands()
	}

	if !ensureIRODSCommands(tb, commands) {
		return ""
	}

	unlock := Serial(tb)
	defer unlock()

	unique := filepath.Join(base, "ibackup_test_"+randomHex(tb, irodsRandomHexBytes))
	setTestCollectionEnv(tb, unique)

	ensureIRODSCollection(tb, unique)
	cleanupIRODSCollection(tb, unique)

	return unique
}

// NewIRODSCmd returns a retrying iCommands runner for tests.
func NewIRODSCmd(tb testing.TB, commands ...string) *IRODSCmd {
	tb.Helper()

	if len(commands) == 0 {
		commands = defaultIRODSCommands()
	}

	if !ensureIRODSCommands(tb, commands) {
		return nil
	}

	return &IRODSCmd{
		logger:      tb,
		timeout:     irodsCommandTimeout,
		maxAttempts: irodsRetryMaxAttempts,
		backoff:     irodsRetryBackoff,
	}
}

// NewIRODSCmdNoTB returns a retrying iCommands runner without test helpers.
func NewIRODSCmdNoTB(timeout time.Duration) *IRODSCmd {
	return &IRODSCmd{
		timeout:     timeout,
		maxAttempts: irodsRetryMaxAttempts,
		backoff:     irodsRetryBackoff,
	}
}

// Run executes an iCommand with retries for transient failures.
func (cmd *IRODSCmd) Run(command string, args ...string) ([]byte, error) {
	if cmd == nil {
		return nil, errIRODSCmdNil
	}

	return runIRODSCommandWithRetry(
		cmd.logger,
		cmd.timeout,
		cmd.maxAttempts,
		cmd.backoff,
		command,
		args...,
	)
}

// IRM runs irm with retry handling.
func (cmd *IRODSCmd) IRM(args ...string) ([]byte, error) {
	return cmd.Run("irm", args...)
}

// IMKDIR runs imkdir with retry handling.
func (cmd *IRODSCmd) IMKDIR(args ...string) ([]byte, error) {
	return cmd.Run("imkdir", args...)
}

// ILS runs ils with retry handling.
func (cmd *IRODSCmd) ILS(args ...string) ([]byte, error) {
	return cmd.Run("ils", args...)
}

// IGET runs iget with retry handling.
func (cmd *IRODSCmd) IGET(args ...string) ([]byte, error) {
	return cmd.Run("iget", args...)
}

// IPUT runs iput with retry handling.
func (cmd *IRODSCmd) IPUT(args ...string) ([]byte, error) {
	return cmd.Run("iput", args...)
}

// IMETA runs imeta with retry handling.
func (cmd *IRODSCmd) IMETA(args ...string) ([]byte, error) {
	return cmd.Run("imeta", args...)
}

// ICHMOD runs ichmod with retry handling.
func (cmd *IRODSCmd) ICHMOD(args ...string) ([]byte, error) {
	return cmd.Run("ichmod", args...)
}

// IUSERINFO runs iuserinfo with retry handling.
func (cmd *IRODSCmd) IUSERINFO(args ...string) ([]byte, error) {
	return cmd.Run("iuserinfo", args...)
}

func irodsBase(tb testing.TB) string {
	tb.Helper()

	base := os.Getenv("IBACKUP_TEST_COLLECTION_BASE")
	if base == "" {
		base = os.Getenv("IBACKUP_TEST_COLLECTION")
		if base != "" {
			_ = os.Setenv("IBACKUP_TEST_COLLECTION_BASE", base)
		}
	}

	if base == "" {
		tb.Skip("skipping iRODS tests since IBACKUP_TEST_COLLECTION[_BASE] not set")

		return ""
	}

	return base
}

func defaultIRODSCommands() []string {
	return []string{"imkdir", "irm", "ils", "iget", "imeta", "ichmod"}
}

func ensureIRODSCommands(tb testing.TB, commands []string) bool {
	tb.Helper()

	for _, command := range commands {
		available, err := checkIRODSCommand(command)
		if err != nil {
			tb.Fatalf("error checking iRODS command %s: %v", command, err)
		}

		if !available {
			tb.Skipf("skipping iRODS tests since the iCommand '%s' is not available", command)

			return false
		}
	}

	return true
}

func checkIRODSCommand(command string) (bool, error) {
	_, err := exec.LookPath(command)
	if err != nil {
		if errors.Is(err, exec.ErrNotFound) {
			return false, nil
		}

		return false, fmt.Errorf("error checking iRODS command %s: %w", command, err)
	}

	return true, nil
}

func runIRODSCommandWithRetry(
	logger irodsLogger,
	timeout time.Duration,
	maxAttempts int,
	backoff time.Duration,
	command string,
	args ...string,
) ([]byte, error) {
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		out, err := runIRODSCommandOnce(timeout, command, args...)
		if err == nil || shouldTreatMissingPath(command, out) {
			return out, nil
		}

		if !shouldRetryIRODS(out, err, attempt, maxAttempts) {
			logRetriesExhausted(logger, command, args, attempt, maxAttempts, err, out)

			return out, err
		}

		time.Sleep(backoff)
		backoff *= 2
	}

	return nil, fmt.Errorf("%w running %s %v", errIRODSRetriesExhausted, command, args)
}

func runIRODSCommandOnce(timeout time.Duration, command string, args ...string) ([]byte, error) {
	ctx, cancelFn := context.WithTimeout(context.Background(), timeout)
	defer cancelFn()

	return exec.CommandContext(ctx, command, args...).CombinedOutput() //nolint:gosec
}

func shouldTreatMissingPath(command string, out []byte) bool {
	return command == "irm" && isIRODSMissingPath(string(out))
}

func shouldRetryIRODS(out []byte, err error, attempt int, maxAttempts int) bool {
	if isIRODSMissingPath(string(out)) || isIRODSInvalidArgument(string(out)) {
		return false
	}

	if isIRODSAlreadyHasItem(string(out)) {
		return false
	}

	return isTransientIRODSError(string(out), err) && attempt < maxAttempts
}

func logRetriesExhausted(
	logger irodsLogger,
	command string,
	args []string,
	attempt int,
	maxAttempts int,
	err error,
	out []byte,
) {
	if logger == nil || !isTransientIRODSError(string(out), err) || attempt < maxAttempts {
		return
	}

	logger.Logf(
		"exhausted iRODS retries running %s %v (attempt %d/%d): %v; output: %s",
		command,
		args,
		attempt,
		maxAttempts,
		err,
		strings.TrimSpace(string(out)),
	)
}

func isIRODSMissingPath(output string) bool {
	return strings.Contains(output, "does not exist") || strings.Contains(output, "No rows found")
}

func isIRODSInvalidArgument(output string) bool {
	return strings.Contains(output, "CAT_INVALID_ARGUMENT") ||
		strings.Contains(output, "Input path is not a collection and not a dataObj")
}

func isTransientIRODSError(output string, err error) bool {
	if err == nil {
		return false
	}

	transientMarkers := []string{
		"connectToRhost error",
		"SYS_HEADER_READ_LEN_ERR",
		"SYS_SOCK_CONNECT_ERR",
		"connection refused",
		"Connection refused",
		"Failed to connect",
		"remote addresses",
	}

	for _, marker := range transientMarkers {
		if strings.Contains(output, marker) {
			return true
		}
	}

	return false
}

func isIRODSAlreadyHasItem(output string) bool {
	return strings.Contains(output, "CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME")
}

// Serial locks a shared test mutex and returns an unlock function.
func Serial(tb testing.TB) func() {
	tb.Helper()
	serialMu.Lock()

	return func() {
		serialMu.Unlock()
	}
}

func randomHex(tb testing.TB, bytesLen int) string {
	tb.Helper()

	buf := make([]byte, bytesLen)
	if _, err := rand.Read(buf); err != nil {
		tb.Fatalf("failed to read random bytes: %v", err)
	}

	return hex.EncodeToString(buf)
}

func setTestCollectionEnv(tb testing.TB, collection string) {
	tb.Helper()

	if envSetter, ok := tb.(interface{ Setenv(key, value string) }); ok {
		envSetter.Setenv("IBACKUP_TEST_COLLECTION", collection)

		return
	}

	_ = os.Setenv("IBACKUP_TEST_COLLECTION", collection)
}

func ensureIRODSCollection(tb testing.TB, collection string) {
	tb.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), irodsCommandTimeout)
	defer cancel()

	_ = exec.CommandContext(ctx, "irm", "-rf", collection).Run() //nolint:errcheck,gosec

	if err := exec.CommandContext(ctx, "imkdir", "-p", collection).Run(); err != nil { //nolint:gosec
		tb.Fatalf("failed to create iRODS collection %s: %v", collection, err)
	}
}

func cleanupIRODSCollection(tb testing.TB, collection string) {
	tb.Helper()

	tb.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), irodsCommandTimeout)
		defer cancel()

		exec.CommandContext(ctx, "irm", "-rf", collection).Run() //nolint:errcheck,gosec
	})
}
