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
	"errors"
	"os"
	"testing"
	"time"

	"github.com/wtsi-ssg/wr/backoff"
	btime "github.com/wtsi-ssg/wr/backoff/time"
	"github.com/wtsi-ssg/wr/retry"
)

const (
	waitNextSecondTimeout  = 2 * time.Second
	waitNextSecondInterval = 10 * time.Millisecond
	retryTimeout           = 5 * time.Second
)

// ErrFileUnchanged is returned when a file modification time is unchanged.
var ErrFileUnchanged = errors.New("file did not change")

// RequireStable asserts condition remains true for the given duration.
func RequireStable(
	tb testing.TB,
	duration time.Duration,
	interval time.Duration,
	condition func() bool,
	message string,
) {
	tb.Helper()

	if message == "" {
		message = "condition"
	}

	deadline := time.Now().Add(duration)

	for {
		if !condition() {
			tb.Fatalf("expected %s to remain stable", message)
		}

		if time.Now().After(deadline) {
			return
		}

		time.Sleep(interval)
	}
}

// RetryUntilWorks retries f until it returns nil or the default timeout.
func RetryUntilWorks(tb testing.TB, f func() error) error {
	tb.Helper()

	return retryUntilWorks(tb, f, retryTimeout, btime.SecondsRangeBackoff())
}

// RetryUntilWorksCustom retries f until it returns nil or timeout expires.
func RetryUntilWorksCustom(tb testing.TB, f func() error, timeout time.Duration, wait time.Duration) error {
	tb.Helper()

	return retryUntilWorks(tb, f, timeout, &backoff.Backoff{
		Min:     wait,
		Max:     wait,
		Factor:  1,
		Sleeper: &btime.Sleeper{},
	})
}

// WaitForFile waits for a file to exist, returning false on timeout.
func WaitForFile(tb testing.TB, path string) bool {
	tb.Helper()

	err := RetryUntilWorks(tb, func() error {
		_, err := os.Stat(path)

		return err
	})

	return err == nil
}

// WaitForFileChange waits for a file's mod time to change.
func WaitForFileChange(tb testing.TB, path string, lastMod time.Time) bool {
	tb.Helper()

	err := RetryUntilWorks(tb, func() error {
		stat, errs := os.Stat(path)
		if errs == nil && stat.ModTime().After(lastMod) {
			return nil
		}

		if errs == nil {
			return ErrFileUnchanged
		}

		return errs
	})

	return err == nil
}

func retryUntilWorks(tb testing.TB, f func() error, timeout time.Duration, backoff *backoff.Backoff) error {
	tb.Helper()

	ctx, cancelFn := context.WithTimeout(context.Background(), timeout)
	defer cancelFn()

	status := retry.Do(ctx, f, &retry.UntilNoError{}, backoff, "RetryUntilWorks")

	return status.Err
}

// WaitForNextSecond blocks until time.Now advances to the next second.
func WaitForNextSecond(tb testing.TB, start time.Time) {
	tb.Helper()
	Eventually(tb, waitNextSecondTimeout, waitNextSecondInterval, func() bool {
		return time.Now().Unix() > start.Unix()
	}, "time to advance")
}

// Eventually waits until condition returns true or the timeout expires.
func Eventually(tb testing.TB, timeout time.Duration, interval time.Duration, condition func() bool, message string) {
	tb.Helper()

	deadline := time.Now().Add(timeout)

	for {
		if condition() {
			return
		}

		if time.Now().After(deadline) {
			if message == "" {
				message = "condition"
			}

			tb.Fatalf("timed out waiting for %s", message)
		}

		time.Sleep(interval)
	}
}
