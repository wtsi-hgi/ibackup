/*******************************************************************************
 * Copyright (c) 2023 Genome Research Ltd.
 *
 * Authors: Michael Woolnough <mw31@sanger.ac.uk>
 *          Sendu Bala <sb10@sanger.ac.uk>
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

package internal

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/wtsi-ssg/wr/backoff"
	btime "github.com/wtsi-ssg/wr/backoff/time"
	"github.com/wtsi-ssg/wr/retry"
)

const (
	fileCheckFrequency = 10 * time.Millisecond
	retryTimeout       = 5 * time.Second
	userPerms          = 0700
)

var ErrFileUnchanged = errors.New("file did not change")

// WaitForFile waits for up to 5 seconds for the given path to exist, and
// returns false if it doesn't.
func WaitForFile(t *testing.T, path string) bool {
	t.Helper()

	err := RetryUntilWorks(t, func() error {
		_, err := os.Stat(path)

		return err
	})

	return err == nil
}

// RetryUntilWorks retries the given function until it no longer returns an
// error, or until 5 seconds have passed. It waits a small, increasing interval
// of time between each try.
func RetryUntilWorks(t *testing.T, f func() error) error {
	t.Helper()

	return retryUntilWorks(t, f, retryTimeout, btime.SecondsRangeBackoff())
}

// RetryUntilWorksCustom retries the given function until it no longer returns
// an error, or until timeout has passed. It waits the given wait between each
// try.
func RetryUntilWorksCustom(t *testing.T, f func() error, timeout time.Duration, wait time.Duration) error {
	t.Helper()

	return retryUntilWorks(t, f, timeout, &backoff.Backoff{
		Min:     wait,
		Max:     wait,
		Factor:  1,
		Sleeper: &btime.Sleeper{},
	})
}

func retryUntilWorks(t *testing.T, f func() error, retryTimeout time.Duration, backoff *backoff.Backoff) error {
	t.Helper()

	ctx, cancelFn := context.WithTimeout(context.Background(), retryTimeout)
	defer cancelFn()

	status := retry.Do(ctx, f, &retry.UntilNoError{}, backoff, "RetryUntilWorks")

	if status.Err != nil {
		t.Logf("%s (%s)", status.StoppedBecause, status.Err)
	}

	return status.Err
}

// WaitForFileChange waits for up to 5 seconds for the given path to change, and
// returns false if it doesn't.
func WaitForFileChange(t *testing.T, path string, lastMod time.Time) bool {
	t.Helper()

	err := RetryUntilWorks(t, func() error {
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

// CreateTestFile creates a file at the given path with the given content. It
// creates any directories the path needs as necessary.
func CreateTestFile(t *testing.T, path, contents string) {
	t.Helper()

	dir := filepath.Dir(path)

	err := os.MkdirAll(dir, userPerms)
	if err != nil {
		t.Fatalf("mkdir failed: %s", err)
	}

	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create failed: %s", err)
	}

	_, err = f.WriteString(contents)
	if err != nil {
		t.Fatalf("close failed: %s", err)
	}

	err = f.Close()
	if err != nil {
		t.Fatalf("close failed: %s", err)
	}
}

// CreateTestFileOfLength creates a file at the given path with the given number
// of bytes of content. It creates any directories the path needs as necessary.
func CreateTestFileOfLength(t *testing.T, path string, n int) {
	t.Helper()

	b := make([]byte, n)
	for i := range b {
		b[i] = 1
	}

	CreateTestFile(t, path, string(b))
}
