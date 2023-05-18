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
	"os"
	"path/filepath"
	"testing"
	"time"
)

const (
	fileCheckFrequency = 10 * time.Millisecond
	fileCheckTimeout   = 5 * time.Second
	userPerms          = 0700
)

// WaitForFile waits for up to 5 seconds for the given path to exist, and
// returns false if it doesn't.
func WaitForFile(path string) bool {
	ticker := time.NewTicker(fileCheckFrequency)
	defer ticker.Stop()

	timeout := time.NewTimer(fileCheckTimeout)

	for {
		select {
		case <-timeout.C:
			return false
		case <-ticker.C:
			_, err := os.Stat(path)
			if err == nil {
				return true
			}
		}
	}
}

// WaitForFileChange waits for up to 5 seconds for the given path to change, and
// returns false if it doesn't.
func WaitForFileChange(path string, lastMod time.Time) bool {
	ticker := time.NewTicker(fileCheckFrequency)
	defer ticker.Stop()

	timeout := time.NewTimer(fileCheckTimeout)

	for {
		select {
		case <-timeout.C:
			return false
		case <-ticker.C:
			stat, err := os.Stat(path)
			if err == nil && stat.ModTime().After(lastMod) {
				return true
			}
		}
	}
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
