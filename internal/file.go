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
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	. "github.com/smartystreets/goconvey/convey" //nolint:revive,staticcheck
	"github.com/wtsi-hgi/ibackup/statter"
)

const (
	UserPerms = 0700
)

// InitStatter initialises the external walker and statter program, building it
// from source if not specified by the IBACKUP_TEST_STATTER env var and it
// cannot be found in the PATH.
func InitStatter(t *testing.T) {
	t.Helper()

	statterExe := os.Getenv("IBACKUP_TEST_STATTER")
	if statterExe == "" {
		tmp := t.TempDir()

		So(BuildStatter(tmp), ShouldBeNil)

		statterExe = filepath.Join(tmp, "statter")

		t.Setenv("PATH", tmp+":"+os.Getenv("PATH"))
	}

	So(statter.Init(statterExe), ShouldBeNil)
}

// BuildStatter builds an external statter and walk program from source.
func BuildStatter(path string) error {
	cmd := exec.Command("go", "install", "github.com/wtsi-hgi/statter@latest") //nolint:noctx

	cmd.Env = append(os.Environ(), "GOBIN="+path)

	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("statter build failed: %w: %s", err, strings.TrimSpace(string(out)))
	}

	return nil
}

// CreateTestFile creates a file at the given path with the given content. It
// creates any directories the path needs as necessary.
func CreateTestFile(t *testing.T, path, contents string) {
	t.Helper()

	dir := filepath.Dir(path)

	err := os.MkdirAll(dir, UserPerms)
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
