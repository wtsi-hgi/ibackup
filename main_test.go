/*******************************************************************************
 * Copyright (c) 2023 Genome Research Ltd.
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

package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const app = "ibackup"

func TestMain(m *testing.M) {
	if err := exec.Command("make", "build").Run(); err != nil {
		panic(err)
	}

	key := os.Getenv("IBACKUP_TEST_KEY")
	if key == "" {
		panic("missing key")
	}

	ldapServer := os.Getenv("IBACKUP_TEST_LDAP_SERVER")
	if ldapServer == "" {
		panic("missing ldap server")
	}

	ldapLookup := os.Getenv("IBACKUP_TEST_LDAP_LOOKUP")
	if ldapLookup == "" {
		panic("missing ldap lookup")
	}

	serverURL := os.Getenv("IBACKUP_TEST_SERVER_URL")
	if serverURL == "" {
		panic("no server url")
	}

	os.Setenv("IBACKUP_SERVER_URL", serverURL)

	serverCert := os.Getenv("IBACKUP_TEST_SERVER_CERT")
	if serverCert == "" {
		panic("no server cert")
	}

	os.Setenv("IBACKUP_SERVER_CERT", serverCert)

	dir, err := ioutil.TempDir("", "ibackup-test")
	if err != nil {
		panic(err)
	}

	os.Setenv("XDG_STATE_HOME", dir)

	cmd := exec.Command("./"+app, "server", "-k", key, "--logfile",
		filepath.Join(dir, "log"), "-s", ldapServer, "-l", ldapLookup, "--debug",
		filepath.Join(dir, "db"),
	)

	//fmt.Printf("tempdir: %s\ncmd: %s\n", dir, cmd.String())

	if err := cmd.Start(); err != nil {
		panic(err)
	}

	worked := false

	for i := 0; i < 100; i++ {
		clientCmd := exec.Command("./"+app, "status")
		clientCmd.Run()

		if clientCmd.ProcessState.ExitCode() == 0 {
			worked = true

			break
		}

		time.Sleep(50 * time.Millisecond)
	}

	if !worked {
		panic("timeout on server starting")
	}

	code := m.Run()

	if err := cmd.Process.Kill(); err != nil {
		panic(err)
	}

	cmd.Wait() //nolint:errcheck

	os.Remove(app)
	os.RemoveAll(dir)
	os.Exit(code)
}

func runBinary(args ...string) (int, string) {
	cmd := exec.Command("./"+app, args...)

	outB, err := cmd.CombinedOutput()
	out := string(outB)
	out = strings.TrimRight(out, "\n")
	lines := strings.Split(out, "\n")

	for n, line := range lines {
		if strings.HasPrefix(line, "t=") {
			pos := strings.IndexByte(line, '"')
			lines[n] = line[pos+1 : len(line)-1]
		}

		if strings.HasPrefix(line, "Discovery:") {
			lines[n] = line[:10]
		}
	}

	if err != nil {
		fmt.Printf("binary gave error: %s\n", err)
	}

	return cmd.ProcessState.ExitCode(), strings.Join(lines, "\n")
}

func confirmOutput(t *testing.T, args []string, expectedCode int, expected string) {
	exitCode, actual := runBinary(args...)

	if exitCode != expectedCode {
		t.Fatalf("unexpected error code, actual = %d, expected = %d", exitCode, expectedCode)
	}

	if actual != expected {
		t.Fatalf("actual:\n%s\n\nexpected:\n%s", actual, expected)
	}
}

func TestCliArgs(t *testing.T) {
	t.Run("no backup sets", func(t *testing.T) {
		confirmOutput(t, []string{"status"}, 0, `Global put queue status: 0 queued; 0 reserved to be worked on; 0 failed
Global put client status (/10): 0 creating collections; 0 currently uploading
no backup sets`)

		// expectedCode := 1
		// if exitCode != expectedCode {
		// 	t.Fatalf("unexpected error code, actual = %d, expected = %d", exitCode, expectedCode)
		// }

		// expected := "you must supply --url"

		// if !reflect.DeepEqual(actual, expected) {
		// 	t.Fatalf("actual = %s, expected = %s", actual, expected)
		// }
	})

	dir := t.TempDir()
	someDir := filepath.Join(dir, "some/dir")
	os.MkdirAll(someDir, 0755)
	if exitCode, _ := runBinary("add", "--name", "testAdd", "--transformer", "prefix="+dir+":/remote", "--path", someDir); exitCode != 0 {
		t.Fatalf("failed to add file: exit code %d", exitCode)
	}

	t.Run("path transformation", func(t *testing.T) {
		confirmOutput(t, []string{"status"}, 0, `Global put queue status: 0 queued; 0 reserved to be worked on; 0 failed
Global put client status (/10): 0 creating collections; 0 currently uploading

Name: testAdd
Transformer: prefix=`+dir+`:/remote
Monitored: false; Archive: false
Status: pending upload
Discovery:
Num files: pending; Size files: pending
Uploaded: 0; Failed: 0; Missing: 0
Directories:
  `+someDir+" => /remote/some/dir")
	})
}
