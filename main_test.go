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
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/phayes/freeport"
	. "github.com/smartystreets/goconvey/convey"
)

const app = "ibackup"
const userPerms = 0700

// TestMain builds ourself, starts a test server, runs client tests against the
// server and cleans up afterwards. It's a full e2e integration test.
func TestMain(m *testing.M) {
	var exitCode int
	defer func() {
		os.Exit(exitCode)
	}()

	d1 := buildSelf()
	if d1 == nil {
		return
	}

	defer d1()

	d2, worked := startTestServer()

	if d2 != nil {
		defer d2()
	}

	if !worked {
		exitCode = 2

		return
	}

	exitCode = m.Run()
}

func buildSelf() func() {
	if err := exec.Command("make", "build").Run(); err != nil {
		failMainTest(err.Error())

		return nil
	}

	return func() { os.Remove(app) }
}

func failMainTest(err string) {
	fmt.Println(err) //nolint:forbidigo
}

func startTestServer() (func(), bool) {
	dir, err := os.MkdirTemp("", "ibackup-test")
	if err != nil {
		failMainTest(err.Error())

		return nil, false
	}

	os.Setenv("XDG_STATE_HOME", dir)

	tv, errStr := prepareConfig(dir)
	if errStr != "" {
		failMainTest(errStr)

		return func() { os.RemoveAll(dir) }, false
	}

	logFile := filepath.Join(dir, "log")

	cmd := exec.Command("./"+app, "server", "-k", tv.key, "--logfile", //nolint:gosec
		logFile, "-s", tv.ldapServer, "-l", tv.ldapLookup, "--debug",
		filepath.Join(dir, "db"),
	)

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if errs := cmd.Start(); errs != nil {
		failMainTest(errs.Error())

		return func() { os.RemoveAll(dir) }, false
	}

	worked := waitForServer()

	return func() {
		if errk := cmd.Process.Kill(); err != nil {
			failMainTest(errk.Error())
		}

		errw := cmd.Wait()
		if errw != nil && errw.Error() != "signal: killed" {
			failMainTest(errw.Error())
		}

		// content, errr := os.ReadFile(logFile)
		// if errr == nil {
		// 	fmt.Printf("\nserver log: %s\n", string(content))
		// }

		os.RemoveAll(dir)
	}, worked
}

type testVars struct {
	key        string
	ldapServer string
	ldapLookup string
}

// prepareConfig creates a key and cert to use with a server and looks at
// IBACKUP_TEST_* env vars to set SERVER vars as well.
func prepareConfig(dir string) (*testVars, string) {
	tv := &testVars{}

	serverURL := os.Getenv("IBACKUP_TEST_SERVER_URL")
	if serverURL == "" {
		port, err := freeport.GetFreePort()
		if err != nil {
			return nil, err.Error()
		}

		serverURL = fmt.Sprintf("localhost:%d", port)
	}

	host, _, err := net.SplitHostPort(serverURL)
	if err != nil {
		return nil, err.Error()
	}

	keyPath := filepath.Join(dir, "key.pem")
	certPath := filepath.Join(dir, "cert.pem")

	cmd := exec.Command("openssl", "req", "-x509", "-newkey", "rsa:4096", "-keyout",
		keyPath, "-out", certPath, "-sha256", "-days", "365", "-subj", "/CN="+host, "-addext",
		"subjectAltName = DNS:"+host, "-nodes")

	outb, err := cmd.CombinedOutput()
	if err != nil {
		return nil, "could not create openssl cert: " + err.Error() + "\n" + string(outb) + "\n" + cmd.String()
	}

	tv.key = keyPath

	tv.ldapServer = os.Getenv("IBACKUP_TEST_LDAP_SERVER")
	tv.ldapLookup = os.Getenv("IBACKUP_TEST_LDAP_LOOKUP")

	os.Setenv("IBACKUP_SERVER_URL", serverURL)
	os.Setenv("IBACKUP_SERVER_CERT", certPath)

	return tv, ""
}

func waitForServer() bool {
	worked := false

	var lastClientOutput []byte

	var lastClientErr error

	for i := 0; i < 100; i++ {
		clientCmd := exec.Command("./"+app, "status")

		lastClientOutput, lastClientErr = clientCmd.CombinedOutput()

		if clientCmd.ProcessState.ExitCode() == 0 {
			worked = true

			break
		}

		time.Sleep(50 * time.Millisecond)
	}

	if !worked {
		failMainTest("timeout on server starting: " + lastClientErr.Error() + "\n" + string(lastClientOutput))
	}

	return worked
}

func TestStatus(t *testing.T) {
	SkipConvey("With no server, status fails", t, func() {
		confirmOutput(t, []string{"status"}, 1, "you must supply --url")
	})

	Convey("With no sets defined, status returns no sets", t, func() {
		confirmOutput(t, []string{"status"}, 0, `Global put queue status: 0 queued; 0 reserved to be worked on; 0 failed
Global put client status (/10): 0 creating collections; 0 currently uploading
no backup sets`)
	})

	Convey("Given an added set defined with a directory", t, func() {
		dir := t.TempDir()
		someDir := filepath.Join(dir, "some/dir")

		err := os.MkdirAll(someDir, userPerms)
		So(err, ShouldBeNil)

		exitCode, _ := runBinary(t, "add", "--name", "testAdd", "--transformer",
			"prefix="+dir+":/remote", "--path", someDir)
		So(exitCode, ShouldEqual, 0)

		<-time.After(250 * time.Millisecond)

		Convey("Status tells you where input directories would get uploaded to", func() {
			confirmOutput(t, []string{"status"}, 0, `Global put queue status: 0 queued; 0 reserved to be worked on; 0 failed
Global put client status (/10): 0 creating collections; 0 currently uploading

Name: testAdd
Transformer: prefix=`+dir+`:/remote
Monitored: false; Archive: false
Status: complete
Discovery:
Num files: 0; Size files: 0 B
Uploaded: 0; Failed: 0; Missing: 0
Completed in: 0s
Directories:
  `+someDir+" => /remote/some/dir")
		})
	})

	Convey("Given an added set defined with files", t, func() {
		dir := t.TempDir()
		tempTestFile, err := os.CreateTemp(dir, "testFileSet")
		So(err, ShouldBeNil)

		_, err = io.WriteString(tempTestFile, dir+`/path/to/some/file
`+dir+`/path/to/other/file`)
		So(err, ShouldBeNil)

		exitCode, _ := runBinary(t, "add", "--files", tempTestFile.Name(),
			"--name", "testAddFiles", "--transformer", "prefix="+dir+":/remote")
		So(exitCode, ShouldEqual, 0)

		<-time.After(250 * time.Millisecond)

		Convey("Status tells you an example of where input files would get uploaded to", func() {
			confirmOutput(t, []string{"status", "--name", "testAddFiles"}, 0,
				`Global put queue status: 2 queued; 0 reserved to be worked on; 0 failed
Global put client status (/10): 0 creating collections; 0 currently uploading

Name: testAddFiles
Transformer: prefix=`+dir+`:/remote
Monitored: false; Archive: false
Status: pending upload
Discovery:
Num files: 2; Size files: 0 B (and counting)
Uploaded: 0; Failed: 0; Missing: 2
Example File: `+dir+`/path/to/other/file => /remote/path/to/other/file`)
		})
	})
}

func confirmOutput(t *testing.T, args []string, expectedCode int, expected string) {
	t.Helper()

	exitCode, actual := runBinary(t, args...)

	So(exitCode, ShouldEqual, expectedCode)
	So(actual, ShouldEqual, expected)
}

func runBinary(t *testing.T, args ...string) (int, string) {
	t.Helper()

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
		t.Logf("binary gave error: %s\noutput was: %s\n", err, string(outB))
	}

	return cmd.ProcessState.ExitCode(), strings.Join(lines, "\n")
}
