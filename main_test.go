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

package main

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/phayes/freeport"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/ibackup/internal"
	"github.com/wtsi-hgi/ibackup/put"
)

const app = "ibackup"
const userPerms = 0700

type TestServer struct {
	key          string
	cert         string
	ldapServer   string
	ldapLookup   string
	url          string
	dir          string
	dbFile       string
	backupFile   string
	remoteDBFile string
	logFile      string
	env          []string

	cmd *exec.Cmd
}

func NewTestServer(t *testing.T) *TestServer {
	t.Helper()

	dir := t.TempDir()

	s := new(TestServer)

	s.prepareFilePaths(dir)
	s.prepareConfig()

	s.startServer()

	return s
}

func (s *TestServer) prepareFilePaths(dir string) {
	s.dir = dir
	s.dbFile = filepath.Join(s.dir, "db")
	s.logFile = filepath.Join(s.dir, "log")

	home, err := os.UserHomeDir()
	So(err, ShouldBeNil)

	s.env = []string{"XDG_STATE_HOME=" + s.dir, "PATH=" + os.Getenv("PATH"), "HOME=" + home}
}

// prepareConfig creates a key and cert to use with a server and looks at
// IBACKUP_TEST_* env vars to set SERVER vars as well.
func (s *TestServer) prepareConfig() {
	s.url = os.Getenv("IBACKUP_TEST_SERVER_URL")
	if s.url == "" {
		port, err := freeport.GetFreePort()
		So(err, ShouldBeNil)

		s.url = fmt.Sprintf("localhost:%d", port)
	}

	host, _, err := net.SplitHostPort(s.url)
	So(err, ShouldBeNil)

	s.key = filepath.Join(s.dir, "key.pem")
	s.cert = filepath.Join(s.dir, "cert.pem")

	cmd := exec.Command( //nolint:gosec
		"openssl", "req", "-x509", "-newkey", "rsa:4096", "-keyout", s.key,
		"-out", s.cert, "-sha256", "-days", "365", "-subj", "/CN="+host,
		"-addext", "subjectAltName = DNS:"+host, "-nodes",
	)

	_, err = cmd.CombinedOutput()
	So(err, ShouldBeNil)

	s.ldapServer = os.Getenv("IBACKUP_TEST_LDAP_SERVER")
	s.ldapLookup = os.Getenv("IBACKUP_TEST_LDAP_LOOKUP")
}

func (s *TestServer) startServer() {
	args := []string{"server", "--cert", s.cert, "--key", s.key, "--logfile", s.logFile,
		"-s", s.ldapServer, "-l", s.ldapLookup, "--url", s.url, "--debug"}

	if s.remoteDBFile != "" {
		args = append(args, "--remote_backup", s.remoteDBFile)
	}

	args = append(args, s.dbFile)

	if s.backupFile != "" {
		args = append(args, s.backupFile)
	}

	s.cmd = exec.Command("./"+app, args...)
	s.cmd.Env = s.env
	s.cmd.Stdout = os.Stdout
	s.cmd.Stderr = os.Stderr

	err := s.cmd.Start()
	So(err, ShouldBeNil)

	s.waitForServer()

	servers = append(servers, s)
}

func (s *TestServer) waitForServer() {
	worked := false

	cmd := []string{"status", "--cert", s.cert, "--url", s.url}

	for i := 0; i < 100; i++ {
		clientCmd := exec.Command("./"+app, cmd...)
		clientCmd.Env = []string{"XDG_STATE_HOME=" + s.dir}

		if clientCmd.Run() == nil {
			worked = true

			break
		}

		time.Sleep(50 * time.Millisecond)
	}

	So(worked, ShouldEqual, true)
}

func (s *TestServer) runBinary(t *testing.T, args ...string) (int, string) {
	t.Helper()

	args = append([]string{"--url", s.url, "--cert", s.cert}, args...)

	cmd := exec.Command("./"+app, args...)
	cmd.Env = s.env

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
		var exitError *exec.ExitError
		if !errors.As(err, &exitError) {
			t.Logf("binary gave error: %s\noutput was: %s\n", err, string(outB))
		}
	}

	return cmd.ProcessState.ExitCode(), strings.Join(lines, "\n")
}

func (s *TestServer) confirmOutput(t *testing.T, args []string, expectedCode int, expected string) {
	t.Helper()

	exitCode, actual := s.runBinary(t, args...)

	So(exitCode, ShouldEqual, expectedCode)
	So(actual, ShouldEqual, expected)
}

func (s *TestServer) addSetForTesting(t *testing.T, name, transformer, path string) {
	t.Helper()

	exitCode, _ := s.runBinary(t, "add", "--name", name, "--transformer", transformer, "--path", path)
	So(exitCode, ShouldEqual, 0)

	<-time.After(250 * time.Millisecond)
}

func (s *TestServer) Shutdown() error {
	err := s.cmd.Process.Signal(os.Interrupt)

	errCh := make(chan error, 1)

	go func() { errCh <- s.cmd.Wait() }()

	select {
	case errb := <-errCh:
		return errors.Join(err, errb)
	case <-time.After(5 * time.Second):
		return errors.Join(err, s.cmd.Process.Kill())
	}
}

var servers []*TestServer //nolint:gochecknoglobals

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

	exitCode = m.Run()

	for _, s := range servers {
		if err := s.Shutdown(); err != nil {
			fmt.Println("error shutting down server: ", err) //nolint:forbidigo
		}
	}
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

func TestNoServer(t *testing.T) {
	Convey("With no server, status fails", t, func() {
		s := new(TestServer)

		s.confirmOutput(t, []string{"status"}, 1, "you must supply --url")
	})
}

func TestStatus(t *testing.T) {
	Convey("With a started server", t, func() {
		s := NewTestServer(t)
		So(s, ShouldNotBeNil)

		Convey("With no sets defined, status returns no sets", func() {
			s.confirmOutput(t, []string{"status"}, 0, `Global put queue status: 0 queued; 0 reserved to be worked on; 0 failed
Global put client status (/10): 0 creating collections; 0 currently uploading
no backup sets`)
		})

		Convey("Given an added set defined with a directory", func() {
			transformer, localDir, remoteDir := prepareForSetWithEmptyDir(t)
			s.addSetForTesting(t, "testAdd", transformer, localDir)

			Convey("Status tells you where input directories would get uploaded to", func() {
				s.confirmOutput(t, []string{"status"}, 0, `Global put queue status: 0 queued; 0 reserved to be worked on; 0 failed
Global put client status (/10): 0 creating collections; 0 currently uploading

Name: testAdd
Transformer: `+transformer+`
Monitored: false; Archive: false
Status: complete
Discovery:
Num files: 0; Symlinks: 0; Hardlinks: 0; Size files: 0 B
Uploaded: 0; Failed: 0; Missing: 0
Completed in: 0s
Directories:
  `+localDir+" => "+remoteDir)
			})
		})

		Convey("Given an added set defined with files", func() {
			dir := t.TempDir()
			tempTestFile, err := os.CreateTemp(dir, "testFileSet")
			So(err, ShouldBeNil)

			_, err = io.WriteString(tempTestFile, dir+`/path/to/some/file
`+dir+`/path/to/other/file`)
			So(err, ShouldBeNil)

			exitCode, _ := s.runBinary(t, "add", "--files", tempTestFile.Name(),
				"--name", "testAddFiles", "--transformer", "prefix="+dir+":/remote")
			So(exitCode, ShouldEqual, 0)

			<-time.After(250 * time.Millisecond)

			Convey("Status tells you an example of where input files would get uploaded to", func() {
				s.confirmOutput(t, []string{"status", "--name", "testAddFiles"}, 0,
					`Global put queue status: 2 queued; 0 reserved to be worked on; 0 failed
Global put client status (/10): 0 creating collections; 0 currently uploading

Name: testAddFiles
Transformer: prefix=`+dir+`:/remote
Monitored: false; Archive: false
Status: pending upload
Discovery:
Num files: 2; Symlinks: 0; Hardlinks: 0; Size files: 0 B (and counting)
Uploaded: 0; Failed: 0; Missing: 2
Example File: `+dir+`/path/to/other/file => /remote/path/to/other/file`)
			})
		})

		Convey("Given an added set defined with a non-humgen dir and humgen transformer, it warns about the issue", func() {
			_, localDir, _ := prepareForSetWithEmptyDir(t)
			s.addSetForTesting(t, "badHumgen", "humgen", localDir)

			s.confirmOutput(t, []string{"status", "-n", "badHumgen"}, 0,
				`Global put queue status: 0 queued; 0 reserved to be worked on; 0 failed
Global put client status (/10): 0 creating collections; 0 currently uploading

Name: badHumgen
Transformer: humgen
Monitored: false; Archive: false
Status: complete
Discovery:
Num files: 0; Symlinks: 0; Hardlinks: 0; Size files: 0 B
Uploaded: 0; Failed: 0; Missing: 0
Completed in: 0s
Directories:
your transformer didn't work: not a valid humgen lustre path [`+localDir+`/file.txt]
  `+localDir)
		})

		Convey("Given an added set with an inaccessible subfolder, print the error to the user", func() {
			transformer, localDir, remote := prepareForSetWithEmptyDir(t)
			badPermDir := filepath.Join(localDir, "bad-perms-dir")
			err := os.Mkdir(badPermDir, userPerms)
			So(err, ShouldBeNil)

			err = os.Chmod(badPermDir, 0)
			So(err, ShouldBeNil)

			defer func() {
				err = os.Chmod(filepath.Dir(badPermDir), userPerms)
				So(err, ShouldBeNil)
			}()

			s.addSetForTesting(t, "badPerms", transformer, localDir)

			s.confirmOutput(t, []string{"status", "-n", "badPerms"}, 0,
				`Global put queue status: 0 queued; 0 reserved to be worked on; 0 failed
Global put client status (/10): 0 creating collections; 0 currently uploading

Name: badPerms
Transformer: `+transformer+`
Monitored: false; Archive: false
Status: complete
Warning: open `+badPermDir+`: permission denied
Discovery:
Num files: 0; Symlinks: 0; Hardlinks: 0; Size files: 0 B
Uploaded: 0; Failed: 0; Missing: 0
Completed in: 0s
Directories:
  `+localDir+" => "+remote)
		})

		Convey("Given an added set defined with a humgen transformer, the remote directory is correct", func() {
			humgenFile := "/lustre/scratch125/humgen/teams/hgi/mercury/ibackup/file_for_testsuite.do_not_delete"
			humgenDir := filepath.Dir(humgenFile)

			if _, err := os.Stat(humgenDir); err != nil {
				SkipConvey("skip humgen transformer test since not in humgen", func() {})

				return
			}

			s.addSetForTesting(t, "humgenSet", "humgen", humgenFile)

			s.confirmOutput(t, []string{"status", "-n", "humgenSet"}, 0,
				`Global put queue status: 1 queued; 0 reserved to be worked on; 0 failed
Global put client status (/10): 0 creating collections; 0 currently uploading

Name: humgenSet
Transformer: humgen
Monitored: false; Archive: false
Status: pending upload
Discovery:
Num files: 1; Symlinks: 0; Hardlinks: 0; Size files: 0 B (and counting)
Uploaded: 0; Failed: 0; Missing: 0
Example File: `+humgenFile+" => /humgen/teams/hgi/scratch125/mercury/ibackup/file_for_testsuite.do_not_delete")
		})

		Convey("You can add a set with links and their counts show correctly", func() {
			dir := t.TempDir()
			regularPath := filepath.Join(dir, "reg")
			linkPath := filepath.Join(dir, "link")
			symPath := filepath.Join(dir, "sym")
			symPath2 := filepath.Join(dir, "sym2")

			f, err := os.Create(regularPath)
			So(err, ShouldBeNil)
			_, err = f.WriteString("regular")
			So(err, ShouldBeNil)
			err = f.Close()
			So(err, ShouldBeNil)

			err = os.Link(regularPath, linkPath)
			So(err, ShouldBeNil)

			err = os.Symlink(linkPath, symPath)
			So(err, ShouldBeNil)
			err = os.Symlink(symPath, symPath2)
			So(err, ShouldBeNil)

			exitCode, _ := s.runBinary(t, "add", "-p", dir,
				"--name", "testLinks", "--transformer", "prefix="+dir+":/remote")
			So(exitCode, ShouldEqual, 0)

			<-time.After(250 * time.Millisecond)

			s.confirmOutput(t, []string{"status", "--name", "testLinks"}, 0,
				`Global put queue status: 4 queued; 0 reserved to be worked on; 0 failed
Global put client status (/10): 0 creating collections; 0 currently uploading

Name: testLinks
Transformer: prefix=`+dir+`:/remote
Monitored: false; Archive: false
Status: pending upload
Discovery:
Num files: 4; Symlinks: 2; Hardlinks: 1; Size files: 0 B (and counting)
Uploaded: 0; Failed: 0; Missing: 0
Directories:
  `+dir+" => /remote")
		})
	})
}

// prepareForSetWithEmptyDir creates a tempdir with a subdirectory inside it, and
// returns a prefix transformer, the directory created and the remote upload
// location.
func prepareForSetWithEmptyDir(t *testing.T) (string, string, string) {
	t.Helper()

	dir := t.TempDir()
	someDir := filepath.Join(dir, "some/dir")

	err := os.MkdirAll(someDir, userPerms)
	So(err, ShouldBeNil)

	transformer := "prefix=" + dir + ":/remote"

	return transformer, someDir, "/remote/some/dir"
}

func TestStuck(t *testing.T) {
	convey := Convey
	conveyText := "In server mode, put exits early if there are long-time stuck uploads"
	// This test takes at least 1 minute to run, so is disabled by default.
	// This test can be enables by setting the ENABLE_STUCK_TEST env var.
	if os.Getenv("ENABLE_STUCK_TEST") == "" {
		convey = SkipConvey
		conveyText += " (set ENABLE_STUCK_TEST to enable)"
	}

	convey(conveyText, t, func() {
		s := NewTestServer(t)
		So(s, ShouldNotBeNil)

		remoteDBPath := remoteDBBackupPath()
		if remoteDBPath == "" {
			SkipConvey("skipping iRODS backup test since IBACKUP_TEST_COLLECTION not set", func() {})

			return
		}

		remoteDir := filepath.Dir(remoteDBPath)
		_, localDir, _ := prepareForSetWithEmptyDir(t)
		transformer := fmt.Sprintf("prefix=%s:%s", localDir, remoteDir)
		stuckFilePath := filepath.Join(localDir, "stuck.fifo")

		err := syscall.Mkfifo(stuckFilePath, userPerms)
		So(err, ShouldBeNil)

		go func() {
			f, _ := os.OpenFile(stuckFilePath, os.O_WRONLY, userPerms) //nolint:errcheck

			f.Write([]byte{0}) //nolint:errcheck
		}()

		s.addSetForTesting(t, "stuckPutTest", transformer, stuckFilePath)

		exitCode, out := s.runBinary(t, "put", "--server", os.Getenv("IBACKUP_SERVER_URL"), "--stuck-timeout", "10s")
		So(exitCode, ShouldEqual, 0)
		So(out, ShouldContainSubstring, put.ErrStuckTimeout)
	})
}

func remoteDBBackupPath() string {
	collection := os.Getenv("IBACKUP_TEST_COLLECTION")
	if collection == "" {
		return ""
	}

	return filepath.Join(collection, "db.bk")
}

func TestBackup(t *testing.T) {
	Convey("Adding a set causes a database backup locally and remotely", t, func() {
		remotePath := remoteDBBackupPath()
		if remotePath == "" {
			SkipConvey("skipping iRODS backup test since IBACKUP_TEST_COLLECTION not set", func() {})

			return
		}

		dir := t.TempDir()
		s := new(TestServer)
		s.prepareFilePaths(dir)
		s.prepareConfig()

		s.backupFile = filepath.Join(dir, "db.bak")
		s.remoteDBFile = remotePath

		s.startServer()

		transformer, localDir, remoteDir := prepareForSetWithEmptyDir(t)
		s.addSetForTesting(t, "testForBackup", transformer, localDir)

		<-time.After(time.Second) // wait for both backup cycles to run

		localBackupExists := internal.WaitForFile(s.backupFile)
		So(localBackupExists, ShouldBeTrue)

		ticker := time.NewTicker(1 * time.Second)
		timeout := time.NewTimer(30 * time.Second)
		tdir := t.TempDir()
		gotPath := filepath.Join(tdir, "remote.db")

		var igetErr error

	igetLoop:
		for {
			select {
			case <-ticker.C:
				cmd := exec.Command("iget", "-K", remotePath, gotPath)

				igetErr = cmd.Run()
				if igetErr == nil {
					break igetLoop
				}
			case <-timeout.C:
				break igetLoop
			}
		}

		ticker.Stop()
		timeout.Stop()

		So(igetErr, ShouldBeNil)

		ri, err := os.Stat(gotPath)
		So(err, ShouldBeNil)

		li, err := os.Stat(s.backupFile)
		So(err, ShouldBeNil)

		So(li.Size(), ShouldEqual, ri.Size())

		hashFile := func(path string) string {
			f, err := os.Open(path)
			So(err, ShouldBeNil)

			defer f.Close()

			s := sha256.New()
			_, err = io.Copy(s, f)
			So(err, ShouldBeNil)

			return fmt.Sprintf("%x", s.Sum(nil))
		}

		bh := hashFile(s.backupFile)
		rh := hashFile(gotPath)
		So(bh, ShouldEqual, rh)

		Convey("Running a server with the retrieved db works correctly", func() {
			bs := new(TestServer)
			bs.prepareFilePaths(tdir)
			bs.dbFile = gotPath
			bs.prepareConfig()

			bs.startServer()

			bs.confirmOutput(t, []string{
				"status", "-n", "testForBackup"}, 0, `Global put queue status: 0 queued; 0 reserved to be worked on; 0 failed
Global put client status (/10): 0 creating collections; 0 currently uploading

Name: testForBackup
Transformer: `+transformer+`
Monitored: false; Archive: false
Status: complete
Discovery:
Num files: 0; Symlinks: 0; Hardlinks: 0; Size files: 0 B
Uploaded: 0; Failed: 0; Missing: 0
Completed in: 0s
Directories:
  `+localDir+` => `+remoteDir)
		})
	})
}
