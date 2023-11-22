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
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/phayes/freeport"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/ibackup/internal"
	btime "github.com/wtsi-ssg/wr/backoff/time"
	"github.com/wtsi-ssg/wr/retry"
)

const app = "ibackup"
const userPerms = 0700

const noBackupSets = `Global put queue status: 0 queued; 0 reserved to be worked on; 0 failed
Global put client status (/10): 0 creating collections; 0 currently uploading
no backup sets`

var errTwoBackupsNotSeen = errors.New("2 backups were not seen")

type TestServer struct {
	key                  string
	cert                 string
	ldapServer           string
	ldapLookup           string
	url                  string
	dir                  string
	dbFile               string
	backupFile           string
	remoteDBFile         string
	logFile              string
	schedulerDeployment  string
	remoteHardlinkPrefix string
	env                  []string
	stopped              bool

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

	s.env = []string{
		"XDG_STATE_HOME=" + s.dir,
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + home,
		"IRODS_ENVIRONMENT_FILE=" + os.Getenv("IRODS_ENVIRONMENT_FILE"),
		"GEM_HOME=" + os.Getenv("GEM_HOME"),
	}
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
		"-s", s.ldapServer, "-l", s.ldapLookup, "--url", s.url}

	if s.schedulerDeployment != "" {
		args = append(args, "-w", s.schedulerDeployment)
	} else {
		args = append(args, "--debug")
	}

	if s.remoteDBFile != "" {
		args = append(args, "--remote_backup", s.remoteDBFile)
	}

	if s.remoteHardlinkPrefix != "" {
		args = append(args, "--hardlinks_collection", s.remoteHardlinkPrefix)
	}

	args = append(args, s.dbFile)

	if s.backupFile != "" {
		args = append(args, s.backupFile)
	}

	s.stopped = false
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
	ctx, cancelFn := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelFn()

	cmd := []string{"status", "--cert", s.cert, "--url", s.url}

	status := retry.Do(ctx, func() error {
		clientCmd := exec.Command("./"+app, cmd...)
		clientCmd.Env = []string{"XDG_STATE_HOME=" + s.dir}

		return clientCmd.Run()
	}, &retry.UntilNoError{}, btime.SecondsRangeBackoff(), "waiting for server to start")

	So(status.Err, ShouldBeNil)
}

func normaliseOutput(out string) string {
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

	return strings.Join(lines, "\n")
}

func (s *TestServer) runBinary(t *testing.T, args ...string) (int, string) {
	t.Helper()

	cmd := s.clientCmd(args)

	outB, err := cmd.CombinedOutput()
	out := string(outB)
	out = strings.TrimRight(out, "\n")
	out = normaliseOutput(out)

	if err != nil {
		t.Logf("\nbinary gave error: %s\noutput was: %s\n", err, out)
	} else if cmd.ProcessState.ExitCode() != 0 {
		t.Logf("\nno error, but non-0 exit; binary output: %s\n", out)
	}

	return cmd.ProcessState.ExitCode(), out
}

func (s *TestServer) clientCmd(args []string) *exec.Cmd {
	args = append([]string{"--url", s.url, "--cert", s.cert}, args...)

	cmd := exec.Command("./"+app, args...)
	cmd.Env = s.env

	return cmd
}

func (s *TestServer) confirmOutput(t *testing.T, args []string, expectedCode int, expected string) {
	t.Helper()

	exitCode, actual := s.runBinary(t, args...)

	So(exitCode, ShouldEqual, expectedCode)
	So(actual, ShouldEqual, expected)
}

func (s *TestServer) confirmOutputContains(t *testing.T, args []string, expectedCode int, expected string) {
	t.Helper()

	exitCode, actual := s.runBinary(t, args...)

	So(exitCode, ShouldEqual, expectedCode)
	So(actual, ShouldContainSubstring, expected)
}

var ErrStatusNotFound = errors.New("status not found")

func (s *TestServer) addSetForTesting(t *testing.T, name, transformer, path string) {
	t.Helper()

	exitCode, _ := s.runBinary(t, "add", "--name", name, "--transformer", transformer, "--path", path)

	So(exitCode, ShouldEqual, 0)

	s.waitForStatus(name, "\nDiscovery: completed", 5*time.Second)
}

func (s *TestServer) waitForStatus(name, statusToFind string, timeout time.Duration) {
	ctx, cancelFn := context.WithTimeout(context.Background(), timeout)
	defer cancelFn()

	cmd := []string{"status", "--name", name, "--url", s.url, "--cert", s.cert}

	status := retry.Do(ctx, func() error {
		clientCmd := exec.Command("./"+app, cmd...)
		clientCmd.Env = s.env

		output, err := clientCmd.CombinedOutput()
		if err != nil {
			return err
		}

		if strings.Contains(string(output), statusToFind) {
			return nil
		}

		return ErrStatusNotFound
	}, &retry.UntilNoError{}, btime.SecondsRangeBackoff(), "waiting for matching status")

	So(status.Err, ShouldBeNil)
}

func (s *TestServer) Shutdown() error {
	if s.stopped {
		return nil
	}

	s.stopped = true
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

// interactiveAdd does an add for sets that already exist, where a question
// will be asked: provide the answer 'y' to do the add.
func (s *TestServer) interactiveAdd(setName, answer, transformer, path string) error {
	cmd := s.clientCmd([]string{"add", "--name", setName, "--transformer", transformer, "--path", path})

	wc, err := cmd.StdinPipe()
	So(err, ShouldBeNil)

	err = cmd.Start()
	So(err, ShouldBeNil)

	_, err = wc.Write([]byte(answer + "\n"))
	So(err, ShouldBeNil)

	return cmd.Wait()
}

func (s *TestServer) getDiscoveryLineFromStatus(setName string) string {
	cmd := s.clientCmd([]string{"status", "--name", setName})

	outB, err := cmd.CombinedOutput()
	So(err, ShouldBeNil)

	var discovery string

	for _, line := range strings.Split(string(outB), "\n") {
		if strings.HasPrefix(line, "Discovery:") {
			discovery = line

			break
		}
	}

	So(discovery, ShouldNotBeBlank)

	return discovery
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

	remotePath := os.Getenv("IBACKUP_TEST_COLLECTION")
	if remotePath == "" {
		return
	}

	exec.Command("irm", "-r", remotePath).Run() //nolint:errcheck
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
	const toRemote = " => /remote"

	Convey("With a started server", t, func() {
		s := NewTestServer(t)
		So(s, ShouldNotBeNil)

		Convey("With no sets defined, status returns no sets", func() {
			s.confirmOutput(t, []string{"status"}, 0, noBackupSets)
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
Uploaded: 0; Failed: 0; Missing: 0; Abnormal: 0
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

			s.waitForStatus("testAddFiles", "Status: complete", 1*time.Second)

			Convey("Status tells you an example of where input files would get uploaded to", func() {
				s.confirmOutput(t, []string{"status", "--name", "testAddFiles"}, 0,
					`Global put queue status: 2 queued; 0 reserved to be worked on; 0 failed
Global put client status (/10): 0 creating collections; 0 currently uploading

Name: testAddFiles
Transformer: prefix=`+dir+`:/remote
Monitored: false; Archive: false
Status: complete
Discovery:
Num files: 2; Symlinks: 0; Hardlinks: 0; Size files: 0 B
Uploaded: 0; Failed: 0; Missing: 2; Abnormal: 0
Completed in: 0s
Example File: `+dir+`/path/to/other/file => /remote/path/to/other/file`)
			})
		})

		Convey("Given an added set defined with a non-humgen dir and humgen transformer, it warns about the issue", func() {
			_, localDir, _ := prepareForSetWithEmptyDir(t)
			s.addSetForTesting(t, "badHumgen", "humgen", localDir)

			expected := `Global put queue status: 0 queued; 0 reserved to be worked on; 0 failed
Global put client status (/10): 0 creating collections; 0 currently uploading

Name: badHumgen
Transformer: humgen
Monitored: false; Archive: false
Status: complete
Discovery:
Num files: 0; Symlinks: 0; Hardlinks: 0; Size files: 0 B
Uploaded: 0; Failed: 0; Missing: 0; Abnormal: 0
Completed in: 0s
Directories:
your transformer didn't work: not a valid humgen lustre path [` + localDir + `/file.txt]
  ` + localDir

			s.confirmOutput(t, []string{"status", "-n", "badHumgen"}, 0, expected)
			s.confirmOutput(t, []string{"status", "-c"}, 0, expected)
			s.confirmOutput(t, []string{"status", "-f"}, 0, noBackupSets)
			s.confirmOutput(t, []string{"status", "-i"}, 0, noBackupSets)
			s.confirmOutput(t, []string{"status", "-q"}, 0, noBackupSets)
		})

		Convey("Given an added set defined with a local path containing the localPrefix "+
			"in the middle and a prefix transformer, it correctly prefixes the remotePrefix", func() {
			dir := t.TempDir()
			localDir := filepath.Join(dir, "a", "b", "c")

			err := os.MkdirAll(localDir, userPerms)
			So(err, ShouldBeNil)

			transformer := "prefix=/a/b:/remote"
			s.addSetForTesting(t, "oddPrefix", transformer, localDir)

			s.confirmOutput(t, []string{"status", "-n", "oddPrefix"}, 0,
				`Global put queue status: 0 queued; 0 reserved to be worked on; 0 failed
Global put client status (/10): 0 creating collections; 0 currently uploading

Name: oddPrefix
Transformer: `+transformer+`
Monitored: false; Archive: false
Status: complete
Discovery:
Num files: 0; Symlinks: 0; Hardlinks: 0; Size files: 0 B
Uploaded: 0; Failed: 0; Missing: 0; Abnormal: 0
Completed in: 0s
Directories:
  `+localDir+toRemote+localDir)
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
Uploaded: 0; Failed: 0; Missing: 0; Abnormal: 0
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
Uploaded: 0; Failed: 0; Missing: 0; Abnormal: 0
Example File: `+humgenFile+" => /humgen/teams/hgi/scratch125/mercury/ibackup/file_for_testsuite.do_not_delete")
		})

		Convey("You can add a set with links and their counts show correctly", func() {
			dir := t.TempDir()
			regularPath := filepath.Join(dir, "reg")
			linkPath := filepath.Join(dir, "link")
			symPath := filepath.Join(dir, "sym")
			symPath2 := filepath.Join(dir, "sym2")

			internal.CreateTestFile(t, regularPath, "regular")

			err := os.Link(regularPath, linkPath)
			So(err, ShouldBeNil)

			err = os.Symlink(linkPath, symPath)
			So(err, ShouldBeNil)
			err = os.Symlink(symPath, symPath2)
			So(err, ShouldBeNil)

			exitCode, _ := s.runBinary(t, "add", "-p", dir,
				"--name", "testLinks", "--transformer", "prefix="+dir+":/remote")
			So(exitCode, ShouldEqual, 0)

			s.waitForStatus("testLinks", "Status: pending upload", 1*time.Second)

			s.confirmOutput(t, []string{"status", "--name", "testLinks"}, 0,
				`Global put queue status: 4 queued; 0 reserved to be worked on; 0 failed
Global put client status (/10): 0 creating collections; 0 currently uploading

Name: testLinks
Transformer: prefix=`+dir+`:/remote
Monitored: false; Archive: false
Status: pending upload
Discovery:
Num files: 4; Symlinks: 2; Hardlinks: 1; Size files: 0 B (and counting)
Uploaded: 0; Failed: 0; Missing: 0; Abnormal: 0
Directories:
  `+dir+toRemote)
		})

		Convey("Sets added with friendly monitor durations show the correct monitor duration", func() {
			dir := t.TempDir()

			setName := "testAddMonitor"
			exitCode, _ := s.runBinary(t, "add", "--path", dir,
				"--name", setName, "--transformer", "prefix="+dir+":/remote",
				"--monitor", "4d")

			So(exitCode, ShouldEqual, 0)

			s.waitForStatus(setName, "Monitored: 4d", 5*time.Second)

			setName = "testAddMonitorWeek"
			exitCode, _ = s.runBinary(t, "add", "--path", dir,
				"--name", setName, "--transformer", "prefix="+dir+":/remote",
				"--monitor", "2w")

			So(exitCode, ShouldEqual, 0)

			s.waitForStatus(setName, "Monitored: 2w", 5*time.Second)
		})

		Convey("When requesting statuses for all users, requesters are shown in output", func() {
			transformer, local, remote := prepareForSetWithEmptyDir(t)
			s.addSetForTesting(t, "setForRequesterPrinting", transformer, local)

			currentUser, err := user.Current()
			So(err, ShouldBeNil)

			currentUserName := currentUser.Username

			s.confirmOutput(t, []string{"status", "--user", "all"}, 0,
				`Global put queue status: 0 queued; 0 reserved to be worked on; 0 failed
Global put client status (/10): 0 creating collections; 0 currently uploading

Name: setForRequesterPrinting
Requester: `+currentUserName+`
Transformer: `+transformer+`
Monitored: false; Archive: false
Status: complete
Discovery:
Num files: 0; Symlinks: 0; Hardlinks: 0; Size files: 0 B
Uploaded: 0; Failed: 0; Missing: 0; Abnormal: 0
Completed in: 0s
Directories:
  `+local+" => "+remote)
		})

		Convey("Given an abnormal file", func() {
			dir := t.TempDir()
			fifoPath := filepath.Join(dir, "fifo")
			err := syscall.Mkfifo(fifoPath, userPerms)
			So(err, ShouldBeNil)

			Convey("When you add a set with the file, status tells you it's abnormal", func() {
				exitCode, _ := s.runBinary(t, "add", "--path", fifoPath,
					"--name", "testAddFifo", "--transformer", "prefix="+dir+":/remote")
				So(exitCode, ShouldEqual, 0)

				s.waitForStatus("testAddFifo", "Status: complete", 1*time.Second)

				s.confirmOutput(t, []string{"status", "--name", "testAddFifo", "--details"}, 0,
					`Global put queue status: 0 queued; 0 reserved to be worked on; 0 failed
Global put client status (/10): 0 creating collections; 0 currently uploading

Name: testAddFifo
Transformer: prefix=`+dir+`:/remote
Monitored: false; Archive: false
Status: complete
Discovery:
Num files: 1; Symlinks: 0; Hardlinks: 0; Size files: 0 B
Uploaded: 0; Failed: 0; Missing: 0; Abnormal: 1
Completed in: 0s
Example File: `+dir+`/fifo => /remote/fifo

Path	Status	Size	Attempts	Date	Error
`+fifoPath+`	abnormal	0 B	0	-	`)
			})

			Convey("When you add a set with the file in a dir, status tells you it's empty", func() {
				exitCode, _ := s.runBinary(t, "add", "--path", dir,
					"--name", "testAddFifoDir", "--transformer", "prefix="+dir+":/remote")
				So(exitCode, ShouldEqual, 0)

				s.waitForStatus("testAddFifoDir", "Status: complete", 1*time.Second)

				s.confirmOutput(t, []string{"status", "--name", "testAddFifoDir"}, 0,
					`Global put queue status: 0 queued; 0 reserved to be worked on; 0 failed
Global put client status (/10): 0 creating collections; 0 currently uploading

Name: testAddFifoDir
Transformer: prefix=`+dir+`:/remote
Monitored: false; Archive: false
Status: complete
Discovery:
Num files: 0; Symlinks: 0; Hardlinks: 0; Size files: 0 B
Uploaded: 0; Failed: 0; Missing: 0; Abnormal: 0
Completed in: 0s
Directories:
  `+dir+toRemote)
			})
		})
	})
}

func TestFileStatus(t *testing.T) {
	Convey("With a started server", t, func() {
		s := NewTestServer(t)
		So(s, ShouldNotBeNil)

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

			s.waitForStatus("testAddFiles", "Status: complete", 1*time.Second)
			err = s.Shutdown()
			So(err, ShouldBeNil)

			Convey("You can request the status of a file in a set", func() {
				exitCode, out := s.runBinary(t, "filestatus", "--database", s.dbFile, dir+"/path/to/some/file")
				So(exitCode, ShouldEqual, 0)
				So(out, ShouldContainSubstring, "destination: /remote/path/to/some/file")
			})
		})
	})
}

// prepareForSetWithEmptyDir creates a tempdir with a subdirectory inside it,
// and returns a prefix transformer, the directory created and the remote upload
// location.
func prepareForSetWithEmptyDir(t *testing.T) (string, string, string) {
	t.Helper()

	dir := t.TempDir()
	sourceDir := filepath.Join(dir, "source")

	err := os.MkdirAll(sourceDir, userPerms)
	So(err, ShouldBeNil)

	remoteDir := filepath.Join(dir, "remote")

	err = os.MkdirAll(remoteDir, userPerms)
	So(err, ShouldBeNil)

	transformer := "prefix=" + dir + ":" + remoteDir

	return transformer, sourceDir, filepath.Join(remoteDir, "source")
}

var errMismatchedDBBackupSizes = errors.New("mismatched db backup sizes")

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

		tdir := t.TempDir()
		gotPath := filepath.Join(tdir, "remote.db")

		s.startServer()

		transformer, localDir, remoteDir := prepareForSetWithEmptyDir(t)
		s.addSetForTesting(t, "testForBackup", transformer, localDir)

		versionsSeen := 0

		sizeRe := regexp.MustCompile(` (\d+)\s+\d{4}-\d{2}-\d{2}\.\d{2}:\d{2}`)
		foundSize := ""

		internal.RetryUntilWorksCustom(t, func() error { //nolint:errcheck
			out, err := exec.Command("ils", "-l", remotePath).CombinedOutput()
			if err != nil {
				return err
			}

			sizeStr := sizeRe.FindString(string(out))
			if sizeStr != foundSize {
				versionsSeen++
				foundSize = sizeStr
			}

			if versionsSeen == 2 {
				return nil
			}

			return errTwoBackupsNotSeen
		}, 5*time.Second, 0)

		localBackupExists := internal.WaitForFile(t, s.backupFile)
		So(localBackupExists, ShouldBeTrue)

		err := internal.RetryUntilWorksCustom(t, func() error {
			cmd := exec.Command("iget", "-Kf", remotePath, gotPath)

			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Logf("iget failed: %s\n%s\n", err, string(out))

				return err
			}

			li, err := os.Stat(s.backupFile)
			if err != nil {
				return err
			}
			ri, err := os.Stat(gotPath)
			if err != nil {
				return err
			}

			if ri.Size() != li.Size() {
				return errMismatchedDBBackupSizes
			}

			return nil
		}, 30*time.Second, 1*time.Second)

		So(err, ShouldBeNil)

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
		So(rh, ShouldEqual, bh)

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
Uploaded: 0; Failed: 0; Missing: 0; Abnormal: 0
Completed in: 0s
Directories:
  `+localDir+` => `+remoteDir)
		})
	})
}

func remoteDBBackupPath() string {
	collection := os.Getenv("IBACKUP_TEST_COLLECTION")
	if collection == "" {
		return ""
	}

	return filepath.Join(collection, "db.bk")
}

func TestPuts(t *testing.T) {
	Convey("Given a server configured with a remote hardlink location", t, func() {
		remotePath := os.Getenv("IBACKUP_TEST_COLLECTION")
		if remotePath == "" {
			SkipConvey("skipping iRODS backup test since IBACKUP_TEST_COLLECTION not set", func() {})

			return
		}

		schedulerDeployment := os.Getenv("IBACKUP_TEST_SCHEDULER")
		if schedulerDeployment == "" {
			SkipConvey("skipping iRODS backup test since IBACKUP_TEST_SCHEDULER not set", func() {})

			return
		}

		dir := t.TempDir()
		s := new(TestServer)
		s.prepareFilePaths(dir)
		s.prepareConfig()

		s.schedulerDeployment = schedulerDeployment
		s.remoteHardlinkPrefix = filepath.Join(remotePath, "hardlinks")
		s.backupFile = filepath.Join(dir, "db.bak")

		s.startServer()

		path := t.TempDir()
		transformer := "prefix=" + path + ":" + remotePath

		Convey("Putting a set with hardlinks uploads an empty file and special inode file", func() {
			file := filepath.Join(path, "file")
			link1 := filepath.Join(path, "hardlink1")
			link2 := filepath.Join(path, "hardlink2")

			remoteFile := filepath.Join(remotePath, "file")
			remoteLink1 := filepath.Join(remotePath, "hardlink1")
			remoteLink2 := filepath.Join(remotePath, "hardlink2")

			internal.CreateTestFile(t, file, "some data")

			err := os.Link(file, link1)
			So(err, ShouldBeNil)

			err = os.Link(file, link2)
			So(err, ShouldBeNil)

			s.addSetForTesting(t, "hardlinkTest", transformer, path)

			s.waitForStatus("hardlinkTest", "\nStatus: complete", 60*time.Second)

			output := getRemoteMeta(remoteFile)
			So(output, ShouldNotContainSubstring, "ibackup:hardlink")

			const expectedPrefix = "attribute: ibackup:hardlink\nvalue: "

			output = getRemoteMeta(remoteLink1)
			So(output, ShouldContainSubstring, expectedPrefix+link1)

			output = getRemoteMeta(remoteLink2)
			So(output, ShouldContainSubstring, expectedPrefix+link2)

			attrFind := "attribute: ibackup:remotehardlink\nvalue: "
			attrPos := strings.Index(output, attrFind)
			So(attrPos, ShouldNotEqual, -1)

			remoteInode := output[attrPos+len(attrFind):]
			nlPos := strings.Index(remoteInode, "\n")
			So(nlPos, ShouldNotEqual, -1)

			remoteInode = remoteInode[:nlPos]
			So(remoteInode, ShouldStartWith, s.remoteHardlinkPrefix)

			output = getRemoteMeta(remoteInode)
			So(output, ShouldContainSubstring, expectedPrefix+file)

			Convey("and summary tells you about the set", func() {
				exitCode, out := s.runBinary(t, "summary", "--database", s.backupFile)
				So(exitCode, ShouldEqual, 0)
				So(out, ShouldContainSubstring, "Total size: 9 B")
			})
		})

		Convey("Adding a failing set then re-adding it still allows retrying the failures", func() {
			path := t.TempDir()
			file := filepath.Join(path, "file")
			internal.CreateTestFile(t, file, "some data")

			err := os.Chmod(file, 0)
			So(err, ShouldBeNil)

			setName := "failTest"
			s.addSetForTesting(t, setName, transformer, path)

			statusLine := "0 reserved to be worked on; 1 failed"

			s.waitForStatus(setName, statusLine, 30*time.Second)

			expected := `Global put queue status: 1 queued; 0 reserved to be worked on; 1 failed
Global put client status (/10): 0 creating collections; 0 currently uploading
no backup sets`
			s.confirmOutput(t, []string{"status", "-c"}, 0, expected)
			s.confirmOutput(t, []string{"status", "-q"}, 0, expected)

			expected = `Name: failTest`
			s.confirmOutputContains(t, []string{"status", "-f"}, 0, expected)
			s.confirmOutputContains(t, []string{"status", "-i"}, 0, expected)

			s.confirmOutput(t, []string{"retry", "--name", setName, "--failed"},
				0, "initated retry of 1 failed entries")

			s.waitForStatus(setName, statusLine, 30*time.Second)

			err = s.interactiveAdd(setName, "y", transformer, path)
			So(err, ShouldBeNil)

			s.waitForStatus(setName, statusLine, 30*time.Second)

			s.confirmOutput(t, []string{"retry", "--name", setName, "--failed"},
				0, "initated retry of 1 failed entries")
		})
	})
}

func getRemoteMeta(path string) string {
	output, err := exec.Command("imeta", "ls", "-d", path).CombinedOutput()
	So(err, ShouldBeNil)
	So(string(output), ShouldContainSubstring, "ibackup:set")

	return string(output)
}

func TestManualMode(t *testing.T) {
	Convey("when using a manual put command, files are uploaded correctly", t, func() {
		remotePath := os.Getenv("IBACKUP_TEST_COLLECTION")
		if remotePath == "" {
			SkipConvey("skipping iRODS backup test since IBACKUP_TEST_COLLECTION not set", func() {})

			return
		}

		tmpDir := t.TempDir()
		file1 := filepath.Join(tmpDir, "file1")
		file2 := filepath.Join(tmpDir, "file2")

		remote1 := remotePath + "/" + "file1"
		remote2 := remotePath + "/" + "file2"

		fileContents1 := "abc"
		fileContents2 := "1234"

		internal.CreateTestFile(t, file1, fileContents1)
		internal.CreateTestFile(t, file2, fileContents2)

		files := file1 + "	" + remote1 + "\n"
		files += file2 + "	" + remote2 + "\n"

		cmd := exec.Command("./"+app, "put")
		cmd.Stdin = strings.NewReader(files)

		output, err := cmd.CombinedOutput()
		So(err, ShouldBeNil)
		So(cmd.ProcessState.ExitCode(), ShouldEqual, 0)

		out := normaliseOutput(string(output))
		So(out, ShouldEqual, "2 uploaded (0 replaced); 0 skipped; 0 failed; 0 missing\n")

		got1 := file1 + ".got"
		got2 := file2 + ".got"

		getFileFromIRODS(remote1, got1)
		getFileFromIRODS(remote2, got2)

		confirmFileContents(got1, fileContents1)
		confirmFileContents(got2, fileContents2)
	})
}

func TestReAdd(t *testing.T) {
	Convey("After starting a server and preparing for a set", t, func() {
		s := NewTestServer(t)
		So(s, ShouldNotBeNil)

		transformer, localDir, _ := prepareForSetWithEmptyDir(t)

		name := "aSet"

		Convey("Re-adding a set asks for confirmation", func() {
			s.addSetForTesting(t, name, transformer, localDir)

			<-time.After(time.Second)

			firstDiscovery := s.getDiscoveryLineFromStatus(name)

			err := s.interactiveAdd(name, "n", transformer, localDir)
			So(err, ShouldNotBeNil)

			secondDiscovery := s.getDiscoveryLineFromStatus(name)
			So(secondDiscovery, ShouldEqual, firstDiscovery)

			err = s.interactiveAdd(name, "y", transformer, localDir)
			So(err, ShouldBeNil)

			s.waitForStatus(name, "\nDiscovery: completed", 5*time.Second)

			thirdDiscovery := s.getDiscoveryLineFromStatus(name)
			So(thirdDiscovery, ShouldNotEqual, firstDiscovery)
		})
	})
}

func getFileFromIRODS(remotePath, localPath string) {
	cmd := exec.Command("iget", "-K", remotePath, localPath)

	err := cmd.Run()
	So(err, ShouldBeNil)
	So(cmd.ProcessState.ExitCode(), ShouldEqual, 0)
}

func confirmFileContents(file, expectedContents string) {
	f, err := os.Open(file)
	So(err, ShouldBeNil)

	data, err := io.ReadAll(f)
	So(err, ShouldBeNil)

	So(string(data), ShouldEqual, expectedContents)
}
