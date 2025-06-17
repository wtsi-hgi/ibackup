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
	b64 "encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/phayes/freeport"
	. "github.com/smartystreets/goconvey/convey"
	gas "github.com/wtsi-hgi/go-authserver"
	"github.com/wtsi-hgi/ibackup/baton"
	"github.com/wtsi-hgi/ibackup/cmd"
	"github.com/wtsi-hgi/ibackup/internal"
	"github.com/wtsi-hgi/ibackup/server"
	"github.com/wtsi-hgi/ibackup/set"
	"github.com/wtsi-hgi/ibackup/transfer"
	btime "github.com/wtsi-ssg/wr/backoff/time"
	"github.com/wtsi-ssg/wr/retry"
)

const (
	app       = "ibackup"
	userPerms = 0700
)

const noBackupSets = `Global put queue status: 0 queued; 0 reserved to be worked on; 0 failed
Global put client status (/10): 0 iRODS connections; 0 creating collections; 0 currently uploading
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
	debouncePeriod       string

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

	path := os.Getenv("PATH")

	if _, err = baton.GetBatonHandler(); err != nil {
		path = path + ":" + getFakeBaton(dir)
	}

	s.env = []string{
		"XDG_STATE_HOME=" + s.dir,
		"PATH=" + path,
		"HOME=" + home,
		"IRODS_ENVIRONMENT_FILE=" + os.Getenv("IRODS_ENVIRONMENT_FILE"),
		"GEM_HOME=" + os.Getenv("GEM_HOME"),
		"IBACKUP_SLACK_TOKEN=" + os.Getenv("IBACKUP_SLACK_TOKEN"),
		"IBACKUP_SLACK_CHANNEL=" + os.Getenv("IBACKUP_SLACK_CHANNEL"),
	}
}

func getFakeBaton(dir string) string {
	fakeBatonDir := filepath.Join(dir, "baton")
	err := os.Mkdir(fakeBatonDir, 0755)
	So(err, ShouldBeNil)

	fakeBatonFile := filepath.Join(fakeBatonDir, "baton-do")
	f, errc := os.Create(fakeBatonFile)
	So(errc, ShouldBeNil)

	err = f.Close()
	So(err, ShouldBeNil)

	return fakeBatonDir
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

	s.debouncePeriod = "5"
}

func (s *TestServer) startServer() {
	args := []string{
		"server", "--cert", s.cert, "--key", s.key, "--logfile", s.logFile,
		"-s", s.ldapServer, "-l", s.ldapLookup, "--url", s.url, "--slack_debounce", s.debouncePeriod,
	}

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

func (s *TestServer) runBinaryWithNoLogging(t *testing.T, args ...string) (int, string) {
	t.Helper()

	cmd := s.clientCmd(args)

	outB, _ := cmd.CombinedOutput() //nolint:errcheck
	out := string(outB)
	out = strings.TrimRight(out, "\n")
	out = normaliseOutput(out)

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

	exitCode, actual := s.runBinaryWithNoLogging(t, args...)

	So(exitCode, ShouldEqual, expectedCode)
	So(actual, ShouldContainSubstring, expected)
}

func (s *TestServer) confirmOutputDoesNotContain(t *testing.T, args []string, expectedCode int, //nolint:unparam
	expected string,
) {
	t.Helper()

	exitCode, actual := s.runBinary(t, args...)

	So(exitCode, ShouldEqual, expectedCode)
	So(actual, ShouldNotContainSubstring, expected)
}

var ErrStatusNotFound = errors.New("status not found")

func (s *TestServer) addSetForTesting(t *testing.T, name, transformer, path string) {
	t.Helper()

	exitCode, _ := s.runBinary(t, "add", "--name", name, "--transformer", transformer, "--path", path)

	So(exitCode, ShouldEqual, 0)

	s.waitForStatus(name, "\nDiscovery: completed", 20*time.Second)
}

func (s *TestServer) addSetForTestingWithItems(t *testing.T, name, transformer, path string) {
	t.Helper()

	s.addSetForTestingWithFlags(t, name, transformer, "--items", path, "--monitor", "1h", "--monitor-removals")
}

func (s *TestServer) addSetForTestingWithFlag(t *testing.T, name, transformer, path, flag, data string) {
	t.Helper()

	s.addSetForTestingWithFlags(t, name, transformer, "--path", path, flag, data)
}

func (s *TestServer) addSetForTestingWithFlags(t *testing.T, name, transformer string, flags ...string) {
	t.Helper()

	args := []string{"add", "--name", name, "--transformer", transformer}
	args = append(args, flags...)
	exitCode, _ := s.runBinary(t, args...)

	So(exitCode, ShouldEqual, 0)

	s.waitForStatus(name, "\nDiscovery: completed", 20*time.Second)
	s.waitForStatus(name, "\nStatus: complete", 20*time.Second)
}

func (s *TestServer) removePath(t *testing.T, name, path string, numFiles int) {
	t.Helper()

	exitCode, _ := s.runBinary(t, "remove", "--name", name, "--path", path)
	So(exitCode, ShouldEqual, 0)

	s.waitForStatus(name, fmt.Sprintf("Removal status: %d / %d objects removed", numFiles, numFiles), 10*time.Second)
}

func (s *TestServer) waitForStatus(name, statusToFind string, timeout time.Duration) {
	ctx, cancelFn := context.WithTimeout(context.Background(), timeout)
	defer cancelFn()

	cmd := []string{"status", "--name", name, "--url", s.url, "--cert", s.cert}

	var output []byte

	var err error

	status := retry.Do(ctx, func() error {
		clientCmd := exec.Command("./"+app, cmd...)
		clientCmd.Env = s.env

		output, err = clientCmd.CombinedOutput()
		if err != nil {
			return err
		}

		if strings.Contains(string(output), statusToFind) {
			return nil
		}

		return ErrStatusNotFound
	}, &retry.UntilNoError{}, btime.SecondsRangeBackoff(), "waiting for matching status")

	if status.Err != nil {
		fmt.Printf("\nfailed to see set %s get status: %s\n%s\n", name, statusToFind, string(output)) //nolint:forbidigo
	}

	So(status.Err, ShouldBeNil)
}

func (s *TestServer) waitForStatusWithUser(name, statusToFind, user string, timeout time.Duration) {
	ctx, cancelFn := context.WithTimeout(context.Background(), timeout)
	defer cancelFn()

	cmd := []string{"status", "--name", name, "--url", s.url, "--cert", s.cert, "--user", user}

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

	if status.Err != nil {
		fmt.Printf("\nfailed to see set %s get status: %s\n", name, statusToFind) //nolint:forbidigo
	}

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
func (s *TestServer) interactiveAdd(setName, answer, transformer, argName, path string) error {
	cmd := s.clientCmd([]string{"add", "--name", setName, "--transformer", transformer, "--" + argName, path})

	wc, err := cmd.StdinPipe()
	So(err, ShouldBeNil)

	err = cmd.Start()
	So(err, ShouldBeNil)

	_, err = wc.Write([]byte(answer + "\n"))
	So(err, ShouldBeNil)

	return cmd.Wait()
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

	resetIRODS()
}

func buildSelf() func() {
	if err := exec.Command("make", "build").Run(); err != nil {
		failMainTest(err.Error())

		return nil
	}

	return func() { os.Remove(app) }
}

func resetIRODS() {
	remotePath := os.Getenv("IBACKUP_TEST_COLLECTION")
	if remotePath == "" {
		return
	}

	exec.Command("irm", "-r", remotePath).Run() //nolint:errcheck
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

func TestList(t *testing.T) {
	Convey("With a started server", t, func() {
		s := NewTestServer(t)
		So(s, ShouldNotBeNil)

		Convey("With no --name given, list returns an error", func() {
			s.confirmOutput(t, []string{"list"}, 1, "--name must be set")
		})

		Convey("With --local and --remote given, list returns an error", func() {
			s.confirmOutput(t, []string{"list", "--name", "test", "--local", "--remote"},
				1, "--local and --remote are mutually exclusive")
		})

		Convey("Given an added set defined with files", func() {
			dir := t.TempDir()
			tempTestFile, err := os.CreateTemp(dir, "testFileSet")
			So(err, ShouldBeNil)

			_, err = io.WriteString(tempTestFile, dir+`/path/to/some/file
`+dir+`/path/to/other/file`)
			So(err, ShouldBeNil)

			Convey("And a valid transformer for the path", func() {
				exitCode, _ := s.runBinary(t, "add", "--files", tempTestFile.Name(),
					"--name", "testAddFiles", "--transformer", "prefix="+dir+":/remote")
				So(exitCode, ShouldEqual, 0)

				s.waitForStatus("testAddFiles", "Status: complete", 1*time.Second)

				Convey("list tells the local path and remote path for every file in the set", func() {
					s.confirmOutput(t, []string{"list", "--name", "testAddFiles"}, 0,
						dir+"/path/to/other/file\t"+"/remote/path/to/other/file\n"+
							dir+"/path/to/some/file\t"+"/remote/path/to/some/file")
				})

				Convey("list and --local tells the local path for every file in the set", func() {
					s.confirmOutput(t, []string{"list", "--name", "testAddFiles", "--local"}, 0,
						dir+"/path/to/other/file\n"+
							dir+"/path/to/some/file")
				})

				Convey("list and --remote tells the remote path for every file in the set", func() {
					s.confirmOutput(t, []string{"list", "--name", "testAddFiles", "--remote"}, 0,
						"/remote/path/to/other/file\n"+
							"/remote/path/to/some/file")
				})

				Convey("list and --size shows the file size for each file in the set", func() {
					s.confirmOutput(t, []string{"list", "--name", "testAddFiles", "--size"}, 0,
						dir+"/path/to/other/file\t"+"/remote/path/to/other/file\t0\n"+
							dir+"/path/to/some/file\t"+"/remote/path/to/some/file\t0")
				})

				Convey("list with --local and --size shows the local path and size for each file", func() {
					s.confirmOutput(t, []string{"list", "--name", "testAddFiles", "--local", "--size"}, 0,
						dir+"/path/to/other/file\t0\n"+
							dir+"/path/to/some/file\t0")
				})

				Convey("list with --remote and --size shows the remote path and size for each file", func() {
					s.confirmOutput(t, []string{"list", "--name", "testAddFiles", "--remote", "--size"}, 0,
						"/remote/path/to/other/file\t0\n"+
							"/remote/path/to/some/file\t0")
				})

				Convey("list with --base64 encodes all paths", func() {
					exitCode, output := s.runBinary(t, "list", "--name", "testAddFiles", "--base64")
					So(exitCode, ShouldEqual, 0)

					localPath1 := b64.StdEncoding.EncodeToString([]byte(dir + `/path/to/other/file`))
					remotePath1 := b64.StdEncoding.EncodeToString([]byte(`/remote/path/to/other/file`))
					localPath2 := b64.StdEncoding.EncodeToString([]byte(dir + `/path/to/some/file`))
					remotePath2 := b64.StdEncoding.EncodeToString([]byte(`/remote/path/to/some/file`))

					So(output, ShouldEqual, localPath1+"\t"+remotePath1+"\n"+localPath2+"\t"+remotePath2)
				})

				Convey("list with --base64 and --local encodes only local paths", func() {
					exitCode, output := s.runBinary(t, "list", "--name", "testAddFiles", "--base64", "--local")
					So(exitCode, ShouldEqual, 0)

					localPath1 := b64.StdEncoding.EncodeToString([]byte(dir + `/path/to/other/file`))
					localPath2 := b64.StdEncoding.EncodeToString([]byte(dir + `/path/to/some/file`))

					So(output, ShouldEqual, localPath1+"\n"+localPath2)
				})

				Convey("list with --base64 and --remote encodes only remote paths", func() {
					exitCode, output := s.runBinary(t, "list", "--name", "testAddFiles", "--base64", "--remote")
					So(exitCode, ShouldEqual, 0)

					remotePath1 := b64.StdEncoding.EncodeToString([]byte(`/remote/path/to/other/file`))
					remotePath2 := b64.StdEncoding.EncodeToString([]byte(`/remote/path/to/some/file`))

					So(output, ShouldEqual, remotePath1+"\n"+remotePath2)
				})

				Convey("list with --base64 and --size encodes paths and shows size", func() {
					exitCode, output := s.runBinary(t, "list", "--name", "testAddFiles", "--base64", "--size")
					So(exitCode, ShouldEqual, 0)

					localPath1 := b64.StdEncoding.EncodeToString([]byte(dir + `/path/to/other/file`))
					remotePath1 := b64.StdEncoding.EncodeToString([]byte(`/remote/path/to/other/file`))
					localPath2 := b64.StdEncoding.EncodeToString([]byte(dir + `/path/to/some/file`))
					remotePath2 := b64.StdEncoding.EncodeToString([]byte(`/remote/path/to/some/file`))

					So(output, ShouldEqual, localPath1+"\t"+remotePath1+"\t0\n"+localPath2+"\t"+remotePath2+"\t0")
				})
			})

			Convey("And an invalid transformer for the path", func() {
				exitCode, _ := s.runBinary(t, "add", "--files", tempTestFile.Name(),
					"--name", "testAddFiles", "--transformer", "humgen")
				So(exitCode, ShouldEqual, 0)

				Convey("list returns an error", func() {
					s.confirmOutput(t, []string{"list", "--name", "testAddFiles"}, 1,
						"your transformer didn't work: not a valid humgen lustre path ["+
							dir+"/path/to/other/file]")
				})
			})
		})
	})

	Convey("Given a server configured with a upload location and scheduler", t, func() {
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

		serverDir := t.TempDir()
		s := new(TestServer)
		s.prepareFilePaths(serverDir)
		s.prepareConfig()

		s.schedulerDeployment = schedulerDeployment
		s.backupFile = filepath.Join(serverDir, "db.bak")

		s.startServer()

		Convey("Given a set with files of different sizes that were uploaded", func() {
			dir := t.TempDir()
			file1 := filepath.Join(dir, "file1")
			file2 := filepath.Join(dir, "file2")
			fofn := filepath.Join(dir, "fofn")

			internal.CreateTestFile(t, file1, "1")
			internal.CreateTestFile(t, file2, "22")
			internal.CreateTestFile(t, fofn, fmt.Sprintf("%s\n%s\n/non/existant.file\n", file1, file2))

			file1Size := "1"
			file2Size := "2"
			setName := "testUploadedDiffSizeFiles"

			exitCode, _ := s.runBinary(t, "add", "-f", fofn,
				"--name", setName, "--transformer", "prefix="+dir+":"+remotePath)
			So(exitCode, ShouldEqual, 0)
			s.waitForStatus(setName, "Status: complete", 5*time.Second)

			Convey("list with --uploaded and --size only shows uploaded file sizes", func() {
				s.confirmOutputContains(t, []string{"list", "--name", setName, "--uploaded"}, 0,
					file1+"\t"+remotePath+"/file1\n")
				s.confirmOutputContains(t, []string{"list", "--name", setName, "--uploaded"}, 0,
					file2+"\t"+remotePath+"/file2")
				s.confirmOutputDoesNotContain(t, []string{"list", "--name", setName, "--uploaded"}, 0,
					"/non/existant.file")
				s.confirmOutputContains(t, []string{"list", "--name", setName}, 0,
					"/non/existant.file")

				s.confirmOutputContains(t, []string{"list", "--name", setName, "--uploaded", "--size"}, 0,
					file1+"\t"+remotePath+"/file1\t"+file1Size)
				s.confirmOutputContains(t, []string{"list", "--name", setName, "--uploaded", "--size"}, 0,
					file2+"\t"+remotePath+"/file2\t"+file2Size)

				s.confirmOutputContains(t, []string{"list", "--name", setName, "--uploaded", "--local", "--size"}, 0,
					file1+"\t"+file1Size)
				s.confirmOutputContains(t, []string{"list", "--name", setName, "--uploaded", "--local", "--size"}, 0,
					file2+"\t"+file2Size)

				s.confirmOutputContains(t, []string{"list", "--name", setName, "--uploaded", "--remote", "--size"}, 0,
					remotePath+"/file1\t"+file1Size)
				s.confirmOutputContains(t, []string{"list", "--name", setName, "--uploaded", "--remote", "--size"}, 0,
					remotePath+"/file2\t"+file2Size)

				Convey("With the server stopped, list with --all and --database works correctly", func() {
					dir2 := t.TempDir()
					file3 := filepath.Join(dir2, "file3")
					internal.CreateTestFile(t, file3, "333")

					setName2 := "testAnotherSet"
					exitCode, _ := s.runBinary(t, "add", "-p", dir2,
						"--name", setName2, "--transformer", "prefix="+dir2+":"+remotePath)
					So(exitCode, ShouldEqual, 0)
					s.waitForStatus(setName2, "Status: complete", 5*time.Second)

					err := s.Shutdown()
					So(err, ShouldBeNil)

					exitCode, output := s.runBinary(t, "list", "--all", "--database", s.dbFile)
					So(exitCode, ShouldEqual, 0)

					So(output, ShouldContainSubstring, file1)
					So(output, ShouldContainSubstring, file2)
					So(output, ShouldContainSubstring, file3)

					exitCode, output = s.runBinary(t, "list", "--all", "--database", s.dbFile, "--uploaded", "--size")
					So(exitCode, ShouldEqual, 0)

					So(output, ShouldContainSubstring, file1+"\t"+remotePath+"/file1\t"+file1Size)
					So(output, ShouldContainSubstring, file2+"\t"+remotePath+"/file2\t"+file2Size)
					So(output, ShouldContainSubstring, file3+"\t"+remotePath+"/file3")

					exitCode, output = s.runBinary(t, "list", "--all", "--database", s.dbFile, "--uploaded", "--local", "--size")
					So(exitCode, ShouldEqual, 0)

					So(output, ShouldContainSubstring, file1+"\t"+file1Size)
					So(output, ShouldContainSubstring, file2+"\t"+file2Size)
					So(output, ShouldContainSubstring, file3)
					So(output, ShouldNotContainSubstring, remotePath)

					exitCode, output = s.runBinary(t, "list", "--all", "--database", s.dbFile, "--remote")
					So(exitCode, ShouldEqual, 0)

					So(output, ShouldContainSubstring, remotePath+"/file1")
					So(output, ShouldContainSubstring, remotePath+"/file2")
					So(output, ShouldContainSubstring, remotePath+"/file3")
					So(output, ShouldNotContainSubstring, file1)
				})
			})
		})

		Convey("list with invalid option combinations returns an error", func() {
			s.confirmOutput(t, []string{"list", "--all"}, 1, "--all requires --database to be set")
			s.confirmOutput(t, []string{"list", "--name", "testSet", "--all", "--database", "db.file"}, 1,
				"--name and --all are mutually exclusive")
		})
	})
}

func TestStatus(t *testing.T) {
	const toRemote = " => /remote"

	Convey("With a started server", t, func() {
		s := NewTestServer(t)
		So(s, ShouldNotBeNil)

		reviewDate := time.Now().AddDate(0, 6, 0).Format("2006-01-02")
		removalDate := time.Now().AddDate(1, 0, 0).Format("2006-01-02")

		Convey("With no sets defined, status returns no sets", func() {
			s.confirmOutput(t, []string{"status"}, 0, noBackupSets)
		})

		Convey("Given an added set defined with a directory", func() {
			transformer, localDir, remoteDir := prepareForSetWithEmptyDir(t)
			s.addSetForTesting(t, "testAdd", transformer, localDir)

			Convey("Status tells you where input directories would get uploaded to", func() {
				s.confirmOutput(t, []string{"status"}, 0, `Global put queue status: 0 queued; 0 reserved to be worked on; 0 failed
Global put client status (/10): 0 iRODS connections; 0 creating collections; 0 currently uploading

Name: testAdd
Transformer: `+transformer+`
Reason: backup
Review date: `+reviewDate+`
Removal date: `+removalDate+`
Monitored: false; Archive: false
Status: complete
Discovery:
Num files: 0; Symlinks: 0; Hardlinks: 0; Size (total/recently uploaded/recently removed): 0 B / 0 B / 0 B
Uploaded: 0; Replaced: 0; Skipped: 0; Failed: 0; Missing: 0; Abnormal: 0
Completed in: 0s
Directories:
  `+localDir+" => "+remoteDir)
			})
		})

		Convey("Given an added set defined with a directory and user metadata", func() {
			meta := "testKey2=testVal2;ibackup:user:testKey=testVal"
			setName := "testMeta"

			transformer, localDir, remoteDir := prepareForSetWithEmptyDir(t)
			exitCode, _ := s.runBinary(t, "add", "--name", setName, "--transformer",
				transformer, "--path", localDir, "--metadata", meta)
			So(exitCode, ShouldEqual, 0)
			s.waitForStatus(setName, "\nDiscovery: completed", 5*time.Second)

			Convey("Status tells you the user metadata", func() {
				s.confirmOutput(t, []string{"status"}, 0, `Global put queue status: 0 queued; 0 reserved to be worked on; 0 failed
Global put client status (/10): 0 iRODS connections; 0 creating collections; 0 currently uploading

Name: `+setName+`
Transformer: `+transformer+`
Reason: backup
Review date: `+reviewDate+`
Removal date: `+removalDate+`
User metadata: testKey=testVal;testKey2=testVal2
Monitored: false; Archive: false
Status: complete
Discovery:
Num files: 0; Symlinks: 0; Hardlinks: 0; Size (total/recently uploaded/recently removed): 0 B / 0 B / 0 B
Uploaded: 0; Replaced: 0; Skipped: 0; Failed: 0; Missing: 0; Abnormal: 0
Completed in: 0s
Directories:
  `+localDir+" => "+remoteDir)

				meta = "testKey=testValNew;testKey2=testVal2;testKey3=testVal3"
				setName := "testMeta2"

				cmd := s.clientCmd([]string{
					"add", "--name", setName, "--transformer", transformer,
					"--path", localDir, "--metadata", meta, "--reason", "archive", "--remove", "2999-01-01",
				})
				err := cmd.Run()
				So(err, ShouldBeNil)

				s.confirmOutput(t, []string{"status", "-n", setName}, 0,
					`Global put queue status: 0 queued; 0 reserved to be worked on; 0 failed
Global put client status (/10): 0 iRODS connections; 0 creating collections; 0 currently uploading

Name: `+setName+`
Transformer: `+transformer+`
Reason: archive
Review date: `+time.Now().AddDate(1, 0, 0).Format("2006-01-02")+`
Removal date: 2999-01-01
User metadata: `+meta+`
Monitored: false; Archive: false
Status: complete
Discovery:
Num files: 0; Symlinks: 0; Hardlinks: 0; Size (total/recently uploaded/recently removed): 0 B / 0 B / 0 B
Uploaded: 0; Replaced: 0; Skipped: 0; Failed: 0; Missing: 0; Abnormal: 0
Completed in: 0s
Directories:
  `+localDir+" => "+remoteDir)

				meta = "testKey=testValNew;testKey2=testVal2;testKey3=testVal3"
				setName = "testMeta3"

				cmd = s.clientCmd([]string{
					"add", "--name", setName, "--transformer", transformer,
					"--path", localDir,
				})
				err = cmd.Run()
				So(err, ShouldBeNil)

				s.confirmOutput(t, []string{"status", "-n", setName}, 0,
					`Global put queue status: 0 queued; 0 reserved to be worked on; 0 failed
Global put client status (/10): 0 iRODS connections; 0 creating collections; 0 currently uploading

Name: `+setName+`
Transformer: `+transformer+`
Reason: backup
Review date: `+reviewDate+`
Removal date: `+removalDate+`
Monitored: false; Archive: false
Status: complete
Discovery:
Num files: 0; Symlinks: 0; Hardlinks: 0; Size (total/recently uploaded/recently removed): 0 B / 0 B / 0 B
Uploaded: 0; Replaced: 0; Skipped: 0; Failed: 0; Missing: 0; Abnormal: 0
Completed in: 0s
Directories:
  `+localDir+" => "+remoteDir)

				setName = "testMeta4"

				cmd = s.clientCmd([]string{
					"add", "--name", setName, "--transformer", transformer,
					"--path", localDir, "--reason", "backup",
				})
				err = cmd.Run()
				So(err, ShouldBeNil)

				s.confirmOutput(t, []string{"status", "-n", setName}, 0,
					`Global put queue status: 0 queued; 0 reserved to be worked on; 0 failed
Global put client status (/10): 0 iRODS connections; 0 creating collections; 0 currently uploading

Name: `+setName+`
Transformer: `+transformer+`
Reason: backup
Review date: `+reviewDate+`
Removal date: `+removalDate+`
Monitored: false; Archive: false
Status: complete
Discovery:
Num files: 0; Symlinks: 0; Hardlinks: 0; Size (total/recently uploaded/recently removed): 0 B / 0 B / 0 B
Uploaded: 0; Replaced: 0; Skipped: 0; Failed: 0; Missing: 0; Abnormal: 0
Completed in: 0s
Directories:
  `+localDir+" => "+remoteDir)
			})
		})

		Convey("Given multiple added sets defined with a directory", func() {
			transformer, localDir, remoteDir := prepareForSetWithEmptyDir(t)
			s.addSetForTesting(t, "c", transformer, localDir)
			s.addSetForTesting(t, "a", transformer, localDir)
			s.addSetForTesting(t, "b", transformer, localDir)

			Convey("Status is ordered alphabetically by default", func() {
				s.confirmOutput(t, []string{"status"}, 0, `Global put queue status: 0 queued; 0 reserved to be worked on; 0 failed
Global put client status (/10): 0 iRODS connections; 0 creating collections; 0 currently uploading

Name: a
Transformer: `+transformer+`
Reason: backup
Review date: `+reviewDate+`
Removal date: `+removalDate+`
Monitored: false; Archive: false
Status: complete
Discovery:
Num files: 0; Symlinks: 0; Hardlinks: 0; Size (total/recently uploaded/recently removed): 0 B / 0 B / 0 B
Uploaded: 0; Replaced: 0; Skipped: 0; Failed: 0; Missing: 0; Abnormal: 0
Completed in: 0s
Directories:
  `+localDir+" => "+remoteDir+`

-----

Name: b
Transformer: `+transformer+`
Reason: backup
Review date: `+reviewDate+`
Removal date: `+removalDate+`
Monitored: false; Archive: false
Status: complete
Discovery:
Num files: 0; Symlinks: 0; Hardlinks: 0; Size (total/recently uploaded/recently removed): 0 B / 0 B / 0 B
Uploaded: 0; Replaced: 0; Skipped: 0; Failed: 0; Missing: 0; Abnormal: 0
Completed in: 0s
Directories:
  `+localDir+" => "+remoteDir+`

-----

Name: c
Transformer: `+transformer+`
Reason: backup
Review date: `+reviewDate+`
Removal date: `+removalDate+`
Monitored: false; Archive: false
Status: complete
Discovery:
Num files: 0; Symlinks: 0; Hardlinks: 0; Size (total/recently uploaded/recently removed): 0 B / 0 B / 0 B
Uploaded: 0; Replaced: 0; Skipped: 0; Failed: 0; Missing: 0; Abnormal: 0
Completed in: 0s
Directories:
  `+localDir+" => "+remoteDir)
			})

			Convey("Status with '--order recent' orders the output by recent files", func() {
				s.confirmOutput(t, []string{"status", "--order", "recent"}, 0,
					`Global put queue status: 0 queued; 0 reserved to be worked on; 0 failed
Global put client status (/10): 0 iRODS connections; 0 creating collections; 0 currently uploading

Name: b
Transformer: `+transformer+`
Reason: backup
Review date: `+reviewDate+`
Removal date: `+removalDate+`
Monitored: false; Archive: false
Status: complete
Discovery:
Num files: 0; Symlinks: 0; Hardlinks: 0; Size (total/recently uploaded/recently removed): 0 B / 0 B / 0 B
Uploaded: 0; Replaced: 0; Skipped: 0; Failed: 0; Missing: 0; Abnormal: 0
Completed in: 0s
Directories:
  `+localDir+" => "+remoteDir+`

-----

Name: a
Transformer: `+transformer+`
Reason: backup
Review date: `+reviewDate+`
Removal date: `+removalDate+`
Monitored: false; Archive: false
Status: complete
Discovery:
Num files: 0; Symlinks: 0; Hardlinks: 0; Size (total/recently uploaded/recently removed): 0 B / 0 B / 0 B
Uploaded: 0; Replaced: 0; Skipped: 0; Failed: 0; Missing: 0; Abnormal: 0
Completed in: 0s
Directories:
  `+localDir+" => "+remoteDir+`

-----

Name: c
Transformer: `+transformer+`
Reason: backup
Review date: `+reviewDate+`
Removal date: `+removalDate+`
Monitored: false; Archive: false
Status: complete
Discovery:
Num files: 0; Symlinks: 0; Hardlinks: 0; Size (total/recently uploaded/recently removed): 0 B / 0 B / 0 B
Uploaded: 0; Replaced: 0; Skipped: 0; Failed: 0; Missing: 0; Abnormal: 0
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
Global put client status (/10): 0 iRODS connections; 0 creating collections; 0 currently uploading

Name: testAddFiles
Transformer: prefix=`+dir+`:/remote
Reason: backup
Review date: `+reviewDate+`
Removal date: `+removalDate+`
Monitored: false; Archive: false
Status: complete
Discovery:
Num files: 2; Symlinks: 0; Hardlinks: 0; Size (total/recently uploaded/recently removed): 0 B / 0 B / 0 B
Uploaded: 0; Replaced: 0; Skipped: 0; Failed: 0; Missing: 2; Abnormal: 0
Completed in: 0s
Example File: `+dir+`/path/to/other/file => /remote/path/to/other/file`)
			})

			Convey("Status with --details and --remotepaths displays the remote path for each file", func() {
				s.confirmOutput(t, []string{
					"status", "--name", "testAddFiles",
					"--details", "--remotepaths",
				}, 0,
					`Global put queue status: 2 queued; 0 reserved to be worked on; 0 failed
Global put client status (/10): 0 iRODS connections; 0 creating collections; 0 currently uploading

Name: testAddFiles
Transformer: prefix=`+dir+`:/remote
Reason: backup
Review date: `+reviewDate+`
Removal date: `+removalDate+`
Monitored: false; Archive: false
Status: complete
Discovery:
Num files: 2; Symlinks: 0; Hardlinks: 0; Size (total/recently uploaded/recently removed): 0 B / 0 B / 0 B
Uploaded: 0; Replaced: 0; Skipped: 0; Failed: 0; Missing: 2; Abnormal: 0
Completed in: 0s
Example File: `+dir+`/path/to/other/file => /remote/path/to/other/file

Local Path	Remote Path	Status	Size	Attempts	Date	Error`+"\n"+
						dir+"/path/to/other/file\t/remote/path/to/other/file\tmissing\t0 B\t0\t-\t\n"+
						dir+"/path/to/some/file\t/remote/path/to/some/file\tmissing\t0 B\t0\t-\t")
			})
		})

		Convey("Given an added set defined with a non-humgen dir and humgen transformer, it warns about the issue", func() {
			_, localDir, _ := prepareForSetWithEmptyDir(t)
			s.addSetForTesting(t, "badHumgen", "humgen", localDir)

			expected := `Global put queue status: 0 queued; 0 reserved to be worked on; 0 failed
Global put client status (/10): 0 iRODS connections; 0 creating collections; 0 currently uploading

Name: badHumgen
Transformer: humgen
Reason: backup
Review date: ` + reviewDate + `
Removal date: ` + removalDate + `
Monitored: false; Archive: false
Status: complete
Discovery:
Num files: 0; Symlinks: 0; Hardlinks: 0; Size (total/recently uploaded/recently removed): 0 B / 0 B / 0 B
Uploaded: 0; Replaced: 0; Skipped: 0; Failed: 0; Missing: 0; Abnormal: 0
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
Global put client status (/10): 0 iRODS connections; 0 creating collections; 0 currently uploading

Name: oddPrefix
Transformer: `+transformer+`
Reason: backup
Review date: `+reviewDate+`
Removal date: `+removalDate+`
Monitored: false; Archive: false
Status: complete
Discovery:
Num files: 0; Symlinks: 0; Hardlinks: 0; Size (total/recently uploaded/recently removed): 0 B / 0 B / 0 B
Uploaded: 0; Replaced: 0; Skipped: 0; Failed: 0; Missing: 0; Abnormal: 0
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
Global put client status (/10): 0 iRODS connections; 0 creating collections; 0 currently uploading

Name: badPerms
Transformer: `+transformer+`
Reason: backup
Review date: `+reviewDate+`
Removal date: `+removalDate+`
Monitored: false; Archive: false
Status: complete
Warning: `+badPermDir+`/: permission denied
Discovery:
Num files: 0; Symlinks: 0; Hardlinks: 0; Size (total/recently uploaded/recently removed): 0 B / 0 B / 0 B
Uploaded: 0; Replaced: 0; Skipped: 0; Failed: 0; Missing: 0; Abnormal: 0
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
Global put client status (/10): 0 iRODS connections; 0 creating collections; 0 currently uploading

Name: humgenSet
Transformer: humgen
Reason: backup
Review date: `+reviewDate+`
Removal date: `+removalDate+`
Monitored: false; Archive: false
Status: pending upload
Discovery:
Num files: 1; Symlinks: 0; Hardlinks: 0; Size (total/recently uploaded/recently removed): 0 B (and counting) / 0 B / 0 B
Uploaded: 0; Replaced: 0; Skipped: 0; Failed: 0; Missing: 0; Abnormal: 0
Example File: `+humgenFile+" => /humgen/teams/hgi/scratch125/mercury/ibackup/file_for_testsuite.do_not_delete")
		})

		Convey("Given an added set defined with a humgen_v2 transformer, the remote directory is correct", func() {
			humgenFile := "/lustre/scratch125/humgen/teams_v2/hgi/mercury/ibackup/file_for_testsuite.do_not_delete"
			humgenDir := filepath.Dir(humgenFile)

			if _, err := os.Stat(humgenDir); err != nil {
				SkipConvey("skip humgen transformer test since not in humgen", func() {})

				return
			}

			s.addSetForTesting(t, "humgenV2Set", "humgen_v2", humgenFile)

			s.confirmOutput(t, []string{"status", "-n", "humgenV2Set"}, 0,
				`Global put queue status: 1 queued; 0 reserved to be worked on; 0 failed
Global put client status (/10): 0 iRODS connections; 0 creating collections; 0 currently uploading

Name: humgenV2Set
Transformer: humgen_v2
Reason: backup
Review date: `+reviewDate+`
Removal date: `+removalDate+`
Monitored: false; Archive: false
Status: pending upload
Discovery:
Num files: 1; Symlinks: 0; Hardlinks: 0; Size (total/recently uploaded/recently removed): 0 B (and counting) / 0 B / 0 B
Uploaded: 0; Replaced: 0; Skipped: 0; Failed: 0; Missing: 0; Abnormal: 0
Example File: `+humgenFile+" => /humgen/teams/hgi/scratch125_v2/mercury/ibackup/file_for_testsuite.do_not_delete")
		})

		Convey("Given an added set defined with a gengen transformer, the remote directory is correct", func() {
			gengenFile := "/lustre/scratch126/gengen/teams/hgi/mercury/ibackup/file_for_testsuite.do_not_delete"
			gengenDir := filepath.Dir(gengenFile)

			if _, err := os.Stat(gengenDir); err != nil {
				SkipConvey("skip gengen transformer test since not in gengen", func() {})

				return
			}

			s.addSetForTesting(t, "gengenSet", "gengen", gengenFile)

			s.confirmOutput(t, []string{"status", "-n", "gengenSet"}, 0,
				`Global put queue status: 1 queued; 0 reserved to be worked on; 0 failed
Global put client status (/10): 0 iRODS connections; 0 creating collections; 0 currently uploading

Name: gengenSet
Transformer: gengen
Reason: backup
Review date: `+reviewDate+`
Removal date: `+removalDate+`
Monitored: false; Archive: false
Status: pending upload
Discovery:
Num files: 1; Symlinks: 0; Hardlinks: 0; Size (total/recently uploaded/recently removed): 0 B (and counting) / 0 B / 0 B
Uploaded: 0; Replaced: 0; Skipped: 0; Failed: 0; Missing: 0; Abnormal: 0
Example File: `+gengenFile+" => /humgen/gengen/teams/hgi/scratch126/mercury/ibackup/file_for_testsuite.do_not_delete")
		})

		Convey("Given an added set defined with a gengen_v2 transformer, the remote directory is correct", func() {
			gengenFile := "/lustre/scratch126/gengen/teams_v2/hgi/mercury/ibackup/file_for_testsuite.do_not_delete"
			gengenDir := filepath.Dir(gengenFile)

			if _, err := os.Stat(gengenDir); err != nil {
				SkipConvey("skip gengen transformer test since not in gengen", func() {})

				return
			}

			s.addSetForTesting(t, "gengenV2Set", "gengen_v2", gengenFile)

			s.confirmOutput(t, []string{"status", "-n", "gengenV2Set"}, 0,
				`Global put queue status: 1 queued; 0 reserved to be worked on; 0 failed
Global put client status (/10): 0 iRODS connections; 0 creating collections; 0 currently uploading

Name: gengenV2Set
Transformer: gengen_v2
Reason: backup
Review date: `+reviewDate+`
Removal date: `+removalDate+`
Monitored: false; Archive: false
Status: pending upload
Discovery:
Num files: 1; Symlinks: 0; Hardlinks: 0; Size (total/recently uploaded/recently removed): 0 B (and counting) / 0 B / 0 B
Uploaded: 0; Replaced: 0; Skipped: 0; Failed: 0; Missing: 0; Abnormal: 0
Example File: `+gengenFile+" => /humgen/gengen/teams/hgi/scratch126_v2/mercury/ibackup/file_for_testsuite.do_not_delete")
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
Global put client status (/10): 0 iRODS connections; 0 creating collections; 0 currently uploading

Name: testLinks
Transformer: prefix=`+dir+`:/remote
Reason: backup
Review date: `+reviewDate+`
Removal date: `+removalDate+`
Monitored: false; Archive: false
Status: pending upload
Discovery:
Num files: 4; Symlinks: 2; Hardlinks: 1; Size (total/recently uploaded/recently removed): 0 B (and counting) / 0 B / 0 B
Uploaded: 0; Replaced: 0; Skipped: 0; Failed: 0; Missing: 0; Abnormal: 0
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

		Convey("Sets added with a monitor that monitors removals displays this", func() {
			dir := t.TempDir()

			setName := "testAddMonitor"
			exitCode, _ := s.runBinary(t, "add", "--path", dir,
				"--name", setName, "--transformer", "prefix="+dir+":/remote",
				"--monitor", "4d", "--monitor-removals")

			So(exitCode, ShouldEqual, 0)

			s.waitForStatus(setName, "Monitored (with removals): 4d", 5*time.Second)
		})

		Convey("When requesting statuses for all users, requesters are shown in output", func() {
			transformer, local, remote := prepareForSetWithEmptyDir(t)
			s.addSetForTesting(t, "setForRequesterPrinting", transformer, local)

			currentUser, err := user.Current()
			So(err, ShouldBeNil)

			currentUserName := currentUser.Username

			s.confirmOutput(t, []string{"status", "--user", "all"}, 0,
				`Global put queue status: 0 queued; 0 reserved to be worked on; 0 failed
Global put client status (/10): 0 iRODS connections; 0 creating collections; 0 currently uploading

Name: setForRequesterPrinting
Requester: `+currentUserName+`
Transformer: `+transformer+`
Reason: backup
Review date: `+reviewDate+`
Removal date: `+removalDate+`
Monitored: false; Archive: false
Status: complete
Discovery:
Num files: 0; Symlinks: 0; Hardlinks: 0; Size (total/recently uploaded/recently removed): 0 B / 0 B / 0 B
Uploaded: 0; Replaced: 0; Skipped: 0; Failed: 0; Missing: 0; Abnormal: 0
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
Global put client status (/10): 0 iRODS connections; 0 creating collections; 0 currently uploading

Name: testAddFifo
Transformer: prefix=`+dir+`:/remote
Reason: backup
Review date: `+reviewDate+`
Removal date: `+removalDate+`
Monitored: false; Archive: false
Status: complete
Discovery:
Num files: 1; Symlinks: 0; Hardlinks: 0; Size (total/recently uploaded/recently removed): 0 B / 0 B / 0 B
Uploaded: 0; Replaced: 0; Skipped: 0; Failed: 0; Missing: 0; Abnormal: 1
Completed in: 0s
Example File: `+dir+`/fifo => /remote/fifo

Local Path	Status	Size	Attempts	Date	Error
`+fifoPath+`	abnormal	0 B	0	-	`)
			})

			Convey("When you add a set with the file in a dir, status tells you it's empty", func() {
				exitCode, _ := s.runBinary(t, "add", "--path", dir,
					"--name", "testAddFifoDir", "--transformer", "prefix="+dir+":/remote")
				So(exitCode, ShouldEqual, 0)

				s.waitForStatus("testAddFifoDir", "Status: complete", 1*time.Second)

				s.confirmOutput(t, []string{"status", "--name", "testAddFifoDir"}, 0,
					`Global put queue status: 0 queued; 0 reserved to be worked on; 0 failed
Global put client status (/10): 0 iRODS connections; 0 creating collections; 0 currently uploading

Name: testAddFifoDir
Transformer: prefix=`+dir+`:/remote
Reason: backup
Review date: `+reviewDate+`
Removal date: `+removalDate+`
Monitored: false; Archive: false
Status: complete
Discovery:
Num files: 0; Symlinks: 0; Hardlinks: 0; Size (total/recently uploaded/recently removed): 0 B / 0 B / 0 B
Uploaded: 0; Replaced: 0; Skipped: 0; Failed: 0; Missing: 0; Abnormal: 0
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

		reviewDate := time.Now().AddDate(0, 6, 0).Format("2006-01-02")
		removalDate := time.Now().AddDate(1, 0, 0).Format("2006-01-02")

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

			return hex.EncodeToString(s.Sum(nil))
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
				"status", "-n", "testForBackup",
			}, 0, `Global put queue status: 0 queued; 0 reserved to be worked on; 0 failed
Global put client status (/10): 0 iRODS connections; 0 creating collections; 0 currently uploading

Name: testForBackup
Transformer: `+transformer+`
Reason: backup
Review date: `+reviewDate+`
Removal date: `+removalDate+`
Monitored: false; Archive: false
Status: complete
Discovery:
Num files: 0; Symlinks: 0; Hardlinks: 0; Size (total/recently uploaded/recently removed): 0 B / 0 B / 0 B
Uploaded: 0; Replaced: 0; Skipped: 0; Failed: 0; Missing: 0; Abnormal: 0
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

		Convey("Status on an added set describes if a complete set has failures", func() {
			file1 := filepath.Join(path, "file1")

			internal.CreateTestFile(t, file1, "some data1")

			err := os.Chmod(file1, 0)
			So(err, ShouldBeNil)

			setName := "failuresTest"
			s.addSetForTesting(t, setName, transformer, path)

			s.waitForStatus(setName, "\nStatus: complete (but with failures - try a retry)", 60*time.Second)
		})

		Convey("Given a file containing directory and file paths", func() {
			dir1 := filepath.Join(path, "path/to/some/dir/")
			dir2 := filepath.Join(path, "path/to/other/dir/")
			subdir1 := filepath.Join(dir1, "subdir/")
			remoteDir1 := filepath.Join(remotePath, "path/to/some/dir/")
			remoteDir2 := filepath.Join(remotePath, "path/to/other/dir/")

			tempTestFileOfPaths, err := os.CreateTemp(dir, "testFileSet")
			So(err, ShouldBeNil)

			err = os.MkdirAll(dir1, 0755)
			So(err, ShouldBeNil)

			err = os.MkdirAll(dir2, 0755)
			So(err, ShouldBeNil)

			err = os.MkdirAll(subdir1, 0755)
			So(err, ShouldBeNil)

			file1 := filepath.Join(dir1, "file1")
			file2 := filepath.Join(subdir1, "file2")
			file3 := filepath.Join(path, "file3")

			internal.CreateTestFile(t, file1, "some data1")
			internal.CreateTestFile(t, file2, "some data2")
			internal.CreateTestFile(t, file3, "some data3")

			_, err = io.WriteString(tempTestFileOfPaths,
				fmt.Sprintf("%s\n%s\n%s\n%s\n%s", file3, file2, file1, dir1, dir2))
			So(err, ShouldBeNil)

			Convey("Add will add all the directories and files except duplicates", func() {
				exitCode, _ := s.runBinary(t, "add", "--items", tempTestFileOfPaths.Name(),
					"--name", "testAddFiles", "--transformer", transformer)
				So(exitCode, ShouldEqual, 0)

				s.confirmOutputContains(t, []string{"status", "--name", "testAddFiles", "-d"},
					0, "Directories:\n  "+
						dir2+" => "+remoteDir2+"\n  "+
						dir1+" => "+remoteDir1)

				s.confirmOutputContains(t, []string{"status", "--name", "testAddFiles", "-d"},
					0, "Example File:")

				s.confirmOutputContains(t, []string{"status", "--name", "testAddFiles", "-d"},
					0, `
Local Path	Status	Size	Attempts	Date	Error`+"\n"+
						file3+"\tpending\t0 B\t0\t-\t\n"+
						file1+"\tpending\t0 B\t0\t-\t\n"+
						file2+"\tpending\t0 B\t0\t-\t")
			})
		})

		Convey("Invalid metadata throws an error", func() {
			file1 := filepath.Join(path, "file1")

			internal.CreateTestFile(t, file1, "some data1")

			setName := "invalidMetadataTest1"
			setMetadata := "testKey=testValue=anotherValue"

			exitCode, err := s.runBinaryWithNoLogging(t, "add", "--name", setName, "--transformer", transformer,
				"--path", path, "--metadata", setMetadata)

			So(exitCode, ShouldEqual, 1)
			So(err, ShouldContainSubstring, "meta must be provided in the form key=value")

			setName = "invalidMetadataTest2"
			setMetadata = "ibackup:set=invalidMetadataTest2"

			exitCode, err = s.runBinaryWithNoLogging(t, "add", "--name", setName, "--transformer", transformer,
				"--path", path, "--metadata", setMetadata)

			So(exitCode, ShouldEqual, 1)
			So(err, ShouldContainSubstring, "namespace is incorrect, must be 'ibackup:user:' or empty")

			setName = "invalidMetadataTest3"
			setMetadata = "ibackup:name=name"

			exitCode, _ = s.runBinaryWithNoLogging(t, "add", "--name", setName, "--transformer", transformer,
				"--path", path, "--metadata", setMetadata)

			So(exitCode, ShouldEqual, 1)

			setName = "invalidMetadataTest4"
			setMetadata = "namespace:ibackup:user:mykey=value"

			exitCode, _ = s.runBinaryWithNoLogging(t, "add", "--name", setName, "--transformer", transformer,
				"--path", path, "--metadata", setMetadata)

			So(exitCode, ShouldEqual, 1)

			setName = "invalidMetadataTest5"
			setMetadata = "mykeyibackup:user:=value"

			exitCode, _ = s.runBinaryWithNoLogging(t, "add", "--name", setName, "--transformer", transformer,
				"--path", path, "--metadata", setMetadata)

			So(exitCode, ShouldEqual, 1)

			setName = "invalidMetadataTest6"
			setMetadata = "ibackup:user:mykey:mysubKey=value"

			exitCode, _ = s.runBinaryWithNoLogging(t, "add", "--name", setName, "--transformer", transformer,
				"--path", path, "--metadata", setMetadata)

			So(exitCode, ShouldEqual, 1)
		})

		Convey("Given a set of files", func() {
			file1 := filepath.Join(path, "file1")
			file2 := filepath.Join(path, "file2")
			file3 := filepath.Join(path, "file3")

			internal.CreateTestFile(t, file1, "some data1")
			internal.CreateTestFile(t, file2, "some data2")
			internal.CreateTestFile(t, file3, "some data3")

			setName := "metadataTest"
			fileNames := []string{"file1", "file2", "file3"}
			now := time.Now()

			Convey("Add will apply default reason/review/remove metadata", func() {
				s.addSetForTesting(t, setName, transformer, path)

				s.waitForStatus(setName, "\nStatus: complete", 5*time.Second)

				testRemoteReviewRemove(t, filepath.Join(remotePath, "file1"),
					"backup", now.AddDate(0, 6, 0), now.AddDate(1, 0, 0))
			})
			Convey("Add with --reason will apply different review/remove metadata", func() {
				s.addSetForTestingWithFlag(t, setName, transformer, path, "--reason", "backup")

				testRemoteReviewRemove(t, filepath.Join(remotePath, "file1"),
					"backup", now.AddDate(0, 6, 0), now.AddDate(1, 0, 0))

				setName += ".archive"

				s.addSetForTestingWithFlag(t, setName, transformer, path, "--reason", "archive")

				testRemoteReviewRemove(t, filepath.Join(remotePath, "file1"),
					"archive", now.AddDate(1, 0, 0), now.AddDate(2, 0, 0))

				setName += ".quarantine"

				s.addSetForTestingWithFlag(t, setName, transformer, path, "--reason", "quarantine")

				testRemoteReviewRemove(t, filepath.Join(remotePath, "file1"),
					"quarantine", now.AddDate(0, 2, 0), now.AddDate(0, 3, 0))
			})

			Convey("Add with --review will apply custom review metadata", func() {
				s.addSetForTestingWithFlag(t, setName, transformer, path, "--review", "4m")

				testRemoteReviewRemove(t, filepath.Join(remotePath, "file1"),
					"backup", now.AddDate(0, 4, 0), now.AddDate(1, 0, 0))
			})

			Convey("Add with --removal will apply custom removal metadata", func() {
				s.addSetForTestingWithFlag(t, setName, transformer, path, "--remove", "3y")

				testRemoteReviewRemove(t, filepath.Join(remotePath, "file1"),
					"backup", now.AddDate(0, 6, 0), now.AddDate(3, 0, 0))
			})

			Convey("Add with invalid --reason/--review/--remove inputs throws an error", func() {
				checkExitCode := func(reason, review, removal string, expectedCode int) {
					exitCode, _ := s.runBinaryWithNoLogging(t, "add", "--name", setName, "--transformer", transformer,
						"--path", path, "--reason", reason, "--review", review, "--remove", removal)
					So(exitCode, ShouldEqual, expectedCode)
				}

				checkExitCode("backup", "4m", "11m", 0)
				checkExitCode("invalidbackupreason", "1y", "1y", 1)
				checkExitCode("backup", "1 year", "2y", 1)
				checkExitCode("backup", "1y", "2 years", 1)
				checkExitCode("backup", "5y", "1y", 1)
				checkExitCode("backup", "1y", "1y", 1)
				checkExitCode("backup", "1d", "1y", 1)
				checkExitCode("backup", "1", "1", 1)
				checkExitCode("backup", "oney", "1y", 1)
			})

			Convey("Add with --metadata adds that metadata to every file in the set", func() {
				attributePrefix := "attribute: ibackup:user:"
				valuePrefix := "\nvalue: "
				setMetadata := "testKey1=testValue1;testKey2=testValue2"

				s.addSetForTestingWithFlag(t, setName, transformer, path, "--metadata", setMetadata)

				for _, fileName := range fileNames {
					output := getRemoteMeta(filepath.Join(remotePath, fileName))
					So(output, ShouldContainSubstring, attributePrefix+"testKey1\n")
					So(output, ShouldContainSubstring, valuePrefix+"testValue1\n")
					So(output, ShouldContainSubstring, attributePrefix+"testKey2\n")
					So(output, ShouldContainSubstring, valuePrefix+"testValue2\n")
				}

				newName := setName + ".v2"
				setMetadata = "testKey2=testValue2Updated"

				s.addSetForTestingWithFlag(t, newName, transformer, path, "--metadata", setMetadata)

				for _, fileName := range fileNames {
					output := getRemoteMeta(filepath.Join(remotePath, fileName))
					So(output, ShouldContainSubstring, attributePrefix+"testKey1\n")
					So(output, ShouldContainSubstring, valuePrefix+"testValue1\n")
					So(output, ShouldContainSubstring, attributePrefix+"testKey2\n")
					So(output, ShouldContainSubstring, valuePrefix+"testValue2Updated\n")
					So(output, ShouldNotContainSubstring, "testValue2\n")
				}

				newName = setName + ".v3"
				setMetadata = "ibackup:user:testKey1=testValue1Updated"

				s.addSetForTestingWithFlag(t, newName, transformer, path, "--metadata", setMetadata)

				for _, fileName := range fileNames {
					output := getRemoteMeta(filepath.Join(remotePath, fileName))
					So(output, ShouldContainSubstring, attributePrefix+"testKey1\n")
					So(output, ShouldContainSubstring, valuePrefix+"testValue1Updated\n")
					So(output, ShouldContainSubstring, attributePrefix+"testKey2\n")

					So(output, ShouldContainSubstring, valuePrefix+"testValue2Updated\n")
					So(output, ShouldNotContainSubstring, "testValue1\n")
				}
			})
			Convey("Repeatedly uploading files that are changed or not changes status details", func() {
				resetIRODS()

				setName = "changingFilesTest"

				s.addSetForTesting(t, setName, transformer, path)

				statusCmd := []string{"status", "--name", setName}

				s.waitForStatus(setName, "\nStatus: uploading", 60*time.Second)
				s.confirmOutputContains(t, statusCmd, 0,
					`Global put queue status: 3 queued; 3 reserved to be worked on; 0 failed
Global put client status (/10): 6 iRODS connections`)

				s.waitForStatus(setName, "\nStatus: complete", 60*time.Second)

				s.confirmOutputContains(t, statusCmd, 0,
					"Uploaded: 3; Replaced: 0; Skipped: 0; Failed: 0; Missing: 0; Abnormal: 0")
				s.confirmOutputContains(t, statusCmd, 0,
					"Num files: 3; Symlinks: 0; Hardlinks: 0; Size (total/recently uploaded/recently removed): 30 B / 30 B / 0 B")

				s.confirmOutputContains(t, statusCmd, 0, "")

				newName := setName + ".v2"
				statusCmd[2] = newName

				s.addSetForTesting(t, newName, transformer, path)
				s.waitForStatus(newName, "\nStatus: complete", 60*time.Second)
				s.confirmOutputContains(t, statusCmd, 0,
					"Uploaded: 0; Replaced: 0; Skipped: 3; Failed: 0; Missing: 0; Abnormal: 0")
				s.confirmOutputContains(t, statusCmd, 0,
					"Num files: 3; Symlinks: 0; Hardlinks: 0; Size (total/recently uploaded/recently removed): 30 B / 0 B / 0 B")

				newName = setName + ".v3"
				statusCmd[2] = newName

				internal.CreateTestFile(t, file2, "some data2 updated")

				s.addSetForTesting(t, newName, transformer, path)
				s.waitForStatus(newName, "\nStatus: complete", 60*time.Second)
				s.confirmOutputContains(t, statusCmd, 0,
					"Uploaded: 0; Replaced: 1; Skipped: 2; Failed: 0; Missing: 0; Abnormal: 0")
				s.confirmOutputContains(t, statusCmd, 0,
					"Num files: 3; Symlinks: 0; Hardlinks: 0; Size (total/recently uploaded/recently removed): 38 B / 18 B / 0 B")

				internal.CreateTestFile(t, file2, "less data")
				exitCode, _ := s.runBinary(t, "retry", "--name", newName, "-a")
				So(exitCode, ShouldEqual, 0)

				s.waitForStatus(newName, "\nStatus: complete", 60*time.Second)
				s.confirmOutputContains(t, statusCmd, 0,
					"Uploaded: 0; Replaced: 1; Skipped: 2; Failed: 0; Missing: 0; Abnormal: 0")
				s.confirmOutputContains(t, statusCmd, 0,
					"Num files: 3; Symlinks: 0; Hardlinks: 0; Size (total/recently uploaded/recently removed): 29 B / 9 B / 0 B")
			})
		})

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

			s.waitForStatus("hardlinkTest", "\nStatus: uploading", 60*time.Second)
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

		Convey("Adding a failing set then re-adding it is not possible", func() {
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
Global put client status (/10): 2 iRODS connections; 0 creating collections; 0 currently uploading
no backup sets`
			s.confirmOutput(t, []string{"status", "-c"}, 0, expected)
			s.confirmOutput(t, []string{"status", "-q"}, 0, expected)

			expected = `Name: failTest`
			s.confirmOutputContains(t, []string{"status", "-f"}, 0, expected)
			s.confirmOutputContains(t, []string{"status", "-i"}, 0, expected)

			s.confirmOutput(t, []string{"retry", "--name", setName, "--failed"},
				0, "initated retry of 1 failed entries")

			s.waitForStatus(setName, statusLine, 30*time.Second)

			s.confirmOutputContains(t, []string{"add", "--name", setName, "--transformer", transformer, "--path", path},
				1, cmd.ErrDuplicateSet.Error())
		})
	})
}

func testRemoteReviewRemove(t *testing.T, filepath, reason string, review, remove time.Time) {
	t.Helper()

	reviewStr, removeStr := testTimesToMeta(t, review, remove)

	output := getRemoteMeta(filepath)
	So(output, ShouldContainSubstring, `
attribute: ibackup:reason
value: `+reason+`
`)
	So(output, ShouldContainSubstring, `
attribute: ibackup:review
value: `+reviewStr[:10])
	So(output, ShouldContainSubstring, `
attribute: ibackup:removal
value: `+removeStr[:10])
}

func testTimesToMeta(t *testing.T, reviewDate, removalDate time.Time) (string, string) {
	t.Helper()

	reviewStr, err := reviewDate.UTC().Truncate(time.Second).MarshalText()
	So(err, ShouldBeNil)

	removalStr, err := removalDate.UTC().Truncate(time.Second).MarshalText()
	So(err, ShouldBeNil)

	return string(reviewStr), string(removalStr)
}

func getRemoteMeta(path string) string {
	output, err := exec.Command("imeta", "ls", "-d", path).CombinedOutput()
	So(err, ShouldBeNil)
	So(string(output), ShouldContainSubstring, "ibackup:set")

	return string(output)
}

func removeFileFromIRODS(path string) {
	_, err := exec.Command("irm", "-f", path).CombinedOutput()
	So(err, ShouldBeNil)
}

func addFileToIRODS(localPath, remotePath string) {
	_, err := exec.Command("iput", localPath, remotePath).CombinedOutput()
	So(err, ShouldBeNil)
}

func TestManualMode(t *testing.T) {
	resetIRODS()

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

		fileContents1 := "123"
		fileContents2 := "1234"

		internal.CreateTestFile(t, file1, fileContents1)
		internal.CreateTestFile(t, file2, fileContents2)

		u, err := user.Current()
		So(err, ShouldBeNil)

		uid, err := strconv.ParseUint(u.Uid, 10, 64)
		So(err, ShouldBeNil)

		gids, err := u.GroupIds()
		So(err, ShouldBeNil)

		gidA, err := strconv.ParseUint(gids[0], 10, 64)
		So(err, ShouldBeNil)

		groupA, err := user.LookupGroupId(gids[0])
		So(err, ShouldBeNil)

		var gidB uint64

		if len(gids) == 1 {
			gidB = gidA
		} else {
			gidB, err = strconv.ParseUint(gids[1], 10, 64)
			So(err, ShouldBeNil)
		}

		So(os.Chown(file2, int(uid), int(gidB)), ShouldBeNil) //nolint:gosec

		timeA := time.Unix(987654321, 0)

		So(os.Chtimes(file1, timeA, timeA), ShouldBeNil)

		files := file1 + "\t" + remote1 + "\n"
		files += file2 + "\t" + remote2 + "\n"

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

		confirmFileContents(t, got1, fileContents1)
		confirmFileContents(t, got2, fileContents2)

		Convey("and then you can get them again", func() {
			restoreDir := t.TempDir()

			file1 := filepath.Join(restoreDir, "file1")
			file2 := filepath.Join(restoreDir, "file2")
			file3 := filepath.Join(restoreDir, "file3")
			file4 := filepath.Join(restoreDir, "file4")
			file5 := filepath.Join(restoreDir, "anotherDir", "file5")
			file6 := filepath.Join(restoreDir, "file6")
			file7 := filepath.Join(restoreDir, "file7")
			tmpFile := filepath.Join(restoreDir, fmt.Sprintf(".ibackup.get.%X", sha256.Sum256([]byte("file2"))))

			err = os.WriteFile(
				tmpFile,
				[]byte("bad data"),
				0600,
			)
			So(err, ShouldBeNil)

			_, err = os.Stat(tmpFile)
			So(err, ShouldBeNil)

			files := file1 + "\t" + remote1 + "\n"
			files += file2 + "\t" + remote2 + "\n"

			restoreFiles(t, files, "2 downloaded (0 replaced); 0 skipped; 0 failed; 0 missing\n")

			confirmFileContents(t, file1, fileContents1)
			confirmFileContents(t, file2, fileContents2)

			_, err = os.Stat(tmpFile)
			So(err, ShouldNotBeNil)

			s, err := os.Stat(file1)
			So(err, ShouldBeNil)

			So(int(s.Sys().(*syscall.Stat_t).Gid), ShouldEqual, gidA) //nolint:errcheck,forcetypeassert

			So(s.ModTime(), ShouldEqual, timeA)

			s, err = os.Stat(file2)
			So(err, ShouldBeNil)

			So(int(s.Sys().(*syscall.Stat_t).Gid), ShouldEqual, gidB) //nolint:errcheck,forcetypeassert

			restoreFiles(t, file1+"\t"+remote1+"\n", "0 downloaded (0 replaced); 1 skipped; 0 failed; 0 missing\n")
			restoreFiles(t, file1+"\t"+remote1+"\n", "0 downloaded (0 replaced); 1 skipped; 0 failed; 0 missing\n", "-o")

			timeB := time.Unix(100, 0)

			So(os.Chtimes(file1, timeB, timeB), ShouldBeNil)

			restoreFiles(t, file1+"\t"+remote1+"\n", "1 downloaded (1 replaced); 0 skipped; 0 failed; 0 missing\n", "-o")

			restoreFiles(t, file5+"\t"+remote1+"\n", "1 downloaded (0 replaced); 0 skipped; 0 failed; 0 missing\n", "-o")

			confirmFileContents(t, file5, fileContents1)

			err = os.Remove(file1)
			So(err, ShouldBeNil)

			internal.CreateTestFile(t, file1, "")
			restoreFiles(t, file1+"\t"+remote1+"\n", "1 downloaded (1 replaced); 0 skipped; 0 failed; 0 missing\n")
			confirmFileContents(t, file1, fileContents1)

			So(exec.Command("imeta", "add", "-d", remote2, transfer.MetaKeySymlink, file1).Run(), ShouldBeNil)

			restoreFiles(t, file3+"\t"+remote2+"\n", "1 downloaded (0 replaced); 0 skipped; 0 failed; 0 missing\n")

			link, err := os.Readlink(file3)
			So(err, ShouldBeNil)
			So(link, ShouldEqual, file1)

			So(os.Remove(file3), ShouldBeNil)
			So(os.Symlink("bad", file3), ShouldBeNil)

			restoreFiles(t, file3+"\t"+remote2+"\n", "0 downloaded (0 replaced); 1 skipped; 0 failed; 0 missing\n")

			link, err = os.Readlink(file3)
			So(err, ShouldBeNil)
			So(link, ShouldEqual, "bad")

			restoreFiles(t, file3+"\t"+remote2+"\n", "1 downloaded (1 replaced); 0 skipped; 0 failed; 0 missing\n", "-o")

			link, err = os.Readlink(file3)
			So(err, ShouldBeNil)
			So(link, ShouldEqual, file1)

			So(os.Remove(file3), ShouldBeNil)
			So(os.WriteFile(file3, nil, 0600), ShouldBeNil)

			restoreFiles(t, file3+"\t"+remote2+"\n", "1 downloaded (1 replaced); 0 skipped; 0 failed; 0 missing\n", "-o")

			link, err = os.Readlink(file3)
			So(err, ShouldBeNil)
			So(link, ShouldEqual, file1)

			So(exec.Command("imeta", "add", "-d", remote1, transfer.MetaKeyRemoteHardlink, remote2).Run(), ShouldBeNil)

			restoreFiles(
				t,
				file4+"\t"+remote1+"\n",
				fmt.Sprintf("[1/1] Hardlink skipped: %s\\t%s\n0 downloaded (0 replaced); 1 skipped; 0 failed; 0 missing\n",
					file4, remote2),
			)

			So(exec.Command("imeta", "rm", "-d", remote1, transfer.MetaKeyRemoteHardlink, remote2).Run(), ShouldBeNil)
			So(
				exec.Command("imeta", "mod", "-d", remote1, transfer.MetaKeyGroup, groupA.Name, "v:root").Run(),
				ShouldBeNil,
			)

			restoreFiles(t, file6+"\t"+remote1+"\n",
				"[1/1] "+file6+" warning: lchown "+file6+": operation not permitted\n"+
					"1 downloaded (0 replaced); 0 skipped; 0 failed; 0 missing\n")

			So(exec.Command("imeta", "rm", "-d", remote2, transfer.MetaKeyMtime).Run(), ShouldBeNil)

			restoreFiles(t, file7+"\t"+remote2+"\n",
				"1 downloaded (0 replaced); 0 skipped; 0 failed; 0 missing\n")

			restoreFiles(t, file7+"\t"+remote2+"\n",
				"0 downloaded (0 replaced); 1 skipped; 0 failed; 0 missing\n")
		})
	})
}

func restoreFiles(t *testing.T, files, expectedOutput string, args ...string) {
	t.Helper()

	cmd := exec.Command("./"+app, append([]string{"get"}, args...)...) //nolint:gosec
	cmd.Stdin = strings.NewReader(files)

	output, err := cmd.CombinedOutput()
	So(err, ShouldBeNil)
	So(cmd.ProcessState.ExitCode(), ShouldEqual, 0)

	out := normaliseOutput(string(output))
	So(out, ShouldEqual, expectedOutput)
}

func TestReAdd(t *testing.T) {
	Convey("After starting a server and preparing for a set", t, func() {
		s := NewTestServer(t)
		So(s, ShouldNotBeNil)

		transformer, localDir, _ := prepareForSetWithEmptyDir(t)

		name := "aSet"

		Convey("Re-adding a set with the same name fails", func() {
			s.addSetForTesting(t, name, transformer, localDir)

			<-time.After(time.Second)

			s.confirmOutputContains(t, []string{"add", "--name", name, "--transformer", transformer, "--path", localDir}, 1,
				"set with this name already exists")
		})
	})
}

func getFileFromIRODS(remotePath, localPath string) {
	cmd := exec.Command("iget", "-K", remotePath, localPath)

	err := cmd.Run()
	So(err, ShouldBeNil)
	So(cmd.ProcessState.ExitCode(), ShouldEqual, 0)
}

func confirmFileContents(t *testing.T, file, expectedContents string) {
	t.Helper()

	f, err := os.Open(file)
	So(err, ShouldBeNil)

	data, err := io.ReadAll(f)
	So(err, ShouldBeNil)

	So(string(data), ShouldEqual, expectedContents)
}

func TestRemove(t *testing.T) {
	resetIRODS()

	SkipConvey("Given a server", t, func() {
		remotePath := os.Getenv("IBACKUP_TEST_COLLECTION")
		if remotePath == "" {
			SkipConvey("skipping iRODS backup test since IBACKUP_TEST_COLLECTION not set", func() {})

			return
		}

		remotePath = filepath.Join(remotePath, "test_remove")

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

		Convey("And an invalid set name, remove returns an error", func() {
			invalidSetName := "invalid_set"

			s.confirmOutputContains(t, []string{"remove", "--name", invalidSetName, "--path", path},
				1, fmt.Sprintf("set with that id does not exist [%s]", invalidSetName))
		})

		Convey("And an added set with files and folders", func() {
			dir := t.TempDir()

			linkPath := filepath.Join(path, "link")
			symPath := filepath.Join(path, "sym")
			testDir := filepath.Join(path, "path/to/some/")
			dir1 := filepath.Join(testDir, "dir")
			dir2 := filepath.Join(path, "path/to/other/dir/")

			tempTestFileOfPaths, err := os.CreateTemp(dir, "testFileSet")
			So(err, ShouldBeNil)

			err = os.MkdirAll(dir1, 0755)
			So(err, ShouldBeNil)

			err = os.MkdirAll(dir2, 0755)
			So(err, ShouldBeNil)

			file1 := filepath.Join(path, "file1")
			file2 := filepath.Join(path, "file2")
			file3 := filepath.Join(dir1, "file3")
			file4 := filepath.Join(testDir, "dir_not_removed")
			file5 := filepath.Join(path, "file5")

			internal.CreateTestFile(t, file1, "some data1")
			internal.CreateTestFile(t, file2, "some data2")
			internal.CreateTestFile(t, file3, "some data3")
			internal.CreateTestFile(t, file4, "some data4")
			internal.CreateTestFile(t, file5, "some data50")

			err = os.Link(file1, linkPath)
			So(err, ShouldBeNil)

			remoteLink := filepath.Join(remotePath, "link")

			err = os.Symlink(file2, symPath)
			So(err, ShouldBeNil)

			_, err = io.WriteString(tempTestFileOfPaths,
				fmt.Sprintf("%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s", file1, file2, file4, file5, dir1, dir2, linkPath, symPath))
			So(err, ShouldBeNil)

			setName := "testRemoveFiles1"

			resetIRODS()

			s.addSetForTestingWithItems(t, setName, transformer, tempTestFileOfPaths.Name())

			Convey("Remove removes the file from the set", func() {
				exitCode, _ := s.runBinary(t, "remove", "--name", setName, "--path", file2)

				So(exitCode, ShouldEqual, 0)

				s.confirmOutputContains(t, []string{"status", "--name", setName, "-d"},
					0, "Removal status: 0 / 1 objects removed")

				s.waitForStatus(setName, "Removal status: 1 / 1 objects removed", 5*time.Second)

				s.confirmOutputContains(t, []string{"status", "--name", setName, "-d"},
					0, file1)

				s.confirmOutputDoesNotContain(t, []string{"status", "--name", setName, "-d"},
					0, file2)

				Convey("Remove again will remove another object and status will update accordingly", func() {
					s.confirmOutputContains(t, []string{"status", "--name", setName, "-d"},
						0, "Num files: 6; Symlinks: 1; Hardlinks: 1; Size "+
							"(total/recently uploaded/recently removed): 41 B / 51 B / 10 B\n"+
							"Uploaded: 6; Replaced: 0; Skipped: 0; Failed: 0; Missing: 0; Abnormal: 0")

					exitCode, _ = s.runBinary(t, "remove", "--name", setName, "--path", dir1)

					So(exitCode, ShouldEqual, 0)

					s.waitForStatus(setName, "Removal status: 2 / 2 objects removed", 5*time.Second)

					s.confirmOutputDoesNotContain(t, []string{"status", "--name", setName, "-d"},
						0, file3)

					s.confirmOutputContains(t, []string{"status", "--name", setName, "-d"},
						0, "Num files: 5; Symlinks: 1; Hardlinks: 1; Size "+
							"(total/recently uploaded/recently removed): 31 B / 51 B / 10 B\n"+
							"Uploaded: 5; Replaced: 0; Skipped: 0; Failed: 0; Missing: 0; Abnormal: 0")

					So(os.Remove(file5), ShouldBeNil)

					exitCode, _ = s.runBinary(t, "retry", "--name", setName, "-a")

					s.waitForStatus(setName, "\nDiscovery: completed", 10*time.Second)
					s.waitForStatus(setName, "\nStatus: complete", 10*time.Second)

					s.confirmOutputContains(t, []string{"status", "--name", setName, "-d"},
						0, "Num files: 4; Symlinks: 1; Hardlinks: 1; Size "+
							"(total/recently uploaded/recently removed): 20 B / 0 B / 11 B\n"+
							"Uploaded: 0; Replaced: 0; Skipped: 4; Failed: 0; Missing: 0; Abnormal: 0")

					exitCode, _ = s.runBinary(t, "retry", "--name", setName, "-a")

					s.waitForStatus(setName, "\nDiscovery: completed", 10*time.Second)
					s.waitForStatus(setName, "\nStatus: complete", 10*time.Second)

					s.confirmOutputContains(t, []string{"status", "--name", setName, "-d"},
						0, "Num files: 4; Symlinks: 1; Hardlinks: 1; Size "+
							"(total/recently uploaded/recently removed): 20 B / 0 B / 0 B\n"+
							"Uploaded: 0; Replaced: 0; Skipped: 4; Failed: 0; Missing: 0; Abnormal: 0")
				})

				Convey("And you can re-add the set again", func() {
					err = s.interactiveAdd(setName, "y", transformer, "items", tempTestFileOfPaths.Name())
					So(err, ShouldBeNil)

					s.waitForStatus(setName, "\nStatus: complete", 10*time.Second)

					statusCmd := []string{"status", "--name", setName, "-d"}
					s.confirmOutputContains(t, statusCmd, 0, file2)
					s.confirmOutputDoesNotContain(t, statusCmd, 0, "Removal status")
					s.confirmOutputContains(t, statusCmd, 0,
						"(total/recently uploaded/recently removed): 51 B / 10 B / 0 B\n")
				})
			})

			Convey("Remove removes the dir from the set", func() {
				s.removePath(t, setName, dir1, 2)

				s.confirmOutputContains(t, []string{"status", "--name", setName, "-d"},
					0, dir2)

				s.confirmOutputDoesNotContain(t, []string{"status", "--name", setName, "-d"},
					0, dir1+"/")

				s.confirmOutputDoesNotContain(t, []string{"status", "--name", setName, "-d"},
					0, dir1+" => ")
			})

			Convey("Remove removes the dir from the set even if it no longer exists", func() {
				err = os.RemoveAll(dir1)
				So(err, ShouldBeNil)

				s.removePath(t, setName, dir1, 2)

				s.confirmOutputContains(t, []string{"status", "--name", setName, "-d"},
					0, dir2)

				s.confirmOutputDoesNotContain(t, []string{"status", "--name", setName, "-d"},
					0, dir1+"/")

				s.confirmOutputDoesNotContain(t, []string{"status", "--name", setName, "-d"},
					0, dir1+" => ")
			})

			Convey("Remove removes an empty dir from the set", func() {
				s.removePath(t, setName, dir2, 1)

				s.confirmOutputContains(t, []string{"status", "--name", setName, "-d"},
					0, dir1)

				s.confirmOutputDoesNotContain(t, []string{"status", "--name", setName, "-d"},
					0, dir2+"/")

				s.confirmOutputDoesNotContain(t, []string{"status", "--name", setName, "-d"},
					0, dir2+" => ")
			})

			Convey("Remove removes the dir even if it is not specified in the set but is part of it", func() {
				dir3 := filepath.Join(dir1, "dir")
				err = os.MkdirAll(dir3, 0755)
				So(err, ShouldBeNil)

				file5 := filepath.Join(dir3, "file5")
				internal.CreateTestFile(t, file5, "some data3")

				setName = "nestedDirSet"

				s.addSetForTesting(t, setName, transformer, dir1)
				s.waitForStatus(setName, "\nStatus: complete", 5*time.Second)

				s.removePath(t, setName, dir3, 2)

				s.confirmOutputContains(t, []string{"status", "--name", setName, "-d"},
					0, dir1)

				s.confirmOutputDoesNotContain(t, []string{"status", "--name", setName, "-d"},
					0, dir3+"/")

				resetIRODS()
			})

			Convey("Given an added set with a folder containing a nested folder", func() {
				dir3 := filepath.Join(dir1, "dir")
				err = os.MkdirAll(dir3, 0755)
				So(err, ShouldBeNil)

				file5 := filepath.Join(dir3, "file5")
				internal.CreateTestFile(t, file5, "some data3")

				setName = "nestedDirSet"

				s.addSetForTesting(t, setName, transformer, dir1)
				s.waitForStatus(setName, "\nStatus: complete", 5*time.Second)

				Convey("Remove removes the nested dir even though it wasnt specified in the set", func() {
					s.removePath(t, setName, dir3, 2)

					s.confirmOutputContains(t, []string{"status", "--name", setName, "-d"},
						0, dir1)

					s.confirmOutputDoesNotContain(t, []string{"status", "--name", setName, "-d"},
						0, dir3+"/")
				})

				Convey("Remove on the parent folder submits itself and all children to be removed", func() {
					exitCode, _ := s.runBinary(t, "remove", "--name", setName, "--path", dir1)

					So(exitCode, ShouldEqual, 0)

					s.confirmOutputContains(t, []string{"status", "--name", setName, "-d"},
						0, "Removal status: 0 / 4 objects removed")

					s.waitForStatus(setName, "Removal status: 4 / 4 objects removed", 5*time.Second)
				})
			})

			Convey("Remove takes a flag --items and removes all provided files and dirs from the set", func() {
				tempTestFileOfPathsToRemove, errt := os.CreateTemp(dir, "testFileSet")
				So(errt, ShouldBeNil)

				_, err = io.WriteString(tempTestFileOfPathsToRemove,
					fmt.Sprintf("%s\n%s", file1, dir1))
				So(err, ShouldBeNil)

				exitCode, _ := s.runBinary(t, "remove", "--name", setName, "--items", tempTestFileOfPathsToRemove.Name())

				So(exitCode, ShouldEqual, 0)

				s.waitForStatus(setName, "Removal status: 3 / 3 objects removed", 5*time.Second)

				s.confirmOutputContains(t, []string{"status", "--name", setName, "-d"},
					0, file2)

				s.confirmOutputDoesNotContain(t, []string{"status", "--name", setName, "-d"},
					0, file1)

				s.confirmOutputContains(t, []string{"status", "--name", setName, "-d"},
					0, dir2)

				s.confirmOutputDoesNotContain(t, []string{"status", "--name", setName, "-d"},
					0, dir1+"/")

				s.confirmOutputDoesNotContain(t, []string{"status", "--name", setName, "-d"},
					0, dir1+" => ")
			})

			Convey("Remove with --items still works as expected with duplicates", func() {
				tempTestFileOfPathsToRemove, errt := os.CreateTemp(dir, "testFileSet")
				So(errt, ShouldBeNil)

				_, err = io.WriteString(tempTestFileOfPathsToRemove,
					fmt.Sprintf("%s\n%s\n%s\n%s", file1, file1, dir1, dir1))
				So(err, ShouldBeNil)

				exitCode, _ := s.runBinary(t, "remove", "--name", setName, "--items", tempTestFileOfPathsToRemove.Name())

				So(exitCode, ShouldEqual, 0)

				s.waitForStatus(setName, "Removal status: 3 / 3 objects removed", 5*time.Second)
			})

			Convey("if the server dies during removal, the removal will continue upon server startup", func() {
				tempTestFileOfPathsToRemove1, errt := os.CreateTemp(dir, "testFileSet")
				So(errt, ShouldBeNil)

				_, err = io.WriteString(tempTestFileOfPathsToRemove1,
					fmt.Sprintf("%s\n%s", file1, dir1))
				So(err, ShouldBeNil)

				tempTestFileOfPathsToRemove2, errt := os.CreateTemp(dir, "testFileSet")
				So(errt, ShouldBeNil)

				_, err = io.WriteString(tempTestFileOfPathsToRemove2,
					fmt.Sprintf("%s\n%s\n%s\n%s\n%s", file2, file4, dir2, linkPath, symPath))
				So(err, ShouldBeNil)

				exitCode, _ := s.runBinary(t, "remove", "--name", setName, "--items", tempTestFileOfPathsToRemove1.Name())

				So(exitCode, ShouldEqual, 0)

				exitCode, _ = s.runBinary(t, "remove", "--name", setName, "--items", tempTestFileOfPathsToRemove2.Name())

				So(exitCode, ShouldEqual, 0)

				err = s.Shutdown()
				So(err, ShouldBeNil)

				s.startServer()

				s.waitForStatus(setName, "Removal status: 8 / 8 objects removed", 5*time.Second)
			})

			Convey("Remove removes the provided file from iRODS", func() {
				output, erro := exec.Command("ils", remotePath).CombinedOutput()
				So(erro, ShouldBeNil)
				So(string(output), ShouldContainSubstring, "file1")

				s.removePath(t, setName, file1, 1)

				output, err = exec.Command("ils", remotePath).CombinedOutput()
				So(err, ShouldBeNil)
				So(string(output), ShouldNotContainSubstring, "file1")
			})

			Convey("Remove with a hardlink removes both the hardlink file and inode file", func() {
				remoteInode := getMetaValue(getRemoteMeta(filepath.Join(remotePath, "link")), "ibackup:remotehardlink")

				_, err = exec.Command("ils", remoteInode).CombinedOutput()
				So(err, ShouldBeNil)

				s.removePath(t, setName, linkPath, 1)

				_, err = exec.Command("ils", remoteLink).CombinedOutput()
				So(err, ShouldNotBeNil)

				_, err = exec.Command("ils", remoteInode).CombinedOutput()
				So(err, ShouldNotBeNil)
			})

			Convey("And another set with a hardlink to the same file", func() {
				linkPath2 := filepath.Join(path, "link2")

				err = os.Link(file1, linkPath2)
				So(err, ShouldBeNil)

				s.addSetForTesting(t, "testHardlinks", transformer, linkPath2)

				s.waitForStatus("testHardlinks", "\nStatus: complete", 10*time.Second)

				Convey("Removing a hardlink does not remove the inode file", func() {
					remoteInode := getMetaValue(getRemoteMeta(filepath.Join(remotePath, "link")), "ibackup:remotehardlink")

					_, err = exec.Command("ils", remoteInode).CombinedOutput()
					So(err, ShouldBeNil)

					s.removePath(t, setName, linkPath, 1)

					_, err = exec.Command("ils", remoteLink).CombinedOutput()
					So(err, ShouldNotBeNil)

					_, err = exec.Command("ils", remoteInode).CombinedOutput()
					So(err, ShouldBeNil)
				})
			})

			Convey("Remove removes the provided dir from iRODS", func() {
				output, errc := exec.Command("ils", "-r", remotePath).CombinedOutput()
				So(errc, ShouldBeNil)
				So(string(output), ShouldContainSubstring, "path/to/some/dir")
				So(string(output), ShouldContainSubstring, "file3\n")

				s.removePath(t, setName, dir1, 2)

				output, err = exec.Command("ils", "-r", remotePath).CombinedOutput()
				So(err, ShouldBeNil)
				So(string(output), ShouldContainSubstring, "dir_not_removed\n")
				So(string(output), ShouldNotContainSubstring, "path/to/some/dir")
				So(string(output), ShouldNotContainSubstring, "file3\n")
			})

			Convey("And if you remove a file nested in an otherwise empty dir", func() {
				s.removePath(t, setName, file3, 1)

				output, errc := exec.Command("ils", "-r", remotePath).CombinedOutput()
				So(errc, ShouldBeNil)
				So(string(output), ShouldNotContainSubstring, "path/to/some/dir")
				So(string(output), ShouldNotContainSubstring, "file3\n")

				Convey("You can remove its parent folder from the db", func() {
					s.confirmOutputContains(t, []string{"status", "--name", setName, "-d"},
						0, dir1)

					s.removePath(t, setName, dir1, 1)

					s.confirmOutputDoesNotContain(t, []string{"status", "--name", setName, "-d"},
						0, dir1+"/")

					s.confirmOutputDoesNotContain(t, []string{"status", "--name", setName, "-d"},
						0, dir1+" => ")
				})
			})

			Convey("And a new file added to a directory already in the set", func() {
				file5 := filepath.Join(dir1, "file5")
				internal.CreateTestFile(t, file5, "some data5")

				Convey("Remove returns an error if you try to remove just the file", func() {
					s.confirmOutputContains(t, []string{"remove", "--name", setName, "--path", file5},
						1, fmt.Sprintf("path(s) do not belong to the backup set : [%s] [%s]", file5, setName))
				})

				Convey("Remove ignores the file if you remove the directory", func() {
					exitCode, _ := s.runBinary(t, "remove", "--name", setName, "--path", dir1)
					So(exitCode, ShouldEqual, 0)
				})
			})

			Convey("And a new directory", func() {
				dir3 := filepath.Join(path, "path/to/new/dir/")

				Convey("Remove returns an error if you try to remove the directory that doesn't exist", func() {
					s.confirmOutputContains(t, []string{"remove", "--name", setName, "--path", dir3},
						1, fmt.Sprintf("path(s) do not belong to the backup set : [%s] [%s]", dir3, setName))
				})

				err = os.MkdirAll(dir3, 0755)
				So(err, ShouldBeNil)

				Convey("Remove returns an error if you try to remove the directory", func() {
					s.confirmOutputContains(t, []string{"remove", "--name", setName, "--path", dir3},
						1, fmt.Sprintf("path(s) do not belong to the backup set : [%s] [%s]", dir3, setName))
				})
			})

			Convey("And a set with the same files added by a different user", func() {
				user, erru := user.Current()
				So(erru, ShouldBeNil)

				setName2 := "different_user_set"

				exitCode, _ := s.runBinary(t, "add", "--name", setName2, "--transformer",
					transformer, "--items", tempTestFileOfPaths.Name(), "--user", "testUser")

				So(exitCode, ShouldEqual, 0)

				s.waitForStatusWithUser(setName2, "\nStatus: complete", "testUser", 20*time.Second)

				Convey("Remove removes the metadata related to the set", func() {
					output := getRemoteMeta(filepath.Join(remotePath, "file1"))
					So(output, ShouldContainSubstring, setName)
					So(output, ShouldContainSubstring, setName2)

					s.removePath(t, setName, file1, 1)

					output = getRemoteMeta(filepath.Join(remotePath, "file1"))
					So(output, ShouldNotContainSubstring, setName)
					So(output, ShouldContainSubstring, setName2)
				})

				Convey("Remove does not try and fail to remove the provided dir from iRODS", func() {
					output, errc := exec.Command("ils", "-r", remotePath).CombinedOutput()
					So(errc, ShouldBeNil)
					So(string(output), ShouldContainSubstring, "path/to/some/dir")

					s.removePath(t, setName, dir1, 2)

					output, errc = exec.Command("ils", "-r", remotePath).CombinedOutput()
					So(errc, ShouldBeNil)
					So(string(output), ShouldContainSubstring, "path/to/some/dir")
				})

				Convey("Remove does not remove the provided file from iRODS", func() {
					s.removePath(t, setName, file1, 1)

					output, errc := exec.Command("ils", remotePath).CombinedOutput()
					So(errc, ShouldBeNil)
					So(string(output), ShouldContainSubstring, "file1")
				})

				Convey("Remove on a file removes the user as a requester", func() {
					s.removePath(t, setName, file1, 1)

					requesters := getMetaValue(getRemoteMeta(filepath.Join(remotePath, "file1")), "ibackup:requesters")

					So(requesters, ShouldNotContainSubstring, user.Username)
				})

				Convey("And a second set with the same files added by the same user", func() {
					setName3 := "same_user_set"

					s.addSetForTestingWithItems(t, setName3, transformer, tempTestFileOfPaths.Name())

					Convey("Remove keeps the user as a requester", func() {
						s.removePath(t, setName, file1, 1)

						requesters := getMetaValue(getRemoteMeta(filepath.Join(remotePath, "file1")), "ibackup:requesters")

						So(requesters, ShouldContainSubstring, user.Username)
					})
				})
			})

			Convey("And a set with the same files and name added by a different user", func() {
				user, erru := user.Current()
				So(erru, ShouldBeNil)

				setName2 := setName

				exitCode, _ := s.runBinary(t, "add", "--name", setName2, "--transformer",
					transformer, "--items", tempTestFileOfPaths.Name(), "--user", "testUser")

				So(exitCode, ShouldEqual, 0)

				s.waitForStatusWithUser(setName2, "\nStatus: complete", "testUser", 20*time.Second)

				Convey("Remove on a file removes the user as a requester but not the file", func() {
					s.removePath(t, setName, file1, 1)

					requesters := getMetaValue(getRemoteMeta(filepath.Join(remotePath, "file1")), "ibackup:requesters")

					So(requesters, ShouldNotContainSubstring, user.Username)
				})

				Convey("And a second set with the same files added by the same user", func() {
					setName3 := "same_user_set"

					s.addSetForTestingWithItems(t, setName3, transformer, tempTestFileOfPaths.Name())

					Convey("Remove keeps the user as a requester", func() {
						s.removePath(t, setName, file1, 1)

						requesters := getMetaValue(getRemoteMeta(filepath.Join(remotePath, "file1")), "ibackup:requesters")

						So(requesters, ShouldContainSubstring, user.Username)
					})
				})
			})

			Convey("If a file fails to be removed, the error is displayed on the file", func() {
				removeFileFromIRODS(filepath.Join(remotePath, "file1"))

				exitCode, _ := s.runBinary(t, "remove", "--name", setName, "--path", file1)

				So(exitCode, ShouldEqual, 0)

				time.Sleep(2 * time.Second)

				s.confirmOutputContains(t, []string{"status", "--name", setName, "-d"},
					0, fmt.Sprintf("list operation failed: Path '%s'", filepath.Join(remotePath, "file1")))

				Convey("And displays the error in set status if not fixed", func() {
					s.waitForStatus(setName, fmt.Sprintf("Error: Error when removing: list operation failed: Path '%s'",
						filepath.Join(remotePath, "file1")), 30*time.Second)
				})

				Convey("And succeeds if issue is fixed during retries", func() {
					addFileToIRODS(file1, filepath.Join(remotePath, "file1"))

					s.waitForStatus(setName, "Removal status: 1 / 1 objects removed", 30*time.Second)
				})
			})

			Convey("And if you make this set read-only", func() {
				exitCode, _ := s.runBinary(t, "edit", "--name", setName, "--make-readonly")
				So(exitCode, ShouldEqual, 0)

				Convey("You can no longer remove files from it", func() {
					s.confirmOutputContains(t, []string{"remove", "--name", setName, "--path", file1}, 1,
						set.ErrSetIsNotWritable)
				})
			})
		})

		Convey("Given a set with a file that failed to upload", func() {
			dir3 := filepath.Join(path, "dir3")

			err := os.MkdirAll(dir3, 0755)
			So(err, ShouldBeNil)

			file5 := filepath.Join(dir3, "file5")
			internal.CreateTestFile(t, file5, "some data1")

			err = os.Chmod(file5, 0000)
			So(err, ShouldBeNil)

			setName := "setWithFailures"

			s.addSetForTesting(t, setName, transformer, dir3)

			s.waitForStatus(setName, "\nStatus: complete (but with failures", 10*time.Second)

			Convey("Remove will still work", func() {
				exitCode, _ := s.runBinary(t, "remove", "--name", setName, "--path", file5)

				So(exitCode, ShouldEqual, 0)

				s.confirmOutputContains(t, []string{"status", "--name", setName, "-d"},
					0, "Removal status: 0 / 1 objects removed")

				s.waitForStatus(setName, "Removal status: 1 / 1 objects removed", 10*time.Second)
			})
		})
	})
}

func getMetaValue(meta, key string) string {
	attrFind := "attribute: " + key + "\nvalue: "
	attrPos := strings.Index(meta, attrFind)
	So(attrPos, ShouldNotEqual, -1)

	value := meta[attrPos+len(attrFind):]
	nlPos := strings.Index(value, "\n")
	So(nlPos, ShouldNotEqual, -1)

	return value[:nlPos]
}

func TestEdit(t *testing.T) {
	Convey("With a started server", t, func() {
		t.Setenv("IBACKUP_TEST_LDAP_SERVER", "")
		t.Setenv("IBACKUP_TEST_LDAP_LOOKUP", "")

		s := NewTestServer(t)
		So(s, ShouldNotBeNil)

		Convey("With no --name given, edit returns an error", func() {
			s.confirmOutputContains(t, []string{"edit"}, 1, "Error: required flag(s) \"name\" not set")
		})

		Convey("Edit on nonexisting set produces an error", func() {
			s.confirmOutputContains(t, []string{"edit", "--name", "badSet"}, 1, "set with that id does not exist")
		})

		Convey("You can specify either --make-readonly or --disable-readonly", func() {
			s.confirmOutputContains(t, []string{"edit", "--make-readonly", "--disable-readonly"}, 1, cmd.ErrInvalidEdit.Error())
		})

		Convey("Given a transformer", func() {
			remotePath := os.Getenv("IBACKUP_TEST_COLLECTION")
			if remotePath == "" {
				SkipConvey("skipping iRODS backup test since IBACKUP_TEST_COLLECTION not set", func() {})

				return
			}

			path := t.TempDir()
			transformer := "prefix=" + path + ":" + remotePath

			Convey("And a monitored set", func() {
				setName := "monitoredSet"
				s.addSetForTestingWithFlag(t, setName, transformer, path, "--monitor", "1d")

				s.confirmOutputContains(t, []string{"status", "--name", setName}, 0, "Monitored: 1d;")

				Convey("You can disable monitoring", func() {
					exitCode, _ := s.runBinary(t, "edit", "--name", setName, "--stop-monitor")
					So(exitCode, ShouldEqual, 0)

					s.confirmOutputContains(t, []string{"status", "--name", setName}, 0, "Monitored: false;")
				})
			})

			Convey("And a set with monitored removals", func() {
				setName := "monitoredRemovalsSet"
				s.addSetForTestingWithFlags(t, setName, transformer, "--path", path, "--monitor", "1d", "--monitor-removals")

				s.confirmOutputContains(t, []string{"status", "--name", setName}, 0, "Monitored (with removals): 1d;")

				Convey("You can disable monitoring removals", func() {
					exitCode, _ := s.runBinary(t, "edit", "--name", setName, "--stop-monitor-removals")
					So(exitCode, ShouldEqual, 0)

					s.confirmOutputContains(t, []string{"status", "--name", setName}, 0, "Monitored: 1d;")
				})
			})

			Convey("And a set marked as archive", func() {
				setName := "archiveSet"
				s.addSetForTestingWithFlags(t, setName, transformer, "--path", path, "--archive")

				s.confirmOutputContains(t, []string{"status", "--name", setName}, 0, "Archive: true\n")

				Convey("You can disable archive mode", func() {
					exitCode, _ := s.runBinary(t, "edit", "--name", setName, "--stop-archiving")
					So(exitCode, ShouldEqual, 0)

					s.confirmOutputContains(t, []string{"status", "--name", setName}, 0, "Archive: false\n")
				})
			})

			Convey("And a set", func() {
				setName := "readOnlySet"
				s.addSetForTesting(t, setName, transformer, path)

				Convey("You can make it readonly", func() {
					exitCode, _ := s.runBinary(t, "edit", "--name", setName, "--make-readonly")
					So(exitCode, ShouldEqual, 0)

					s.confirmOutputContains(t, []string{"status", "--name", setName}, 0, "Read-only: true\n")

					Convey("And you can no longer edit it", func() {
						s.confirmOutputContains(t, []string{"edit", "--name", setName}, 1,
							set.ErrSetIsNotWritable)
					})

					Convey("And you can no longer retry it", func() {
						s.confirmOutputContains(t, []string{"retry", "--name", setName, "--all"}, 1,
							set.ErrSetIsNotWritable)
					})

					Convey("And admin can make it writable", func() {
						exitCode, _ := s.runBinary(t, "edit", "--name", setName, "--disable-readonly")
						So(exitCode, ShouldEqual, 0)
					})
				})
			})

			Convey("And a read-only set made by a different user", func() {
				user := "root"
				setName := "rootSet"
				fakeDir := generateFakeJWT(t, s, user)
				originalEnv := slices.Clone(s.env)
				s.env = append(slices.DeleteFunc(s.env, func(str string) bool {
					return strings.HasPrefix(str, "XDG_STATE_HOME")
				}), "XDG_STATE_HOME="+fakeDir)

				exitCode, _ := s.runBinary(t, "add", "--name", setName, "--transformer", transformer,
					"--path", path, "--user", user)
				So(exitCode, ShouldEqual, 0)

				s.waitForStatusWithUser(setName, "\nDiscovery: completed", user, 10*time.Second)

				exitCode, _ = s.runBinary(t, "edit", "--name", setName, "--user", user, "--make-readonly")
				So(exitCode, ShouldEqual, 0)

				s.confirmOutputContains(t, []string{"status", "--name", setName, "--user", user}, 0, "Read-only: true\n")

				Convey("And that user cannot make it writable", func() {
					s.confirmOutputContains(t, []string{"edit", "--name", setName, "--user", user, "--disable-readonly"}, 1,
						server.ErrNotAdmin.Error())
				})

				Convey("Admin can make it writable", func() {
					s.env = originalEnv
					exitCode, _ := s.runBinary(t, "edit", "--name", setName, "--user", user, "--disable-readonly")
					So(exitCode, ShouldEqual, 0)

					s.confirmOutputDoesNotContain(t, []string{"status", "--name", setName, "--user", user}, 0, "Read-only: true")
				})
			})
		})
	})
}

func generateFakeJWT(t *testing.T, s *TestServer, user string) string {
	t.Helper()

	defer t.Setenv("XDG_STATE_HOME", os.Getenv("XDG_STATE_HOME"))

	fakeDir := t.TempDir()

	t.Setenv("XDG_STATE_HOME", fakeDir)

	c, err := gas.NewClientCLI(".ibackup.jwt", ".ibackup.token", s.url, s.cert, false)
	So(err, ShouldBeNil)
	So(c.Login(user, "password"), ShouldBeNil)

	return fakeDir
}
