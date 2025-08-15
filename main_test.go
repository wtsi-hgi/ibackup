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
	"github.com/wtsi-ssg/wr/backoff"
	btime "github.com/wtsi-ssg/wr/backoff/time"
	"github.com/wtsi-ssg/wr/retry"
)

const (
	app               = "ibackup"
	userPerms         = 0700
	alternateUsername = "root"
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

func NewUploadingTestServer(t *testing.T, withDBBackup bool) (*TestServer, string) {
	t.Helper()

	dir := t.TempDir()

	s := new(TestServer)

	s.prepareFilePaths(dir)
	s.prepareConfig()

	remotePath := os.Getenv("IBACKUP_TEST_COLLECTION")
	if remotePath == "" {
		t.Skip("skipping iRODS backup test since IBACKUP_TEST_COLLECTION not set")
	}

	schedulerDeployment := os.Getenv("IBACKUP_TEST_SCHEDULER")
	if schedulerDeployment == "" {
		t.Skip("skipping iRODS backup test since IBACKUP_TEST_SCHEDULER not set")
	}

	s.schedulerDeployment = schedulerDeployment
	s.remoteHardlinkPrefix = filepath.Join(remotePath, "hardlinks")

	if withDBBackup {
		s.backupFile = filepath.Join(dir, "db.bak")
	}

	s.startServer()

	return s, remotePath
}

func checkICommands(t *testing.T, commands ...string) {
	t.Helper()

	for _, command := range commands {
		iCommandsAvailable, err := checkIRODSCommand(command)
		So(err, ShouldBeNil)

		if !iCommandsAvailable {
			t.Skipf("skipping iRODS backup tests since the iCommand '%s' is not available", command)
		}
	}
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

func (s *TestServer) impersonateUser(t *testing.T, username string) []string {
	t.Helper()

	fakeDir := generateFakeJWT(t, s, username)
	originalEnv := slices.Clone(s.env)
	s.env = append(slices.DeleteFunc(s.env, func(str string) bool {
		return strings.HasPrefix(str, "XDG_STATE_HOME")
	}), "XDG_STATE_HOME="+fakeDir)

	return originalEnv
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

	waitTimeout := 20 * time.Second

	args := []string{"add", "--name", name, "--transformer", transformer}
	args = append(args, flags...)
	exitCode, _ := s.runBinary(t, args...)

	So(exitCode, ShouldEqual, 0)

	if index := slices.Index(flags, "--user"); index != -1 {
		s.waitForStatusWithUser(name, "\nDiscovery: completed", flags[index+1], waitTimeout)
		s.waitForStatusWithUser(name, "\nStatus: complete", flags[index+1], waitTimeout)
	} else {
		s.waitForStatus(name, "\nDiscovery: completed", 20*time.Second)
		s.waitForStatus(name, "\nStatus: complete", 20*time.Second)
	}
}

func (s *TestServer) removePath(t *testing.T, name, path string, numFiles int) {
	t.Helper()

	exitCode, _ := s.runBinary(t, "remove", "--name", name, "--path", path)
	So(exitCode, ShouldEqual, 0)

	s.waitForStatus(name, fmt.Sprintf("Removal status: %d / %d objects removed", numFiles, numFiles), 10*time.Second)
}

func (s *TestServer) trashRemovePath(t *testing.T, name, path string, numFiles int) {
	t.Helper()

	exitCode, _ := s.runBinary(t, "trash", "--remove", "--name", name, "--path", path)
	So(exitCode, ShouldEqual, 0)

	statusMsg := fmt.Sprintf("Removal status: %d / %d objects removed", numFiles, numFiles)
	s.waitForStatus(set.TrashPrefix+name, statusMsg, 10*time.Second)
}

func (s *TestServer) waitForStatus(name, statusToFind string, timeout time.Duration) {
	s.waitForStatusWithFlags(name, statusToFind, timeout)
}

func (s *TestServer) waitForStatusWithUser(name, statusToFind, user string, timeout time.Duration) {
	s.waitForStatusWithFlags(name, statusToFind, timeout, "--user", user)
}

func (s *TestServer) waitForStatusWithFlags(name, statusToFind string, timeout time.Duration, args ...string) {
	ctx, cancelFn := context.WithTimeout(context.Background(), timeout)
	defer cancelFn()

	cmd := []string{"status", "--name", name, "--url", s.url, "--cert", s.cert}
	cmd = append(cmd, args...)

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
	}, &retry.UntilNoError{}, &backoff.Backoff{
		Min:     10 * time.Millisecond,
		Max:     100 * time.Millisecond,
		Factor:  2,
		Sleeper: &btime.Sleeper{},
	}, "waiting for matching status")

	if status.Err != nil {
		fmt.Printf("\nfailed to see set %s get status: %s\n%s\n", name, statusToFind, string(output)) //nolint:forbidigo
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
						dir+"/path/to/other/file\t/remote/path/to/other/file\n"+
							dir+"/path/to/some/file\t/remote/path/to/some/file")
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
						dir+"/path/to/other/file\t/remote/path/to/other/file\t0\n"+
							dir+"/path/to/some/file\t/remote/path/to/some/file\t0")
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

	Convey("With a started uploading server", t, func() {
		resetIRODS()

		s, remote := NewUploadingTestServer(t, false)
		So(s, ShouldNotBeNil)

		Convey("Given an added set defined with files that are uploaded", func() {
			dir := t.TempDir()
			tempTestFile, err := os.CreateTemp(dir, "testFileSet")
			So(err, ShouldBeNil)

			uploadFile1Path := filepath.Join(dir, "file1")
			uploadFile2Path := filepath.Join(dir, "file2")

			_, err = io.WriteString(tempTestFile, uploadFile1Path+"\n"+uploadFile2Path+"\n")
			So(err, ShouldBeNil)

			So(os.MkdirAll(dir+"/path/to/other/", 0700), ShouldBeNil)
			internal.CreateTestFile(t, uploadFile1Path, "data")
			internal.CreateTestFile(t, uploadFile2Path, "data")

			setName := "testAddFiles_" + time.Now().Format("20060102150405")

			s.addSetForTestingWithFlags(t, setName, "prefix="+dir+":"+remote, "--items", tempTestFile.Name())

			s.waitForStatus(setName, "Status: complete", 20*time.Second)

			Convey("You can delete one of the files which doesn't affect list output and re-trigger the discovery", func() {
				So(os.Remove(uploadFile2Path), ShouldBeNil)

				s.confirmOutput(t, []string{"list", "--name", setName, "--local", "--uploaded"}, 0,
					uploadFile1Path+"\n"+uploadFile2Path)
				s.confirmOutput(t, []string{"list", "--name", setName, "--local", "--last-state"}, 0,
					uploadFile1Path+"\n"+uploadFile2Path)

				exitCode, _ := s.runBinary(t, "sync", "--name", setName)
				So(exitCode, ShouldEqual, 0)
				s.waitForStatus(setName, "Status: complete", 20*time.Second)

				Convey("Then list --uploaded shows all uploaded files, including orphans", func() {
					s.confirmOutput(t, []string{"list", "--name", setName, "--local", "--uploaded"}, 0,
						uploadFile1Path+"\n"+uploadFile2Path)
				})

				Convey("Then list --last-state shows only uploaded files that still existed locally at last discovery",
					func() {
						s.confirmOutput(t, []string{"list", "--name", setName, "--local", "--last-state"}, 0,
							uploadFile1Path)
						s.confirmOutputDoesNotContain(t, []string{"list", "--name", setName, "--local", "--last-state"}, 0,
							uploadFile2Path)
					})
			})
		})
	})

	Convey("Given a server configured with a upload location and scheduler", t, func() {
		s, remotePath := NewUploadingTestServer(t, true)

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

			s.addSetForTestingWithFlags(t, setName, "prefix="+dir+":"+remotePath, "-f", fofn)

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
Uploaded: 0; Replaced: 0; Skipped: 0; Failed: 0; Missing: 0; Orphaned: 0; Abnormal: 0
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
Uploaded: 0; Replaced: 0; Skipped: 0; Failed: 0; Missing: 0; Orphaned: 0; Abnormal: 0
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
Uploaded: 0; Replaced: 0; Skipped: 0; Failed: 0; Missing: 0; Orphaned: 0; Abnormal: 0
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
Uploaded: 0; Replaced: 0; Skipped: 0; Failed: 0; Missing: 0; Orphaned: 0; Abnormal: 0
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
Uploaded: 0; Replaced: 0; Skipped: 0; Failed: 0; Missing: 0; Orphaned: 0; Abnormal: 0
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
Uploaded: 0; Replaced: 0; Skipped: 0; Failed: 0; Missing: 0; Orphaned: 0; Abnormal: 0
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
Uploaded: 0; Replaced: 0; Skipped: 0; Failed: 0; Missing: 0; Orphaned: 0; Abnormal: 0
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
Uploaded: 0; Replaced: 0; Skipped: 0; Failed: 0; Missing: 0; Orphaned: 0; Abnormal: 0
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
Uploaded: 0; Replaced: 0; Skipped: 0; Failed: 0; Missing: 0; Orphaned: 0; Abnormal: 0
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
Uploaded: 0; Replaced: 0; Skipped: 0; Failed: 0; Missing: 0; Orphaned: 0; Abnormal: 0
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
Uploaded: 0; Replaced: 0; Skipped: 0; Failed: 0; Missing: 0; Orphaned: 0; Abnormal: 0
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
Uploaded: 0; Replaced: 0; Skipped: 0; Failed: 0; Missing: 2; Orphaned: 0; Abnormal: 0
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
Uploaded: 0; Replaced: 0; Skipped: 0; Failed: 0; Missing: 2; Orphaned: 0; Abnormal: 0
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
Uploaded: 0; Replaced: 0; Skipped: 0; Failed: 0; Missing: 0; Orphaned: 0; Abnormal: 0
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
Uploaded: 0; Replaced: 0; Skipped: 0; Failed: 0; Missing: 0; Orphaned: 0; Abnormal: 0
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
Uploaded: 0; Replaced: 0; Skipped: 0; Failed: 0; Missing: 0; Orphaned: 0; Abnormal: 0
Completed in: 0s
Directories:
  `+localDir+" => "+remote)
		})

		Convey("Given an added set defined with a humgen transformer and a v2 path, the remote directory is correct", func() {
			humgenFile := "/lustre/scratch125/humgen/teams_v2/hgi/mercury/ibackup/file_for_testsuite.do_not_delete"
			humgenDir := filepath.Dir(humgenFile)

			if _, err := os.Stat(humgenDir); err != nil {
				SkipConvey("skip humgen transformer test since not in humgen", func() {})

				return
			}

			s.addSetForTesting(t, "humgenV2Set", "humgen", humgenFile)

			s.confirmOutput(t, []string{"status", "-n", "humgenV2Set"}, 0,
				`Global put queue status: 1 queued; 0 reserved to be worked on; 0 failed
Global put client status (/10): 0 iRODS connections; 0 creating collections; 0 currently uploading

Name: humgenV2Set
Transformer: humgen
Reason: backup
Review date: `+reviewDate+`
Removal date: `+removalDate+`
Monitored: false; Archive: false
Status: pending upload
Discovery:
Num files: 1; Symlinks: 0; Hardlinks: 0; Size (total/recently uploaded/recently removed): 0 B (and counting) / 0 B / 0 B
Uploaded: 0; Replaced: 0; Skipped: 0; Failed: 0; Missing: 0; Orphaned: 0; Abnormal: 0
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
Uploaded: 0; Replaced: 0; Skipped: 0; Failed: 0; Missing: 0; Orphaned: 0; Abnormal: 0
Example File: `+gengenFile+" => /humgen/gengen/teams/hgi/scratch126/mercury/ibackup/file_for_testsuite.do_not_delete")
		})

		Convey("Given an added set defined with a gengen transformer and v2 path, the remote directory is correct", func() {
			gFile := "/lustre/scratch126/gengen/teams_v2/hgi/mercury/ibackup/file_for_testsuite.do_not_delete"
			gengenDir := filepath.Dir(gFile)

			if _, err := os.Stat(gengenDir); err != nil {
				SkipConvey("skip gengen transformer test since not in gengen", func() {})

				return
			}

			s.addSetForTesting(t, "gengenV2Set", "gengen", gFile)

			s.confirmOutput(t, []string{"status", "-n", "gengenV2Set"}, 0,
				`Global put queue status: 1 queued; 0 reserved to be worked on; 0 failed
Global put client status (/10): 0 iRODS connections; 0 creating collections; 0 currently uploading

Name: gengenV2Set
Transformer: gengen
Reason: backup
Review date: `+reviewDate+`
Removal date: `+removalDate+`
Monitored: false; Archive: false
Status: pending upload
Discovery:
Num files: 1; Symlinks: 0; Hardlinks: 0; Size (total/recently uploaded/recently removed): 0 B (and counting) / 0 B / 0 B
Uploaded: 0; Replaced: 0; Skipped: 0; Failed: 0; Missing: 0; Orphaned: 0; Abnormal: 0
Example File: `+gFile+" => /humgen/gengen/teams/hgi/scratch126_v2/mercury/ibackup/file_for_testsuite.do_not_delete")
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
Uploaded: 0; Replaced: 0; Skipped: 0; Failed: 0; Missing: 0; Orphaned: 0; Abnormal: 0
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
Uploaded: 0; Replaced: 0; Skipped: 0; Failed: 0; Missing: 0; Orphaned: 0; Abnormal: 0
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
Uploaded: 0; Replaced: 0; Skipped: 0; Failed: 0; Missing: 0; Orphaned: 0; Abnormal: 1
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
Uploaded: 0; Replaced: 0; Skipped: 0; Failed: 0; Missing: 0; Orphaned: 0; Abnormal: 0
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

		checkICommands(t, "ils")

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
Uploaded: 0; Replaced: 0; Skipped: 0; Failed: 0; Missing: 0; Orphaned: 0; Abnormal: 0
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
		checkICommands(t, "imeta")

		s, remotePath := NewUploadingTestServer(t, true)

		path := t.TempDir()
		transformer := "prefix=" + path + ":" + remotePath

		Convey("You cannot add sets starting with a trash prefix", func() {
			setName := set.TrashPrefix + "mySet"
			exitCode, output := s.runBinaryWithNoLogging(t, "add", "--name", setName,
				"--transformer", transformer, "--path", path)
			So(exitCode, ShouldEqual, 1)
			So(output, ShouldContainSubstring, server.ErrTrashSetName.Error())
		})

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
			dir := t.TempDir()

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
					"Uploaded: 3; Replaced: 0; Skipped: 0; Failed: 0; Missing: 0; Orphaned: 0; Abnormal: 0")
				s.confirmOutputContains(t, statusCmd, 0,
					"Num files: 3; Symlinks: 0; Hardlinks: 0; Size (total/recently uploaded/recently removed): 30 B / 30 B / 0 B")

				s.confirmOutputContains(t, statusCmd, 0, "")

				newName := setName + ".v2"
				statusCmd[2] = newName

				s.addSetForTesting(t, newName, transformer, path)
				s.waitForStatus(newName, "\nStatus: complete", 60*time.Second)
				s.confirmOutputContains(t, statusCmd, 0,
					"Uploaded: 0; Replaced: 0; Skipped: 3; Failed: 0; Missing: 0; Orphaned: 0; Abnormal: 0")
				s.confirmOutputContains(t, statusCmd, 0,
					"Num files: 3; Symlinks: 0; Hardlinks: 0; Size (total/recently uploaded/recently removed): 30 B / 0 B / 0 B")

				newName = setName + ".v3"
				statusCmd[2] = newName

				internal.CreateTestFile(t, file2, "some data2 updated")

				s.addSetForTesting(t, newName, transformer, path)
				s.waitForStatus(newName, "\nStatus: complete", 60*time.Second)
				s.confirmOutputContains(t, statusCmd, 0,
					"Uploaded: 0; Replaced: 1; Skipped: 2; Failed: 0; Missing: 0; Orphaned: 0; Abnormal: 0")
				s.confirmOutputContains(t, statusCmd, 0,
					"Num files: 3; Symlinks: 0; Hardlinks: 0; Size (total/recently uploaded/recently removed): 38 B / 18 B / 0 B")

				internal.CreateTestFile(t, file2, "less data")
				exitCode, _ := s.runBinary(t, "sync", "--name", newName)
				So(exitCode, ShouldEqual, 0)

				s.waitForStatus(newName, "\nStatus: complete", 60*time.Second)
				s.confirmOutputContains(t, statusCmd, 0,
					"Uploaded: 0; Replaced: 1; Skipped: 2; Failed: 0; Missing: 0; Orphaned: 0; Abnormal: 0")
				s.confirmOutputContains(t, statusCmd, 0,
					"Num files: 3; Symlinks: 0; Hardlinks: 0; Size (total/recently uploaded/recently removed): 29 B / 9 B / 0 B")
			})

			Convey("Syncing a set with locally removed files will show orphaned status", func() {
				resetIRODS()

				setName := "setWithOrphanedFiles"

				file4 := filepath.Join(path, "file4")
				err := os.Link(file1, file4)
				So(err, ShouldBeNil)

				s.addSetForTesting(t, setName, transformer, path)

				s.waitForStatus(setName, "\nStatus: complete", 20*time.Second)

				statusCmd := []string{"status", "--name", setName, "-d"}

				exitCode, output := s.runBinary(t, statusCmd...)
				So(exitCode, ShouldEqual, 0)
				So(output, ShouldContainSubstring, "Hardlinks: 1;")
				So(output, ShouldContainSubstring, file1+"\tuploaded\t10 B\t")
				So(output, ShouldContainSubstring, file2+"\tuploaded\t")

				err = os.Remove(file1)
				So(err, ShouldBeNil)

				err = os.Remove(file4)
				So(err, ShouldBeNil)

				exitCode, _ = s.runBinary(t, "sync", "--name", setName)
				So(exitCode, ShouldEqual, 0)

				s.waitForStatus(setName, "\nStatus: complete", 10*time.Second)

				exitCode, output = s.runBinary(t, statusCmd...)
				So(exitCode, ShouldEqual, 0)

				So(output, ShouldContainSubstring, "Hardlinks: 1;")
				So(output, ShouldContainSubstring, "Missing: 0;")
				So(output, ShouldContainSubstring, "Orphaned: 2;")
				So(output, ShouldContainSubstring, "(total/recently uploaded/recently removed): 30 B / 0 B / 0 B")

				So(output, ShouldContainSubstring, file1+"\torphaned\t10 B\t")
				So(output, ShouldContainSubstring, file2+"\tskipped\t")
				So(output, ShouldContainSubstring, file4+"\torphaned\t0 B\t")
			})

			Convey("Syncing a set with locally removed dirs will show orphaned status", func() {
				setName := "setWithOrphanedDirs"
				s.addSetForTesting(t, setName, transformer, path)

				s.waitForStatus(setName, "\nStatus: complete", 10*time.Second)

				statusCmd := []string{"status", "--name", setName, "-d"}

				exitCode, output := s.runBinary(t, statusCmd...)
				So(exitCode, ShouldEqual, 0)
				So(output, ShouldContainSubstring, path+" => "+remotePath)

				err := os.RemoveAll(path)
				So(err, ShouldBeNil)

				exitCode, _ = s.runBinary(t, "sync", "--name", setName)
				So(exitCode, ShouldEqual, 0)

				s.waitForStatus(setName, "\nStatus: complete", 10*time.Second)

				exitCode, output = s.runBinary(t, statusCmd...)
				So(exitCode, ShouldEqual, 0)
				So(output, ShouldContainSubstring, path+" (missing)"+" => "+remotePath+" (orphaned)")

				exitCode, output = s.runBinary(t, statusCmd...)
				So(exitCode, ShouldEqual, 0)
				So(output, ShouldContainSubstring, "Orphaned: 3;")
				So(output, ShouldContainSubstring, file1+"\torphaned\t10 B\t")
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

			s.confirmOutput(t, []string{"retry", "--name", setName},
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

func addRemoteMeta(remotePath, key, value string) {
	_, err := exec.Command("imeta", "add", "-d", remotePath, key, value).CombinedOutput()
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
			file8 := filepath.Join(restoreDir, "file8")
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

			restoreFiles(
				t,
				file8+"\t"+remote1+"\n",
				"1 downloaded (0 replaced); 0 skipped; 0 failed; 0 missing\n",
				"--hardlinks_as_normal",
			)
			confirmFileContents(t, file8, fileContents2)

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
	Convey("Given a server", t, func() {
		checkICommands(t, "imeta")

		s, remotePath := NewUploadingTestServer(t, false)

		path := t.TempDir()
		transformer := "prefix=" + path + ":" + remotePath

		Convey("And an invalid set name, remove returns an error", func() {
			invalidSetName := "invalidSet"

			s.confirmOutputContains(t, []string{"remove", "--name", invalidSetName, "--path", path},
				1, fmt.Sprintf("set with that id does not exist [%s]", invalidSetName))
		})

		Convey("And an added set with files and folders", func() {
			dir := t.TempDir()

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

			_, err = io.WriteString(tempTestFileOfPaths,
				fmt.Sprintf("%s\n%s\n%s\n%s\n%s\n%s", file1, file2, file4, file5, dir1, dir2))
			So(err, ShouldBeNil)

			setName := "testRemoveFiles1"

			resetIRODS()

			s.addSetForTestingWithItems(t, setName, transformer, tempTestFileOfPaths.Name())

			Convey("Remove removes the file from the set and moves it to the trash set", func() {
				exitCode, output := s.runBinary(t, "remove", "--name", setName, "--path", file2)

				So(exitCode, ShouldEqual, 0)

				s.confirmOutputContains(t, []string{"status", "--name", setName, "-d"},
					0, "Removal status: 0 / 1 objects removed")

				s.waitForStatus(setName, "Removal status: 1 / 1 objects removed", 5*time.Second)

				trashSetName := set.TrashPrefix + setName

				Convey("Status with setname and --trash will display the trashed set", func() {
					exitCode, output = s.runBinary(t, "status", "--name", setName, "--trash", "-d")
					So(exitCode, ShouldEqual, 0)
					So(output, ShouldContainSubstring, file2+"\t"+time.Now().Format("06/01/02")+"\tuploaded\t10 B")
				})

				Convey("Status with no name and --trash will only display trashed sets", func() {
					exitCode, output = s.runBinary(t, "status", "--trash")
					So(exitCode, ShouldEqual, 0)
					So(output, ShouldContainSubstring, "Name: "+trashSetName)
					So(output, ShouldNotContainSubstring, "Name: "+setName)
				})

				Convey("Status with no name and without --trash will not display the trashed sets", func() {
					exitCode, output = s.runBinary(t, "status")
					So(exitCode, ShouldEqual, 0)
					So(output, ShouldContainSubstring, "Name: "+setName)
					So(output, ShouldNotContainSubstring, "Name: "+trashSetName)
				})

				Convey("And changes the set metadata in iRODS to a .trash set", func() {
					sets := getMetaValue(getRemoteMeta(filepath.Join(remotePath, "file2")), transfer.MetaKeySets)
					setsSlice := strings.Split(sets, ",")

					So(setsSlice, ShouldContain, trashSetName)
					So(setsSlice, ShouldNotContain, setName)
				})

				Convey("And you cannot remove from the trash set", func() {
					s.confirmOutputContains(t, []string{"remove", "--name", trashSetName, "--path", path},
						1, server.ErrTrashSetName.Error())
				})

				Convey("And you cannot edit the trash set", func() {
					s.confirmOutputContains(t, []string{"edit", "--name", trashSetName, "--add", path},
						1, server.ErrTrashSetName.Error())

					s.confirmOutputContains(t, []string{"edit", "--name", trashSetName, "--make-readonly"},
						1, server.ErrTrashSetName.Error())
				})

				Convey("And you cannot sync or retry the trash set", func() {
					s.confirmOutputContains(t, []string{"sync", "--name", trashSetName},
						1, server.ErrTrashSetName.Error())

					s.confirmOutputContains(t, []string{"retry", "--name", trashSetName},
						1, server.ErrTrashSetName.Error())
				})

				Convey("Remove again will remove another object and status will update accordingly", func() {
					s.confirmOutputContains(t, []string{"status", "--name", setName, "-d"},
						0, "Num files: 4; Symlinks: 0; Hardlinks: 0; Size "+
							"(total/recently uploaded/recently removed): 41 B / 51 B / 10 B\n"+
							"Uploaded: 4; Replaced: 0; Skipped: 0; Failed: 0; Missing: 0; Orphaned: 0; Abnormal: 0")

					exitCode, _ = s.runBinary(t, "remove", "--name", setName, "--path", dir1)
					So(exitCode, ShouldEqual, 0)

					s.waitForStatus(setName, "Removal status: 2 / 2 objects removed", 5*time.Second)

					exitCode, output := s.runBinary(t, "status", "--name", setName, "-d")
					So(exitCode, ShouldEqual, 0)

					So(output, ShouldNotContainSubstring, file3)
					So(output, ShouldContainSubstring,
						"Num files: 3; Symlinks: 0; Hardlinks: 0; Size "+
							"(total/recently uploaded/recently removed): 31 B / 51 B / 10 B\n"+
							"Uploaded: 3; Replaced: 0; Skipped: 0; Failed: 0; Missing: 0; Orphaned: 0; Abnormal: 0")

					Convey("And status will remain correct after two-way sync is triggered", func() {
						So(os.Remove(file5), ShouldBeNil)

						exitCode, _ = s.runBinary(t, "sync", "--name", setName)

						s.waitForStatus(setName, "Removal status: 1 / 1 objects removed", 5*time.Second)

						s.waitForStatus(setName, "\nDiscovery: completed", 10*time.Second)
						s.waitForStatus(setName, "\nStatus: complete", 10*time.Second)

						s.confirmOutputContains(t, []string{"status", "--name", setName, "-d"},
							0, "Num files: 2; Symlinks: 0; Hardlinks: 0; Size "+
								"(total/recently uploaded/recently removed): 20 B / 0 B / 11 B\n"+
								"Uploaded: 0; Replaced: 0; Skipped: 2; Failed: 0; Missing: 0; Orphaned: 0; Abnormal: 0")

						exitCode, _ = s.runBinary(t, "sync", "--name", setName)

						s.waitForStatus(setName, "\nDiscovery: completed", 10*time.Second)
						s.waitForStatus(setName, "\nStatus: complete", 10*time.Second)

						s.confirmOutputContains(t, []string{"status", "--name", setName, "-d"},
							0, "Num files: 2; Symlinks: 0; Hardlinks: 0; Size "+
								"(total/recently uploaded/recently removed): 20 B / 0 B / 0 B\n"+
								"Uploaded: 0; Replaced: 0; Skipped: 2; Failed: 0; Missing: 0; Orphaned: 0; Abnormal: 0")
					})
				})
			})

			Convey("And with another set with the same name made by a different user", func() {
				anotherUser := "anotherUser"
				s.addSetForTestingWithFlag(t, setName, transformer, file2, "--user", anotherUser)
				s.waitForStatusWithUser(setName, "\nStatus: complete", anotherUser, 10*time.Second)

				Convey("Remove will not remove the set name from the metadata", func() {
					exitCode, _ := s.runBinary(t, "remove", "--name", setName, "--path", file2)
					So(exitCode, ShouldEqual, 0)

					s.waitForStatus(setName, "Removal status: 1 / 1 objects removed", 5*time.Second)

					remoteFile := filepath.Join(remotePath, "file2")
					sets := getMetaValue(getRemoteMeta(remoteFile), transfer.MetaKeySets)
					setsSlice := strings.Split(sets, ",")
					So(setsSlice, ShouldContain, setName)
					So(setsSlice, ShouldContain, set.TrashPrefix+setName)
				})
			})

			Convey("Remove removes the dir from the set", func() {
				s.removePath(t, setName, dir1, 2)

				Convey("And status is updated accordingly", func() {
					exitCode, output := s.runBinary(t, "status", "--name", setName, "-d")
					So(exitCode, ShouldEqual, 0)
					So(output, ShouldContainSubstring, dir2)
					So(output, ShouldNotContainSubstring, dir1+"/")
					So(output, ShouldNotContainSubstring, dir1+" => ")

					s.confirmOutputContains(t, []string{"status", "--name", set.TrashPrefix + setName, "-d"},
						0, dir1)
				})

				Convey("And sets any files inside to be trashed", func() {
					remoteFile := filepath.Join(remotePath, "path/to/some/dir/file3")

					sets := getMetaValue(getRemoteMeta(remoteFile), transfer.MetaKeySets)

					setsSlice := strings.Split(sets, ",")
					So(setsSlice, ShouldNotContain, setName)
					So(setsSlice, ShouldContain, set.TrashPrefix+setName)
				})
			})

			Convey("Remove removes the dir from the set even if it no longer exists", func() {
				err = os.RemoveAll(dir1)
				So(err, ShouldBeNil)

				s.removePath(t, setName, dir1, 2)

				exitCode, output := s.runBinary(t, "status", "--name", setName, "-d")
				So(exitCode, ShouldEqual, 0)
				So(output, ShouldContainSubstring, dir2)
				So(output, ShouldNotContainSubstring, dir1+"/")
				So(output, ShouldNotContainSubstring, dir1+" => ")

				s.confirmOutputContains(t, []string{"status", "--name", set.TrashPrefix + setName, "-d"},
					0, dir1)
			})

			Convey("Remove removes an empty dir from the set", func() {
				s.removePath(t, setName, dir2, 1)

				exitCode, output := s.runBinary(t, "status", "--name", setName, "-d")
				So(exitCode, ShouldEqual, 0)
				So(output, ShouldContainSubstring, dir1)
				So(output, ShouldNotContainSubstring, dir2+"/")
				So(output, ShouldNotContainSubstring, dir2+" => ")

				s.confirmOutputContains(t, []string{"status", "--name", set.TrashPrefix + setName, "-d"},
					0, dir2)
			})

			Convey("Given an added set with a folder containing a nested folder", func() {
				dir3 := filepath.Join(dir1, "dir")
				err = os.MkdirAll(dir3, 0755)
				So(err, ShouldBeNil)

				file5 = filepath.Join(dir3, "file5")
				internal.CreateTestFile(t, file5, "some data3")

				setName = "nestedDirSet"

				s.addSetForTesting(t, setName, transformer, dir1)
				s.waitForStatus(setName, "\nStatus: complete", 5*time.Second)

				Convey("Remove removes the nested dir even though it wasnt specified in the set", func() {
					s.removePath(t, setName, dir3, 2)

					exitCode, output := s.runBinary(t, "status", "--name", setName, "-d")
					So(exitCode, ShouldEqual, 0)
					So(output, ShouldContainSubstring, dir1)
					So(output, ShouldNotContainSubstring, dir3+"/")

					s.confirmOutputContains(t, []string{"status", "--name", set.TrashPrefix + setName, "-d"},
						0, dir3)
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

				exitCode, output := s.runBinary(t, "status", "--name", setName, "-d")
				So(exitCode, ShouldEqual, 0)

				So(output, ShouldContainSubstring, file2)
				So(output, ShouldNotContainSubstring, file1)
				So(output, ShouldContainSubstring, dir2)
				So(output, ShouldNotContainSubstring, dir1+"/")
				So(output, ShouldNotContainSubstring, dir1+" => ")

				exitCode, output = s.runBinary(t, "status", "--name", set.TrashPrefix+setName, "-d")
				So(exitCode, ShouldEqual, 0)

				So(output, ShouldContainSubstring, file1)
				So(output, ShouldContainSubstring, dir1)
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
					fmt.Sprintf("%s\n%s\n%s", file2, file4, dir2))
				So(err, ShouldBeNil)

				exitCode, _ := s.runBinary(t, "remove", "--name", setName, "--items", tempTestFileOfPathsToRemove1.Name())

				So(exitCode, ShouldEqual, 0)

				exitCode, _ = s.runBinary(t, "remove", "--name", setName, "--items", tempTestFileOfPathsToRemove2.Name())

				So(exitCode, ShouldEqual, 0)

				err = s.Shutdown()
				So(err, ShouldBeNil)

				s.startServer()

				s.waitForStatus(setName, "Removal status: 6 / 6 objects removed", 5*time.Second)
			})

			Convey("And a new file added to a directory already in the set", func() {
				file5 = filepath.Join(dir1, "file5")
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

			Convey("If a file fails to be removed, the error is displayed on the file", func() {
				file1remote := filepath.Join(remotePath, "file1")

				removeFileFromIRODS(file1remote)

				exitCode, _ := s.runBinary(t, "remove", "--name", setName, "--path", file1)
				So(exitCode, ShouldEqual, 0)

				errorMsg := fmt.Sprintf("file does not exist [%s]", file1remote)

				s.waitForStatusWithFlags(setName, "failed to remove: "+errorMsg, 2*time.Second, "-d")

				Convey("And displays the error in set status if not fixed", func() {
					s.waitForStatus(setName, "Error: Error when removing: "+errorMsg, 20*time.Second)
				})

				Convey("And succeeds if issue is fixed during retries", func() {
					curUser, e := user.Current()
					So(e, ShouldBeNil)

					addFileToIRODS(file1, file1remote)
					addRemoteMeta(file1remote, transfer.MetaKeySets, setName)
					addRemoteMeta(file1remote, transfer.MetaKeyRequester, curUser.Username)

					s.waitForStatus(setName, "Removal status: 1 / 1 objects removed", 10*time.Second)
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

			checkIfRemoveFullyCompleted := func(setName, fileName string) {
				exitCode, _ := s.runBinary(t, "remove", "--name", setName, "--path", fileName)
				So(exitCode, ShouldEqual, 0)
				s.confirmOutputContains(t, []string{"status", "--name", setName, "-d"},
					0, "Removal status: 0 / 1 objects removed")
				s.waitForStatus(setName, "Removal status: 1 / 1 objects removed", 10*time.Second)

				s.confirmOutputDoesNotContain(t, []string{"status", "--name", set.TrashPrefix + setName, "-d"},
					0, fileName)
			}

			Convey("Given a set with a file that failed to upload", func() {
				dir3 := filepath.Join(path, "dir3")
				err = os.MkdirAll(dir3, userPerms)
				So(err, ShouldBeNil)

				file := filepath.Join(dir3, "file")
				internal.CreateTestFile(t, file, "some data1")

				err = os.Chmod(file, 0000)
				So(err, ShouldBeNil)

				setNameWithFailures := "setWithFailures"
				s.addSetForTesting(t, setNameWithFailures, transformer, dir3)
				s.waitForStatus(setNameWithFailures, "\nStatus: complete (but with failures", 10*time.Second)

				Convey("Remove will still work", func() {
					checkIfRemoveFullyCompleted(setNameWithFailures, file)
				})
			})

			Convey("If you sync to retry one of the uploaded files and it does not upload", func() {
				statusCmd := []string{"status", "--name", setName, "-d"}
				s.confirmOutputContains(t, statusCmd, 0, "file1\tuploaded")

				err = os.Remove(file1)
				So(err, ShouldBeNil)

				e := syscall.Mkfifo(file1, userPerms)
				So(e, ShouldBeNil)

				exitCode, _ := s.runBinary(t, "sync", "--name", setName)
				So(exitCode, ShouldEqual, 0)

				s.waitForStatus(setName, "\nStatus: complete", 10*time.Second)

				s.confirmOutputContains(t, statusCmd, 0, "file1\tabnormal")

				Convey("If you remove that file, its metadata in iRODS will be changed", func() {
					s.removePath(t, setName, file1, 1)

					remoteMeta := getRemoteMeta(filepath.Join(remotePath, "file1"))
					sets := getMetaValue(remoteMeta, transfer.MetaKeySets)
					setsSlice := strings.Split(sets, ",")

					So(setsSlice, ShouldNotContain, setName)
					So(setsSlice, ShouldContain, set.TrashPrefix+setName)
				})
			})

			Convey("Given a set with a missing file", func() {
				file := filepath.Join(path, "missing-file")
				internal.CreateTestFileOfLength(t, file, 1)

				setNameWithMissingFile := "setWithMissingFile"
				s.addSetForTesting(t, setNameWithMissingFile, transformer, path)

				e := os.Remove(file)
				So(e, ShouldBeNil)

				s.waitForStatus(setNameWithMissingFile, "\nStatus: complete", 10*time.Second)
				s.confirmOutputContains(t, []string{"status", "--name", setNameWithMissingFile, "-d"}, 0, file+"\tmissing")

				Convey("Remove will still work", func() {
					checkIfRemoveFullyCompleted(setNameWithMissingFile, file)
				})
			})

			Convey("Given a set with an abnormal file", func() {
				file := filepath.Join(path, "abnormal-file")
				e := syscall.Mkfifo(file, userPerms)
				So(e, ShouldBeNil)

				setNameWithAbnormalFile := "setWithAbnormalFile"
				s.addSetForTesting(t, setNameWithAbnormalFile, transformer, file)

				s.waitForStatus(setNameWithAbnormalFile, "\nStatus: complete", 10*time.Second)
				s.confirmOutputContains(t, []string{"status", "--name", setNameWithAbnormalFile, "-d"}, 0, file+"\tabnormal")

				Convey("Remove will still work", func() {
					checkIfRemoveFullyCompleted(setNameWithAbnormalFile, file)
				})
			})

			Convey("Remove --set removes all the files from the set "+
				"and moves it to the trash set, then deletes the set itself", func() {
				exitCode, _ := s.runBinary(t, "remove", "--name", setName, "--set")
				So(exitCode, ShouldEqual, 0)

				ctx, cancelFn := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancelFn()

				cmd := []string{"status", "--name", setName, "--url", s.url, "--cert", s.cert}

				status := retry.Do(ctx, func() error {
					clientCmd := exec.Command("./"+app, cmd...)
					clientCmd.Env = s.env

					output, errc := clientCmd.CombinedOutput()
					if errc != nil {
						return err
					}

					if strings.Contains(string(output), "set with that id does not exist") {
						return nil
					}

					return fmt.Errorf("set %s still exists", setName) //nolint:err113
				}, &retry.UntilNoError{}, &backoff.Backoff{
					Min:     10 * time.Millisecond,
					Max:     100 * time.Millisecond,
					Factor:  2,
					Sleeper: &btime.Sleeper{},
				}, "waiting for matching status")

				So(status.Err, ShouldBeNil)

				trashSetName := set.TrashPrefix + setName

				Convey("And changes the set metadata in iRODS to a .trash set", func() {
					sets := getMetaValue(getRemoteMeta(filepath.Join(remotePath, "file2")), transfer.MetaKeySets)
					setsSlice := strings.Split(sets, ",")

					So(setsSlice, ShouldContain, trashSetName)
					So(setsSlice, ShouldNotContain, setName)
				})
			})
		})

		Convey("And a set made by a non-admin user", func() {
			user := alternateUsername
			setName := "nonAdminSet"
			originalEnv := s.impersonateUser(t, user)

			s.addSetForTestingWithFlag(t, setName, transformer, path, "--user", user)

			Convey("If you trash files from the set", func() {
				exitCode, _ := s.runBinary(t, "remove", "--name", setName, "--path", path, "--user", user)
				So(exitCode, ShouldEqual, 0)

				removalStatus := "Removal status: 1 / 1 objects removed"

				s.waitForStatusWithUser(setName, removalStatus, user, 10*time.Second)

				trashSetName := set.TrashPrefix + setName

				Convey("Status will not display the trash files to the non-admin user", func() {
					s.confirmOutputDoesNotContain(t, []string{"status", "--user", user}, 0, trashSetName)
				})

				Convey("Status will not display the trash files to a non-admin user even with the name specified", func() {
					exitCode, _ = s.runBinaryWithNoLogging(t, "status", "--name", trashSetName, "--user", user)
					So(exitCode, ShouldEqual, 1)
				})

				Convey("Status will not allow the non-admin user to use the --trash Status flag", func() {
					exitCode, _ = s.runBinaryWithNoLogging(t, "status", "--name", setName, "--trash", "--user", user)
					So(exitCode, ShouldEqual, 1)
				})

				Convey("Status with --trash will display the trash files to an admin user", func() {
					s.env = originalEnv
					s.confirmOutputContains(t, []string{"status", "--trash", "--user", user}, 0, trashSetName)

					exitCodeR, output := s.runBinary(t, "status", "--name", setName, "--trash", "--user", user, "-d")
					So(exitCodeR, ShouldEqual, 0)
					So(output, ShouldContainSubstring, path)
				})

				Convey("List will work on the trash set for an admin", func() {
					s.env = originalEnv

					exitCode, _ = s.runBinary(t, "list", "--name", trashSetName, "--user", user)
					So(exitCode, ShouldEqual, 0)

					exitCode, _ = s.runBinary(t, "list", "--name", setName, "--trash", "--user", user)
					So(exitCode, ShouldEqual, 0)
				})

				Convey("List will not work on the trash set for a non-admin", func() {
					s.confirmOutputContains(t, []string{"list", "--name", trashSetName, "--user", user},
						1, server.ErrBadSet.Error())

					exitCode, _ = s.runBinaryWithNoLogging(t, "list", "--name", setName, "--trash", "--user", user)
					So(exitCode, ShouldEqual, 1)
				})
			})
		})
	})
}

func TestTrashRemove(t *testing.T) {
	resetIRODS()

	Convey("Given a server", t, func() {
		checkICommands(t, "ils", "imeta", "ichmod")

		s, remotePath := NewUploadingTestServer(t, false)

		path := t.TempDir()
		transformer := "prefix=" + path + ":" + remotePath
		timeout := 5 * time.Second

		Convey("And an invalid set name, trash returns an error", func() {
			invalidSetName := "invalid_name_set"

			s.confirmOutputContains(t, []string{"trash", "--remove", "--name", invalidSetName, "--path", path},
				1, fmt.Sprintf("set with that id does not exist [%s]", set.TrashPrefix+invalidSetName))
		})

		Convey("Trash won't work without --remove provided", func() {
			invalidSetName := "invalid_input_set"

			s.confirmOutputContains(t, []string{"trash", "--name", invalidSetName, "--path", path},
				1, cmd.ErrTrashRemove.Error())
		})

		Convey("Trash won't work with both --expired and --path provided", func() {
			invalidSetName := "too_many_options_set"

			s.confirmOutputContains(t, []string{"trash", "--remove", "--name", invalidSetName, "--path", path, "--expired"},
				1, cmd.ErrTrashItems.Error())
		})

		Convey("Trash won't work with both --all-expired and --name provided", func() {
			invalidSetName := "too_many_options_set"

			s.confirmOutputContains(t, []string{"trash", "--remove", "--name", invalidSetName, "--all-expired"},
				1, cmd.ErrTrashName.Error())
		})

		Convey("And a set with files and folders", func() {
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

			setName := "testTrashFiles1"
			trashSetName := set.TrashPrefix + setName

			resetIRODS()

			s.addSetForTestingWithItems(t, setName, transformer, tempTestFileOfPaths.Name())

			Convey("Trash remove will not work on sets without trashed files", func() {
				s.confirmOutputContains(t, []string{"trash", "--remove", "--name", setName, "--path", path},
					1, server.ErrBadSet.Error())
			})

			Convey("And if you remove one file from this set", func() {
				exitCode, _ := s.runBinary(t, "remove", "--name", setName, "--path", file1)
				So(exitCode, ShouldEqual, 0)

				Convey("You won't be able to trash remove a different file from this set", func() {
					s.confirmOutputContains(t, []string{"trash", "--remove", "--name", setName, "--path", file2},
						1, set.ErrPathNotInSet)
				})
			})

			Convey("And if you remove some files and folders from this set", func() {
				exitCode, _ := s.runBinary(t, "remove", "--name", setName, "--items", tempTestFileOfPaths.Name())
				So(exitCode, ShouldEqual, 0)

				s.waitForStatus(setName, "Removal status: 9 / 9 objects removed", timeout)

				Convey("Trash remove will permanently remove a file from the set", func() {
					output, erro := exec.Command("ils", remotePath).CombinedOutput()
					So(erro, ShouldBeNil)
					So(string(output), ShouldContainSubstring, "file2")

					s.trashRemovePath(t, setName, file2, 1)

					s.confirmOutputDoesNotContain(t, []string{"status", "--name", trashSetName, "-d"},
						0, file2)

					Convey("And from iRODS", func() {
						output, err = exec.Command("ils", remotePath).CombinedOutput()
						So(err, ShouldBeNil)
						So(string(output), ShouldNotContainSubstring, "file2")
					})
				})

				Convey("Trash remove removes the dir from the set", func() {
					output, errc := exec.Command("ils", "-r", remotePath).CombinedOutput()
					So(errc, ShouldBeNil)
					So(string(output), ShouldContainSubstring, "path/to/some/dir")
					So(string(output), ShouldContainSubstring, "file3\n")

					s.trashRemovePath(t, setName, dir1, 2)

					exitCode, outputStr := s.runBinary(t, "status", "--name", trashSetName, "-d")
					So(exitCode, ShouldEqual, 0)
					So(outputStr, ShouldContainSubstring, dir2)
					So(outputStr, ShouldNotContainSubstring, dir1+"/")
					So(outputStr, ShouldNotContainSubstring, dir1+" => ")

					Convey("And from iRODS", func() {
						output, err = exec.Command("ils", "-r", remotePath).CombinedOutput()
						So(err, ShouldBeNil)
						So(string(output), ShouldContainSubstring, "dir_not_removed\n")
						So(string(output), ShouldNotContainSubstring, "path/to/some/dir")
						So(string(output), ShouldNotContainSubstring, "file3\n")
					})
				})

				Convey("Trash remove removes the dir from the set even if it no longer exists", func() {
					err = os.RemoveAll(dir1)
					So(err, ShouldBeNil)

					s.trashRemovePath(t, setName, dir1, 2)

					exitCode, outputStr := s.runBinary(t, "status", "--name", trashSetName, "-d")
					So(exitCode, ShouldEqual, 0)
					So(outputStr, ShouldContainSubstring, dir2)
					So(outputStr, ShouldNotContainSubstring, dir1+"/")
					So(outputStr, ShouldNotContainSubstring, dir1+" => ")
				})

				Convey("Trash remove removes an empty dir from the set", func() {
					s.trashRemovePath(t, setName, dir2, 1)

					exitCode, outputStr := s.runBinary(t, "status", "--name", trashSetName, "-d")
					So(exitCode, ShouldEqual, 0)
					So(outputStr, ShouldContainSubstring, dir1)
					So(outputStr, ShouldNotContainSubstring, dir2+"/")
					So(outputStr, ShouldNotContainSubstring, dir2+" => ")
				})

				Convey("Trash remove takes a flag --items and removes all provided files and dirs from the set", func() {
					tempTestFileOfPathsToRemove, errt := os.CreateTemp(dir, "testFileSet")
					So(errt, ShouldBeNil)

					_, err = io.WriteString(tempTestFileOfPathsToRemove,
						fmt.Sprintf("%s\n%s", file1, dir1))
					So(err, ShouldBeNil)

					exitCode, _ := s.runBinary(t, "trash", "--remove", "--name", setName,
						"--items", tempTestFileOfPathsToRemove.Name())
					So(exitCode, ShouldEqual, 0)

					s.waitForStatus(trashSetName, "Removal status: 3 / 3 objects removed", timeout)

					exitCode, output := s.runBinary(t, "status", "--name", trashSetName, "-d")
					So(exitCode, ShouldEqual, 0)

					So(output, ShouldContainSubstring, file2)
					So(output, ShouldNotContainSubstring, file1)
					So(output, ShouldContainSubstring, dir2)
					So(output, ShouldNotContainSubstring, dir1+"/")
					So(output, ShouldNotContainSubstring, dir1+" => ")
				})

				Convey("Trash remove with --items still works as expected with duplicates", func() {
					tempTestFileOfPathsToRemove, errt := os.CreateTemp(dir, "testFileSet")
					So(errt, ShouldBeNil)

					_, err = io.WriteString(tempTestFileOfPathsToRemove,
						fmt.Sprintf("%s\n%s\n%s\n%s", file1, file1, dir1, dir1))
					So(err, ShouldBeNil)

					exitCode, _ := s.runBinary(t, "trash", "--remove", "--name", setName,
						"--items", tempTestFileOfPathsToRemove.Name())
					So(exitCode, ShouldEqual, 0)

					s.waitForStatus(trashSetName, "Removal status: 3 / 3 objects removed", timeout)
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

					exitCode, _ := s.runBinary(t, "trash", "--remove", "--name", setName,
						"--items", tempTestFileOfPathsToRemove1.Name())
					So(exitCode, ShouldEqual, 0)

					exitCode, _ = s.runBinary(t, "trash", "--remove", "--name", setName,
						"--items", tempTestFileOfPathsToRemove2.Name())
					So(exitCode, ShouldEqual, 0)

					err = s.Shutdown()
					So(err, ShouldBeNil)

					s.startServer()

					s.waitForStatus(trashSetName, "Removal status: 8 / 8 objects removed", timeout)
				})

				Convey("Trash remove with a hardlink removes both the hardlink file and inode file", func() {
					remoteInode := getMetaValue(getRemoteMeta(filepath.Join(remotePath, "link")), "ibackup:remotehardlink")

					_, err = exec.Command("ils", remoteInode).CombinedOutput()
					So(err, ShouldBeNil)

					s.trashRemovePath(t, setName, linkPath, 1)

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

					s.waitForStatus("testHardlinks", "\nStatus: complete", timeout)

					Convey("Removing a hardlink does not remove the inode file", func() {
						remoteInode := getMetaValue(getRemoteMeta(filepath.Join(remotePath, "link")), "ibackup:remotehardlink")

						_, err = exec.Command("ils", remoteInode).CombinedOutput()
						So(err, ShouldBeNil)

						s.trashRemovePath(t, setName, linkPath, 1)

						_, err = exec.Command("ils", remoteLink).CombinedOutput()
						So(err, ShouldNotBeNil)

						_, err = exec.Command("ils", remoteInode).CombinedOutput()
						So(err, ShouldBeNil)
					})
				})

				Convey("And if you trash remove a file nested in an otherwise empty dir", func() {
					s.trashRemovePath(t, setName, file3, 1)

					output, errc := exec.Command("ils", "-r", remotePath).CombinedOutput()
					So(errc, ShouldBeNil)
					So(string(output), ShouldNotContainSubstring, "path/to/some/dir")
					So(string(output), ShouldNotContainSubstring, "file3\n")

					Convey("You can trash remove its parent folder from the db", func() {
						s.confirmOutputContains(t, []string{"status", "--name", trashSetName, "-d"},
							0, dir1)

						s.trashRemovePath(t, setName, dir1, 1)

						exitCode, outputStr := s.runBinary(t, "status", "--name", trashSetName, "-d")
						So(exitCode, ShouldEqual, 0)
						So(outputStr, ShouldNotContainSubstring, dir1+"/")
						So(outputStr, ShouldNotContainSubstring, dir1+" => ")
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

					Convey("If you trash remove the file, instead, removes the metadata related to the set", func() {
						output := getRemoteMeta(filepath.Join(remotePath, "file1"))
						So(output, ShouldContainSubstring, trashSetName)
						So(output, ShouldContainSubstring, setName2)

						s.trashRemovePath(t, setName, file1, 1)

						Convey("The file is still in iRODS", func() {
							outputBytes, errc := exec.Command("ils", remotePath).CombinedOutput()
							So(errc, ShouldBeNil)
							So(string(outputBytes), ShouldContainSubstring, "file1")

							Convey("Metadata related to the set is removed", func() {
								output = getRemoteMeta(filepath.Join(remotePath, "file1"))
								So(output, ShouldNotContainSubstring, trashSetName)
								So(output, ShouldContainSubstring, setName2)

								Convey("Metadata related to the set's user is removed", func() {
									requesters := getMetaValue(getRemoteMeta(filepath.Join(remotePath, "file1")), transfer.MetaKeyRequester)
									So(requesters, ShouldNotContainSubstring, user.Username)
								})
							})
						})
					})

					Convey("Trash remove does not try and fail to remove the provided dir from iRODS", func() {
						output, errc := exec.Command("ils", "-r", remotePath).CombinedOutput()
						So(errc, ShouldBeNil)
						So(string(output), ShouldContainSubstring, "path/to/some/dir")

						s.trashRemovePath(t, setName, dir1, 2)

						output, errc = exec.Command("ils", "-r", remotePath).CombinedOutput()
						So(errc, ShouldBeNil)
						So(string(output), ShouldContainSubstring, "path/to/some/dir")
					})

					Convey("Trash remove on a file removes the user as a requester", func() {
						s.trashRemovePath(t, setName, file1, 1)

						requesters := getMetaValue(getRemoteMeta(filepath.Join(remotePath, "file1")), transfer.MetaKeyRequester)

						So(requesters, ShouldNotContainSubstring, user.Username)
					})

					Convey("And a second set with the same files added by the same user", func() {
						setName3 := "same_user_set"

						s.addSetForTestingWithItems(t, setName3, transformer, tempTestFileOfPaths.Name())

						Convey("Trash remove keeps the user as a requester", func() {
							s.trashRemovePath(t, setName, file1, 1)

							requesters := getMetaValue(getRemoteMeta(filepath.Join(remotePath, "file1")), transfer.MetaKeyRequester)

							So(requesters, ShouldContainSubstring, user.Username)
						})
					})
				})

				Convey("And a set with the same files and name added by a different user", func() {
					setName2 := setName
					username := "testUser"

					exitCode, _ := s.runBinary(t, "add", "--name", setName2, "--transformer",
						transformer, "--items", tempTestFileOfPaths.Name(), "--user", username)

					So(exitCode, ShouldEqual, 0)

					s.waitForStatusWithUser(setName2, "\nStatus: complete", username, 20*time.Second)

					Convey("If the different user also removes files and folders", func() {
						exitCode, _ := s.runBinary(t, "remove", "--name", setName, "--user", username,
							"--items", tempTestFileOfPaths.Name())
						So(exitCode, ShouldEqual, 0)

						s.waitForStatusWithUser(setName, "Removal status: 9 / 9 objects removed", username, timeout)

						Convey("If you trash remove a file from one set", func() {
							s.trashRemovePath(t, setName, file1, 1)

							Convey("The set name will still appear in the metadata", func() {
								sets := getMetaValue(getRemoteMeta(filepath.Join(remotePath, "file1")), transfer.MetaKeySets)
								So(sets, ShouldContainSubstring, trashSetName)
							})
						})
					})
				})

				Convey("If a file fails to be removed, the error is displayed on the file", func() {
					file1remote := filepath.Join(remotePath, "file1")

					curUser, e := user.Current()
					So(e, ShouldBeNil)

					usernameRE := regexp.MustCompile(`^[A-Za-z0-9_.-]+$`)
					So(usernameRE.MatchString(curUser.Username), ShouldBeTrue)

					_, err = exec.Command("ichmod", "read", curUser.Username, file1remote).CombinedOutput() //nolint:gosec
					So(err, ShouldBeNil)

					defer func() {
						exec.Command("ichmod", "own", curUser.Username, file1remote).CombinedOutput() //nolint:errcheck,gosec
					}()

					exitCode, _ := s.runBinary(t, "trash", "--remove", "--name", setName, "--path", file1)
					So(exitCode, ShouldEqual, 0)

					errorMsg := "CAT_NO_ACCESS_PERMISSION"

					s.waitForStatusWithFlags(trashSetName, errorMsg, 2*time.Second, "-d")

					Convey("And displays the error in trashed set status if not fixed", func() {
						statusMsg := "Error: Error when removing: remove operation failed: Failed to remove data object:"
						s.waitForStatus(trashSetName, statusMsg, 30*time.Second)
					})

					Convey("And succeeds if issue is fixed during retries", func() {
						_, err = exec.Command("ichmod", "own", curUser.Username, file1remote).CombinedOutput() //nolint:gosec
						So(err, ShouldBeNil)

						s.waitForStatus(trashSetName, "Removal status: 1 / 1 objects removed", 10*time.Second)
					})
				})
			})

			Convey("Given an added set with a folder containing a nested folder", func() {
				dir3 := filepath.Join(dir1, "dir")
				err = os.MkdirAll(dir3, 0755)
				So(err, ShouldBeNil)

				file5 := filepath.Join(dir3, "file5")
				internal.CreateTestFile(t, file5, "some data3")

				setName = "nestedDirSet"
				trashSetName = set.TrashPrefix + setName

				s.addSetForTesting(t, setName, transformer, dir1)
				s.waitForStatus(setName, "\nStatus: complete", timeout)

				Convey("With the parent folder and all children removed", func() {
					s.removePath(t, setName, dir1, 4)

					Convey("Trash remove on the parent folder submits itself and all children to be permanently removed", func() {
						exitCode, _ := s.runBinary(t, "trash", "--remove", "--name", setName, "--path", dir1)
						So(exitCode, ShouldEqual, 0)

						s.confirmOutputContains(t, []string{"status", "--name", trashSetName, "-d"},
							0, "Removal status: 0 / 4 objects removed")

						s.waitForStatus(trashSetName, "Removal status: 4 / 4 objects removed", timeout)
					})
				})
			})

			Convey("If remove fails to trash a file", func() {
				file1remote := filepath.Join(remotePath, "file1")
				removeFileFromIRODS(file1remote)

				exitCode, _ := s.runBinary(t, "remove", "--name", setName, "--path", file1)
				So(exitCode, ShouldEqual, 0)

				s.waitForStatusWithFlags(setName, "failed to remove: file does not exist", timeout, "-d")

				Convey("You cannot permanently remove that file", func() {
					exitCode, output := s.runBinaryWithNoLogging(t, "trash", "--remove", "--name", setName, "--path", file1)
					So(exitCode, ShouldEqual, 1)
					So(output, ShouldContainSubstring, set.ErrPathNotInSet)
				})
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
	SkipConvey("With a started server", t, func() {
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
			s.confirmOutputContains(t, []string{"edit", "--make-readonly", "--disable-readonly"},
				1, cmd.ErrInvalidEditRO.Error())
		})

		Convey("Given a transformer", func() {
			remotePath := os.Getenv("IBACKUP_TEST_COLLECTION")
			if remotePath == "" {
				SkipConvey("skipping iRODS backup test since IBACKUP_TEST_COLLECTION not set", func() {})

				return
			}

			path := t.TempDir()
			transformer := "prefix=" + path + ":" + remotePath

			Convey("And a set with the wrong transformer for the path", func() {
				setName := "testEditInvalidTransformer"
				file1 := filepath.Join(path, "file1")
				internal.CreateTestFileOfLength(t, file1, 1)

				exitCode, _ := s.runBinary(t, "add", "--path", file1,
					"--name", setName, "--transformer", "humgen")
				So(exitCode, ShouldEqual, 0)

				s.confirmOutputContains(t, []string{"status", "--name", setName}, 0, "your transformer didn't work")

				Convey("You can edit the set to fix the transformer", func() {
					exitCode, _ := s.runBinary(t, "edit", "--name", setName, "--transformer", transformer)
					So(exitCode, ShouldEqual, 0)

					s.confirmOutputDoesNotContain(t, []string{"status", "--name", setName}, 0, "your transformer didn't work")
				})

				Convey("You cannot edit the set to have an invalid transformer", func() {
					exitCode, output := s.runBinaryWithNoLogging(t, "edit", "--name", setName, "--transformer", "badTransformer")
					So(exitCode, ShouldEqual, 1)
					So(output, ShouldContainSubstring, "invalid transformer")
				})
			})

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

					Convey("You can re-enable monitoring removals", func() {
						exitCode, _ = s.runBinary(t, "edit", "--name", setName, "--monitor-removals")
						So(exitCode, ShouldEqual, 0)

						s.confirmOutputContains(t, []string{"status", "--name", setName}, 0, "Monitored (with removals): 1d;")
					})

					Convey("You can't re-enable monitoring removal without enabling monitoring", func() {
						exitCode, _ := s.runBinary(t, "edit", "--name", setName, "--stop-monitor")
						So(exitCode, ShouldEqual, 0)

						s.confirmOutputContains(t, []string{"status", "--name", setName}, 0, "Monitored: false;")

						s.confirmOutputContains(t, []string{"edit", "--name", setName, "--monitor-removals"}, 1,
							cmd.ErrInvalidEditMonitorRemovalsUnmonitored.Error())

						exitCode, _ = s.runBinary(t, "edit", "--name", setName, "--monitor", "1d", "--monitor-removals")
						So(exitCode, ShouldEqual, 0)

						s.confirmOutputContains(t, []string{"status", "--name", setName}, 0, "Monitored (with removals): 1d;")
					})
				})

				Convey("You cannot use both --monitor-removals and --stop-monitor-removals together", func() {
					s.confirmOutputContains(t, []string{"edit", "--name", setName, "--monitor-removals", "1h",
						"--stop-monitor-removals"}, 1,
						cmd.ErrInvalidEditMonitorRemovals.Error())
				})
			})

			Convey("And a non-monitored set", func() {
				setName := "nonMonitoredSet"
				s.addSetForTesting(t, setName, transformer, path)

				s.confirmOutputContains(t, []string{"status", "--name", setName}, 0, "Monitored: false;")

				Convey("You cannot use both --monitor and --stop-monitor together", func() {
					s.confirmOutputContains(t, []string{"edit", "--name", setName, "--monitor", "1h", "--stop-monitor"}, 1,
						cmd.ErrInvalidEditMonitor.Error())
				})

				Convey("You can enable monitoring via edit", func() {
					exitCode, _ := s.runBinary(t, "edit", "--name", setName, "--monitor", "2w")
					So(exitCode, ShouldEqual, 0)

					s.confirmOutputContains(t, []string{"status", "--name", setName}, 0, "Monitored: 2w;")

					Convey("You can update monitoring duration via edit", func() {
						exitCode, _ := s.runBinary(t, "edit", "--name", setName, "--monitor", "1d")
						So(exitCode, ShouldEqual, 0)

						s.confirmOutputContains(t, []string{"status", "--name", setName}, 0, "Monitored: 1d;")
						exitCode2, _ := s.runBinary(t, "edit", "--name", setName, "--monitor", "3d")
						So(exitCode2, ShouldEqual, 0)

						s.confirmOutputContains(t, []string{"status", "--name", setName}, 0, "Monitored: 3d;")
					})
				})

				Convey("You can't set monitor duration below 1h", func() {
					exitCode, stderr := s.runBinary(t, "edit", "--name", setName, "--monitor", "30m")
					So(exitCode, ShouldNotEqual, 0)
					So(stderr, ShouldContainSubstring, "monitor duration must be 1h0m0s or more, not 30m0s")
				})

				Convey("You can't set monitor duration to an invalid string", func() {
					exitCode, stderr := s.runBinary(t, "edit", "--name", setName, "--monitor", "foobar")
					So(exitCode, ShouldNotEqual, 0)
					So(stderr, ShouldContainSubstring, "invalid monitor duration: time: invalid duration \"foobar\"")
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

					Convey("You can re-enable archive mode", func() {
						exitCode, _ := s.runBinary(t, "edit", "--name", setName, "--archive")
						So(exitCode, ShouldEqual, 0)

						s.confirmOutputContains(t, []string{"status", "--name", setName}, 0, "Archive: true\n")

						s.confirmOutputContains(t, []string{"edit", "--name", setName, "--archive", "--stop-archiving"}, 1,
							cmd.ErrInvalidEditArchive.Error())
					})
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

					Convey("And you can no longer sync it", func() {
						s.confirmOutputContains(t, []string{"sync", "--name", setName}, 1,
							set.ErrSetIsNotWritable)
					})

					Convey("And admin can make it writable", func() {
						exitCode, _ := s.runBinary(t, "edit", "--name", setName, "--disable-readonly")
						So(exitCode, ShouldEqual, 0)
					})
				})
			})

			Convey("And a set with a file", func() {
				file1 := filepath.Join(path, "file1")
				internal.CreateTestFileOfLength(t, file1, 1)

				setName := "setWithAFile"
				s.addSetForTesting(t, setName, transformer, path)

				Convey("If the set is not complete, you cannot change the transformer", func() {
					exitCode, output := s.runBinaryWithNoLogging(t, "edit", "--name", setName, "--transformer", "humgen")
					So(exitCode, ShouldEqual, 1)
					So(output, ShouldContainSubstring, set.ErrTransformerInUse)
				})
			})

			Convey("And with multiple sets", func() {
				setName := "hideSet"
				visibleSet1 := "visibleSet1"
				visibleSet2 := "visibleSet2"

				s.addSetForTesting(t, setName, transformer, path)
				s.addSetForTesting(t, visibleSet1, transformer, path)
				s.addSetForTesting(t, visibleSet2, transformer, path)

				exitCode, _ := s.runBinary(t, "edit", "--name", setName, "--hide")
				So(exitCode, ShouldEqual, 0)

				Convey("`status` shows only visible sets", func() {
					exitCode, statusOutput := s.runBinary(t, "status")
					So(exitCode, ShouldEqual, 0)

					So(statusOutput, ShouldNotContainSubstring, setName)
					So(statusOutput, ShouldContainSubstring, visibleSet1)
					So(statusOutput, ShouldContainSubstring, visibleSet2)
				})

				Convey("`status --show-hidden` shows all sets", func() {
					exitCode, statusOutput := s.runBinary(t, "status", "--show-hidden")
					So(exitCode, ShouldEqual, 0)

					So(statusOutput, ShouldContainSubstring, visibleSet1)
					So(statusOutput, ShouldContainSubstring, visibleSet2)
					So(statusOutput, ShouldContainSubstring, setName)
				})

				Convey("Unhiding brings it back to normal view", func() {
					exitCode, _ := s.runBinary(t, "edit", "--name", setName, "--unhide")
					So(exitCode, ShouldEqual, 0)

					exitCode, statusOutput := s.runBinary(t, "status")
					So(exitCode, ShouldEqual, 0)
					So(statusOutput, ShouldContainSubstring, setName)
				})

				Convey("You cannot both --hide and --unhide a set", func() {
					s.confirmOutputContains(t, []string{"edit", "--name", setName, "--hide", "--unhide"}, 1,
						cmd.ErrInvalidEditHide.Error())
				})
			})

			Convey("And a read-only set made by a different user", func() {
				user := alternateUsername
				setName := "rootSet"
				originalEnv := s.impersonateUser(t, user)

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

			Convey("And a set with a description", func() {
				setName := "descSet"
				s.addSetForTestingWithFlags(t, setName, transformer, "--path", path, "--description", "desc1")

				s.confirmOutputContains(t, []string{"status", "--name", setName}, 0, "Description: desc1\n")

				Convey("You can edit the description", func() {
					exitCode, _ := s.runBinary(t, "edit", "--name", setName, "--description", "desc2")
					So(exitCode, ShouldEqual, 0)

					s.confirmOutputContains(t, []string{"status", "--name", setName}, 0, "Description: desc2\n")
				})
			})

			Convey("And a set with the default reason, review and removal date", func() {
				setName := "reasonSet"
				reviewDate := fmt.Sprintf("%d-01-01", time.Now().Year()+9)
				removalDate := fmt.Sprintf("%d-01-01", time.Now().Year()+10)

				s.addSetForTestingWithFlags(t, setName, transformer, "--path", path)
				exitCode, statusOutput := s.runBinary(t, "status", "--name", setName)
				So(exitCode, ShouldEqual, 0)

				So(statusOutput, ShouldContainSubstring, "Reason: backup\n")
				So(statusOutput, ShouldContainSubstring, "Review date: ")
				So(statusOutput, ShouldContainSubstring, "Removal date: ")

				Convey("You can edit the reason, review and removal date along with arbitrary metadata", func() {
					exitCode, _ := s.runBinary(t, "edit", "--name", setName, "--reason", "archive",
						"--review", reviewDate, "--removal-date", removalDate,
						"--metadata", "foo=bar;baz=qux")
					So(exitCode, ShouldEqual, 0)

					exitCode, statusOutput := s.runBinary(t, "status", "--name", setName)
					So(exitCode, ShouldEqual, 0)

					So(statusOutput, ShouldContainSubstring, "Reason: archive\n")
					So(statusOutput, ShouldContainSubstring, "Review date: "+reviewDate+"\n")
					So(statusOutput, ShouldContainSubstring, "Removal date: "+removalDate+"\n")
					So(statusOutput, ShouldContainSubstring, "User metadata: baz=qux;foo=bar\n")
				})

				Convey("You can edit just the removal date", func() {
					exitCode, _ := s.runBinary(t, "edit", "--name", setName, "--removal-date", removalDate)
					So(exitCode, ShouldEqual, 0)

					exitCode, statusOutput := s.runBinary(t, "status", "--name", setName)
					So(exitCode, ShouldEqual, 0)

					So(statusOutput, ShouldContainSubstring, "Reason: backup\n")
					So(statusOutput, ShouldContainSubstring, fmt.Sprintf("Removal date: %s\n", removalDate))

					Convey("Then edit the reason", func() {
						exitCode, _ := s.runBinary(t, "edit", "--name", setName, "--reason", "archive")
						So(exitCode, ShouldEqual, 0)

						exitCode, statusOutput := s.runBinary(t, "status", "--name", setName)
						So(exitCode, ShouldEqual, 0)

						So(statusOutput, ShouldContainSubstring, "Reason: archive\n")
						So(statusOutput, ShouldContainSubstring, "Removal date: "+removalDate+"\n")
					})
				})

				Convey("You can edit just the reason", func() {
					exitCode, _ := s.runBinary(t, "edit", "--name", setName, "--reason", "quarantine")
					So(exitCode, ShouldEqual, 0)

					s.confirmOutputContains(t, []string{"status", "--name", setName}, 0, "Reason: quarantine\n")

					Convey("Then edit the removal date", func() {
						exitCode, _ := s.runBinary(t, "edit", "--name", setName, "--removal-date", removalDate)
						So(exitCode, ShouldEqual, 0)

						exitCode, statusOutput := s.runBinary(t, "status", "--name", setName)
						So(exitCode, ShouldEqual, 0)

						So(statusOutput, ShouldContainSubstring, "Reason: quarantine\n")
						So(statusOutput, ShouldContainSubstring, "Removal date: "+removalDate+"\n")
					})
				})
			})
		})
	})

	Convey("With a started uploading server", t, func() {
		s, remotePath := NewUploadingTestServer(t, false)
		So(s, ShouldNotBeNil)

		path := t.TempDir()
		transformer := "prefix=" + path + ":" + remotePath

		resetIRODS()

		timeout := 10 * time.Second

		Convey("And some files", func() {
			setDir1 := filepath.Join(path, "dir1")
			err := os.Mkdir(setDir1, userPerms)
			So(err, ShouldBeNil)

			setDir2 := filepath.Join(path, "dir2")
			err = os.Mkdir(setDir2, userPerms)
			So(err, ShouldBeNil)

			setFile1 := filepath.Join(setDir1, "file1")
			internal.CreateTestFileOfLength(t, setFile1, 1)

			setFile2 := filepath.Join(setDir2, "file2")
			internal.CreateTestFileOfLength(t, setFile2, 1)

			addFileAndCheckOrphan := func(setName, addFile, orphanFile string) {
				exitCode, _ := s.runBinary(t, "edit", "--name", setName, "--add", addFile)
				So(exitCode, ShouldEqual, 0)

				s.waitForStatus(setName, "Status: complete", timeout)
				s.waitForStatus(setName, "Uploaded: 1", timeout)

				exitCode, output := s.runBinaryWithNoLogging(t, "status", "--name", setName, "-d")
				So(exitCode, ShouldEqual, 0)

				So(output, ShouldContainSubstring, "Num files: 2")
				So(output, ShouldContainSubstring, "Uploaded: 1;")
				So(output, ShouldContainSubstring, "Orphaned: 1;")
				So(output, ShouldContainSubstring, "Size (total/recently uploaded/recently removed): 2 B / 1 B / 0 B")
				So(output, ShouldContainSubstring, orphanFile+"\torphaned\t1 B")
				So(output, ShouldContainSubstring, addFile+"\tuploaded")
			}

			Convey("And a set with a specified file", func() {
				setName := "testSet"

				s.addSetForTesting(t, setName, transformer, setFile1)
				s.waitForStatus(setName, "Status: complete", timeout)

				Convey("You can add the parent folder for the file and there's no duplication", func() {
					exitCode, _ := s.runBinary(t, "edit", "--name", setName, "--add", setDir1)
					So(exitCode, ShouldEqual, 0)

					s.waitForStatus(setName, "Status: complete", timeout)

					exitCode, output := s.runBinaryWithNoLogging(t, "status", "--name", setName, "-d")
					So(exitCode, ShouldEqual, 0)

					So(output, ShouldContainSubstring, "Num files: 1")
					So(output, ShouldContainSubstring, setFile1)
				})
			})

			Convey("And a set with a folder containing a file", func() {
				setName := "testSet"

				s.addSetForTesting(t, setName, transformer, setDir1)
				s.waitForStatus(setName, "Status: complete", timeout)

				Convey("If there are uploaded files, you cannot change the transformer", func() {
					exitCode, output := s.runBinaryWithNoLogging(t, "edit", "--name", setName, "--transformer", "humgen")
					So(exitCode, ShouldEqual, 1)
					So(output, ShouldContainSubstring, set.ErrTransformerAlreadyUsed)
				})

				Convey("You cannot add a non-existing file to a set", func() {
					exitCode, output := s.runBinaryWithNoLogging(t, "edit", "--name", setName, "--add", "badFile")
					So(exitCode, ShouldEqual, 1)
					So(output, ShouldContainSubstring, "badFile: no such file or directory")
				})

				Convey("You can add a file that is already in the set", func() {
					exitCode, _ := s.runBinary(t, "edit", "--name", setName, "--add", setFile1)
					So(exitCode, ShouldEqual, 0)

					s.waitForStatus(setName, "Status: complete", timeout)

					exitCode, output := s.runBinaryWithNoLogging(t, "status", "--name", setName, "-d")
					So(exitCode, ShouldEqual, 0)

					So(output, ShouldContainSubstring, "Num files: 1")
					So(output, ShouldContainSubstring, setFile1+"\tskipped")
				})

				Convey("And a file of paths", func() {
					setFile3 := filepath.Join(setDir1, "file3")
					internal.CreateTestFileOfLength(t, setFile3, 1)

					setFile4 := filepath.Join(setDir1, "file4")
					internal.CreateTestFileOfLength(t, setFile4, 1)

					tempTestFile, errc := os.CreateTemp(path, "testFileSet")
					So(errc, ShouldBeNil)

					_, err = io.WriteString(tempTestFile, setDir2+"\n"+setFile3+"\n"+setFile4+"\n")
					So(err, ShouldBeNil)

					Convey("Admins can add multiple files at once", func() {
						exitCode, _ := s.runBinary(t, "edit", "--name", setName, "--add-items", tempTestFile.Name())
						So(exitCode, ShouldEqual, 0)

						exitCode, output := s.runBinaryWithNoLogging(t, "status", "--name", setName, "-d")
						So(exitCode, ShouldEqual, 0)

						So(output, ShouldContainSubstring, "Num files: 4")
						So(output, ShouldContainSubstring, setFile3)
					})

					Convey("And a set made by a non-admin user", func() {
						user := alternateUsername
						nonAdminSetName := "nonAdminSet"
						_ = s.impersonateUser(t, user)

						s.addSetForTestingWithFlag(t, nonAdminSetName, transformer, setFile1, "--user", user)

						Convey("Non admins cannot use the --list flag", func() {
							exitCode, _ := s.runBinaryWithNoLogging(t, "edit", "--name", nonAdminSetName,
								"--user", user, "--add-items", tempTestFile.Name())
							So(exitCode, ShouldEqual, 1)
						})
					})
				})

				SkipConvey("You can add a file back to the set after removing it", func() {
					exitCode, _ := s.runBinary(t, "remove", "--name", setName, "--path", setFile1)
					So(exitCode, ShouldEqual, 0)

					s.waitForStatus(setName, "Removal status: 1 / 1 objects removed", 5*time.Second)

					exitCode, _ = s.runBinary(t, "edit", "--name", setName, "--add", setFile1)
					So(exitCode, ShouldEqual, 0)

					s.waitForStatus(setName, "Status: complete", timeout)

					exitCode, output := s.runBinaryWithNoLogging(t, "status", "--name", setName, "-d")
					So(exitCode, ShouldEqual, 0)

					So(output, ShouldContainSubstring, "Num files: 1")
					So(output, ShouldContainSubstring, setFile1+"\tuploaded")
				})

				Convey("You can add another file to this set", func() {
					exitCode, _ := s.runBinary(t, "edit", "--name", setName, "--add", setFile2)
					So(exitCode, ShouldEqual, 0)

					s.waitForStatus(setName, "Status: complete", timeout)

					exitCode, output := s.runBinaryWithNoLogging(t, "status", "--name", setName, "-d")
					So(exitCode, ShouldEqual, 0)

					So(output, ShouldContainSubstring, "Num files: 2")
					So(output, ShouldContainSubstring, setFile1)
					So(output, ShouldContainSubstring, setFile2+"\tuploaded")
				})

				Convey("You can add a newly created nested file to this set", func() {
					setFile3 := filepath.Join(setDir1, "file3")
					internal.CreateTestFileOfLength(t, setFile3, 1)

					exitCode, _ := s.runBinary(t, "edit", "--name", setName, "--add", setFile3)
					So(exitCode, ShouldEqual, 0)

					s.waitForStatus(setName, "Status: complete", timeout)

					exitCode, output := s.runBinaryWithNoLogging(t, "status", "--name", setName, "-d")
					So(exitCode, ShouldEqual, 0)

					So(output, ShouldContainSubstring, "Num files: 2")
					So(output, ShouldContainSubstring, setFile1)
					So(output, ShouldContainSubstring, setFile3+"\tuploaded")
				})

				Convey("You can add another file even if the first one no longer exists", func() {
					err = os.Remove(setFile1)
					So(err, ShouldBeNil)

					addFileAndCheckOrphan(setName, setFile2, setFile1)
				})

				Convey("You can add a folder to this set", func() {
					exitCode, _ := s.runBinary(t, "edit", "--name", setName, "--add", setDir2)
					So(exitCode, ShouldEqual, 0)

					s.waitForStatus(setName, "Status: complete", timeout)

					exitCode, output := s.runBinaryWithNoLogging(t, "status", "--name", setName, "-d")
					So(exitCode, ShouldEqual, 0)

					So(output, ShouldContainSubstring, "Num files: 2")
					So(output, ShouldContainSubstring, setFile1)
					So(output, ShouldContainSubstring, setFile2+"\tuploaded")
					So(output, ShouldContainSubstring, setDir2+" =>")
				})

				Convey("If you remove a file from a set, it won't be added when you add it's parent folder to a set", func() {
					exitCode, _ := s.runBinary(t, "remove", "--name", setName, "--path", setFile1)
					So(exitCode, ShouldEqual, 0)

					s.waitForStatus(setName, "Removal status: 1 / 1 objects removed", 5*time.Second)

					exitCode, _ = s.runBinary(t, "edit", "--name", setName, "--add", setDir1)
					So(exitCode, ShouldEqual, 0)

					s.waitForStatus(setName, "Status: complete", timeout)

					exitCode, output := s.runBinaryWithNoLogging(t, "status", "--name", setName, "-d")
					So(exitCode, ShouldEqual, 0)

					So(output, ShouldNotContainSubstring, setFile1)
				})

				Convey("If the set has failed removals, you can still add new files to the set", func() {
					removeFileFromIRODS(filepath.Join(filepath.Join(remotePath, "dir1"), "file1"))

					exitCode, _ := s.runBinary(t, "remove", "--name", setName, "--path", setFile1)

					So(exitCode, ShouldEqual, 0)

					s.waitForStatus(setName, "Error: Error when removing:", 30*time.Second)

					exitCode, _ = s.runBinary(t, "edit", "--name", setName, "--add", setFile2)
					So(exitCode, ShouldEqual, 0)

					s.waitForStatus(setName, "Status: complete", timeout)

					s.confirmOutputContains(t, []string{"status", "--name", setName, "-d"}, 0, setFile2)
				})
			})

			Convey("And a set with discovered file", func() {
				setName := "TestDiscoveredFiles"

				s.addSetForTesting(t, setName, transformer, setDir1)
				s.waitForStatus(setName, "Status: complete", timeout)

				SkipConvey("You can add a file back to the set after removing its parent folder", func() {
					exitCode, _ := s.runBinary(t, "remove", "--name", setName, "--path", setDir1)
					So(exitCode, ShouldEqual, 0)

					s.waitForStatus(setName, "Removal status: 2 / 2 objects removed", 5*time.Second)

					exitCode, _ = s.runBinary(t, "edit", "--name", setName, "--add", setDir1)
					So(exitCode, ShouldEqual, 0)

					s.waitForStatus(setName, "Status: complete", timeout)

					exitCode, output := s.runBinaryWithNoLogging(t, "status", "--name", setName, "-d")
					So(exitCode, ShouldEqual, 0)

					So(output, ShouldContainSubstring, "Num files: 1")
					So(output, ShouldContainSubstring, setFile1+"\tuploaded")
				})

				Convey("If you remove a folder and then add it back with children", func() {
					exitCode, _ := s.runBinary(t, "remove", "--name", setName, "--path", setDir1)
					So(exitCode, ShouldEqual, 0)

					s.waitForStatus(setName, "Removal status: 2 / 2 objects removed", 5*time.Second)

					setDir3 := filepath.Join(setDir1, "dir3")
					err = os.Mkdir(setDir3, userPerms)
					So(err, ShouldBeNil)

					setFile3 := filepath.Join(setDir3, "file3")
					internal.CreateTestFileOfLength(t, setFile3, 3)

					setFile4 := filepath.Join(setDir3, "file4")
					internal.CreateTestFileOfLength(t, setFile4, 3)

					exitCode, _ = s.runBinary(t, "edit", "--name", setName, "--add", setDir1)
					So(exitCode, ShouldEqual, 0)

					s.waitForStatus(setName, "Status: complete", timeout)

					exitCode, output := s.runBinaryWithNoLogging(t, "status", "--name", setName, "-d")
					So(exitCode, ShouldEqual, 0)

					So(output, ShouldContainSubstring, "Num files: 3")

					Convey("And if you remove a child folder, you will not discover files inside that folder", func() {
						exitCode, _ = s.runBinary(t, "remove", "--name", setName, "--path", setDir3)
						So(exitCode, ShouldEqual, 0)

						s.waitForStatus(setName, "Removal status: 3 / 3 objects removed", 5*time.Second)

						exitCode, _ = s.runBinary(t, "edit", "--name", setName, "--add", setDir1)
						So(exitCode, ShouldEqual, 0)

						s.waitForStatus(setName, "Status: complete", timeout)

						exitCode, output = s.runBinaryWithNoLogging(t, "status", "--name", setName, "-d")
						So(exitCode, ShouldEqual, 0)

						So(output, ShouldNotContainSubstring, setFile3)
					})
				})

				Convey("You can add another file even if the initial folder no longer exists", func() {
					err = os.RemoveAll(setDir1)
					So(err, ShouldBeNil)

					addFileAndCheckOrphan(setName, setFile2, setFile1)
				})
			})
		})

		Convey("And a set with a lot of files", func() {
			setName := "bigSet"
			numFiles := 10

			setDir := filepath.Join(path, "bigSetDir")
			err := os.Mkdir(setDir, userPerms)
			So(err, ShouldBeNil)

			for i := range numFiles {
				file := filepath.Join(setDir, fmt.Sprintf("file-%d", i))
				internal.CreateTestFileOfLength(t, file, 1)
			}

			s.addSetForTesting(t, setName, transformer, setDir)
			s.waitForStatus(setName, "Status: complete", 10*time.Second)

			Convey("You cannot add files to the set during removals", func() {
				exitCode, _ := s.runBinary(t, "remove", "--name", setName, "--path", setDir)
				So(exitCode, ShouldEqual, 0)

				exitCode, output := s.runBinaryWithNoLogging(t, "edit", "--name", setName, "--add", setDir)
				So(exitCode, ShouldEqual, 1)
				So(output, ShouldContainSubstring, set.ErrPendingRemovals)
			})
		})
	})
}

func TestSync(t *testing.T) {
	Convey("With a started uploading server", t, func() {
		s, remotePath := NewUploadingTestServer(t, false)
		So(s, ShouldNotBeNil)

		path := t.TempDir()
		transformer := "prefix=" + path + ":" + remotePath

		resetIRODS()

		timeout := 10 * time.Second

		Convey("sync requires --name", func() {
			s.confirmOutputContains(t, []string{"sync"}, 1,
				"Error: required flag(s) \"name\" not set")
		})

		Convey("And a set with some uploaded files, one of which is then deleted locally and "+
			"a new file added locally", func() {
			setDir := filepath.Join(path, "dir1")
			err := os.Mkdir(setDir, userPerms)
			So(err, ShouldBeNil)

			setFile1 := filepath.Join(setDir, "file1")
			internal.CreateTestFileOfLength(t, setFile1, 1)

			setFileToBeDeleted := filepath.Join(setDir, "file_to_be_deleted")
			internal.CreateTestFileOfLength(t, setFileToBeDeleted, 1)

			setName := "discoveredSet"

			s.addSetForTesting(t, setName, transformer, setDir)
			s.waitForStatus(setName, "Status: complete", timeout)

			exitCode, output := s.runBinaryWithNoLogging(t, "status", "--name", setName, "-d")
			So(exitCode, ShouldEqual, 0)

			So(output, ShouldContainSubstring, "Num files: 2")
			So(output, ShouldContainSubstring, "Uploaded: 2;")
			So(output, ShouldNotContainSubstring, "Orphaned: 1;")
			So(output, ShouldContainSubstring, "Size (total/recently uploaded/recently removed): 2 B / 2 B / 0 B")
			So(output, ShouldContainSubstring, setFileToBeDeleted)

			err = os.Remove(setFileToBeDeleted)
			So(err, ShouldBeNil)

			setFile2 := filepath.Join(setDir, "file2")
			internal.CreateTestFileOfLength(t, setFile2, 1)

			Convey("sync requires a valid --name", func() {
				s.confirmOutputContains(t, []string{"sync", "--name", "invalid"}, 1,
					server.ErrBadSet.Error())
			})

			Convey("sync without --delete uploads new files without deleting locally deleted ones", func() {
				exitCode, _ = s.runBinary(t, "sync", "--name", setName)
				So(exitCode, ShouldEqual, 0)

				s.waitForStatus(setName, "Status: complete", timeout)

				exitCode, output = s.runBinaryWithNoLogging(t, "status", "--name", setName, "-d")
				So(exitCode, ShouldEqual, 0)

				So(output, ShouldContainSubstring, "Num files: 3")
				So(output, ShouldContainSubstring, "Uploaded: 1;")
				So(output, ShouldContainSubstring, "Skipped: 1;")
				So(output, ShouldContainSubstring, "Orphaned: 1;")
				So(output, ShouldContainSubstring, "Size (total/recently uploaded/recently removed): 3 B / 1 B / 0 B")
				So(output, ShouldContainSubstring, setFileToBeDeleted)
			})

			Convey("sync with --delete uploads new files and deletes locally deleted ones", func() {
				exitCode, _ = s.runBinary(t, "sync", "--name", setName, "--delete")
				So(exitCode, ShouldEqual, 0)

				s.waitForStatus(setName, "Removal status: 1 / 1 objects removed", timeout)
				s.waitForStatus(setName, "Status: complete", timeout)

				exitCode, output = s.runBinaryWithNoLogging(t, "status", "--name", setName, "-d")
				So(exitCode, ShouldEqual, 0)

				So(output, ShouldContainSubstring, "Num files: 2")
				So(output, ShouldContainSubstring, "Uploaded: 1;")
				So(output, ShouldContainSubstring, "Skipped: 1;")
				So(output, ShouldNotContainSubstring, "Orphaned: 1;")
				So(output, ShouldContainSubstring, "Size (total/recently uploaded/recently removed): 2 B / 1 B / 1 B")
				So(output, ShouldNotContainSubstring, setFileToBeDeleted)
			})
		})
	})
}

func TestRetry(t *testing.T) {
	Convey("With a started uploading server", t, func() {
		s, remotePath := NewUploadingTestServer(t, false)
		So(s, ShouldNotBeNil)

		path := t.TempDir()
		transformer := "prefix=" + path + ":" + remotePath

		resetIRODS()

		timeout := 30 * time.Second

		Convey("retry requires --name", func() {
			s.confirmOutputContains(t, []string{"retry"}, 1,
				"Error: required flag(s) \"name\" not set")
		})

		Convey("And a set with a file that fails to upload", func() {
			setDir := filepath.Join(path, "dir")
			err := os.Mkdir(setDir, userPerms)
			So(err, ShouldBeNil)

			setFile1 := filepath.Join(setDir, "file1")
			internal.CreateTestFileOfLength(t, setFile1, 1)

			setFileToBeRetried := filepath.Join(setDir, "file_to_be_retried")
			internal.CreateTestFileOfLength(t, setFileToBeRetried, 1)

			err = os.Chmod(setFileToBeRetried, 0)
			So(err, ShouldBeNil)

			setName := "failTest"
			s.addSetForTesting(t, setName, transformer, path)

			s.waitForStatus(setName, "0 reserved to be worked on; 1 failed", timeout)
			s.waitForStatus(setName, "Status: complete", timeout)

			exitCode, output := s.runBinaryWithNoLogging(t, "status", "--name", setName, "-d")
			So(exitCode, ShouldEqual, 0)

			So(output, ShouldContainSubstring, "Num files: 2")
			So(output, ShouldContainSubstring, "Uploaded: 1;")
			So(output, ShouldContainSubstring, "Failed: 1;")

			Convey("retry uploads the previously failed file after fixing the issue", func() {
				err = os.Chmod(setFileToBeRetried, 0444)
				So(err, ShouldBeNil)

				exitCode, _ = s.runBinary(t, "retry", "--name", setName)
				So(exitCode, ShouldEqual, 0)

				s.waitForStatus(setName, "Status: complete\n", timeout)

				exitCode, output = s.runBinaryWithNoLogging(t, "status", "--name", setName, "-d")
				So(exitCode, ShouldEqual, 0)

				So(output, ShouldContainSubstring, "Num files: 2")
				So(output, ShouldContainSubstring, "Uploaded: 2;")
				So(output, ShouldContainSubstring, "Failed: 0;")
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
