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

// Package speed holds benchmarks of ibackup's critical upload path, for the
// `make speed` regression gate. See developers/README.md.
//
// The benchmarks are copied into a worktree of a baseline revision and run
// there too, so they must only use APIs that exist in every revision being
// compared. The log15 import is the one exception: the gate rewrites it to
// match the baseline's go.mod.
package speed

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	wrclient "github.com/VertebrateResequencing/wr/client"
	"github.com/inconshreveable/log15/v3"
	gas "github.com/wtsi-hgi/go-authserver"
	"github.com/wtsi-hgi/ibackup/internal"
	"github.com/wtsi-hgi/ibackup/server"
	"github.com/wtsi-hgi/ibackup/set"
	"github.com/wtsi-hgi/ibackup/statter"
	"github.com/wtsi-hgi/ibackup/transfer"
)

const (
	// numUploadFiles and numRemoveFiles small files are in the sets of the
	// upload and remove benchmarks. Removals are done one at a time, so are
	// much slower per file. In every group of hardlinkGroup files, the last
	// numHardlinks are hardlinks to the first, so that hardlink handling is
	// benchmarked too.
	numUploadFiles = 2000
	numRemoveFiles = 1000
	hardlinkGroup  = 20
	numHardlinks   = 2

	// numPutClients put clients work on the set at once, like the put jobs
	// the server would have wr run.
	numPutClients = 4

	minMBperSecondUploadSpeed = 10
	minTimeForUpload          = 1 * time.Minute
	maxStuckTime              = 1 * time.Hour

	pollInterval = 5 * time.Millisecond
	waitTimeout  = 5 * time.Minute

	// maxIdleRounds rounds of put clients in a row may get no requests while
	// the set is incomplete, with the wait between rounds doubling up to
	// maxIdleWait, before the upload is judged stalled: about 4s in all. Each
	// round makes new connections, so the limit stops a stalled upload using
	// up the host's ephemeral ports.
	maxIdleRounds = 10
	maxIdleWait   = time.Second

	statterEnvVar = "IBACKUP_TEST_STATTER"

	// allowHardlinkSkipsEnvVar, when set, makes the upload checks accept
	// hardlinks counted as skipped instead of uploaded. The gate sets it for
	// the base tree only. Before the server claimed hardlinks for one put
	// client at a time, two clients could upload the same inode at once and
	// one would then skip its link: 1727e87 skipped up to 7 of the 2000 files
	// per op. Head must upload every file.
	allowHardlinkSkipsEnvVar = "IBACKUP_SPEED_ALLOW_HARDLINK_SKIPS"
)

var errTimeout = errors.New("timed out")

// fixture is the unchanging input for one benchmark function call: the local
// files to back up, a TLS cert and the statter.
type fixture struct {
	numFiles int
	dir      string
	localDir string
	certPath string
	keyPath  string
	user     string
}

func newFixture(b *testing.B, numFiles int) *fixture {
	b.Helper()

	u, err := user.Current()
	if err != nil {
		b.Fatal(err)
	}

	f := &fixture{numFiles: numFiles, dir: b.TempDir(), user: u.Username}
	f.localDir = filepath.Join(f.dir, "local")

	initStatter(b, f.dir)
	f.createCert(b)
	f.createFiles(b)

	return f
}

func (f *fixture) createCert(b *testing.B) {
	b.Helper()

	f.certPath = filepath.Join(f.dir, "cert")
	f.keyPath = filepath.Join(f.dir, "key")

	cmd := exec.Command("openssl", "req", "-new", "-newkey", "rsa:2048", //nolint:noctx,gosec
		"-days", "1", "-nodes", "-x509", "-subj", "/CN=localhost",
		"-addext", "subjectAltName = DNS:localhost",
		"-keyout", f.keyPath, "-out", f.certPath)

	if out, err := cmd.CombinedOutput(); err != nil {
		b.Fatalf("openssl failed: %s: %s", err, out)
	}
}

func (f *fixture) createFiles(b *testing.B) {
	b.Helper()

	for i := range f.numFiles {
		dir := filepath.Join(f.localDir, strconv.Itoa(i/100))
		path := filepath.Join(dir, strconv.Itoa(i))

		if err := os.MkdirAll(dir, 0o700); err != nil {
			b.Fatal(err)
		}

		var err error

		if pos := i % hardlinkGroup; pos >= hardlinkGroup-numHardlinks {
			err = os.Link(filepath.Join(dir, strconv.Itoa(i-pos)), path)
		} else {
			err = os.WriteFile(path, []byte(path), 0o600)
		}

		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkUpload measures a set of numUploadFiles files going through discovery,
// request reservation by numPutClients put clients, transfer by the local
// mock handler, result reporting and set completion.
func BenchmarkUpload(b *testing.B) {
	f := newFixture(b, numUploadFiles)

	b.ReportAllocs()

	for b.Loop() {
		b.StopTimer()

		e := newEnv(b, f)

		b.StartTimer()

		given, got := e.uploadSet(b)

		b.StopTimer()

		e.checkUpload(b, given, got)
		e.close(b)

		b.StartTimer()
	}
}

func newEnv(b *testing.B, f *fixture) *env {
	b.Helper()

	dir, err := os.MkdirTemp(f.dir, "env")
	if err != nil {
		b.Fatal(err)
	}

	e := &env{
		f:         f,
		handler:   internal.GetLocalHandler(),
		remoteDir: filepath.Join(dir, "remote"),
		logger:    log15.New(),
	}

	// put clients normally log to a file; format the logs, but discard them
	e.logger.SetHandler(log15.StreamHandler(io.Discard, log15.LogfmtFormat()))

	e.startServer(b, dir)

	return e
}

// BenchmarkRemove measures the removal (trashing) of all files of an uploaded
// set of numRemoveFiles files.
func BenchmarkRemove(b *testing.B) {
	f := newFixture(b, numRemoveFiles)

	b.ReportAllocs()

	for b.Loop() {
		b.StopTimer()

		e := newEnv(b, f)
		given, got := e.uploadSet(b)
		e.checkUpload(b, given, got)

		b.StartTimer()

		e.removeSet(b, given)

		b.StopTimer()

		e.checkRemoved(b, given)
		e.close(b)

		b.StartTimer()
	}
}

// env is a running server with an empty database and an admin client of it.
// Each benchmark op gets a new one, so that every op does the same work.
type env struct {
	f           *fixture
	handler     *internal.LocalHandler
	remoteDir   string
	submissions string
	subsFD      int
	addr        string
	token       string
	client      *server.Client
	logger      log15.Logger
	stop        func() error
}

func (e *env) startServer(b *testing.B, dir string) {
	b.Helper()

	s, err := server.New(server.Config{HTTPLogger: io.Discard, StorageHandler: e.handler})
	if err != nil {
		b.Fatal(err)
	}

	tokenPath := filepath.Join(dir, "token")
	allow := func(_, _ string) (bool, string) { return true, "1" }

	if err = s.EnableAuthWithServerToken(e.f.certPath, e.f.keyPath, tokenPath, allow); err != nil {
		b.Fatal(err)
	}

	if err = s.MakeQueueEndPoints(); err != nil {
		b.Fatal(err)
	}

	if err = s.LoadSetDB(filepath.Join(dir, "set.db"), ""); err != nil {
		b.Fatal(err)
	}

	s.SetRemoteHardlinkLocation(filepath.Join(e.remoteDir, "hardlinks"))

	e.pretendSubmissions(b, dir)

	err = s.EnableJobSubmission("put", "development", "", "", "", "", numPutClients, e.logger)
	if err != nil {
		b.Fatal(err)
	}

	e.addr, e.stop, err = gas.StartTestServer(s, e.f.certPath, e.f.keyPath)
	if err != nil {
		b.Fatal(err)
	}

	e.token, err = gas.Login(gas.NewClientRequest(e.addr, e.f.certPath), e.f.user, "pass")
	if err != nil {
		b.Fatal(err)
	}

	e.client = e.newClient()
}

// pretendSubmissions makes the server's put job submission record jobs to a
// file in dir, without wr. A file descriptor is used, rather than "Y", because
// wr before v0.38 panics when stopping a pretend scheduler without one. Old wr
// closes the descriptor when the server stops, while new wr duplicates it, so
// close() closes it only if it is still open.
func (e *env) pretendSubmissions(b *testing.B, dir string) {
	b.Helper()

	e.submissions = filepath.Join(dir, "submissions")

	fd, err := syscall.Open(e.submissions, syscall.O_WRONLY|syscall.O_CREAT|syscall.O_APPEND|syscall.O_CLOEXEC, 0o600)
	if err != nil {
		b.Fatal(err)
	}

	e.subsFD = fd
	wrclient.PretendSubmissions = strconv.Itoa(fd)
}

func (e *env) newClient() *server.Client {
	return server.NewClient(e.addr, e.f.certPath, e.token)
}

func (e *env) close(b *testing.B) {
	b.Helper()

	if err := e.stop(); err != nil {
		b.Fatal(err)
	}

	e.closeSubmissions(b)
}

// closeSubmissions closes the pretend submissions descriptor if the stopped
// server left it open. Old wr has already closed it, and its number may now
// belong to another file, so it is closed only while it is still the
// submissions file.
func (e *env) closeSubmissions(b *testing.B) {
	b.Helper()

	var fdStat, fileStat syscall.Stat_t

	if syscall.Fstat(e.subsFD, &fdStat) != nil {
		return
	}

	e.must(b, syscall.Stat(e.submissions, &fileStat))

	if fdStat.Dev != fileStat.Dev || fdStat.Ino != fileStat.Ino {
		return
	}

	e.must(b, syscall.Close(e.subsFD))
}

// uploadSet adds a set of the fixture's files, discovers them, and has put
// clients upload them until the set is complete. It returns the set as given
// and as completed.
func (e *env) uploadSet(b *testing.B) (*set.Set, *set.Set) {
	b.Helper()

	given := &set.Set{
		Name:        "speed",
		Requester:   e.f.user,
		Transformer: "prefix=" + e.f.localDir + ":" + e.remoteDir,
	}

	e.must(b, e.client.AddOrUpdateSet(given))
	e.must(b, e.client.MergeDirs(given.ID(), []string{e.f.localDir}))
	e.must(b, e.client.TriggerDiscovery(given.ID(), false))

	e.waitForQueuedRequests(b)

	return given, e.runPutClientsUntilComplete(b, given)
}

// checkUpload fails the benchmark unless every file of the completed set got
// was uploaded, so that a broken path that skips work cannot look fast.
func (e *env) checkUpload(b *testing.B, given, got *set.Set) {
	b.Helper()

	want := uint64(e.f.numFiles) //nolint:gosec

	var skipped uint64
	if os.Getenv(allowHardlinkSkipsEnvVar) != "" {
		skipped = e.countSkippedHardlinks(b, given)
	}

	allDone := got.NumFiles == want && got.Uploaded == want-skipped && got.Skipped == skipped &&
		got.Replaced == 0 && got.Failed == 0 && got.Missing == 0

	if !allDone {
		b.Fatalf("set completed with %d files, %d uploaded, %d replaced, %d skipped, %d failed, "+
			"%d missing; want %d uploaded and %d skipped hardlinks", got.NumFiles, got.Uploaded,
			got.Replaced, got.Skipped, got.Failed, got.Missing, want-skipped, skipped)
	}

	e.checkRemoteFiles(b)
	e.checkSubmissions(b)
}

// countSkippedHardlinks returns how many of the given set's files were
// skipped, failing the benchmark if any of those is not a hardlink.
func (e *env) countSkippedHardlinks(b *testing.B, given *set.Set) uint64 {
	b.Helper()

	entries, err := e.client.GetFiles(given.ID())
	e.must(b, err)

	var skipped uint64

	for _, entry := range entries {
		if entry.Status != set.Skipped {
			continue
		}

		if entry.Type != set.Hardlink {
			b.Fatalf("%s was skipped, but is not a hardlink", entry.Path)
		}

		skipped++
	}

	return skipped
}

// checkRemoteFiles fails the benchmark unless every local file has a remote
// copy of the same size, or an empty one for a hardlink, and there is an
// uploaded inode file for every group of hardlinks.
func (e *env) checkRemoteFiles(b *testing.B) {
	b.Helper()

	var checked, bad int

	err := filepath.WalkDir(e.f.localDir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}

		checked++

		if !e.hasRemoteCopy(path) {
			bad++
		}

		return nil
	})
	e.must(b, err)

	inodes := countNonEmptyFiles(b, filepath.Join(e.remoteDir, "hardlinks"))

	if checked != e.f.numFiles || bad != 0 || inodes != e.f.numFiles/hardlinkGroup {
		b.Fatalf("checked %d local files: %d lack a remote copy; %d hardlink inode files; want %d",
			checked, bad, inodes, e.f.numFiles/hardlinkGroup)
	}
}

func countNonEmptyFiles(b *testing.B, dir string) int {
	b.Helper()

	var n int

	err := filepath.WalkDir(dir, func(_ string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}

		info, err := d.Info()
		if err == nil && info.Size() > 0 {
			n++
		}

		return err
	})
	if err != nil {
		b.Fatal(err)
	}

	return n
}

// hasRemoteCopy returns true if the remote path of the given local file has
// the same size, or is empty when the local file is a hardlink.
func (e *env) hasRemoteCopy(local string) bool {
	rel, err := filepath.Rel(e.f.localDir, local)
	if err != nil {
		return false
	}

	lInfo, lErr := os.Stat(local)
	rInfo, rErr := os.Stat(filepath.Join(e.remoteDir, rel))

	if lErr != nil || rErr != nil {
		return false
	}

	if rInfo.Size() == lInfo.Size() {
		return true
	}

	st, ok := lInfo.Sys().(*syscall.Stat_t)

	return ok && st.Nlink > 1 && rInfo.Size() == 0
}

// checkSubmissions fails the benchmark if the server did not submit any put
// jobs, which would mean that it does not schedule uploads.
func (e *env) checkSubmissions(b *testing.B) {
	b.Helper()

	info, err := os.Stat(e.submissions)
	e.must(b, err)

	if info.Size() == 0 {
		b.Fatal("the server submitted no put jobs")
	}
}

func (e *env) must(b *testing.B, err error) {
	b.Helper()

	if err != nil {
		b.Fatal(err)
	}
}

// waitForSet polls the server until done returns true for the given set, and
// returns the set as last seen.
func (e *env) waitForSet(b *testing.B, given *set.Set, done func(*set.Set) bool) *set.Set {
	b.Helper()

	deadline := time.Now().Add(waitTimeout)

	for {
		got, err := e.client.GetSetByID(given.Requester, given.ID())
		e.must(b, err)

		if done(got) {
			return got
		}

		if time.Now().After(deadline) {
			b.Fatalf("%s waiting for set: status %s, %d files, %d uploaded, %d failed, %d of %d removed",
				errTimeout, got.Status, got.NumFiles, got.Uploaded, got.Failed,
				got.NumObjectsRemoved, got.NumObjectsToBeRemoved)
		}

		time.Sleep(pollInterval)
	}
}

// waitForQueuedRequests waits until discovery has queued an upload request for
// every file. wr would only start put clients after that.
func (e *env) waitForQueuedRequests(b *testing.B) {
	b.Helper()

	deadline := time.Now().Add(waitTimeout)

	for {
		qs, err := e.client.GetQueueStatus()
		e.must(b, err)

		if qs.Total >= e.f.numFiles {
			return
		}

		if time.Now().After(deadline) {
			b.Fatalf("%s waiting for %d queued requests: %d queued", errTimeout, e.f.numFiles, qs.Total)
		}

		time.Sleep(pollInterval)
	}
}

// runPutClientsUntilComplete starts numPutClients put clients, and starts more
// whenever they have all exited and the set is still incomplete, as the
// server's put job submission would. It fails the benchmark if more than
// maxIdleRounds rounds of clients in a row got no requests while the set stayed
// incomplete, waiting longer after each such round, since the set would then
// never complete.
func (e *env) runPutClientsUntilComplete(b *testing.B, given *set.Set) *set.Set {
	b.Helper()

	deadline := time.Now().Add(waitTimeout)
	idleRounds := 0
	wait := pollInterval

	for {
		handled := e.runPutClients(b)

		got, err := e.client.GetSetByID(given.Requester, given.ID())
		e.must(b, err)

		if got.Status == set.Complete {
			return got
		}

		idleRounds, wait = nextIdleWait(handled, idleRounds, wait)

		if idleRounds > maxIdleRounds {
			b.Fatalf("upload stalled: %d rounds of put clients in a row got no requests: status %s, "+
				"%d of %d uploaded, %d skipped, %d failed",
				idleRounds, got.Status, got.Uploaded, got.NumFiles, got.Skipped, got.Failed)
		}

		if time.Now().After(deadline) {
			b.Fatalf("%s waiting for upload: status %s, %d of %d uploaded, %d failed",
				errTimeout, got.Status, got.Uploaded, got.NumFiles, got.Failed)
		}

		time.Sleep(wait)
	}
}

// nextIdleWait returns the number of consecutive rounds of put clients that got
// no requests, and how long to wait before the next round, after a round that
// handled the given number of requests.
func nextIdleWait(handled, idleRounds int, wait time.Duration) (int, time.Duration) {
	if handled > 0 {
		return 0, pollInterval
	}

	return idleRounds + 1, min(wait*2, maxIdleWait) //nolint:mnd
}

// runPutClients runs numPutClients put clients until they all exit, and returns
// how many requests they got between them.
func (e *env) runPutClients(b *testing.B) int {
	b.Helper()

	var wg sync.WaitGroup

	errs := make([]error, numPutClients)
	handled := make([]int, numPutClients)

	for i := range numPutClients {
		wg.Go(func() { handled[i], errs[i] = e.putClient() })
	}

	wg.Wait()

	e.must(b, errors.Join(errs...))

	var total int
	for _, n := range handled {
		total += n
	}

	return total
}

// putClient does what `ibackup put -s` does: it repeatedly gets requests from
// the server, uploads them and sends the results back, until there are no more
// requests. It returns how many requests it got.
func (e *env) putClient() (int, error) {
	client := e.newClient()
	handled := 0

	for {
		requests, err := client.GetSomeUploadRequests()
		if err != nil || len(requests) == 0 {
			return handled, err
		}

		handled += len(requests)

		p, err := transfer.New(e.handler, requests)
		if err != nil {
			return handled, err
		}

		if err = p.CreateCollections(); err != nil {
			return handled, err
		}

		uploadStarts, uploadResults, skipResults := p.Put()

		err = client.SendPutResultsToServer(uploadStarts, uploadResults, skipResults,
			minMBperSecondUploadSpeed, minTimeForUpload, maxStuckTime, e.logger)

		p.Cleanup()

		if err != nil {
			return handled, fmt.Errorf("sending put results: %w", err)
		}
	}
}

// removeSet does what `ibackup remove` does to all the given set's files, and
// waits for the removals to finish.
func (e *env) removeSet(b *testing.B, given *set.Set) {
	b.Helper()

	e.must(b, e.client.TrashFilesAndDirs(given.ID(), []string{e.f.localDir}))

	e.waitForSet(b, given, func(got *set.Set) bool {
		return got.NumObjectsToBeRemoved > 0 && got.NumObjectsRemoved == got.NumObjectsToBeRemoved
	})
}

// checkRemoved fails the benchmark unless the given set has no files left in
// the database and every remote file's sets metadata names the set's trash set
// instead of the set, so that a broken removal path that skips work cannot look
// fast.
func (e *env) checkRemoved(b *testing.B, given *set.Set) {
	b.Helper()

	entries, err := e.client.GetFiles(given.ID())
	e.must(b, err)

	if len(entries) != 0 {
		b.Fatalf("%d files are still in the set after removal", len(entries))
	}

	var checked, bad int

	err = filepath.WalkDir(e.f.localDir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}

		checked++

		if !e.isTrashedRemotely(b, path, given.Name) {
			bad++
		}

		return nil
	})
	e.must(b, err)

	if checked != e.f.numFiles || bad != 0 {
		b.Fatalf("checked %d local files: %d remote copies' sets metadata lacks %s%s or still has %s",
			checked, bad, set.TrashPrefix, given.Name, given.Name)
	}
}

// isTrashedRemotely returns true if the sets metadata of the given local file's
// remote copy names the trash set of the named set, but not the set itself.
func (e *env) isTrashedRemotely(b *testing.B, local, setName string) bool {
	b.Helper()

	rel, err := filepath.Rel(e.f.localDir, local)
	e.must(b, err)

	meta, err := e.handler.GetMeta(filepath.Join(e.remoteDir, rel))
	e.must(b, err)

	sets := strings.Split(meta[transfer.MetaKeySets], ",")

	return slices.Contains(sets, set.TrashPrefix+setName) && !slices.Contains(sets, setName)
}

// initStatter uses the statter at $IBACKUP_TEST_STATTER, or else builds one in
// dir.
func initStatter(b *testing.B, dir string) {
	b.Helper()

	exe := os.Getenv(statterEnvVar)
	if exe == "" {
		if err := internal.BuildStatter(dir); err != nil {
			b.Fatal(err)
		}

		exe = filepath.Join(dir, "statter")
	}

	if err := statter.Init(exe); err != nil {
		b.Fatal(err)
	}
}
