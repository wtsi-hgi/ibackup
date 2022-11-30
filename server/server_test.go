/*******************************************************************************
 * Copyright (c) 2022 Genome Research Ltd.
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

package server

import (
	"context"
	"os"
	"os/user"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-resty/resty/v2"
	. "github.com/smartystreets/goconvey/convey"
	gas "github.com/wtsi-hgi/go-authserver"
	"github.com/wtsi-hgi/ibackup/put"
	"github.com/wtsi-hgi/ibackup/set"
)

const userPerms = 0700

func TestServer(t *testing.T) {
	u, err := user.Current()
	if err != nil {
		t.Fatalf("could not get current user: %s", err)
	}

	admin := u.Username

	localDir := t.TempDir()
	exampleSet := &set.Set{
		Name:        "set1",
		Requester:   "jim",
		Transformer: "prefix=" + localDir + ":/remote",
		Monitor:     false,
	}

	exampleSet2 := &set.Set{
		Name:        "set2",
		Requester:   exampleSet.Requester,
		Transformer: exampleSet.Transformer,
		Monitor:     true,
	}

	exampleSet3 := &set.Set{
		Name:        "set3",
		Requester:   exampleSet.Requester,
		Transformer: exampleSet.Transformer,
		Monitor:     false,
	}

	Convey("Given a Server", t, func() {
		logWriter := gas.NewStringLogger()
		s := New(logWriter)

		Convey("You can Start the Server", func() {
			certPath, keyPath, err := gas.CreateTestCert(t)
			So(err, ShouldBeNil)

			addr, dfunc, err := gas.StartTestServer(s, certPath, keyPath)
			So(err, ShouldBeNil)
			defer func() {
				errd := dfunc()
				So(errd, ShouldBeNil)
			}()

			client := resty.New()
			client.SetRootCertificate(certPath)

			Convey("The jwt endpoint works after enabling it", func() {
				err = s.EnableAuth(certPath, keyPath, func(u, p string) (bool, string) {
					return true, "1"
				})
				So(err, ShouldBeNil)

				token, errl := gas.Login(addr, certPath, exampleSet.Requester, "pass")
				So(errl, ShouldBeNil)
				So(token, ShouldNotBeBlank)

				Convey("And then you can LoadSetDB and use client methods AddOrUpdateSet and GetSets", func() {
					path := createDBLocation(t)
					err = s.LoadSetDB(path)
					So(err, ShouldBeNil)

					client := NewClient(addr, certPath, token)

					err = client.AddOrUpdateSet(exampleSet)
					So(err, ShouldBeNil)

					sets, err := client.GetSets(exampleSet.Requester)
					So(err, ShouldBeNil)
					So(len(sets), ShouldEqual, 1)
					So(sets[0], ShouldResemble, exampleSet)

					_, err = client.GetSets("foo")
					So(err, ShouldNotBeNil)

					exampleSet.Requester = "foo"
					err = client.AddOrUpdateSet(exampleSet)
					So(err, ShouldNotBeNil)
					exampleSet.Requester = "jim"

					err = client.AddOrUpdateSet(exampleSet)
					So(err, ShouldBeNil)

					Convey("And then you can set file and directory entries, trigger discovery and get all file statuses", func() {
						files, dirs, discovers := createTestBackupFiles(t, localDir)
						dirs = append(dirs, filepath.Join(localDir, "missing"))

						err = client.SetFiles(exampleSet.ID(), files)
						So(err, ShouldBeNil)

						err = client.SetDirs(exampleSet.ID(), dirs)
						So(err, ShouldBeNil)

						gotDirs, err := client.GetDirs(exampleSet.ID())
						So(err, ShouldBeNil)
						So(len(gotDirs), ShouldEqual, 3)
						So(gotDirs[0].Path, ShouldEqual, dirs[0])
						So(gotDirs[1].Path, ShouldEqual, dirs[1])
						So(gotDirs[2].Path, ShouldEqual, dirs[2])

						err = os.Remove(files[1])
						So(err, ShouldBeNil)

						t := time.Now()

						var racRequests []*put.Request
						racCalls := 0
						racCalled := make(chan bool, 1)

						s.queue.SetReadyAddedCallback(func(queuename string, allitemdata []interface{}) {
							racRequests = make([]*put.Request, len(allitemdata))

							for i, item := range allitemdata {
								r, ok := item.(*put.Request)

								if !ok {
									racCalled <- false

									return
								}

								racRequests[i] = r
							}

							racCalls++
							racCalled <- true
						})

						err = client.TriggerDiscovery(exampleSet.ID())
						So(err, ShouldBeNil)

						ok := <-racCalled
						So(ok, ShouldBeTrue)
						So(len(racRequests), ShouldEqual, len(files)-1+len(discovers))
						So(racRequests[0], ShouldResemble, &put.Request{
							Local:     files[0],
							Remote:    "/remote/a",
							Requester: exampleSet.Requester,
							Set:       exampleSet.Name,
						})

						entries, err := client.GetFiles(exampleSet.ID())
						So(err, ShouldBeNil)
						So(len(entries), ShouldEqual, len(files)+len(discovers))

						So(entries[0].Status, ShouldEqual, set.Pending)
						So(entries[1].Status, ShouldEqual, set.Missing)
						So(entries[2].Path, ShouldContainSubstring, "c/d/g")
						So(entries[3].Path, ShouldContainSubstring, "c/d/h")
						So(entries[4].Path, ShouldContainSubstring, "e/f/i/j/k/l")

						entries, err = client.GetDirs(exampleSet.ID())
						So(err, ShouldBeNil)
						So(len(entries), ShouldEqual, len(dirs))
						So(entries[2].Status, ShouldEqual, set.Missing)

						gotSet, err := client.GetSetByID(exampleSet.Requester, exampleSet.ID())
						So(err, ShouldBeNil)
						So(gotSet.LastDiscovery, ShouldHappenAfter, t)
						So(gotSet.Missing, ShouldEqual, 1)
						So(gotSet.NumFiles, ShouldEqual, 5)
						So(gotSet.Status, ShouldEqual, set.PendingUpload)

						gotSetByName, err := client.GetSetByName(exampleSet.Requester, exampleSet.Name)
						So(err, ShouldBeNil)
						So(gotSetByName, ShouldResemble, gotSet)

						err = client.AddOrUpdateSet(exampleSet2)
						So(err, ShouldBeNil)

						err = client.SetFiles(exampleSet2.ID(), files)
						So(err, ShouldBeNil)

						err = client.SetDirs(exampleSet2.ID(), nil)
						So(err, ShouldBeNil)

						t = time.Now()

						err = client.TriggerDiscovery(exampleSet2.ID())
						So(err, ShouldBeNil)

						ok = <-racCalled
						So(ok, ShouldBeTrue)
						So(len(racRequests), ShouldEqual, len(files)-1+len(discovers)+len(files)-1)

						entries, err = client.GetFiles(exampleSet2.ID())
						So(err, ShouldBeNil)
						So(len(entries), ShouldEqual, len(files))

						gotSet, err = client.GetSetByID(exampleSet2.Requester, exampleSet2.ID())
						So(err, ShouldBeNil)
						So(gotSet.LastDiscovery, ShouldHappenAfter, t)
						So(gotSet.Missing, ShouldEqual, 1)
						So(gotSet.NumFiles, ShouldEqual, 2)

						err = client.AddOrUpdateSet(exampleSet3)
						So(err, ShouldBeNil)

						err = client.SetFiles(exampleSet3.ID(), nil)
						So(err, ShouldBeNil)

						err = client.SetDirs(exampleSet3.ID(), dirs)
						So(err, ShouldBeNil)

						t = time.Now()

						err = client.TriggerDiscovery(exampleSet3.ID())
						So(err, ShouldBeNil)

						ok = <-racCalled
						So(ok, ShouldBeTrue)
						So(len(racRequests), ShouldEqual, len(files)-1+len(discovers)+len(files)-1+len(discovers))

						entries, err = client.GetFiles(exampleSet3.ID())
						So(err, ShouldBeNil)
						So(len(entries), ShouldEqual, len(discovers))

						gotSet, err = client.GetSetByID(exampleSet3.Requester, exampleSet3.ID())
						So(err, ShouldBeNil)
						So(gotSet.LastDiscovery, ShouldHappenAfter, t)
						So(gotSet.Missing, ShouldEqual, 0)
						So(gotSet.NumFiles, ShouldEqual, 3)

						So(racCalls, ShouldEqual, 3)
						err = client.TriggerDiscovery(exampleSet.ID())
						So(err, ShouldBeNil)

						<-time.After(100 * time.Millisecond)
						So(racCalls, ShouldEqual, 3)

						s.queue.TriggerReadyAddedCallback(context.Background())

						ok = <-racCalled
						So(ok, ShouldBeTrue)
						So(racCalls, ShouldEqual, 4)
						So(len(racRequests), ShouldEqual, len(files)-1+len(discovers)+len(files)-1+len(discovers))

						Convey("After discovery, admin can get upload requests and update file entry status", func() {
							entries, err = client.GetFiles(exampleSet.ID())
							So(err, ShouldBeNil)

							_, err = client.GetSomeUploadRequests()
							So(err, ShouldNotBeNil)

							token, errl = gas.Login(addr, certPath, admin, "pass")
							So(errl, ShouldBeNil)

							client = NewClient(addr, certPath, token)

							requests, err := client.GetSomeUploadRequests()
							So(err, ShouldBeNil)
							So(len(requests), ShouldEqual, 8)
							So(requests[0].Local, ShouldEqual, entries[0].Path)

							requests[0].Status = put.RequestStatusUploaded

							err = client.UpdateFileStatus(requests[0])
							So(err, ShouldBeNil)

							entries, err = client.GetFiles(exampleSet.ID())
							So(err, ShouldBeNil)
							So(entries[0].Status, ShouldEqual, set.Uploaded)

							gotSet, err = client.GetSetByID(exampleSet.Requester, exampleSet.ID())
							So(err, ShouldBeNil)
							So(gotSet.Uploaded, ShouldEqual, 1)
							So(gotSet.NumFiles, ShouldEqual, 5)

							stats := s.queue.Stats()
							So(stats.Items, ShouldEqual, 7)

							requests[1].Status = put.RequestStatusFailed

							err = client.UpdateFileStatus(requests[1])
							So(err, ShouldBeNil)

							entries, err = client.GetFiles(exampleSet.ID())
							So(err, ShouldBeNil)
							So(entries[2].Path, ShouldEqual, requests[1].Local)
							So(entries[2].Status, ShouldEqual, set.Failed)

							gotSet, err = client.GetSetByID(exampleSet.Requester, exampleSet.ID())
							So(err, ShouldBeNil)
							So(gotSet.Status, ShouldEqual, set.Uploading)
							So(gotSet.Uploaded, ShouldEqual, 1)
							So(gotSet.Failed, ShouldEqual, 1)

							stats = s.queue.Stats()
							So(stats.Items, ShouldEqual, 7)
							So(stats.Running, ShouldEqual, 6)
							So(stats.Ready, ShouldEqual, 1)

							err = client.UpdateFileStatus(requests[1])
							So(err, ShouldNotBeNil)

							frequests, err := client.GetSomeUploadRequests()
							So(err, ShouldBeNil)
							So(len(frequests), ShouldEqual, 1)

							frequests[0].Status = put.RequestStatusFailed
							err = client.UpdateFileStatus(frequests[0])
							So(err, ShouldBeNil)

							gotSet, err = client.GetSetByID(exampleSet.Requester, exampleSet.ID())
							So(err, ShouldBeNil)
							So(gotSet.Status, ShouldEqual, set.Uploading)
							So(gotSet.Uploaded, ShouldEqual, 1)
							So(gotSet.Failed, ShouldEqual, 1)

							stats = s.queue.Stats()
							So(stats.Items, ShouldEqual, 7)
							So(stats.Running, ShouldEqual, 6)
							So(stats.Ready, ShouldEqual, 1)

							frequests, err = client.GetSomeUploadRequests()
							So(err, ShouldBeNil)
							So(len(frequests), ShouldEqual, 1)

							frequests[0].Status = put.RequestStatusFailed
							err = client.UpdateFileStatus(frequests[0])
							So(err, ShouldBeNil)

							gotSet, err = client.GetSetByID(exampleSet.Requester, exampleSet.ID())
							So(err, ShouldBeNil)
							So(gotSet.Status, ShouldEqual, set.Failing)
							So(gotSet.Uploaded, ShouldEqual, 1)
							So(gotSet.Failed, ShouldEqual, 1)

							stats = s.queue.Stats()
							So(stats.Items, ShouldEqual, 7)
							So(stats.Running, ShouldEqual, 6)
							So(stats.Buried, ShouldEqual, 1)

							frequests, err = client.GetSomeUploadRequests()
							So(err, ShouldBeNil)
							So(len(frequests), ShouldEqual, 0)

							err = client.stillWorkingOnRequests([]string{requests[0].ID()})
							So(err, ShouldNotBeNil)

							err = client.stillWorkingOnRequests([]string{requests[2].ID()})
							So(err, ShouldBeNil)

							err = client.stillWorkingOnRequests(client.getRequestIDsToTouch())
							So(err, ShouldBeNil)
						})
					})
				})
			})
		})
	})
}

// createDBLocation creates a temporary location for to store a database and
// returns the path to the (non-existent) database file.
func createDBLocation(t *testing.T) string {
	t.Helper()

	tdir := t.TempDir()
	path := filepath.Join(tdir, "set.db")

	return path
}

// createTestBackupFiles creates and returns files, dirs and the files we're
// expecting to discover in the dirs.
func createTestBackupFiles(t *testing.T, dir string) ([]string, []string, []string) {
	t.Helper()

	files := []string{
		filepath.Join(dir, "a"),
		filepath.Join(dir, "b"),
	}

	dirs := []string{
		filepath.Join(dir, "c/d"),
		filepath.Join(dir, "e/f"),
	}

	discovers := []string{
		filepath.Join(dir, "c/d/g"),
		filepath.Join(dir, "c/d/h"),
		filepath.Join(dir, "e/f/i/j/k/l"),
	}

	for i, path := range files {
		createFile(t, path, i+1)
	}

	for i, path := range discovers {
		createFile(t, path, i+1)
	}

	return files, dirs, discovers
}

// createFile creates a file at the given path with the given number of bytes of
// content. It creates any directories the path needs as necessary.
func createFile(t *testing.T, path string, n int) {
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

	b := make([]byte, n)
	for i := range b {
		b[i] = 1
	}

	_, err = f.Write(b)
	if err != nil {
		t.Fatalf("close failed: %s", err)
	}

	err = f.Close()
	if err != nil {
		t.Fatalf("close failed: %s", err)
	}
}
