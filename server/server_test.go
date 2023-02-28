/*******************************************************************************
 * Copyright (c) 2022, 2023 Genome Research Ltd.
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
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/VertebrateResequencing/wr/queue"
	"github.com/inconshreveable/log15"
	. "github.com/smartystreets/goconvey/convey"
	gas "github.com/wtsi-hgi/go-authserver"
	"github.com/wtsi-hgi/ibackup/put"
	"github.com/wtsi-hgi/ibackup/set"
)

const (
	userPerms        = 0700
	numManyTestFiles = 5000
)

func TestClient(t *testing.T) {
	Convey("maxTimeForUpload works with small and large requests", t, func() {
		min := 10 * time.Millisecond

		c := &Client{
			minMBperSecondUploadSpeed: float64(10),
			minTimeForUpload:          min,
		}

		d := c.maxTimeForUpload(&put.Request{Size: 10 * bytesInMiB})
		So(d, ShouldEqual, 1*time.Second)

		d = c.maxTimeForUpload(&put.Request{Size: 1000 * bytesInMiB})
		So(d, ShouldEqual, 100*time.Second)

		d = c.maxTimeForUpload(&put.Request{Size: 1 * bytesInMiB})
		So(d, ShouldEqual, 100*time.Millisecond)

		d = c.maxTimeForUpload(&put.Request{Size: 1})
		So(d, ShouldEqual, min)
	})
}

func TestServer(t *testing.T) {
	u, err := user.Current()
	if err != nil {
		t.Fatalf("could not get current user: %s", err)
	}

	admin := u.Username
	minMBperSecondUploadSpeed := float64(10)
	minTimeForUpload := 1 * time.Minute

	Convey("Given a Server", t, func() {
		logWriter := gas.NewStringLogger()
		s := New(logWriter)

		Convey("You can Start the Server with Auth, MakeQueueEndPoints and LoadSetDB", func() {
			certPath, keyPath, err := gas.CreateTestCert(t)
			So(err, ShouldBeNil)

			err = s.EnableAuth(certPath, keyPath, func(u, p string) (bool, string) {
				return true, "1"
			})
			So(err, ShouldBeNil)

			err = s.MakeQueueEndPoints()
			So(err, ShouldBeNil)

			path := createDBLocation(t)
			err = s.LoadSetDB(path)
			So(err, ShouldBeNil)

			addr, dfunc, err := gas.StartTestServer(s, certPath, keyPath)
			So(err, ShouldBeNil)
			defer func() {
				errd := dfunc()
				So(errd, ShouldBeNil)
			}()

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

			localDir := t.TempDir()
			remoteDir := filepath.Join(localDir, "remote")
			exampleSet := &set.Set{
				Name:        "set1",
				Requester:   "jim",
				Transformer: "prefix=" + localDir + ":" + remoteDir,
				Monitor:     false,
			}

			Convey("Which lets you login", func() {
				token, errl := gas.Login(addr, certPath, "jim", "pass")
				So(errl, ShouldBeNil)
				So(token, ShouldNotBeBlank)

				Convey("And then you use client methods AddOrUpdateSet and GetSets", func() {
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
						files, dirs, discovers, symlinkPath := createTestBackupFiles(t, localDir)
						files = append(files, symlinkPath)
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
						numExistingFiles := len(files) - 2

						tn := time.Now()
						err = client.TriggerDiscovery(exampleSet.ID())
						So(err, ShouldBeNil)

						ok := <-racCalled
						So(ok, ShouldBeTrue)
						So(len(racRequests), ShouldEqual, numExistingFiles+len(discovers))
						So(racRequests[0], ShouldResemble, &put.Request{
							Local:     files[0],
							Remote:    filepath.Join(remoteDir, "a"),
							Requester: exampleSet.Requester,
							Set:       exampleSet.Name,
						})

						entries, err := client.GetFiles(exampleSet.ID())
						So(err, ShouldBeNil)
						So(len(entries), ShouldEqual, len(files)+len(discovers))

						So(entries[0].Status, ShouldEqual, set.Pending)
						So(entries[1].Status, ShouldEqual, set.Missing)
						So(entries[2].Path, ShouldContainSubstring, "c/d/symlink")
						So(entries[2].Status, ShouldEqual, set.Missing)
						So(entries[3].Path, ShouldContainSubstring, "c/d/g")
						So(entries[4].Path, ShouldContainSubstring, "c/d/h")
						So(entries[5].Path, ShouldContainSubstring, "e/f/i/j/k/l")

						entries, err = client.GetDirs(exampleSet.ID())
						So(err, ShouldBeNil)
						So(len(entries), ShouldEqual, len(dirs))
						So(entries[2].Status, ShouldEqual, set.Missing)

						gotSet, err := client.GetSetByID(exampleSet.Requester, exampleSet.ID())
						So(err, ShouldBeNil)
						So(gotSet.LastDiscovery, ShouldHappenAfter, tn)
						So(gotSet.Missing, ShouldEqual, 2)
						So(gotSet.NumFiles, ShouldEqual, 6)
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

						tn = time.Now()

						err = client.TriggerDiscovery(exampleSet2.ID())
						So(err, ShouldBeNil)

						ok = <-racCalled
						So(ok, ShouldBeTrue)
						So(len(racRequests), ShouldEqual, numExistingFiles+len(discovers)+numExistingFiles)

						entries, err = client.GetFiles(exampleSet2.ID())
						So(err, ShouldBeNil)
						So(len(entries), ShouldEqual, len(files))

						gotSet, err = client.GetSetByID(exampleSet2.Requester, exampleSet2.ID())
						So(err, ShouldBeNil)
						So(gotSet.LastDiscovery, ShouldHappenAfter, tn)
						So(gotSet.Missing, ShouldEqual, 2)
						So(gotSet.NumFiles, ShouldEqual, 3)

						err = client.AddOrUpdateSet(exampleSet3)
						So(err, ShouldBeNil)

						err = client.SetFiles(exampleSet3.ID(), nil)
						So(err, ShouldBeNil)

						err = client.SetDirs(exampleSet3.ID(), dirs)
						So(err, ShouldBeNil)

						tn = time.Now()

						err = client.TriggerDiscovery(exampleSet3.ID())
						So(err, ShouldBeNil)

						ok = <-racCalled
						So(ok, ShouldBeTrue)
						So(len(racRequests), ShouldEqual, numExistingFiles+len(discovers)+numExistingFiles+len(discovers))

						entries, err = client.GetFiles(exampleSet3.ID())
						So(err, ShouldBeNil)
						So(len(entries), ShouldEqual, len(discovers))

						gotSet, err = client.GetSetByID(exampleSet3.Requester, exampleSet3.ID())
						So(err, ShouldBeNil)
						So(gotSet.LastDiscovery, ShouldHappenAfter, tn)
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
						So(len(racRequests), ShouldEqual, numExistingFiles+len(discovers)+numExistingFiles+len(discovers))

						Convey("After discovery, admin can get upload requests and update file entry status", func() {
							entries, err = client.GetFiles(exampleSet.ID())
							So(err, ShouldBeNil)

							_, err = client.GetSomeUploadRequests()
							So(err, ShouldNotBeNil)

							token, errl = gas.Login(addr, certPath, admin, "pass")
							So(errl, ShouldBeNil)

							client = NewClient(addr, certPath, token)

							requests, errg := client.GetSomeUploadRequests()
							So(errg, ShouldBeNil)
							So(len(requests), ShouldEqual, 8)
							So(requests[0].Local, ShouldEqual, entries[0].Path)

							requests[0].Status = put.RequestStatusUploading
							err = client.UpdateFileStatus(requests[0])
							So(err, ShouldBeNil)

							entries, err = client.GetFiles(exampleSet.ID())
							So(err, ShouldBeNil)
							So(entries[0].Status, ShouldEqual, set.UploadingEntry)

							gotSet, err = client.GetSetByID(exampleSet.Requester, exampleSet.ID())
							So(err, ShouldBeNil)
							So(gotSet.Status, ShouldEqual, set.Uploading)
							So(gotSet.Uploaded, ShouldEqual, 0)
							So(gotSet.NumFiles, ShouldEqual, 6)

							requests[0].Status = put.RequestStatusUploading
							requests[0].Stuck = put.NewStuck(time.Now())
							err = client.UpdateFileStatus(requests[0])
							So(err, ShouldBeNil)

							entries, err = client.GetFiles(exampleSet.ID())
							So(err, ShouldBeNil)
							So(entries[0].Status, ShouldEqual, set.UploadingEntry)
							So(entries[0].LastError, ShouldContainSubstring, "stuck?")

							gotSet, err = client.GetSetByID(exampleSet.Requester, exampleSet.ID())
							So(err, ShouldBeNil)
							So(gotSet.Status, ShouldEqual, set.Uploading)
							So(gotSet.Uploaded, ShouldEqual, 0)
							So(gotSet.NumFiles, ShouldEqual, 6)
							So(gotSet.Failed, ShouldEqual, 0)
							So(gotSet.Error, ShouldBeBlank)

							requests[0].Status = put.RequestStatusUploaded
							err = client.UpdateFileStatus(requests[0])
							So(err, ShouldBeNil)

							entries, err = client.GetFiles(exampleSet.ID())
							So(err, ShouldBeNil)
							So(entries[0].Status, ShouldEqual, set.Uploaded)
							So(entries[0].LastError, ShouldBeBlank)

							gotSet, err = client.GetSetByID(exampleSet.Requester, exampleSet.ID())
							So(err, ShouldBeNil)
							So(gotSet.Uploaded, ShouldEqual, 1)
							So(gotSet.NumFiles, ShouldEqual, 6)
							So(gotSet.Failed, ShouldEqual, 0)
							So(gotSet.Error, ShouldBeBlank)

							stats := s.queue.Stats()
							So(stats.Items, ShouldEqual, 7)

							requests[1].Status = put.RequestStatusUploading
							err = client.UpdateFileStatus(requests[1])
							So(err, ShouldBeNil)

							requests[1].Status = put.RequestStatusFailed
							err = client.UpdateFileStatus(requests[1])
							So(err, ShouldBeNil)

							entries, err = client.GetFiles(exampleSet.ID())
							So(err, ShouldBeNil)
							So(entries[3].Path, ShouldEqual, requests[1].Local)
							So(entries[3].Status, ShouldEqual, set.Failed)

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

							frequests, errg := client.GetSomeUploadRequests()
							So(errg, ShouldBeNil)
							So(len(frequests), ShouldEqual, 1)

							frequests[0].Status = put.RequestStatusUploading
							err = client.UpdateFileStatus(frequests[0])
							So(err, ShouldBeNil)

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

							frequests[0].Status = put.RequestStatusUploading
							err = client.UpdateFileStatus(frequests[0])
							So(err, ShouldBeNil)

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
							So(err, ShouldBeNil)

							err = client.stillWorkingOnRequests([]string{requests[2].ID()})
							So(err, ShouldBeNil)

							err = client.stillWorkingOnRequests(client.getRequestIDsToTouch())
							So(err, ShouldBeNil)
						})

						Convey("Stuck requests are recorded separately by the server, retrievable with QueueStatus", func() {
							token, errl = gas.Login(addr, certPath, admin, "pass")
							So(errl, ShouldBeNil)

							client = NewClient(addr, certPath, token)

							qs := s.QueueStatus()
							So(qs, ShouldNotBeNil)
							So(len(qs.Stuck), ShouldEqual, 0)
							So(qs.Total, ShouldEqual, 8)
							So(qs.Reserved, ShouldEqual, 0)
							So(qs.Uploading, ShouldEqual, 0)

							qsClient, errs := client.GetQueueStatus()
							So(errs, ShouldBeNil)
							So(qsClient, ShouldResemble, qs)

							requests, errg := client.GetSomeUploadRequests()
							So(errg, ShouldBeNil)

							requests[0].Status = put.RequestStatusUploading

							err = client.UpdateFileStatus(requests[0])
							So(err, ShouldBeNil)

							entries, err = client.GetFiles(exampleSet.ID())
							So(err, ShouldBeNil)
							So(entries[0].Status, ShouldEqual, set.UploadingEntry)
							So(entries[0].LastError, ShouldBeBlank)

							So(len(s.stuckRequests), ShouldEqual, 0)

							requests[0].Stuck = put.NewStuck(time.Now())
							err = client.UpdateFileStatus(requests[0])
							So(err, ShouldBeNil)

							entries, err = client.GetFiles(exampleSet.ID())
							So(err, ShouldBeNil)
							So(entries[0].Status, ShouldEqual, set.UploadingEntry)
							So(entries[0].LastError, ShouldContainSubstring, "stuck?")

							So(len(s.stuckRequests), ShouldEqual, 1)

							qs = s.QueueStatus()
							So(qs, ShouldNotBeNil)
							So(len(qs.Stuck), ShouldEqual, 1)
							So(qs.Total, ShouldEqual, 8)
							So(qs.Reserved, ShouldEqual, 8)
							So(qs.Uploading, ShouldEqual, 1)

							requests[0].Status = put.RequestStatusUploaded

							err = client.UpdateFileStatus(requests[0])
							So(err, ShouldBeNil)

							qs = s.QueueStatus()
							So(qs, ShouldNotBeNil)
							So(len(qs.Stuck), ShouldEqual, 0)
							So(qs.Total, ShouldEqual, 7)
							So(qs.Reserved, ShouldEqual, 7)
							So(qs.Uploading, ShouldEqual, 0)
						})

						Convey("Buried requests can be retrieved, kicked and removed", func() {
							token, errl = gas.Login(addr, certPath, admin, "pass")
							So(errl, ShouldBeNil)

							client = NewClient(addr, certPath, token)

							buried := s.BuriedRequests()
							So(buried, ShouldBeNil)

							var failedRequest *put.Request

							failARequest := func() {
								for i := 0; i < set.AttemptsToBeConsideredFailing; i++ {
									requests, errg := client.GetSomeUploadRequests()
									So(errg, ShouldBeNil)
									So(len(requests), ShouldBeGreaterThan, 0)

									failedRequest = requests[0].Clone()

									failedRequest.Status = put.RequestStatusUploading
									err = client.UpdateFileStatus(failedRequest)
									So(err, ShouldBeNil)

									failedRequest.Status = put.RequestStatusFailed
									failedRequest.Error = "an error"
									err = client.UpdateFileStatus(failedRequest)
									So(err, ShouldBeNil)

									if len(requests) > 1 {
										for i, r := range requests {
											if i == 0 {
												continue
											}

											err = s.queue.Release(context.Background(), r.ID())
											So(err, ShouldBeNil)
										}
									}
								}
							}

							failARequest()

							qs := s.QueueStatus()
							So(qs, ShouldNotBeNil)
							So(len(qs.Stuck), ShouldEqual, 0)
							So(qs.Total, ShouldEqual, 8)
							So(qs.Reserved, ShouldEqual, 0)
							So(qs.Uploading, ShouldEqual, 0)
							So(qs.Failed, ShouldEqual, 1)

							buried = s.BuriedRequests()
							So(buried, ShouldNotBeNil)
							So(len(buried), ShouldEqual, 1)

							cburied, errb := client.BuriedRequests()
							So(errb, ShouldBeNil)
							So(cburied, ShouldResemble, buried)
							So(cburied[0].Error, ShouldEqual, "an error")

							bf := &BuriedFilter{}

							n, errr := client.RetryBuried(bf)
							So(errr, ShouldBeNil)
							So(n, ShouldEqual, 1)

							qs = s.QueueStatus()
							So(qs, ShouldNotBeNil)
							So(qs.Total, ShouldEqual, 8)
							So(qs.Reserved, ShouldEqual, 0)
							So(qs.Uploading, ShouldEqual, 0)
							So(qs.Failed, ShouldEqual, 0)
							So(s.queue.Stats().Ready, ShouldEqual, 8)

							failARequest()

							qs = s.QueueStatus()
							So(qs, ShouldNotBeNil)
							So(qs.Total, ShouldEqual, 8)
							So(qs.Reserved, ShouldEqual, 0)
							So(qs.Uploading, ShouldEqual, 0)
							So(qs.Failed, ShouldEqual, 1)

							n, errr = client.RemoveBuried(bf)
							So(errr, ShouldBeNil)
							So(n, ShouldEqual, 1)

							qs = s.QueueStatus()
							So(qs, ShouldNotBeNil)
							So(qs.Total, ShouldEqual, 7)
							So(qs.Reserved, ShouldEqual, 0)
							So(qs.Uploading, ShouldEqual, 0)
							So(qs.Failed, ShouldEqual, 0)

							failARequest()

							invalid := "invalid"
							bf.User = invalid
							n, errr = client.RetryBuried(bf)
							So(errr, ShouldBeNil)
							So(n, ShouldEqual, 0)

							bf.User = failedRequest.Requester
							bf.Set = invalid
							n, errr = client.RetryBuried(bf)
							So(errr, ShouldBeNil)
							So(n, ShouldEqual, 0)

							bf.Set = failedRequest.Set
							bf.Path = invalid
							n, errr = client.RetryBuried(bf)
							So(errr, ShouldBeNil)
							So(n, ShouldEqual, 0)

							bf.Path = failedRequest.Local
							n, errr = client.RetryBuried(bf)
							So(errr, ShouldBeNil)
							So(n, ShouldEqual, 1)

							failARequest()

							bf = &BuriedFilter{
								User: failedRequest.Requester,
							}
							n, errr = client.RetryBuried(bf)
							So(errr, ShouldBeNil)
							So(n, ShouldEqual, 1)

							failARequest()

							bf.User = failedRequest.Requester
							bf.Set = failedRequest.Set
							n, errr = client.RetryBuried(bf)
							So(errr, ShouldBeNil)
							So(n, ShouldEqual, 1)
						})

						Convey("Uploading requests can be retrieved", func() {
							token, errl = gas.Login(addr, certPath, admin, "pass")
							So(errl, ShouldBeNil)

							client = NewClient(addr, certPath, token)

							uploading, errc := client.UploadingRequests()
							So(errc, ShouldBeNil)
							So(len(uploading), ShouldEqual, 0)

							requests, errg := client.GetSomeUploadRequests()
							So(errg, ShouldBeNil)
							So(len(requests), ShouldBeGreaterThan, 0)

							uploadRequest := requests[0].Clone()

							uploadRequest.Status = put.RequestStatusUploading
							err = client.UpdateFileStatus(uploadRequest)
							So(err, ShouldBeNil)

							uploading, errc = client.UploadingRequests()
							So(errc, ShouldBeNil)
							So(len(uploading), ShouldEqual, 1)
							So(uploading[0].ID(), ShouldEqual, uploadRequest.ID())

							uploadRequest.Status = put.RequestStatusUploaded
							err = client.UpdateFileStatus(uploadRequest)
							So(err, ShouldBeNil)

							uploading, errc = client.UploadingRequests()
							So(errc, ShouldBeNil)
							So(len(uploading), ShouldEqual, 0)
						})

						Convey("Once logged in and with a client", func() {
							token, errl = gas.Login(addr, certPath, admin, "pass")
							So(errl, ShouldBeNil)

							client = NewClient(addr, certPath, token)
							Convey("Client can automatically update server given Putter-style output using SendPutResultsToServer", func() {
								uploadStartsCh := make(chan *put.Request)
								uploadResultsCh := make(chan *put.Request)
								skippedResultsCh := make(chan *put.Request)
								errCh := make(chan error)
								logger := log15.New()

								go func() {
									errCh <- client.SendPutResultsToServer(
										uploadStartsCh, uploadResultsCh, skippedResultsCh,
										minMBperSecondUploadSpeed, minTimeForUpload, logger,
									)
								}()

								requests, errg := client.GetSomeUploadRequests()
								So(errg, ShouldBeNil)
								So(len(requests), ShouldEqual, 8)

								gotSet, err = client.GetSetByID(exampleSet.Requester, exampleSet.ID())
								So(err, ShouldBeNil)
								So(gotSet.Status, ShouldEqual, set.PendingUpload)
								So(gotSet.NumFiles, ShouldEqual, 6)
								So(gotSet.Uploaded, ShouldEqual, 0)

								r := requests[0].Clone()
								r.Status = put.RequestStatusUploading
								r.Size = 1
								uploadStartsCh <- r
								r2 := r.Clone()
								r2.Status = put.RequestStatusUploaded
								uploadResultsCh <- r2

								r3 := requests[1].Clone()
								r3.Status = put.RequestStatusUnmodified
								r3.Size = 2
								skippedResultsCh <- r3

								r4 := requests[2].Clone()
								r4.Status = put.RequestStatusUploading
								r4.Size = 3
								uploadStartsCh <- r4
								r5 := r4.Clone()
								r5.Status = put.RequestStatusReplaced
								uploadResultsCh <- r5

								r6 := requests[3].Clone()
								r6.Status = put.RequestStatusUploading
								r6.Size = 4
								uploadStartsCh <- r6
								r7 := r6.Clone()
								r7.Status = put.RequestStatusFailed
								uploadResultsCh <- r7

								gotSet, err = client.GetSetByID(exampleSet.Requester, exampleSet2.ID())
								So(err, ShouldBeNil)
								So(gotSet.Status, ShouldEqual, set.PendingUpload)
								So(gotSet.NumFiles, ShouldEqual, 3)
								So(gotSet.Uploaded, ShouldEqual, 0)

								r8 := requests[4].Clone()
								r8.Status = put.RequestStatusUploading
								r8.Size = 5
								uploadStartsCh <- r8
								r9 := r8.Clone()
								r9.Stuck = put.NewStuck(time.Now())
								uploadResultsCh <- r9

								close(uploadStartsCh)
								close(uploadResultsCh)
								close(skippedResultsCh)

								err = <-errCh
								So(err, ShouldBeNil)

								gotSet, err = client.GetSetByID(exampleSet.Requester, exampleSet.ID())
								So(err, ShouldBeNil)
								So(gotSet.Status, ShouldEqual, set.Complete)
								So(gotSet.NumFiles, ShouldEqual, 6)
								So(gotSet.SizeFiles, ShouldEqual, 10)
								So(gotSet.Uploaded, ShouldEqual, 3)
								So(gotSet.Failed, ShouldEqual, 1)

								gotSet, err = client.GetSetByID(exampleSet.Requester, exampleSet2.ID())
								So(err, ShouldBeNil)
								So(gotSet.Status, ShouldEqual, set.Uploading)
								So(gotSet.NumFiles, ShouldEqual, 3)
								So(gotSet.Uploaded, ShouldEqual, 0)
								So(gotSet.Error, ShouldBeBlank)
								entries, err = client.GetFiles(exampleSet2.ID())
								So(err, ShouldBeNil)
								So(entries[0].Status, ShouldEqual, set.UploadingEntry)
								So(entries[0].LastError, ShouldContainSubstring, "stuck?")
							})
						})

						Convey("During discovery, you can add a new set", func() {
							files := createManyTestBackupFiles(t)
							dir := filepath.Dir(files[0])

							err = client.SetDirs(exampleSet.ID(), []string{dir})
							So(err, ShouldBeNil)

							err = client.TriggerDiscovery(exampleSet.ID())
							So(err, ShouldBeNil)

							tsCh := make(chan time.Time, 1)
							errCh := make(chan error, 1)
							go func() {
								exampleSet4 := &set.Set{
									Name:        "set4",
									Requester:   exampleSet.Requester,
									Transformer: exampleSet.Transformer,
									Monitor:     false,
								}

								errCh <- client.AddOrUpdateSet(exampleSet4)
								tsCh <- time.Now()
							}()

							ok := <-racCalled
							So(ok, ShouldBeTrue)
							tr := time.Now()

							err = <-errCh
							So(err, ShouldBeNil)
							ts := <-tsCh
							So(ts, ShouldHappenBefore, tr)
						})
					})
				})
			})

			Convey("Which lets you login as admin and add a set", func() {
				token, errl := gas.Login(addr, certPath, admin, "pass")
				So(errl, ShouldBeNil)

				client := NewClient(addr, certPath, token)

				err = client.AddOrUpdateSet(exampleSet)
				So(err, ShouldBeNil)

				_, dirs, discovers, _ := createTestBackupFiles(t, localDir)

				err = client.SetDirs(exampleSet.ID(), dirs)
				So(err, ShouldBeNil)

				gotSet, err := client.GetSetByID(exampleSet.Requester, exampleSet.ID())
				So(err, ShouldBeNil)
				So(gotSet.Status, ShouldEqual, set.PendingDiscovery)
				So(gotSet.NumFiles, ShouldEqual, 0)

				err = client.TriggerDiscovery(exampleSet.ID())
				So(err, ShouldBeNil)

				ok := <-racCalled
				So(ok, ShouldBeTrue)

				gotSet, err = client.GetSetByID(exampleSet.Requester, exampleSet.ID())
				So(err, ShouldBeNil)
				So(gotSet.Status, ShouldEqual, set.PendingUpload)
				So(gotSet.NumFiles, ShouldEqual, len(discovers))
				So(gotSet.Uploaded, ShouldEqual, 0)

				handler, errh := put.GetLocalHandler()
				So(errh, ShouldBeNil)

				logger := log15.New()

				makePutter := func(requests []*put.Request) (*put.Putter, func()) {
					p, errp := put.New(handler, requests)
					So(errp, ShouldBeNil)

					d := func() {
						if errc := p.Cleanup(); errc != nil {
							t.Logf("putter cleanup gave errors: %s", errc)
						}
					}

					err = client.StartingToCreateCollections()
					So(err, ShouldBeNil)
					qs, errg := client.GetQueueStatus()
					So(errg, ShouldBeNil)
					So(qs.CreatingCollections, ShouldEqual, 1)

					err = p.CreateCollections()
					So(err, ShouldBeNil)

					err = client.FinishedCreatingCollections()
					So(err, ShouldBeNil)
					qs, err = client.GetQueueStatus()
					So(err, ShouldBeNil)
					So(qs.CreatingCollections, ShouldEqual, 0)

					return p, d
				}

				Convey("Then you can use a Putter to automatically deal with upload requests", func() {
					requests, errg := client.GetSomeUploadRequests()
					So(errg, ShouldBeNil)
					So(len(requests), ShouldEqual, len(discovers))

					gotSet, err = client.GetSetByID(exampleSet.Requester, exampleSet.ID())
					So(err, ShouldBeNil)
					So(gotSet.Status, ShouldEqual, set.PendingUpload)
					So(gotSet.NumFiles, ShouldEqual, len(discovers))
					So(gotSet.Uploaded, ShouldEqual, 0)
					So(gotSet.Error, ShouldBeBlank)

					p, d := makePutter(requests)
					defer d()

					uploadStarts, uploadResults, skippedResults := p.Put()

					err = client.SendPutResultsToServer(uploadStarts, uploadResults, skippedResults,
						minMBperSecondUploadSpeed, minTimeForUpload, logger)
					So(err, ShouldBeNil)

					gotSet, err = client.GetSetByID(exampleSet.Requester, exampleSet.ID())
					So(err, ShouldBeNil)
					So(gotSet.Status, ShouldEqual, set.Complete)
					So(gotSet.NumFiles, ShouldEqual, len(discovers))
					So(gotSet.Uploaded, ShouldEqual, len(discovers))

					Convey("After completion, re-discovery can find new files and we can re-complete", func() {
						newFile := filepath.Join(dirs[0], "new")
						createFile(t, newFile, 2)
						expectedNumFiles := len(discovers) + 1

						err = client.TriggerDiscovery(exampleSet.ID())
						So(err, ShouldBeNil)

						ok := <-racCalled
						So(ok, ShouldBeTrue)

						gotSet, err = client.GetSetByID(exampleSet.Requester, exampleSet.ID())
						So(err, ShouldBeNil)
						So(gotSet.Status, ShouldEqual, set.PendingUpload)
						So(gotSet.NumFiles, ShouldEqual, expectedNumFiles)
						So(gotSet.Uploaded, ShouldEqual, 0)

						requests, errg = client.GetSomeUploadRequests()
						So(errg, ShouldBeNil)
						So(len(requests), ShouldEqual, expectedNumFiles)

						gotSet, err = client.GetSetByID(exampleSet.Requester, exampleSet.ID())
						So(err, ShouldBeNil)
						So(gotSet.Status, ShouldEqual, set.PendingUpload)
						So(gotSet.NumFiles, ShouldEqual, expectedNumFiles)
						So(gotSet.Uploaded, ShouldEqual, 0)

						p, d = makePutter(requests)
						defer d()

						uploadStarts, uploadResults, skippedResults = p.Put()

						err = client.SendPutResultsToServer(uploadStarts, uploadResults, skippedResults,
							minMBperSecondUploadSpeed, minTimeForUpload, logger)
						So(err, ShouldBeNil)

						gotSet, err = client.GetSetByID(exampleSet.Requester, exampleSet.ID())
						So(err, ShouldBeNil)
						So(gotSet.Status, ShouldEqual, set.Complete)
						So(gotSet.NumFiles, ShouldEqual, expectedNumFiles)
						So(gotSet.Uploaded, ShouldEqual, expectedNumFiles)

						entries, errg := client.GetFiles(exampleSet.ID())
						So(errg, ShouldBeNil)
						So(len(entries), ShouldEqual, expectedNumFiles)

						for _, entry := range entries {
							So(entry.Status, ShouldEqual, set.Uploaded)
						}

						discovers = append(discovers, newFile)

						for _, local := range discovers {
							remote := strings.Replace(local, localDir, remoteDir, 1)

							_, err = os.Stat(remote)
							So(err, ShouldBeNil)
						}
					})
				})

				Convey("The system handles files that become missing", func() {
					requests, errg := client.GetSomeUploadRequests()
					So(errg, ShouldBeNil)
					So(len(requests), ShouldEqual, len(discovers))

					p, d := makePutter(requests)
					defer d()

					os.Remove(discovers[0])
					uploadStarts, uploadResults, skippedResults := p.Put()

					err = client.SendPutResultsToServer(uploadStarts, uploadResults, skippedResults,
						minMBperSecondUploadSpeed, minTimeForUpload, logger)
					So(err, ShouldBeNil)

					gotSet, err = client.GetSetByID(exampleSet.Requester, exampleSet.ID())
					So(err, ShouldBeNil)
					So(gotSet.Status, ShouldEqual, set.Complete)
					So(gotSet.NumFiles, ShouldEqual, len(discovers))
					So(gotSet.Uploaded, ShouldEqual, len(discovers)-1)
					So(gotSet.Missing, ShouldEqual, 1)
					So(gotSet.Error, ShouldBeBlank)
				})

				Convey("The system handles failures with retries", func() {
					for i, local := range discovers {
						remote := strings.Replace(local, localDir, remoteDir, 1)

						switch i {
						case 0:
							handler.MakeStatFail(remote)
						case 1:
							handler.MakePutFail(remote)
						case 2:
							handler.MakeMetaFail(remote)
						}
					}

					for i := 1; i < int(jobRetries)+1; i++ {
						requests, errg := client.GetSomeUploadRequests()
						So(errg, ShouldBeNil)
						So(len(requests), ShouldEqual, len(discovers))

						p, d := makePutter(requests)
						defer d()

						uploadStarts, uploadResults, skippedResults := p.Put()

						err = client.SendPutResultsToServer(uploadStarts, uploadResults, skippedResults,
							minMBperSecondUploadSpeed, minTimeForUpload, logger)
						So(err, ShouldBeNil)

						gotSet, err = client.GetSetByID(exampleSet.Requester, exampleSet.ID())
						So(err, ShouldBeNil)
						So(gotSet.Status, ShouldEqual, set.Complete)
						So(gotSet.NumFiles, ShouldEqual, len(discovers))
						So(gotSet.Uploaded, ShouldEqual, 0)
						So(gotSet.Missing, ShouldEqual, 0)
						So(gotSet.Failed, ShouldEqual, len(discovers))
						So(gotSet.Error, ShouldBeBlank)

						entries, errg := client.GetFiles(exampleSet.ID())
						So(errg, ShouldBeNil)
						So(len(entries), ShouldEqual, len(discovers))

						for j, entry := range entries {
							So(entry.Status, ShouldEqual, set.Failed)
							So(entry.Attempts, ShouldEqual, i)

							switch j {
							case 0:
								So(entry.LastError, ShouldEqual, put.ErrMockStatFail)
							case 1:
								So(entry.LastError, ShouldEqual, put.ErrMockPutFail)
							case 2:
								So(entry.LastError, ShouldEqual, put.ErrMockMetaFail)
							}
						}
					}
				})

				Convey("The system issues a failure when local files hang when opened", func() {
					requests, errg := client.GetSomeUploadRequests()
					So(errg, ShouldBeNil)
					So(len(requests), ShouldEqual, len(discovers))

					p, d := makePutter(requests)
					defer d()

					p.SetFileReadTimeout(1 * time.Millisecond)
					p.SetFileReadTester(func(ctx context.Context, path string) error {
						if path == discovers[0] {
							<-time.After(5 * time.Millisecond)
						}

						return nil
					})

					uploadStarts, uploadResults, skippedResults := p.Put()

					err = client.SendPutResultsToServer(uploadStarts, uploadResults, skippedResults,
						minMBperSecondUploadSpeed, minTimeForUpload, logger)
					So(err, ShouldBeNil)

					gotSet, err = client.GetSetByID(exampleSet.Requester, exampleSet.ID())
					So(err, ShouldBeNil)
					So(gotSet.Status, ShouldEqual, set.Complete)
					So(gotSet.NumFiles, ShouldEqual, len(discovers))
					So(gotSet.Uploaded, ShouldEqual, len(discovers)-1)
					So(gotSet.Missing, ShouldEqual, 0)
					So(gotSet.Failed, ShouldEqual, 1)
					So(gotSet.Error, ShouldBeBlank)

					entries, errg := client.GetFiles(exampleSet.ID())
					So(errg, ShouldBeNil)
					So(len(entries), ShouldEqual, len(discovers))

					for i, entry := range entries {
						if i == 0 {
							So(entry.Status, ShouldEqual, set.Failed)
							So(entry.Attempts, ShouldEqual, 1)
							So(entry.LastError, ShouldContainSubstring, put.ErrReadTimeout)
						} else {
							So(entry.Status, ShouldEqual, set.Uploaded)
							So(entry.Attempts, ShouldEqual, 1)
						}
					}
				})

				Convey("The system warns of possibly stuck uploads", func() {
					handler.MakePutSlow(discovers[0], 300*time.Millisecond)

					requests, errg := client.GetSomeUploadRequests()
					So(errg, ShouldBeNil)
					So(len(requests), ShouldEqual, len(discovers))

					p, d := makePutter(requests)
					defer d()

					errCh := make(chan error)

					go func() {
						uploadStarts, uploadResults, skippedResults := p.Put()

						errCh <- client.SendPutResultsToServer(uploadStarts, uploadResults, skippedResults,
							minMBperSecondUploadSpeed, 100*time.Millisecond, logger)
					}()

					<-time.After(200 * time.Millisecond)
					gotSet, err = client.GetSetByID(exampleSet.Requester, exampleSet.ID())
					So(err, ShouldBeNil)
					So(gotSet.Status, ShouldEqual, set.Uploading)

					entries, errg := client.GetFiles(exampleSet.ID())
					So(errg, ShouldBeNil)
					So(len(entries), ShouldEqual, len(discovers))
					So(entries[0].Status, ShouldEqual, set.UploadingEntry)
					So(entries[0].LastError, ShouldContainSubstring, "upload stuck?")

					So(<-errCh, ShouldBeNil)

					gotSet, err = client.GetSetByID(exampleSet.Requester, exampleSet.ID())
					So(err, ShouldBeNil)
					So(gotSet.Status, ShouldEqual, set.Complete)
					So(gotSet.NumFiles, ShouldEqual, len(discovers))
					So(gotSet.Uploaded, ShouldEqual, len(discovers))
					So(gotSet.Missing, ShouldEqual, 0)
					So(gotSet.Failed, ShouldEqual, 0)
					So(gotSet.Error, ShouldBeBlank)

					entries, errg = client.GetFiles(exampleSet.ID())
					So(errg, ShouldBeNil)
					So(len(entries), ShouldEqual, len(discovers))

					for _, entry := range entries {
						So(entry.Status, ShouldEqual, set.Uploaded)
						So(entry.Attempts, ShouldEqual, 1)
						So(entry.LastError, ShouldBeBlank)
					}
				})

				Convey("numRequestsToReserve returns appropriate numbers", func() {
					So(s.numRequestsToReserve(), ShouldEqual, len(discovers))

					s.numClients = len(discovers)
					So(s.numRequestsToReserve(), ShouldEqual, 1)

					s.numClients++
					So(s.numRequestsToReserve(), ShouldEqual, 1)

					numRequests := 1000
					ids := make([]*queue.ItemDef, numRequests-len(discovers))

					for i := range ids {
						key := fmt.Sprintf("%d", i)
						ids[i] = &queue.ItemDef{
							Key:  key,
							Data: &put.Request{Set: key},
							TTR:  ttr,
						}
					}

					_, _, err := s.queue.AddMany(context.Background(), ids)
					So(err, ShouldBeNil)

					s.numClients = 10
					So(s.numRequestsToReserve(), ShouldEqual, numRequests/s.numClients)

					numExtra := 200
					ids = make([]*queue.ItemDef, numExtra)

					for i := range ids {
						key := fmt.Sprintf("%d.extra", i)
						ids[i] = &queue.ItemDef{
							Key:  key,
							Data: &put.Request{Set: key},
							TTR:  ttr,
						}
					}

					_, _, err = s.queue.AddMany(context.Background(), ids)
					So(err, ShouldBeNil)

					So(s.numRequestsToReserve(), ShouldEqual, maxRequestsToReserve)

					for i := 0; i < s.numClients; i++ {
						rs, errr := s.reserveRequests()
						So(errr, ShouldBeNil)
						So(len(rs), ShouldEqual, maxRequestsToReserve)
					}

					for i := 0; i < s.numClients; i++ {
						rs, errr := s.reserveRequests()
						So(errr, ShouldBeNil)
						So(len(rs), ShouldEqual, numExtra/s.numClients)
					}

					rs, errr := s.reserveRequests()
					So(errr, ShouldBeNil)
					So(len(rs), ShouldEqual, 0)
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
// expecting to discover in the dirs. Also created is a symlink in one of the
// directories; the path to this is returned separately.
func createTestBackupFiles(t *testing.T, dir string) ([]string, []string, []string, string) {
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

	symlinkPath := filepath.Join(dirs[0], "symlink")
	if err := os.Symlink(files[0], symlinkPath); err != nil {
		t.Fatalf("failed to create symlink: %s", err)
	}

	return files, dirs, discovers, symlinkPath
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

// createManyTestBackupFiles creates and returns the paths to many files in a
// new temp directory.
func createManyTestBackupFiles(t *testing.T) []string {
	t.Helper()
	dir := t.TempDir()

	paths := make([]string, numManyTestFiles)

	for i := range paths {
		path := filepath.Join(dir, fmt.Sprintf("%d.file", i))
		paths[i] = path
		createFile(t, path, 1)
	}

	return paths
}
