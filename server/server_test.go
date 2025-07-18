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
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/user"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/VertebrateResequencing/wr/queue"
	"github.com/gin-gonic/gin"
	"github.com/inconshreveable/log15"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/viant/ptrie"
	gas "github.com/wtsi-hgi/go-authserver"
	"github.com/wtsi-hgi/ibackup/internal"
	"github.com/wtsi-hgi/ibackup/remove"
	"github.com/wtsi-hgi/ibackup/set"
	"github.com/wtsi-hgi/ibackup/slack"
	"github.com/wtsi-hgi/ibackup/transfer"
	btime "github.com/wtsi-ssg/wr/backoff/time"
	"github.com/wtsi-ssg/wr/retry"
)

const (
	userPerms        = 0700
	numManyTestFiles = 5000
)

var errNotDiscovered = errors.New("not discovered")
var errNotFinishedRemoving = errors.New("remove not finished")
var errNotAllRemoved = errors.New("not all removals finished")

func TestClient(t *testing.T) {
	Convey("maxTimeForUpload works with small and large requests", t, func() {
		min := 10 * time.Millisecond

		c := &Client{
			minMBperSecondUploadSpeed: float64(10),
			minTimeForUpload:          min,
		}

		d := c.maxTimeForUpload(&transfer.Request{Size: 10 * bytesInMiB})
		So(d, ShouldEqual, 1*time.Second)

		d = c.maxTimeForUpload(&transfer.Request{Size: 1000 * bytesInMiB})
		So(d, ShouldEqual, 100*time.Second)

		d = c.maxTimeForUpload(&transfer.Request{Size: 1 * bytesInMiB})
		So(d, ShouldEqual, 100*time.Millisecond)

		d = c.maxTimeForUpload(&transfer.Request{Size: 1})
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
	maxStuckTime := 1 * time.Hour
	defaultHeartbeatFreq := 1 * time.Minute

	Convey("Given a test cert and db location", t, func() {
		certPath, keyPath, err := gas.CreateTestCert(t)
		So(err, ShouldBeNil)

		dbPath := createDBLocation(t)

		Convey("You can make a Server without a logger configured, but it isn't usable", func() {
			s, errn := New(Config{})
			So(errn, ShouldNotBeNil)
			So(s, ShouldBeNil)
		})

		localDir := t.TempDir()
		remoteDir := filepath.Join(localDir, "remote")
		exampleSet := &set.Set{
			Name:        "set1",
			Requester:   "jim",
			Transformer: "prefix=" + localDir + ":" + remoteDir,
			MonitorTime: 0,
		}

		handler := internal.GetLocalHandler()
		handler.MakeRemoveSlow()

		logWriter := gas.NewStringLogger()
		slackWriter := gas.NewStringLogger()
		conf := Config{
			HTTPLogger:     logWriter,
			StorageHandler: handler,
		}

		Convey("You can make a Server with a logger configured and no slacker", func() {
			conf.StillRunningMsgFreq = 1 * time.Millisecond
			s, errn := New(conf)
			So(errn, ShouldBeNil)

			err = s.EnableAuthWithServerToken(certPath, keyPath, ".ibackup.test.servertoken", func(u, p string) (bool, string) {
				return true, "1"
			})
			So(err, ShouldBeNil)

			err = s.MakeQueueEndPoints()
			So(err, ShouldBeNil)

			err = s.LoadSetDB(dbPath, "")
			So(err, ShouldBeNil)

			addr, dfunc, err := gas.StartTestServer(s, certPath, keyPath)
			So(err, ShouldBeNil)

			defer func() {
				errd := dfunc()
				So(errd, ShouldBeNil)
			}()

			token, errl := gas.Login(gas.NewClientRequest(addr, certPath), "jim", "pass")
			So(errl, ShouldBeNil)
			So(token, ShouldNotBeBlank)

			client := NewClient(addr, certPath, token)

			err = client.AddOrUpdateSet(exampleSet)
			So(err, ShouldBeNil)

			Convey("Endpoints still work with no slacker", func() {
				router := gin.Default()
				router.POST("/irods/:hostpid", s.clientMadeIRODSConnections)
				router.DELETE("/irods/:hostpid", s.clientClosedIRODSConnections)

				ctx := context.Background()
				req, err := http.NewRequestWithContext(ctx, http.MethodPost, "/irods/testPID?nconnections=5", nil)
				So(err, ShouldBeNil)

				w := httptest.NewRecorder()

				router.ServeHTTP(w, req)

				So(http.StatusOK, ShouldEqual, w.Code)

				req, err = http.NewRequestWithContext(ctx, http.MethodDelete, "/irods/testPID", nil)
				So(err, ShouldBeNil)

				w = httptest.NewRecorder()

				router.ServeHTTP(w, req)

				So(http.StatusOK, ShouldEqual, w.Code)
			})
		})

		conf.Slacker = slack.NewMock(slackWriter)

		const (
			serverStartMessage = slack.BoxPrefixInfo + "server starting, loading database" +
				slack.BoxPrefixSuccess + "server loaded database"
			serverRecoveryMessage = slack.BoxPrefixSuccess + "recovery completed"
			serverStartedMessage  = slack.BoxPrefixSuccess + "server started"
		)

		makeAndStartServer := func() (*Server, string, func() error) {
			s, errn := New(conf)
			So(errn, ShouldBeNil)

			err = s.EnableAuthWithServerToken(certPath, keyPath, ".ibackup.test.servertoken", func(u, p string) (bool, string) {
				return true, "1"
			})
			So(err, ShouldBeNil)

			err = s.MakeQueueEndPoints()
			So(err, ShouldBeNil)

			slackWriter.Reset()

			err = s.LoadSetDB(dbPath, "")
			So(err, ShouldBeNil)

			addr, dfunc, errs := gas.StartTestServer(s, certPath, keyPath)
			So(errs, ShouldBeNil)
			So(slackWriter.String(), ShouldStartWith, serverStartMessage)
			So(slackWriter.String(), ShouldContainSubstring, serverRecoveryMessage)
			So(slackWriter.String(), ShouldContainSubstring, serverStartedMessage)

			return s, addr, dfunc
		}

		Convey("You can make a server that frequently logs to slack that it is still running, until it isn't", func() {
			conf.StillRunningMsgFreq = 200 * time.Millisecond
			_, _, dfunc := makeAndStartServer()

			slackWriter.Reset()

			time.Sleep(conf.StillRunningMsgFreq)

			expectedMsg := slack.BoxPrefixInfo + "server is still running"
			So(slackWriter.String(), ShouldEqual, expectedMsg)

			time.Sleep(conf.StillRunningMsgFreq)

			So(slackWriter.String(), ShouldEqual, expectedMsg+expectedMsg)

			err = dfunc()
			So(err, ShouldBeNil)

			time.Sleep(conf.StillRunningMsgFreq)

			So(slackWriter.String(), ShouldEqual, expectedMsg+expectedMsg+slack.BoxPrefixWarn+"server stopped")
		})

		Convey("You can make a Server with a logger configured and setup Auth, MakeQueueEndPoints and LoadSetDB", func() {
			s, addr, dfunc := makeAndStartServer()

			serverStopped := false

			defer func() {
				if serverStopped {
					return
				}

				slackWriter.Reset()

				errd := dfunc()
				So(errd, ShouldBeNil)

				So(slackWriter.String(), ShouldContainSubstring, slack.BoxPrefixWarn+"server stopped")

				slackWriter.Reset()
			}()

			token, errl := gas.Login(gas.NewClientRequest(addr, certPath), admin, "pass")
			So(errl, ShouldBeNil)

			adminClient := NewClient(addr, certPath, token)

			var racRequests []*transfer.Request
			racCalls := 0
			racCalled := make(chan bool, 1)

			s.queue.SetReadyAddedCallback(func(queuename string, allitemdata []interface{}) {
				racRequests = make([]*transfer.Request, len(allitemdata))

				for i, item := range allitemdata {
					r, ok := item.(*transfer.Request)

					if !ok {
						racCalled <- false

						return
					}

					racRequests[i] = r
				}

				racCalls++
				racCalled <- true
			})

			Convey("Which lets you login", func() {
				logWriter.Reset()

				token, errl := gas.Login(gas.NewClientRequest(addr, certPath), "jim", "pass")
				So(errl, ShouldBeNil)
				So(token, ShouldNotBeBlank)

				So(strings.Count(logWriter.String(), "STATUS=200"), ShouldEqual, 1)

				Convey("And then you use client methods AddOrUpdateSet (which logs to slack) and GetSets", func() {
					exampleSet2 := &set.Set{
						Name:        "set2",
						Requester:   exampleSet.Requester,
						Transformer: exampleSet.Transformer,
						MonitorTime: 5 * time.Minute,
					}

					exampleSet3 := &set.Set{
						Name:        "set3",
						Requester:   exampleSet.Requester,
						Transformer: exampleSet.Transformer,
						MonitorTime: 0,
					}

					slackWriter.Reset()

					client := NewClient(addr, certPath, token)

					err = client.AddOrUpdateSet(exampleSet)
					So(err, ShouldBeNil)

					So(slackWriter.String(), ShouldBeBlank)

					So(strings.Count(logWriter.String(), "STATUS=200"), ShouldEqual, 2)

					sets, errg := client.GetSets(exampleSet.Requester)
					So(errg, ShouldBeNil)
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

					Convey("And given two hardlinks to the same file", func() {
						file1local := filepath.Join(localDir, "file1")
						hardlink1local := filepath.Join(localDir, "hardlink1")
						hardlink1Remote := filepath.Join(remoteDir, "hardlink1")
						hardlink2local := filepath.Join(localDir, "hardlink2")
						hardlink2Remote := filepath.Join(remoteDir, "hardlink2")
						inodeRemote := filepath.Join(remoteDir, "inode")

						internal.CreateTestFileOfLength(t, file1local, 1)
						internal.CreateTestFileOfLength(t, inodeRemote, 1)

						createRemoteHardlink(t, s.storageHandler, hardlink1local, hardlink1Remote, file1local, inodeRemote, exampleSet)
						createRemoteHardlink(t, s.storageHandler, hardlink2local, hardlink2Remote, file1local, inodeRemote, exampleSet)

						err = client.MergeFiles(exampleSet.ID(), []string{file1local, hardlink1local, hardlink2local})
						So(err, ShouldBeNil)

						err = client.AddOrUpdateSet(exampleSet)
						So(err, ShouldBeNil)

						err = client.TriggerDiscovery(exampleSet.ID())
						So(err, ShouldBeNil)

						ok := <-racCalled
						So(ok, ShouldBeTrue)

						Convey("You can remove the first hardlink and the inode file will stay", func() {
							remReq := set.RemoveReq{
								Path: hardlink1local,
								Set:  exampleSet,
							}

							err = s.removeFileFromIRODSandDB(&remReq)
							So(err, ShouldBeNil)

							_, err = os.Stat(hardlink1Remote)
							So(err, ShouldNotBeNil)
							So(err.Error(), ShouldContainSubstring, "no such file or directory")

							_, err = os.Stat(inodeRemote)
							So(err, ShouldBeNil)

							Convey("Then you can remove the second hardlink and the inode file will also get removed", func() {
								remReq = set.RemoveReq{
									Path: hardlink2local,
									Set:  exampleSet,
								}

								err = s.removeFileFromIRODSandDB(&remReq)
								So(err, ShouldBeNil)

								_, err = os.Stat(hardlink2Remote)
								So(err, ShouldNotBeNil)
								So(err.Error(), ShouldContainSubstring, "no such file or directory")

								_, err = os.Stat(inodeRemote)
								So(err, ShouldNotBeNil)
								So(err.Error(), ShouldContainSubstring, "no such file or directory")
							})
						})

						Convey("And given an inode bucket that's out of sync with iRODS", func() {
							info, errls := os.Lstat(hardlink1local)
							So(errls, ShouldBeNil)

							statt, ok := info.Sys().(*syscall.Stat_t)
							So(ok, ShouldBeTrue)

							files, errg := s.db.GetFilesFromInode(statt.Ino, s.db.GetMountPointFromPath(hardlink1local))
							So(errg, ShouldBeNil)
							So(files, ShouldContain, hardlink1local)

							err = s.db.RemoveFileFromInode(hardlink1local, statt.Ino)
							So(err, ShouldBeNil)

							files, err = s.db.GetFilesFromInode(statt.Ino, s.db.GetMountPointFromPath(hardlink1local))
							So(err, ShouldBeNil)
							So(files, ShouldNotContain, hardlink1local)

							Convey("You can remove the first hardlink and the inode file will stay", func() {
								remReq := set.RemoveReq{
									Path: hardlink1local,
									Set:  exampleSet,
								}

								err = s.removeFileFromIRODSandDB(&remReq)
								So(err, ShouldBeNil)

								_, err = os.Stat(hardlink1Remote)
								So(err, ShouldNotBeNil)
								So(err.Error(), ShouldContainSubstring, "no such file or directory")

								_, err = os.Stat(inodeRemote)
								So(err, ShouldBeNil)
							})
						})

						Convey("And if you remove the original file and the first hardlink locally and add a third hardlink", func() {
							hardlink3local := filepath.Join(localDir, "hardlink3")
							hardlink3Remote := filepath.Join(remoteDir, "hardlink3")
							createRemoteHardlink(t, s.storageHandler, hardlink3local, hardlink3Remote, file1local, inodeRemote, exampleSet)

							os.Remove(file1local)
							os.Remove(hardlink1local)

							err = client.MergeFiles(exampleSet.ID(), []string{hardlink3local})
							So(err, ShouldBeNil)

							err = client.AddOrUpdateSet(exampleSet)
							So(err, ShouldBeNil)

							err = client.TriggerDiscovery(exampleSet.ID())
							So(err, ShouldBeNil)

							ok := <-racCalled
							So(ok, ShouldBeTrue)

							Convey("Removing the third hardlink does not remove the inode as the database is still in sync with iRODS", func() { //nolint:lll
								remReq := set.RemoveReq{
									Path: hardlink3local,
									Set:  exampleSet,
								}

								err = s.removeFileFromIRODSandDB(&remReq)
								So(err, ShouldBeNil)

								_, err = os.Stat(hardlink3Remote)
								So(err, ShouldNotBeNil)
								So(err.Error(), ShouldContainSubstring, "no such file or directory")

								_, err = os.Stat(inodeRemote)
								So(err, ShouldBeNil)
							})
						})
					})

					Convey("And given a set with a folder and a nested folder with 2 files", func() {
						dir0local := filepath.Join(localDir, "dir0/")
						dir0remote := filepath.Join(remoteDir, "dir0/")

						dir1local := filepath.Join(dir0local, "dir1/")
						dir1remote := filepath.Join(dir0remote, "dir1/")

						dir2local := filepath.Join(dir1local, "dir1/")
						dir2remote := filepath.Join(dir1remote, "dir1/")

						err = os.MkdirAll(dir1local, 0755)
						So(err, ShouldBeNil)

						err = os.MkdirAll(dir1remote, 0755)
						So(err, ShouldBeNil)

						file1local := filepath.Join(dir2local, "file1")
						internal.CreateTestFile(t, file1local, "file content")

						file1remote := filepath.Join(dir2remote, "file1")
						internal.CreateTestFile(t, file1remote, "file content")

						file2remote := filepath.Join(dir2remote, "file2")
						internal.CreateTestFile(t, file2remote, "file content")

						hardlinkMeta := map[string]string{
							transfer.MetaKeySets:      exampleSet.Name,
							transfer.MetaKeyRequester: exampleSet.Requester,
						}

						err = handler.AddMeta(file1remote, hardlinkMeta)
						So(err, ShouldBeNil)

						err = client.MergeDirs(exampleSet.ID(), []string{dir1local})
						So(err, ShouldBeNil)

						err = client.TriggerDiscovery(exampleSet.ID())
						So(err, ShouldBeNil)

						ok := <-racCalled
						So(ok, ShouldBeTrue)

						Convey("Removal on a pending file returns an error", func() {
							err = client.RemoveFilesAndDirs(exampleSet.ID(), []string{file1local})
							So(err, ShouldNotBeNil)
							So(err.Error(), ShouldContainSubstring, set.ErrRemovalWhenSetNotComplete)
						})

						Convey("Removal on a folder with pending files in it returns an error", func() {
							err = client.RemoveFilesAndDirs(exampleSet.ID(), []string{dir2local})
							So(err, ShouldNotBeNil)
							So(err.Error(), ShouldContainSubstring, set.ErrRemovalWhenSetNotComplete)
						})

						Convey("Removal of failed files removes entries from Failed bucket", func() {
							changeSetFilesStatus(1, exampleSet.Name, adminClient, transfer.RequestStatusFailed)

							failedEntries, _, errg := s.db.GetFailedEntries(exampleSet.ID())
							So(errg, ShouldBeNil)
							So(len(failedEntries), ShouldEqual, 1)

							err = client.RemoveFilesAndDirs(exampleSet.ID(), []string{file1local})
							So(err, ShouldBeNil)

							waitForRemovals(t, client, exampleSet)

							failedEntries, _, err = s.db.GetFailedEntries(exampleSet.ID())
							So(err, ShouldBeNil)
							So(len(failedEntries), ShouldEqual, 0)
						})

						Convey("And given all files are uploaded", func() {
							makeGivenSetComplete(1, exampleSet.Name, adminClient)

							Convey("Removal on a file and a dir sets them as complete in the remove bucket", func() {
								err = client.RemoveFilesAndDirs(exampleSet.ID(), []string{file1local, dir2local})
								So(err, ShouldBeNil)

								waitForRemovals(t, client, exampleSet)

								incompleteRemReqs, errg := s.db.GetIncompleteRemoveRequests()
								So(errg, ShouldBeNil)
								So(incompleteRemReqs, ShouldBeEmpty)
							})

							Convey("Removal on a file doesn't remove the dir and doesn't log anything", func() {
								logWriter.Reset()

								err = client.RemoveFilesAndDirs(exampleSet.ID(), []string{file1local})
								So(err, ShouldBeNil)

								waitForRemovals(t, client, exampleSet)

								_, err = os.Stat(file1remote)
								So(err, ShouldNotBeNil)

								_, err = os.Stat(dir1remote)
								So(err, ShouldBeNil)

								So(logWriter.String(), ShouldNotContainSubstring, "dir removal error")
							})

							Convey("Remove on the parent folder removes the nested folder from the db", func() {
								err = client.RemoveFilesAndDirs(exampleSet.ID(), []string{dir1local})
								So(err, ShouldBeNil)

								waitForRemovals(t, client, exampleSet)

								entries, errg := s.db.GetAllDirEntries(exampleSet.ID())
								So(errg, ShouldBeNil)
								So(len(entries), ShouldEqual, 0)
							})

							Convey("If the folder has no access permissions, removal on a file will log the error", func() {
								err = os.Chmod(dir1remote, 0555)
								So(err, ShouldBeNil)

								logWriter.Reset()

								_, err = os.Stat(file1remote)
								So(err, ShouldBeNil)

								err = client.RemoveFilesAndDirs(exampleSet.ID(), []string{file1local})
								So(err, ShouldBeNil)

								waitForRemovals(t, client, exampleSet)

								_, err = os.Stat(file1remote)
								So(err, ShouldNotBeNil)

								_, err = os.Stat(dir2remote)
								So(err, ShouldBeNil)

								So(logWriter.String(), ShouldContainSubstring, "dir removal error")

								os.Chmod(dir1remote, 0755) //nolint:errcheck
							})
						})

						Convey("And having both files in a set", func() {
							file2local := filepath.Join(dir2local, "file2")
							internal.CreateTestFile(t, file2local, "file content")

							err = client.TriggerDiscovery(exampleSet.ID())
							So(err, ShouldBeNil)

							files, errg := client.GetFiles(exampleSet.ID())
							So(errg, ShouldBeNil)
							So(files, ShouldHaveLength, 2)

							Convey("And if you remove one file", func() {
								err = os.Remove(file1local)
								So(err, ShouldBeNil)

								Convey("You can still see both files after rediscovery", func() {
									err = client.TriggerDiscovery(exampleSet.ID())
									So(err, ShouldBeNil)

									files, err = client.GetFiles(exampleSet.ID())
									So(err, ShouldBeNil)
									So(files, ShouldHaveLength, 2)
								})
							})

							Convey("And if you remove one folder", func() {
								err = os.RemoveAll(dir1local)
								So(err, ShouldBeNil)

								dirs, errg := s.db.GetAllDirEntries(exampleSet.ID())
								So(errg, ShouldBeNil)
								So(dirs, ShouldHaveLength, 2)

								Convey("You can still see original files and folders after rediscovery", func() {
									err = client.TriggerDiscovery(exampleSet.ID())
									So(err, ShouldBeNil)

									files, err = client.GetFiles(exampleSet.ID())
									So(err, ShouldBeNil)
									So(files, ShouldHaveLength, 2)

									dirs, err = s.db.GetAllDirEntries(exampleSet.ID())
									So(err, ShouldBeNil)
									So(dirs, ShouldHaveLength, 2)
									So(dirs[0].Status, ShouldEqual, set.Orphaned)
								})
							})
						})
					})

					Convey("And given a set created without a discovered folders bucket", func() {
						dir1 := filepath.Join(localDir, "dir1/")
						dir2 := filepath.Join(dir1, "dir2/")
						dir3 := filepath.Join(dir2, "dir3/")

						err = os.MkdirAll(dir3, 0755)
						So(err, ShouldBeNil)

						file := filepath.Join(dir3, "file")
						internal.CreateTestFile(t, file, "file content")

						err = client.MergeDirs(exampleSet.ID(), []string{dir1})
						So(err, ShouldBeNil)

						err = client.TriggerDiscovery(exampleSet.ID())
						So(err, ShouldBeNil)

						ok := <-racCalled
						So(ok, ShouldBeTrue)

						err = s.db.DeleteDiscoveredFoldersBucket(exampleSet.ID())
						So(err, ShouldBeNil)

						Convey("Remove on a folder not specified should still work", func() {
							makeGivenSetComplete(1, exampleSet.Name, adminClient)

							err = client.RemoveFilesAndDirs(exampleSet.ID(), []string{dir2})
							So(err, ShouldBeNil)

							waitForRemovals(t, client, exampleSet)

							files, errgf := client.GetFiles(exampleSet.ID())
							So(errgf, ShouldBeNil)
							So(len(files), ShouldEqual, 0)

							dirs, errgd := client.GetDirs(exampleSet.ID())
							So(errgd, ShouldBeNil)
							So(len(dirs), ShouldEqual, 1)
						})
					})

					Convey("And given a set with 100 files in one nested folder", func() {
						dir1 := filepath.Join(localDir, "dir1/")
						dir2 := filepath.Join(dir1, "dir2/")
						dir3 := filepath.Join(dir1, "dir3/")

						err = os.MkdirAll(dir1, 0755)
						So(err, ShouldBeNil)

						err = os.MkdirAll(dir2, 0755)
						So(err, ShouldBeNil)

						err = os.MkdirAll(dir3, 0755)
						So(err, ShouldBeNil)

						dirs := []string{dir2, dir3}

						filesInSet := 200
						for i := range filesInSet {
							index := i / 100

							file := filepath.Join(dirs[index], "file"+strconv.Itoa(i))
							internal.CreateTestFile(t, file, "file content")
						}

						err = client.MergeDirs(exampleSet.ID(), []string{dir1})
						So(err, ShouldBeNil)

						err = client.TriggerDiscovery(exampleSet.ID())
						So(err, ShouldBeNil)

						ok := <-racCalled
						So(ok, ShouldBeTrue)

						gotSet, errg := client.GetSetByID(exampleSet.Requester, exampleSet.ID())
						So(errg, ShouldBeNil)

						So(gotSet.NumFiles, ShouldEqual, filesInSet)

						makeGivenSetComplete(200, exampleSet.Name, adminClient)

						Convey("You can trigger removals", func() {
							err = client.RemoveFilesAndDirs(exampleSet.ID(), []string{dir2})
							So(err, ShouldBeNil)

							err = client.RemoveFilesAndDirs(exampleSet.ID(), []string{dir3})
							So(err, ShouldBeNil)

							time.Sleep(50 * time.Millisecond)

							gotSet, errg = client.GetSetByID(exampleSet.Requester, exampleSet.ID())
							So(errg, ShouldBeNil)

							So(gotSet.NumObjectsRemoved, ShouldBeGreaterThan, 0)
							So(gotSet.NumObjectsToBeRemoved, ShouldEqual, filesInSet+2)

							Convey("And you can trigger and complete discovery of the extra file while removals are running", func() {
								fileToBeDiscovered := filepath.Join(dir1, "file"+strconv.Itoa(filesInSet+1))
								internal.CreateTestFile(t, fileToBeDiscovered, "file content")

								err = client.TriggerDiscovery(exampleSet.ID())
								So(err, ShouldBeNil)

								ok = <-racCalled
								So(ok, ShouldBeTrue)

								gotSet, errg = client.GetSetByID(exampleSet.Requester, exampleSet.ID())
								So(errg, ShouldBeNil)
								So(gotSet.NumFiles, ShouldBeBetween, 1, filesInSet)
								So(gotSet.NumObjectsRemoved, ShouldBeLessThan, gotSet.NumObjectsToBeRemoved)

								Convey("And then the removals will still complete", func() {
									time.Sleep(1000 * time.Millisecond)

									gotSet, errg = client.GetSetByID(exampleSet.Requester, exampleSet.ID())
									So(errg, ShouldBeNil)

									func() {
										ctx, cancelFn := context.WithTimeout(context.Background(), 5*time.Second)
										defer cancelFn()

										status := retry.Do(ctx, func() error {
											gotSet, errg = client.GetSetByID(exampleSet.Requester, exampleSet.ID())
											So(errg, ShouldBeNil)

											if gotSet.NumObjectsRemoved == gotSet.NumObjectsToBeRemoved {
												return nil
											}

											return errNotFinishedRemoving
										}, &retry.UntilNoError{}, btime.SecondsRangeBackoff(), "waiting for matching status")

										if status.Err != nil {
											fmt.Printf("\nfailed to see remove finished. %d removed out of %d", //nolint:forbidigo
												gotSet.NumObjectsRemoved, gotSet.NumObjectsToBeRemoved)
										}

										So(status.Err, ShouldBeNil)
									}()

									So(gotSet.NumObjectsRemoved, ShouldEqual, gotSet.NumObjectsToBeRemoved)
									So(gotSet.NumFiles, ShouldEqual, 1)

									files, errgf := client.GetFiles(gotSet.ID())
									So(errgf, ShouldBeNil)
									So(len(files), ShouldEqual, 1)

									dirs, errgd := client.GetDirs(gotSet.ID())
									So(errgd, ShouldBeNil)
									So(len(dirs), ShouldEqual, 1)
								})
							})
						})
					})

					Convey("And then you can set file and directory entries, trigger discovery and get all file statuses", func() {
						files, dirs, discovers, symlinkPath := createTestBackupFiles(t, localDir)
						dirs = append(dirs, filepath.Join(localDir, "missing"))

						err = client.MergeFiles(exampleSet.ID(), files)
						So(err, ShouldBeNil)

						err = client.MergeDirs(exampleSet.ID(), dirs)
						So(err, ShouldBeNil)

						gotDirs, errg := client.GetDirs(exampleSet.ID())
						So(errg, ShouldBeNil)
						So(len(gotDirs), ShouldEqual, 3)
						So(gotDirs[0].Path, ShouldEqual, dirs[0])
						So(gotDirs[1].Path, ShouldEqual, dirs[1])
						So(gotDirs[2].Path, ShouldEqual, dirs[2])

						err = os.Remove(files[1])
						So(err, ShouldBeNil)
						numFiles := len(files)

						gotSet, errg := client.GetSetByID(exampleSet.Requester, exampleSet.ID())
						So(errg, ShouldBeNil)
						So(gotSet.NumFiles, ShouldEqual, 0)
						So(gotSet.Symlinks, ShouldEqual, 0)

						So(slackWriter.String(), ShouldEqual, slack.BoxPrefixInfo+"`jim.set1` stored in db")
						slackWriter.Reset()

						tn := time.Now()
						err = client.TriggerDiscovery(exampleSet.ID())
						So(err, ShouldBeNil)

						ok := <-racCalled
						So(ok, ShouldBeTrue)
						So(len(racRequests), ShouldEqual, numFiles+len(discovers))
						So(racRequests[0], ShouldResemble, &transfer.Request{
							Local:     files[0],
							Remote:    filepath.Join(remoteDir, "a"),
							Requester: exampleSet.Requester,
							Set:       exampleSet.Name,
							Meta:      transfer.NewMeta(),
						})

						entries, errg := client.GetFiles(exampleSet.ID())
						So(errg, ShouldBeNil)
						So(len(entries), ShouldEqual, len(files)+len(discovers))

						So(slackWriter.String(), ShouldEqual, fmt.Sprintf("%s`jim.set1` completed discovery: %d files",
							slack.BoxPrefixInfo, len(entries)))

						So(entries[0].Status, ShouldEqual, set.Pending)
						So(entries[1].Status, ShouldEqual, set.Missing)
						So(entries[2].Path, ShouldContainSubstring, "c/d/g")
						So(entries[3].Path, ShouldContainSubstring, "c/d/h")
						So(entries[4].Path, ShouldContainSubstring, "c/d/symlink")
						So(entries[4].Status, ShouldEqual, set.Pending)
						So(entries[5].Path, ShouldContainSubstring, "e/f/i/j/k/l")

						entries, err = client.GetDirs(exampleSet.ID())
						So(err, ShouldBeNil)
						So(len(entries), ShouldEqual, len(dirs))
						So(entries[2].Status, ShouldEqual, set.Missing)

						gotSet, errg = client.GetSetByID(exampleSet.Requester, exampleSet.ID())
						So(errg, ShouldBeNil)
						So(gotSet.LastDiscovery, ShouldHappenAfter, tn)
						So(gotSet.Missing, ShouldEqual, 1)
						So(gotSet.Symlinks, ShouldEqual, 1)
						So(gotSet.NumFiles, ShouldEqual, 6)
						So(gotSet.Status, ShouldEqual, set.PendingUpload)

						gotSetByName, errg := client.GetSetByName(exampleSet.Requester, exampleSet.Name)
						So(errg, ShouldBeNil)
						So(gotSetByName, ShouldResemble, gotSet)

						err = client.AddOrUpdateSet(exampleSet2)
						So(err, ShouldBeNil)

						set2Files := append(append([]string{}, files...), symlinkPath)
						err = client.MergeFiles(exampleSet2.ID(), set2Files)
						So(err, ShouldBeNil)

						err = client.MergeDirs(exampleSet2.ID(), nil)
						So(err, ShouldBeNil)

						tn = time.Now()

						err = client.TriggerDiscovery(exampleSet2.ID())
						So(err, ShouldBeNil)

						ok = <-racCalled
						So(ok, ShouldBeTrue)
						So(len(racRequests), ShouldEqual, numFiles+len(discovers)+len(set2Files))

						entries, err = client.GetFiles(exampleSet2.ID())
						So(err, ShouldBeNil)
						So(len(entries), ShouldEqual, len(set2Files))

						gotSet, err = client.GetSetByID(exampleSet2.Requester, exampleSet2.ID())
						So(err, ShouldBeNil)
						So(gotSet.LastDiscovery, ShouldHappenAfter, tn)
						So(gotSet.NumFiles, ShouldEqual, len(set2Files))
						So(gotSet.Missing, ShouldEqual, 1)
						So(gotSet.Symlinks, ShouldEqual, 1)

						err = client.AddOrUpdateSet(exampleSet3)
						So(err, ShouldBeNil)

						err = client.MergeFiles(exampleSet3.ID(), nil)
						So(err, ShouldBeNil)

						err = client.MergeDirs(exampleSet3.ID(), dirs)
						So(err, ShouldBeNil)

						tn = time.Now()

						err = client.TriggerDiscovery(exampleSet3.ID())
						So(err, ShouldBeNil)

						ok = <-racCalled
						So(ok, ShouldBeTrue)
						So(len(racRequests), ShouldEqual, numFiles+len(discovers)+len(set2Files)+len(discovers))

						entries, err = client.GetFiles(exampleSet3.ID())
						So(err, ShouldBeNil)
						So(len(entries), ShouldEqual, len(discovers))

						gotSet, err = client.GetSetByID(exampleSet3.Requester, exampleSet3.ID())
						So(err, ShouldBeNil)
						So(gotSet.LastDiscovery, ShouldHappenAfter, tn)
						So(gotSet.Missing, ShouldEqual, 0)
						So(gotSet.NumFiles, ShouldEqual, len(discovers))

						So(racCalls, ShouldEqual, 3)
						err = client.TriggerDiscovery(exampleSet.ID())
						So(err, ShouldBeNil)

						<-time.After(100 * time.Millisecond)
						So(racCalls, ShouldEqual, 3)

						s.queue.TriggerReadyAddedCallback(context.Background())

						ok = <-racCalled
						So(ok, ShouldBeTrue)
						So(racCalls, ShouldEqual, 4)
						So(len(racRequests), ShouldEqual, numFiles+len(discovers)+len(set2Files)+len(discovers))

						expectedRequests := 13

						Convey("After discovery, admin can get upload requests and update file entry status", func() {
							entries, err = client.GetFiles(exampleSet.ID())
							So(err, ShouldBeNil)

							_, err = client.GetSomeUploadRequests()
							So(err, ShouldNotBeNil)

							token, errl = gas.Login(gas.NewClientRequest(addr, certPath), admin, "pass")
							So(errl, ShouldBeNil)

							client = NewClient(addr, certPath, token)

							requests, errg := client.GetSomeUploadRequests()
							So(errg, ShouldBeNil)
							So(len(requests), ShouldEqual, expectedRequests)
							So(requests[0].Local, ShouldEqual, entries[0].Path)

							requests[0].Status = transfer.RequestStatusUploading
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

							requests[0].Status = transfer.RequestStatusUploading
							requests[0].Stuck = transfer.NewStuck(time.Now())
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

							requests[0].Status = transfer.RequestStatusUploaded
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
							So(stats.Items, ShouldEqual, expectedRequests-1)

							requests[1].Status = transfer.RequestStatusUploading
							err = client.UpdateFileStatus(requests[1])
							So(err, ShouldBeNil)

							requests[1].Status = transfer.RequestStatusFailed
							err = client.UpdateFileStatus(requests[1])
							So(err, ShouldBeNil)

							entries, err = client.GetFiles(exampleSet.ID())
							So(err, ShouldBeNil)
							So(entries[1].Path, ShouldEqual, requests[1].Local)
							So(entries[1].Status, ShouldEqual, set.Failed)

							gotSet, err = client.GetSetByID(exampleSet.Requester, exampleSet.ID())
							So(err, ShouldBeNil)
							So(gotSet.Status, ShouldEqual, set.Uploading)
							So(gotSet.Uploaded, ShouldEqual, 1)
							So(gotSet.Failed, ShouldEqual, 1)

							stats = s.queue.Stats()
							So(stats.Items, ShouldEqual, expectedRequests-1)
							So(stats.Running, ShouldEqual, expectedRequests-2)
							So(stats.Ready, ShouldEqual, 1)

							err = client.UpdateFileStatus(requests[1])
							So(err, ShouldNotBeNil)

							frequests, errg := client.GetSomeUploadRequests()
							So(errg, ShouldBeNil)
							So(len(frequests), ShouldEqual, 1)

							frequests[0].Status = transfer.RequestStatusUploading
							err = client.UpdateFileStatus(frequests[0])
							So(err, ShouldBeNil)

							frequests[0].Status = transfer.RequestStatusFailed
							err = client.UpdateFileStatus(frequests[0])
							So(err, ShouldBeNil)

							gotSet, err = client.GetSetByID(exampleSet.Requester, exampleSet.ID())
							So(err, ShouldBeNil)
							So(gotSet.Status, ShouldEqual, set.Uploading)
							So(gotSet.Uploaded, ShouldEqual, 1)
							So(gotSet.Failed, ShouldEqual, 1)

							stats = s.queue.Stats()
							So(stats.Items, ShouldEqual, expectedRequests-1)
							So(stats.Running, ShouldEqual, expectedRequests-2)
							So(stats.Ready, ShouldEqual, 1)

							frequests, err = client.GetSomeUploadRequests()
							So(err, ShouldBeNil)
							So(len(frequests), ShouldEqual, 1)

							frequests[0].Status = transfer.RequestStatusUploading
							err = client.UpdateFileStatus(frequests[0])
							So(err, ShouldBeNil)

							frequests[0].Status = transfer.RequestStatusFailed
							err = client.UpdateFileStatus(frequests[0])
							So(err, ShouldBeNil)

							gotSet, err = client.GetSetByID(exampleSet.Requester, exampleSet.ID())
							So(err, ShouldBeNil)
							So(gotSet.Status, ShouldEqual, set.Failing)
							So(gotSet.Uploaded, ShouldEqual, 1)
							So(gotSet.Failed, ShouldEqual, 1)

							stats = s.queue.Stats()
							So(stats.Items, ShouldEqual, expectedRequests-1)
							So(stats.Running, ShouldEqual, expectedRequests-2)
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

						Convey("Uploading files logs how many uploads are happening at once", func() {
							token, errl = gas.Login(gas.NewClientRequest(addr, certPath), admin, "pass")
							So(errl, ShouldBeNil)

							client = NewClient(addr, certPath, token)

							requests, errg := client.GetSomeUploadRequests()
							So(errg, ShouldBeNil)
							So(len(requests), ShouldEqual, expectedRequests)

							slackWriter.Reset()

							requests[0].Status = transfer.RequestStatusUploading
							err = client.UpdateFileStatus(requests[0])
							So(err, ShouldBeNil)

							So(slackWriter.String(), ShouldEqual, slack.BoxPrefixInfo+"`jim.set1` started uploading files"+
								slack.BoxPrefixInfo+"1 clients uploading")
							slackWriter.Reset()

							requests[1].Status = transfer.RequestStatusUploading
							err = client.UpdateFileStatus(requests[1])
							So(err, ShouldBeNil)

							So(slackWriter.String(), ShouldEqual, slack.BoxPrefixInfo+"2 clients uploading")
							slackWriter.Reset()

							requests[1].Status = transfer.RequestStatusUploading
							requests[1].Stuck = &transfer.Stuck{Host: "host"}
							err = client.UpdateFileStatus(requests[1])
							So(err, ShouldBeNil)

							So(slackWriter.String(), ShouldBeBlank)
							So(s.uploadTracker.numStuck(), ShouldEqual, 1)

							requests[2].Status = transfer.RequestStatusUploading
							err = client.UpdateFileStatus(requests[2])
							So(err, ShouldBeNil)

							So(slackWriter.String(), ShouldEqual, slack.BoxPrefixInfo+"3 clients uploading")
							slackWriter.Reset()

							requests[1].Status = transfer.RequestStatusUploaded
							err = client.UpdateFileStatus(requests[1])
							So(err, ShouldBeNil)

							So(slackWriter.String(), ShouldEqual, slack.BoxPrefixInfo+"2 clients uploading")
							slackWriter.Reset()

							requests[0].Status = transfer.RequestStatusFailed
							err = client.UpdateFileStatus(requests[0])
							So(err, ShouldBeNil)

							So(slackWriter.String(), ShouldEqual, slack.BoxPrefixInfo+"1 clients uploading")
							slackWriter.Reset()

							requests[0].Status = transfer.RequestStatusFailed
							err = client.UpdateFileStatus(requests[0])
							So(err.Error(), ShouldContainSubstring, "not running")

							So(slackWriter.String(), ShouldBeBlank)

							requests[2].Status = transfer.RequestStatusUploaded
							err = client.UpdateFileStatus(requests[2])
							So(err, ShouldBeNil)

							So(slackWriter.String(), ShouldEqual, slack.BoxPrefixInfo+"0 clients uploading")
							So(s.uploadTracker.numStuck(), ShouldEqual, 0)
						})

						Convey("Upload messages are debounced if desired", func() {
							s.Stop()

							serverStopped = true

							slackDebounce := 500 * time.Millisecond
							conf.SlackMessageDebounce = slackDebounce

							_, addr2, dfunc2 := makeAndStartServer()
							defer dfunc2() //nolint:errcheck

							token, errl = gas.Login(gas.NewClientRequest(addr2, certPath), admin, "pass")
							So(errl, ShouldBeNil)

							client = NewClient(addr2, certPath, token)

							requests, errg := client.GetSomeUploadRequests()
							So(errg, ShouldBeNil)

							requests[3].Status = transfer.RequestStatusUploading
							err = client.UpdateFileStatus(requests[3])
							So(err, ShouldBeNil)

							So(slackWriter.String(), ShouldContainSubstring, slack.BoxPrefixInfo+"1 clients uploading")
							slackWriter.Reset()

							requests[3].Status = transfer.RequestStatusFailed
							err = client.UpdateFileStatus(requests[3])
							So(err, ShouldBeNil)

							requests[9].Status = transfer.RequestStatusUploading
							err = client.UpdateFileStatus(requests[9])
							So(err, ShouldBeNil)

							requests[9].Status = transfer.RequestStatusUploaded
							err = client.UpdateFileStatus(requests[9])
							So(err, ShouldBeNil)

							So(slackWriter.String(), ShouldNotContainSubstring, "client")
							slackWriter.Reset()

							<-time.After(slackDebounce)

							So(slackWriter.String(), ShouldEqual, slack.BoxPrefixInfo+"0 clients uploading")
							slackWriter.Reset()

							requests[12].Status = transfer.RequestStatusUploading
							err = client.UpdateFileStatus(requests[12])
							So(err, ShouldBeNil)

							So(slackWriter.String(), ShouldBeBlank)

							requests[12].Status = transfer.RequestStatusFailed
							err = client.UpdateFileStatus(requests[12])
							So(err, ShouldBeNil)

							<-time.After(slackDebounce)

							So(slackWriter.String(), ShouldContainSubstring, slack.BoxPrefixInfo+"1 clients uploading")
							slackWriter.Reset()

							<-time.After(slackDebounce)

							So(slackWriter.String(), ShouldContainSubstring, slack.BoxPrefixInfo+"0 clients uploading")
							slackWriter.Reset()
						})

						Convey("After discovery, monitored sets get discovered again after completion", func() {
							exampleSet2.MonitorTime = 500 * time.Millisecond
							err = client.AddOrUpdateSet(exampleSet2)
							So(err, ShouldBeNil)

							gotSet, err = client.GetSetByID(exampleSet2.Requester, exampleSet2.ID())
							So(err, ShouldBeNil)
							discovered := gotSet.LastDiscovery

							waitForDiscovery(t, client, gotSet)

							gotSet, err = client.GetSetByID(exampleSet2.Requester, exampleSet2.ID())
							So(err, ShouldBeNil)
							So(gotSet.Status, ShouldEqual, set.PendingUpload)
							So(gotSet.LastDiscovery, ShouldEqual, discovered)
							So(gotSet.NumFiles, ShouldEqual, len(set2Files))
							So(gotSet.Missing, ShouldEqual, 1)
							So(gotSet.Symlinks, ShouldEqual, 1)

							entries, err = client.GetFiles(exampleSet2.ID())
							So(err, ShouldBeNil)
							So(len(entries), ShouldEqual, 3)
							So(entries[0].Type, ShouldEqual, set.Regular)
							So(entries[1].Status, ShouldEqual, set.Missing)
							So(entries[2].Type, ShouldEqual, set.Symlink)

							makeSetComplete := func(numExpectedRequests int) {
								requests, errg := adminClient.GetSomeUploadRequests()
								So(errg, ShouldBeNil)
								So(len(requests), ShouldEqual, numExpectedRequests)

								for _, request := range requests {
									if request.Set != exampleSet2.Name || request.Local == entries[1].Path {
										continue
									}

									request.Status = transfer.RequestStatusUploading
									err = adminClient.UpdateFileStatus(request)
									So(err, ShouldBeNil)

									request.Status = transfer.RequestStatusUploaded
									err = adminClient.UpdateFileStatus(request)
									So(err, ShouldBeNil)
								}
							}

							makeSetComplete(expectedRequests)

							gotSet, err = client.GetSetByID(exampleSet2.Requester, exampleSet2.ID())
							So(err, ShouldBeNil)
							So(gotSet.Status, ShouldEqual, set.Complete)
							So(gotSet.LastDiscovery, ShouldEqual, discovered)

							waitForDiscovery(t, client, gotSet)

							gotSet, err = client.GetSetByID(exampleSet2.Requester, exampleSet2.ID())
							So(err, ShouldBeNil)
							So(gotSet.Status, ShouldEqual, set.PendingUpload)
							So(gotSet.LastDiscovery, ShouldHappenAfter, discovered)
							discovered = gotSet.LastDiscovery

							waitForDiscovery(t, client, gotSet)

							gotSet, err = client.GetSetByID(exampleSet2.Requester, exampleSet2.ID())
							So(err, ShouldBeNil)
							So(gotSet.Status, ShouldEqual, set.PendingUpload)
							So(gotSet.LastDiscovery, ShouldEqual, discovered)

							makeSetComplete(2)

							gotSet, err = client.GetSetByID(exampleSet2.Requester, exampleSet2.ID())
							So(err, ShouldBeNil)
							So(gotSet.Status, ShouldEqual, set.Complete)
							So(gotSet.LastDiscovery, ShouldEqual, discovered)

							waitForDiscovery(t, client, gotSet)

							gotSet, err = client.GetSetByID(exampleSet2.Requester, exampleSet2.ID())
							So(err, ShouldBeNil)
							So(gotSet.Status, ShouldEqual, set.PendingUpload)
							So(gotSet.LastDiscovery, ShouldHappenAfter, discovered)
						})

						Convey("After discovery, empty monitored sets get discovered again after the discovery", func() {
							emptySet := &set.Set{
								Name:        "empty",
								Requester:   exampleSet2.Requester,
								Transformer: exampleSet2.Transformer,
								MonitorTime: 500 * time.Millisecond,
							}

							err = client.AddOrUpdateSet(emptySet)
							So(err, ShouldBeNil)

							emptyDir := filepath.Join(localDir, "empty")
							err = os.Mkdir(emptyDir, userPerms)
							So(err, ShouldBeNil)

							err = client.MergeDirs(emptySet.ID(), []string{emptyDir})
							So(err, ShouldBeNil)

							err = client.TriggerDiscovery(emptySet.ID())
							So(err, ShouldBeNil)

							<-time.After(100 * time.Millisecond)

							gotSet, err = client.GetSetByID(emptySet.Requester, emptySet.ID())
							So(err, ShouldBeNil)
							So(gotSet.Status, ShouldEqual, set.Complete)
							discovered := gotSet.LastDiscovery

							waitForDiscovery(t, client, gotSet)

							gotSet, err = client.GetSetByID(emptySet.Requester, emptySet.ID())
							So(err, ShouldBeNil)
							So(gotSet.Status, ShouldEqual, set.Complete)
							So(gotSet.LastDiscovery, ShouldHappenAfter, discovered)
							discovered = gotSet.LastDiscovery

							countDiscovery := func(given *set.Set) int {
								countDiscovered := given.LastDiscovery
								count := 0

								internal.RetryUntilWorksCustom(t, func() error { //nolint:errcheck
									gotSet, err = client.GetSetByID(given.Requester, given.ID())
									So(err, ShouldBeNil)

									if gotSet.LastDiscovery.After(countDiscovered) {
										count++
										countDiscovered = gotSet.LastDiscovery
									}

									return errNotDiscovered
								}, given.MonitorTime*10, given.MonitorTime/10)

								return count
							}

							Convey("Changing discovery from long to short duration works", func() {
								discovers := countDiscovery(gotSet)
								So(discovers, ShouldBeBetweenOrEqual, 9, 11)

								gotSet, err = client.GetSetByID(emptySet.Requester, emptySet.ID())
								So(err, ShouldBeNil)
								So(gotSet.LastDiscovery, ShouldHappenAfter, discovered)
								discovered = gotSet.LastDiscovery

								changedSet := &set.Set{
									Name:        emptySet.Name,
									Requester:   emptySet.Requester,
									Transformer: emptySet.Transformer,
									MonitorTime: 250 * time.Millisecond,
								}

								for range 5 {
									if err = client.AddOrUpdateSet(changedSet); err == nil {
										break
									}
								}

								So(err, ShouldBeNil)

								err = client.TriggerDiscovery(emptySet.ID())
								So(err, ShouldBeNil)

								gotSet, err = client.GetSetByID(emptySet.Requester, emptySet.ID())
								So(err, ShouldBeNil)

								discovers = countDiscovery(gotSet)
								So(discovers, ShouldBeBetweenOrEqual, 9, 11)
							})

							Convey("Changing discovery from short to long duration works", func() {
								gotSet, err = client.GetSetByID(emptySet.Requester, emptySet.ID())
								So(err, ShouldBeNil)
								discovered = gotSet.LastDiscovery

								changedSet := &set.Set{
									Name:        emptySet.Name,
									Requester:   emptySet.Requester,
									Transformer: emptySet.Transformer,
									MonitorTime: 1 * time.Minute,
								}

								err = client.AddOrUpdateSet(changedSet)
								So(err, ShouldBeNil)

								err = client.TriggerDiscovery(emptySet.ID())
								So(err, ShouldBeNil)

								<-time.After(10 * time.Millisecond)

								gotSet.LastDiscovery = time.Now()
								discovers := countDiscovery(gotSet)
								So(discovers, ShouldEqual, 0)
							})

							Convey("Adding a file to an empty set switches to discovery after completion", func() {
								addedFile := filepath.Join(emptyDir, "file.txt")
								internal.CreateTestFile(t, addedFile, "")

								waitForDiscovery(t, client, gotSet)

								gotSet, err = client.GetSetByID(emptySet.Requester, emptySet.ID())
								So(err, ShouldBeNil)
								So(gotSet.Status, ShouldEqual, set.PendingUpload)
								So(gotSet.LastDiscovery, ShouldHappenAfter, discovered)
								discovered = gotSet.LastDiscovery

								waitForDiscovery(t, client, gotSet)

								gotSet, err = client.GetSetByID(emptySet.Requester, emptySet.ID())
								So(err, ShouldBeNil)
								So(gotSet.Status, ShouldEqual, set.PendingUpload)
								So(gotSet.LastDiscovery, ShouldEqual, discovered)
							})
						})

						Convey("Stuck requests are recorded separately by the server, retrievable with QueueStatus", func() {
							token, errl = gas.Login(gas.NewClientRequest(addr, certPath), admin, "pass")
							So(errl, ShouldBeNil)

							client = NewClient(addr, certPath, token)

							qs := s.QueueStatus()
							So(qs, ShouldNotBeNil)
							So(len(qs.Stuck), ShouldEqual, 0)
							So(qs.Total, ShouldEqual, expectedRequests)
							So(qs.Reserved, ShouldEqual, 0)
							So(qs.Uploading, ShouldEqual, 0)

							qsClient, errs := client.GetQueueStatus()
							So(errs, ShouldBeNil)
							So(qsClient, ShouldResemble, qs)

							requests, errg := client.GetSomeUploadRequests()
							So(errg, ShouldBeNil)

							requests[0].Status = transfer.RequestStatusUploading

							err = client.UpdateFileStatus(requests[0])
							So(err, ShouldBeNil)

							entries, err = client.GetFiles(exampleSet.ID())
							So(err, ShouldBeNil)
							So(entries[0].Status, ShouldEqual, set.UploadingEntry)
							So(entries[0].LastError, ShouldBeBlank)

							So(s.uploadTracker.numStuck(), ShouldEqual, 0)

							requests[0].Stuck = transfer.NewStuck(time.Now())
							err = client.UpdateFileStatus(requests[0])
							So(err, ShouldBeNil)

							entries, err = client.GetFiles(exampleSet.ID())
							So(err, ShouldBeNil)
							So(entries[0].Status, ShouldEqual, set.UploadingEntry)
							So(entries[0].LastError, ShouldContainSubstring, "stuck?")

							So(s.uploadTracker.numStuck(), ShouldEqual, 1)

							qs = s.QueueStatus()
							So(qs, ShouldNotBeNil)
							So(len(qs.Stuck), ShouldEqual, 1)
							So(qs.Total, ShouldEqual, expectedRequests)
							So(qs.Reserved, ShouldEqual, expectedRequests)
							So(qs.Uploading, ShouldEqual, 1)

							requests[0].Status = transfer.RequestStatusUploaded

							err = client.UpdateFileStatus(requests[0])
							So(err, ShouldBeNil)

							qs = s.QueueStatus()
							So(qs, ShouldNotBeNil)
							So(len(qs.Stuck), ShouldEqual, 0)
							So(qs.Total, ShouldEqual, expectedRequests-1)
							So(qs.Reserved, ShouldEqual, expectedRequests-1)
							So(qs.Uploading, ShouldEqual, 0)
						})

						Convey("Buried requests can be retrieved, kicked and removed", func() {
							token, errl = gas.Login(gas.NewClientRequest(addr, certPath), admin, "pass")
							So(errl, ShouldBeNil)

							client = NewClient(addr, certPath, token)

							buried := s.BuriedRequests()
							So(buried, ShouldBeNil)

							var failedRequest *transfer.Request

							failARequest := func() {
								for i := 0; i < set.AttemptsToBeConsideredFailing; i++ {
									requests, errg := client.GetSomeUploadRequests()
									So(errg, ShouldBeNil)
									So(len(requests), ShouldBeGreaterThan, 0)

									failedRequest = requests[0].Clone()

									failedRequest.Status = transfer.RequestStatusUploading
									err = client.UpdateFileStatus(failedRequest)
									So(err, ShouldBeNil)

									failedRequest.Status = transfer.RequestStatusFailed
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
							So(qs.Total, ShouldEqual, expectedRequests)
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
							So(qs.Total, ShouldEqual, expectedRequests)
							So(qs.Reserved, ShouldEqual, 0)
							So(qs.Uploading, ShouldEqual, 0)
							So(qs.Failed, ShouldEqual, 0)
							So(s.queue.Stats().Ready, ShouldEqual, expectedRequests)

							failARequest()

							qs = s.QueueStatus()
							So(qs, ShouldNotBeNil)
							So(qs.Total, ShouldEqual, expectedRequests)
							So(qs.Reserved, ShouldEqual, 0)
							So(qs.Uploading, ShouldEqual, 0)
							So(qs.Failed, ShouldEqual, 1)

							n, errr = client.RemoveBuried(bf)
							So(errr, ShouldBeNil)
							So(n, ShouldEqual, 1)

							qs = s.QueueStatus()
							So(qs, ShouldNotBeNil)
							So(qs.Total, ShouldEqual, expectedRequests-1)
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
							token, errl = gas.Login(gas.NewClientRequest(addr, certPath), admin, "pass")
							So(errl, ShouldBeNil)

							client = NewClient(addr, certPath, token)

							uploading, errc := client.UploadingRequests()
							So(errc, ShouldBeNil)
							So(len(uploading), ShouldEqual, 0)

							requests, errg := client.GetSomeUploadRequests()
							So(errg, ShouldBeNil)
							So(len(requests), ShouldBeGreaterThan, 0)

							uploadRequest := requests[0].Clone()

							uploadRequest.Status = transfer.RequestStatusUploading
							err = client.UpdateFileStatus(uploadRequest)
							So(err, ShouldBeNil)

							uploading, errc = client.UploadingRequests()
							So(errc, ShouldBeNil)
							So(len(uploading), ShouldEqual, 1)
							So(uploading[0].ID(), ShouldEqual, uploadRequest.ID())

							uploadRequest.Status = transfer.RequestStatusUploaded
							err = client.UpdateFileStatus(uploadRequest)
							So(err, ShouldBeNil)

							uploading, errc = client.UploadingRequests()
							So(errc, ShouldBeNil)
							So(len(uploading), ShouldEqual, 0)
						})

						Convey("All requests can be retrieved", func() {
							token, errl = gas.Login(gas.NewClientRequest(addr, certPath), admin, "pass")
							So(errl, ShouldBeNil)

							client = NewClient(addr, certPath, token)

							all, errc := client.AllRequests()
							So(errc, ShouldBeNil)
							So(len(all), ShouldBeGreaterThan, 0)

							for _, r := range all {
								So(r.Status, ShouldEqual, transfer.RequestStatusPending)
							}

							requests, errg := client.GetSomeUploadRequests()
							So(errg, ShouldBeNil)
							So(len(requests), ShouldBeGreaterThan, 0)

							uploadRequest := requests[0].Clone()

							uploadRequest.Status = transfer.RequestStatusUploading
							err = client.UpdateFileStatus(uploadRequest)
							So(err, ShouldBeNil)

							all2, errc := client.AllRequests()
							So(errc, ShouldBeNil)
							So(len(all2), ShouldBeGreaterThan, 0)

							pending, reserved, uploading, other := 0, 0, 0, 0

							for _, r := range all2 {
								switch r.Status {
								case transfer.RequestStatusUploading:
									uploading++
								case transfer.RequestStatusPending:
									pending++
								case transfer.RequestStatusReserved:
									reserved++
								default:
									other++
								}
							}

							So(pending, ShouldEqual, len(all)-len(requests))
							So(reserved, ShouldEqual, len(requests)-1)
							So(uploading, ShouldEqual, 1)
							So(other, ShouldEqual, 0)
						})

						Convey("Once logged in and with a client", func() {
							token, errl = gas.Login(gas.NewClientRequest(addr, certPath), admin, "pass")
							So(errl, ShouldBeNil)

							client = NewClient(addr, certPath, token)

							Convey("Client can automatically update server given Putter-style output using SendPutResultsToServer", func() {
								uploadStartsCh := make(chan *transfer.Request)
								uploadResultsCh := make(chan *transfer.Request)
								skippedResultsCh := make(chan *transfer.Request)
								errCh := make(chan error)
								logger := log15.New()

								go func() {
									errCh <- client.SendPutResultsToServer(
										uploadStartsCh, uploadResultsCh, skippedResultsCh,
										minMBperSecondUploadSpeed, minTimeForUpload, maxStuckTime, logger,
									)
								}()

								requests, errg := client.GetSomeUploadRequests()
								So(errg, ShouldBeNil)
								So(len(requests), ShouldEqual, expectedRequests)

								gotSet, err = client.GetSetByID(exampleSet.Requester, exampleSet.ID())
								So(err, ShouldBeNil)
								So(gotSet.Status, ShouldEqual, set.PendingUpload)
								So(gotSet.NumFiles, ShouldEqual, 6)
								So(gotSet.Uploaded, ShouldEqual, 0)
								So(gotSet.Symlinks, ShouldEqual, 1)

								updateRequestStatus := func(origReq *transfer.Request,
									firstStatus, secondStatus transfer.RequestStatus,
									size uint64, startCh, endCh chan *transfer.Request) {
									r := origReq.Clone()
									r.Status = firstStatus
									r.Size = size
									startCh <- r

									if firstStatus != transfer.RequestStatusUploading {
										return
									}

									r2 := r.Clone()
									r2.Status = secondStatus
									endCh <- r2
								}

								updateRequestStatus(
									requests[0],
									transfer.RequestStatusUploading,
									transfer.RequestStatusUploaded,
									1,
									uploadStartsCh,
									uploadResultsCh,
								)

								updateRequestStatus(
									requests[1],
									transfer.RequestStatusUnmodified,
									"",
									2,
									skippedResultsCh,
									nil,
								)

								updateRequestStatus(
									requests[2],
									transfer.RequestStatusUploading,
									transfer.RequestStatusReplaced,
									3,
									uploadStartsCh,
									uploadResultsCh,
								)

								updateRequestStatus(
									requests[3],
									transfer.RequestStatusUploading,
									transfer.RequestStatusFailed,
									4,
									uploadStartsCh,
									uploadResultsCh,
								)

								r := requests[4].Clone()
								r.Symlink = "/path/to/dest"
								r.Status = transfer.RequestStatusUploading
								uploadStartsCh <- r
								r = r.Clone()
								r.Status = transfer.RequestStatusUploaded
								uploadResultsCh <- r

								updateRequestStatus(
									requests[5],
									transfer.RequestStatusUploading,
									transfer.RequestStatusUploaded,
									5,
									uploadStartsCh,
									uploadResultsCh,
								)

								gotSet, err = client.GetSetByID(exampleSet.Requester, exampleSet2.ID())
								So(err, ShouldBeNil)
								So(gotSet.Status, ShouldEqual, set.PendingUpload)
								So(gotSet.NumFiles, ShouldEqual, len(set2Files))
								So(gotSet.Uploaded, ShouldEqual, 0)

								r = requests[6].Clone()
								r.Status = transfer.RequestStatusUploading
								r.Size = 6
								uploadStartsCh <- r
								r = r.Clone()
								r.Stuck = transfer.NewStuck(time.Now())
								uploadResultsCh <- r

								close(uploadStartsCh)
								close(uploadResultsCh)
								close(skippedResultsCh)

								err = <-errCh
								So(err, ShouldBeNil)

								gotSet, err = client.GetSetByID(exampleSet.Requester, exampleSet.ID())
								So(err, ShouldBeNil)
								So(gotSet.Status, ShouldEqual, set.Complete)
								So(gotSet.NumFiles, ShouldEqual, 6)
								So(gotSet.SizeTotal, ShouldEqual, 15)
								So(gotSet.Uploaded, ShouldEqual, 3)
								So(gotSet.Replaced, ShouldEqual, 1)
								So(gotSet.Skipped, ShouldEqual, 1)
								So(gotSet.Failed, ShouldEqual, 1)
								So(gotSet.Symlinks, ShouldEqual, 1)
								So(gotSet.Missing, ShouldEqual, 0)

								gotSet, err = client.GetSetByID(exampleSet.Requester, exampleSet2.ID())
								So(err, ShouldBeNil)
								So(gotSet.Status, ShouldEqual, set.Uploading)
								So(gotSet.NumFiles, ShouldEqual, len(set2Files))
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

							err = client.MergeDirs(exampleSet.ID(), []string{dir})
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
									MonitorTime: 0,
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

					Convey("And given a monitored set with MonitorRemovals option", func() {
						fileMeta := map[string]string{
							transfer.MetaKeySets:      exampleSet.Name,
							transfer.MetaKeyRequester: exampleSet.Requester,
						}

						file1local := filepath.Join(localDir, "file1")
						file1remote := filepath.Join(remoteDir, "file1")

						file2 := filepath.Join(localDir, "file2")

						dir1local := filepath.Join(localDir, "dir1")
						dir1remote := filepath.Join(remoteDir, "dir1")

						file3local := filepath.Join(dir1local, "file3")
						file3remote := filepath.Join(dir1remote, "file3")

						dir2 := filepath.Join(localDir, "dir2")

						dir3 := filepath.Join(dir2, "dir3")

						err = os.Mkdir(dir1local, 0755)
						So(err, ShouldBeNil)

						err = os.MkdirAll(dir1remote, 0755)
						So(err, ShouldBeNil)

						err = os.Mkdir(dir2, 0755)
						So(err, ShouldBeNil)

						err = os.Mkdir(dir3, 0755)
						So(err, ShouldBeNil)

						internal.CreateTestFileOfLength(t, file1local, 1)
						internal.CreateTestFileOfLength(t, file2, 1)
						internal.CreateTestFileOfLength(t, file3local, 1)

						createRemoteObject(t, s.storageHandler, fileMeta, file1remote)
						createRemoteObject(t, s.storageHandler, fileMeta, file3remote)

						listOfFiles := []string{file1local, file2, file3local}

						err = client.MergeFiles(exampleSet.ID(), []string{file1local, file2})
						So(err, ShouldBeNil)

						err = client.MergeDirs(exampleSet.ID(), []string{dir1local, dir2})
						So(err, ShouldBeNil)

						exampleSet.MonitorTime = 500 * time.Millisecond
						exampleSet.MonitorRemovals = true

						err = client.AddOrUpdateSet(exampleSet)
						So(err, ShouldBeNil)

						err = client.TriggerDiscovery(exampleSet.ID())
						So(err, ShouldBeNil)

						ok := <-racCalled
						So(ok, ShouldBeTrue)

						files, errg := client.GetFiles(exampleSet.ID())
						So(errg, ShouldBeNil)

						So(len(files), ShouldEqual, len(listOfFiles))

						waitForRemovals := func(given *set.Set) {
							internal.RetryUntilWorksCustom(t, func() error { //nolint:errcheck
								tickerSet, errg := client.GetSetByID(given.Requester, given.ID())
								So(errg, ShouldBeNil)

								if tickerSet.NumObjectsRemoved == tickerSet.NumObjectsToBeRemoved {
									return nil
								}

								return errNotAllRemoved
							}, time.Second*10, time.Millisecond*100)
						}

						Convey("The monitor can detect locally removed files and remove them from the set", func() {
							err = os.Remove(file1local)
							So(err, ShouldBeNil)

							err = os.Remove(file3local)
							So(err, ShouldBeNil)

							exampleSet.Status = set.Complete

							err = client.AddOrUpdateSet(exampleSet)
							So(err, ShouldBeNil)

							waitForDiscovery(t, client, exampleSet)

							waitForRemovals(exampleSet)

							files, errg := client.GetFiles(exampleSet.ID())
							So(errg, ShouldBeNil)

							So(len(files), ShouldEqual, len(listOfFiles)-2)
							So(files[0].Path, ShouldEqual, file2)

							gotSet, errgs := client.GetSetByID(exampleSet.Requester, exampleSet.ID())
							So(errgs, ShouldBeNil)
							So(gotSet.NumFiles, ShouldEqual, len(listOfFiles)-2)

							_, err = os.Stat(file1remote)
							So(err, ShouldNotBeNil)

							_, err = os.Stat(file3remote)
							So(err, ShouldNotBeNil)
						})

						Convey("The monitor can detect locally removed dirs and remove them and their nested files from the set", func() {
							err = os.RemoveAll(dir1local)
							So(err, ShouldBeNil)

							err = os.RemoveAll(dir3)
							So(err, ShouldBeNil)

							exampleSet.Status = set.Complete

							err = client.AddOrUpdateSet(exampleSet)
							So(err, ShouldBeNil)

							waitForDiscovery(t, client, exampleSet)

							waitForRemovals(exampleSet)

							files, errg := client.GetFiles(exampleSet.ID())
							So(errg, ShouldBeNil)

							So(len(files), ShouldEqual, len(listOfFiles)-1)

							dirs, errg := client.GetDirs(exampleSet.ID())
							So(errg, ShouldBeNil)
							So(len(dirs), ShouldEqual, 1)
							So(dirs[0].Path, ShouldEqual, dir2)

							gotSet, errgs := client.GetSetByID(exampleSet.Requester, exampleSet.ID())
							So(errgs, ShouldBeNil)
							So(gotSet.NumFiles, ShouldEqual, len(listOfFiles)-1)

							_, err = os.Stat(dir1remote)
							So(err, ShouldNotBeNil)
						})

						Convey("The monitor won't detect removed files for a read-only set", func() {
							gotSet, errg := client.GetSetByID(exampleSet.Requester, exampleSet.ID())
							So(errg, ShouldBeNil)

							err = os.Remove(file1local)
							So(err, ShouldBeNil)

							gotSet.Status = set.Complete
							gotSet.MonitorTime = 100 * time.Millisecond
							gotSet.MonitorRemovals = true
							gotSet.ReadOnly = true

							err = client.AddOrUpdateSet(gotSet)
							So(err, ShouldBeNil)

							logWriter.Reset()

							time.Sleep(exampleSet.MonitorTime * 5)

							So(logWriter.String(), ShouldContainSubstring, "Ignore discovery")

							files, errg := client.GetFiles(exampleSet.ID())
							So(errg, ShouldBeNil)
							So(len(files), ShouldEqual, len(listOfFiles))
							So(files[0].Path, ShouldEqual, file1local)
						})
					})

					Convey("Making set read-only stops discovery", func() {
						gotSet, errg := client.GetSetByID(exampleSet.Requester, exampleSet.ID())
						So(errg, ShouldBeNil)

						discovered := gotSet.LastDiscovery

						err = client.TriggerDiscovery(gotSet.ID())
						So(err, ShouldBeNil)

						gotSet, err = client.GetSetByID(gotSet.Requester, gotSet.ID())
						So(err, ShouldBeNil)
						So(gotSet.LastDiscovery, ShouldHappenAfter, discovered)
						discovered = gotSet.LastDiscovery

						gotSet.ReadOnly = true
						err = client.AddOrUpdateSet(gotSet)
						So(err, ShouldBeNil)

						logWriter.Reset()

						err = client.TriggerDiscovery(gotSet.ID())
						So(err, ShouldBeNil)
						So(logWriter.String(), ShouldContainSubstring, "Ignore discovery")

						gotSet, err = client.GetSetByID(gotSet.Requester, gotSet.ID())
						So(err, ShouldBeNil)
						So(gotSet.LastDiscovery, ShouldEqual, discovered)
					})

					Convey("If you have an invalid transformer, you cannot add the set", func() {
						badSet := &set.Set{
							Name:        "setbad",
							Requester:   "jim",
							Transformer: "invalid",
							MonitorTime: 0,
						}

						err = client.AddOrUpdateSet(badSet)
						So(err, ShouldNotBeNil)
						So(err.Error(), ShouldContainSubstring, "invalid transformer")
					})

					Convey("If you have the wrong transformer for a set, discovery fails", func() {
						badSet := &set.Set{
							Name:        "setbad",
							Requester:   "jim",
							Transformer: "humgen",
							MonitorTime: 0,
						}

						_, dirs, expected, _ := createTestBackupFiles(t, localDir)

						err = client.AddOrUpdateSet(badSet)
						So(err, ShouldBeNil)

						err = client.MergeDirs(badSet.ID(), dirs)
						So(err, ShouldBeNil)

						slackWriter.Reset()

						err = client.TriggerDiscovery(badSet.ID())
						So(err, ShouldBeNil)

						<-time.After(300 * time.Millisecond)
						So(racCalls, ShouldEqual, 0)

						gotSet, errg := client.GetSetByID(badSet.Requester, badSet.ID())
						So(errg, ShouldBeNil)
						So(gotSet.Error, ShouldNotBeNil)
						So(slackWriter.String(), ShouldEqual,
							fmt.Sprintf(slack.BoxPrefixInfo+"`jim.setbad` completed discovery: 4 files"+
								slack.BoxPrefixError+"`jim.setbad` is invalid: not a valid humgen lustre path [%s]", expected[0]))

						err = dfunc()
						So(err, ShouldBeNil)

						serverStopped = true

						logWriter.Reset()

						_, _, dfunc2 := makeAndStartServer()

						defer func() {
							errd := dfunc2()
							So(errd, ShouldBeNil)
						}()

						So(logWriter.String(), ShouldEqual, fmt.Sprintf("failed to recover set setbad for jim: "+
							"not a valid humgen lustre path [%s]\n", expected[0]))
						So(slackWriter.String(), ShouldEqual, fmt.Sprintf(serverStartMessage+
							slack.BoxPrefixError+"`jim.setbad` could not be recovered: "+
							"not a valid humgen lustre path [%s]"+serverRecoveryMessage+serverStartedMessage, expected[0]))
					})

					Convey("And if you make the set read-only", func() {
						exampleSet.ReadOnly = true
						err = client.AddOrUpdateSet(exampleSet)
						So(err, ShouldBeNil)

						Convey("You cannot make it writable", func() {
							err = client.AddOrUpdateSetMakingWritable(exampleSet)
							So(err, ShouldNotBeNil)
							So(err.Error(), ShouldContainSubstring, ErrNotAdmin.Error())
						})

						Convey("You cannot make it writable using the update call", func() {
							exampleSet.ReadOnly = false
							err = client.AddOrUpdateSet(exampleSet)
							So(err, ShouldNotBeNil)
							So(err.Error(), ShouldContainSubstring, set.ErrSetIsNotWritable)
						})

						Convey("Admin can make it writable ", func() {
							exampleSet.ReadOnly = false
							err = adminClient.AddOrUpdateSetMakingWritable(exampleSet)
							So(err, ShouldBeNil)
						})
					})
				})

				Convey("But you can't add sets as other users and can only retrieve your own", func() {
					otherUser := "sam"
					exampleSet2 := &set.Set{
						Name:        "set2",
						Requester:   otherUser,
						Transformer: exampleSet.Transformer,
					}

					client := NewClient(addr, certPath, token)

					err = client.AddOrUpdateSet(exampleSet)
					So(err, ShouldBeNil)

					err = client.AddOrUpdateSet(exampleSet2)
					So(err, ShouldNotBeNil)

					got, errg := client.GetSets(otherUser)
					So(errg, ShouldNotBeNil)
					So(len(got), ShouldEqual, 0)

					got, err = client.GetSets(allUsers)
					So(err, ShouldNotBeNil)
					So(len(got), ShouldEqual, 0)
				})
			})

			Convey("Which lets you login as admin", func() {
				token, errl := gas.Login(gas.NewClientRequest(addr, certPath), admin, "pass")
				So(errl, ShouldBeNil)

				client := NewClient(addr, certPath, token)

				handler := internal.GetLocalHandler()

				logger := log15.New()

				backupPath := dbPath + ".bk"

				Convey("and add a set", func() {
					err = client.AddOrUpdateSet(exampleSet)
					So(err, ShouldBeNil)

					_, dirs, discovers, _ := createTestBackupFiles(t, localDir)

					err = client.MergeDirs(exampleSet.ID(), dirs)
					So(err, ShouldBeNil)

					gotSet, errg := client.GetSetByID(exampleSet.Requester, exampleSet.ID())
					So(errg, ShouldBeNil)
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

					Convey("which doesn't result in a backed up db if not requested", func() {
						_, err = os.Stat(backupPath)
						So(err, ShouldNotBeNil)
					})

					Convey("And also add sets as other users and retrieve them all", func() {
						otherUser := "sam"
						exampleSet2 := &set.Set{
							Name:        "set2",
							Requester:   otherUser,
							Transformer: exampleSet.Transformer,
						}

						err = client.AddOrUpdateSet(exampleSet2)
						So(err, ShouldBeNil)

						got, errg := client.GetSets(otherUser)
						So(errg, ShouldBeNil)
						So(len(got), ShouldEqual, 1)
						So(got[0].Name, ShouldResemble, exampleSet2.Name)

						got, err = client.GetSets("foo")
						So(err, ShouldBeNil)
						So(len(got), ShouldEqual, 0)

						got, err = client.GetSets(allUsers)
						So(err, ShouldBeNil)
						So(len(got), ShouldEqual, 2)

						sort.Slice(got, func(i, j int) bool {
							return got[i].Name <= got[j].Name
						})

						So(got[0].Name, ShouldEqual, exampleSet.Name)
						So(got[1].Name, ShouldResemble, exampleSet2.Name)
					})

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

						qs, errg := client.GetQueueStatus()
						So(errg, ShouldBeNil)
						So(qs.IRODSConnections, ShouldEqual, 0)

						slackWriter.Reset()

						err = client.MakingIRODSConnections(2, defaultHeartbeatFreq)
						So(err, ShouldBeNil)

						So(slackWriter.String(), ShouldEqual, slack.BoxPrefixInfo+"2 iRODS connections open")

						p, d := makePutter(t, handler, requests, client)
						defer d()

						qs, err = client.GetQueueStatus()
						So(err, ShouldBeNil)
						So(qs.IRODSConnections, ShouldEqual, 2)

						uploadStarts, uploadResults, skippedResults := p.Put()

						err = client.SendPutResultsToServer(uploadStarts, uploadResults, skippedResults,
							minMBperSecondUploadSpeed, minTimeForUpload, maxStuckTime, logger)
						So(err, ShouldBeNil)

						gotSet, err = client.GetSetByID(exampleSet.Requester, exampleSet.ID())
						So(err, ShouldBeNil)
						So(gotSet.Status, ShouldEqual, set.Complete)
						So(gotSet.NumFiles, ShouldEqual, len(discovers))
						So(gotSet.Uploaded, ShouldEqual, len(discovers))
						So(gotSet.Symlinks, ShouldEqual, 1)

						slackWriter.Reset()

						err = client.MakingIRODSConnections(2, defaultHeartbeatFreq)
						So(err, ShouldBeNil)

						So(slackWriter.String(), ShouldEqual, slack.BoxPrefixInfo+"4 iRODS connections open")

						slackWriter.Reset()

						err = client.ClosedIRODSConnections()
						So(err, ShouldBeNil)

						qs, err = client.GetQueueStatus()
						So(err, ShouldBeNil)
						So(qs.IRODSConnections, ShouldEqual, 0)

						So(slackWriter.String(), ShouldEqual, slack.BoxPrefixInfo+"0 iRODS connections open")

						iRODSTimeout := 100 * time.Millisecond

						Convey("Client is removed from server client queue after being closed", func() {
							client = NewClient(addr, certPath, token)

							errh := client.MakingIRODSConnections(2, iRODSTimeout)
							So(errh, ShouldBeNil)

							hostPID, errh := hostPID()
							So(errh, ShouldBeNil)

							So(s.clientQueue.Touch(hostPID), ShouldBeNil)

							err = client.ClosedIRODSConnections()
							So(err, ShouldBeNil)

							So(s.clientQueue.Touch(hostPID), ShouldNotBeNil)
						})

						Convey("IRODS connections are assumed closed after a period of no contact", func() {
							client = NewClient(addr, certPath, token)

							slackWriter.Reset()

							errh := client.MakingIRODSConnections(2, iRODSTimeout)
							So(errh, ShouldBeNil)

							So(s.iRODSTracker.totalIRODSConnections(), ShouldEqual, 2)

							So(slackWriter.String(), ShouldEqual, slack.BoxPrefixInfo+"2 iRODS connections open")

							slackWriter.Reset()

							<-time.After(6 * iRODSTimeout)

							So(s.iRODSTracker.totalIRODSConnections(), ShouldEqual, 2)

							close(client.heartbeatQuitCh)

							<-time.After(6 * iRODSTimeout)

							So(s.iRODSTracker.totalIRODSConnections(), ShouldEqual, 0)

							So(slackWriter.String(), ShouldContainSubstring, slack.BoxPrefixInfo+"0 iRODS connections open")
						})

						Convey("iRODS messages are debounced if desired", func() {
							s.Stop()

							serverStopped = true

							slackDebounce := 500 * time.Millisecond
							conf.SlackMessageDebounce = slackDebounce

							_, addr2, dfunc2 := makeAndStartServer()
							defer dfunc2() //nolint:errcheck

							token, errl = gas.Login(gas.NewClientRequest(addr2, certPath), admin, "pass")
							So(errl, ShouldBeNil)

							client = NewClient(addr2, certPath, token)

							slackWriter.Reset()

							err = client.MakingIRODSConnections(2, defaultHeartbeatFreq)
							So(err, ShouldBeNil)

							So(slackWriter.String(), ShouldEqual, slack.BoxPrefixInfo+"2 iRODS connections open")
							slackWriter.Reset()

							err = client.MakingIRODSConnections(2, defaultHeartbeatFreq)
							So(err, ShouldBeNil)

							So(slackWriter.String(), ShouldBeBlank)

							<-time.After(slackDebounce)

							So(slackWriter.String(), ShouldEqual, slack.BoxPrefixInfo+"4 iRODS connections open")
							slackWriter.Reset()

							err = client.MakingIRODSConnections(2, defaultHeartbeatFreq)
							So(err, ShouldBeNil)

							So(slackWriter.String(), ShouldBeBlank)
							slackWriter.Reset()

							err = client.ClosedIRODSConnections()
							So(err, ShouldBeNil)

							So(slackWriter.String(), ShouldBeBlank)

							<-time.After(slackDebounce)

							So(slackWriter.String(), ShouldEqual, slack.BoxPrefixInfo+"6 iRODS connections open")
							slackWriter.Reset()

							<-time.After(slackDebounce)

							So(slackWriter.String(), ShouldEqual, slack.BoxPrefixInfo+"0 iRODS connections open")
						})

						Convey("After completion, re-discovery can find new files and we can re-complete", func() {
							newFile := filepath.Join(dirs[0], "new")
							internal.CreateTestFileOfLength(t, newFile, 2)
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

							p, d = makePutter(t, handler, requests, client)
							defer d()

							uploadStarts, uploadResults, skippedResults = p.Put()

							err = client.SendPutResultsToServer(uploadStarts, uploadResults, skippedResults,
								minMBperSecondUploadSpeed, minTimeForUpload, maxStuckTime, logger)
							So(err, ShouldBeNil)

							gotSet, err = client.GetSetByID(exampleSet.Requester, exampleSet.ID())
							So(err, ShouldBeNil)
							So(gotSet.Status, ShouldEqual, set.Complete)
							So(gotSet.NumFiles, ShouldEqual, expectedNumFiles)
							So(gotSet.Uploaded, ShouldEqual, 1)
							So(gotSet.Replaced, ShouldEqual, 0)
							So(gotSet.Skipped, ShouldEqual, expectedNumFiles-1)
							So(gotSet.Symlinks, ShouldEqual, 1)

							entries, errg := client.GetFiles(exampleSet.ID())
							So(errg, ShouldBeNil)
							So(len(entries), ShouldEqual, expectedNumFiles)

							for n, entry := range entries {
								So(entry.Status == set.Uploaded || entry.Status == set.Skipped, ShouldBeTrue)

								if n == 3 {
									So(entry.Type, ShouldEqual, set.Symlink)
								} else {
									So(entry.Type, ShouldEqual, set.Regular)
								}
							}

							discovers = append(discovers, newFile)
							expectedSetSize := uint64(0)

							for _, r := range requests {
								local := r.Local
								remote := strings.Replace(local, localDir, remoteDir, 1)

								info, errs := os.Stat(remote)
								So(errs, ShouldBeNil)

								So(info.Size(), ShouldEqual, r.UploadedSize())
								expectedSetSize += r.UploadedSize()

								if r.Symlink != "" {
									remoteMeta, errr := handler.GetMeta(remote)
									So(errr, ShouldBeNil)
									So(remoteMeta, ShouldNotBeNil)
									So(remoteMeta[transfer.MetaKeySymlink], ShouldEqual, entries[3].Dest)
								}
							}

							So(gotSet.SizeTotal, ShouldEqual, expectedSetSize)
						})
					})

					Convey("The system handles files that become missing", func() {
						requests, errg := client.GetSomeUploadRequests()
						So(errg, ShouldBeNil)
						So(len(requests), ShouldEqual, len(discovers))

						p, d := makePutter(t, handler, requests, client)
						defer d()

						renamed := discovers[0] + ".renamed"
						err = os.Rename(discovers[0], renamed)
						So(err, ShouldBeNil)

						uploadStarts, uploadResults, skippedResults := p.Put()

						err = client.SendPutResultsToServer(uploadStarts, uploadResults, skippedResults,
							minMBperSecondUploadSpeed, minTimeForUpload, maxStuckTime, logger)
						So(err, ShouldBeNil)

						gotSet, err = client.GetSetByID(exampleSet.Requester, exampleSet.ID())
						So(err, ShouldBeNil)
						So(gotSet.Status, ShouldEqual, set.Complete)
						So(gotSet.NumFiles, ShouldEqual, len(discovers))
						So(gotSet.Uploaded, ShouldEqual, len(discovers)-1)
						So(gotSet.Symlinks, ShouldEqual, 1)
						So(gotSet.Missing, ShouldEqual, 1)
						So(gotSet.Error, ShouldBeBlank)

						Convey("After completion, re-discovery can upload old files that were previously missing", func() {
							err = os.Rename(renamed, discovers[0])
							So(err, ShouldBeNil)

							err = client.TriggerDiscovery(exampleSet.ID())
							So(err, ShouldBeNil)

							ok := <-racCalled
							So(ok, ShouldBeTrue)

							gotSet, err = client.GetSetByID(exampleSet.Requester, exampleSet.ID())
							So(err, ShouldBeNil)
							So(gotSet.Status, ShouldEqual, set.PendingUpload)
							So(gotSet.NumFiles, ShouldEqual, len(discovers))
							So(gotSet.Uploaded, ShouldEqual, 0)

							requests, errg = client.GetSomeUploadRequests()
							So(errg, ShouldBeNil)
							So(len(requests), ShouldEqual, len(discovers))

							gotSet, err = client.GetSetByID(exampleSet.Requester, exampleSet.ID())
							So(err, ShouldBeNil)
							So(gotSet.Status, ShouldEqual, set.PendingUpload)
							So(gotSet.NumFiles, ShouldEqual, len(discovers))
							So(gotSet.Uploaded, ShouldEqual, 0)

							p, d = makePutter(t, handler, requests, client)
							defer d()

							uploadStarts, uploadResults, skippedResults = p.Put()

							err = client.SendPutResultsToServer(uploadStarts, uploadResults, skippedResults,
								minMBperSecondUploadSpeed, minTimeForUpload, maxStuckTime, logger)
							So(err, ShouldBeNil)

							gotSet, err = client.GetSetByID(exampleSet.Requester, exampleSet.ID())
							So(err, ShouldBeNil)
							So(gotSet.Status, ShouldEqual, set.Complete)
							So(gotSet.NumFiles, ShouldEqual, len(discovers))
							So(gotSet.Uploaded, ShouldEqual, 1)
							So(gotSet.Replaced, ShouldEqual, 0)
							So(gotSet.Skipped, ShouldEqual, len(discovers)-1)
							So(gotSet.Symlinks, ShouldEqual, 1)

							entries, errg := client.GetFiles(exampleSet.ID())
							So(errg, ShouldBeNil)
							So(len(entries), ShouldEqual, len(discovers))

							for n, entry := range entries {
								So(entry.Status == set.Uploaded || entry.Status == set.Skipped, ShouldBeTrue)

								if n == 2 {
									So(entry.Type, ShouldEqual, set.Symlink)
								} else {
									So(entry.Type, ShouldEqual, set.Regular)
								}
							}

							for n, local := range discovers {
								remote := strings.Replace(local, localDir, remoteDir, 1)

								_, err = os.Stat(remote)

								So(err, ShouldBeNil)

								if n == 3 {
									remoteMeta, errr := handler.GetMeta(remote)
									So(errr, ShouldBeNil)
									So(remoteMeta, ShouldNotBeNil)
									So(remoteMeta[transfer.MetaKeySymlink], ShouldEqual, entries[2].Dest)

									info, errs := os.Stat(remote)
									So(errs, ShouldBeNil)
									So(info.Size(), ShouldEqual, 0)
								}
							}
						})
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

						for i := 1; i <= int(jobRetries); i++ {
							requests, errg := client.GetSomeUploadRequests()
							So(errg, ShouldBeNil)
							if i == 1 {
								So(len(requests), ShouldEqual, len(discovers))
							} else {
								So(len(requests), ShouldEqual, len(discovers)-1)
							}

							p, d := makePutter(t, handler, requests, client)
							defer d()

							uploadStarts, uploadResults, skippedResults := p.Put()

							err = client.SendPutResultsToServer(uploadStarts, uploadResults, skippedResults,
								minMBperSecondUploadSpeed, minTimeForUpload, maxStuckTime, logger)
							So(err, ShouldBeNil)

							entries, errg := client.GetFiles(exampleSet.ID())
							So(errg, ShouldBeNil)
							So(len(entries), ShouldEqual, len(discovers))

							for j, entry := range entries {
								if j == 2 {
									So(entry.Status, ShouldEqual, set.Uploaded)
									So(entry.Type, ShouldEqual, set.Symlink)
									So(entry.Dest, ShouldNotBeBlank)
									So(entry.Attempts, ShouldEqual, 1)
								} else {
									So(entry.Status, ShouldEqual, set.Failed)
									So(entry.Attempts, ShouldEqual, i)
								}

								switch j {
								case 0:
									So(entry.LastError, ShouldEqual, internal.ErrMockStatFail)
								case 1:
									So(entry.LastError, ShouldEqual, internal.ErrMockPutFail)
								case 3:
									So(entry.LastError, ShouldEqual, internal.ErrMockMetaFail)
								}
							}

							gotSet, err = client.GetSetByID(exampleSet.Requester, exampleSet.ID())
							So(err, ShouldBeNil)
							So(gotSet.Status, ShouldEqual, set.Complete)
							So(gotSet.NumFiles, ShouldEqual, len(discovers))
							So(gotSet.Uploaded, ShouldEqual, 1)
							So(gotSet.Missing, ShouldEqual, 0)
							So(gotSet.Failed, ShouldEqual, len(discovers)-1)
							So(gotSet.Symlinks, ShouldEqual, 1)
							So(gotSet.Error, ShouldBeBlank)
						}

						requests, errg := client.GetSomeUploadRequests()
						So(errg, ShouldBeNil)
						So(len(requests), ShouldEqual, 0)
					})

					Convey("The system issues a failure when local files hang when opened", func() {
						requests, errg := client.GetSomeUploadRequests()
						So(errg, ShouldBeNil)
						So(len(requests), ShouldEqual, len(discovers))

						p, d := makePutter(t, handler, requests, client)
						defer d()

						var forceSlowRead transfer.FileReadTester = func(ctx context.Context, path string) error {
							if path == discovers[0] {
								<-time.After(100 * time.Millisecond)
							}

							return nil
						}

						p.SetFileReadTimeout(1 * time.Millisecond)
						p.SetFileReadTester(forceSlowRead)

						uploadStarts, uploadResults, skippedResults := p.Put()

						err = client.SendPutResultsToServer(uploadStarts, uploadResults, skippedResults,
							minMBperSecondUploadSpeed, minTimeForUpload, maxStuckTime, logger)
						So(err, ShouldBeNil)

						gotSet, err = client.GetSetByID(exampleSet.Requester, exampleSet.ID())
						So(err, ShouldBeNil)
						So(gotSet.Status, ShouldEqual, set.Complete)
						So(gotSet.NumFiles, ShouldEqual, len(discovers))
						So(gotSet.Uploaded, ShouldBeLessThanOrEqualTo, len(discovers)-1)
						So(gotSet.Missing, ShouldEqual, 0)
						So(gotSet.Failed, ShouldBeGreaterThanOrEqualTo, 1)
						So(gotSet.Error, ShouldBeBlank)

						if gotSet.Failed > 1 {
							// random test failures in github CI
							SkipConvey("skipping 3 retries test because too many files failed due to random issue", func() {})

							return
						}

						entries, errg := client.GetFiles(exampleSet.ID())
						So(errg, ShouldBeNil)
						So(len(entries), ShouldEqual, len(discovers))

						for i, entry := range entries {
							switch i {
							case 0:
								So(entry.Status, ShouldEqual, set.Failed)
								So(entry.LastError, ShouldContainSubstring, transfer.ErrReadTimeout)
							case 2:
								So(entry.Type, ShouldEqual, set.Symlink)
								So(entry.Status, ShouldEqual, set.Uploaded)
							default:
								So(entry.Type, ShouldEqual, set.Regular)
								So(entry.Status, ShouldEqual, set.Uploaded)
							}

							So(entry.Attempts, ShouldEqual, 1)
						}

						entries, skippedFails, errg := client.GetFailedFiles(exampleSet.ID())
						So(errg, ShouldBeNil)
						So(len(entries), ShouldEqual, 1)
						So(skippedFails, ShouldEqual, 0)

						Convey("and failures get retried 3 times", func() {
							So(s.queue.Stats().Buried, ShouldEqual, 0)

							for i := 2; i <= int(jobRetries); i++ {
								requests, errg := client.GetSomeUploadRequests()
								So(errg, ShouldBeNil)
								So(len(requests), ShouldEqual, 1)

								p, d := makePutter(t, handler, requests, client)
								p.SetFileReadTimeout(1 * time.Millisecond)
								p.SetFileReadTester(forceSlowRead)

								uploadStarts, uploadResults, skippedResults := p.Put()

								err = client.SendPutResultsToServer(uploadStarts, uploadResults, skippedResults,
									minMBperSecondUploadSpeed, minTimeForUpload, maxStuckTime, logger)
								So(err, ShouldBeNil)

								failedEntries, skippedFails, errg := client.GetFailedFiles(exampleSet.ID())
								So(errg, ShouldBeNil)
								So(len(failedEntries), ShouldEqual, 1)
								So(skippedFails, ShouldEqual, 0)
								So(failedEntries[0].Status, ShouldEqual, set.Failed)
								So(failedEntries[0].Attempts, ShouldEqual, i)

								d()
							}

							requests, errg := client.GetSomeUploadRequests()
							So(errg, ShouldBeNil)
							So(len(requests), ShouldEqual, 0)

							So(s.queue.Stats().Buried, ShouldEqual, 1)

							manualRetry := func() {
								retried, errr := client.RetryFailedSetUploads(exampleSet.ID())
								So(errr, ShouldBeNil)
								So(retried, ShouldEqual, 1)

								requests, errgs := client.GetSomeUploadRequests()
								So(errgs, ShouldBeNil)
								So(len(requests), ShouldBeGreaterThanOrEqualTo, 1)

								p, d := makePutter(t, handler, requests, client)
								defer d()

								uploadStarts, uploadResults, skippedResults := p.Put()

								err = client.SendPutResultsToServer(uploadStarts, uploadResults, skippedResults,
									minMBperSecondUploadSpeed, minTimeForUpload, maxStuckTime, logger)
								So(err, ShouldBeNil)

								failedEntries, skippedFails, errgf := client.GetFailedFiles(exampleSet.ID())
								So(errgf, ShouldBeNil)
								So(len(failedEntries), ShouldEqual, 0)
								So(skippedFails, ShouldEqual, 0)

								entries, errg = client.GetFiles(exampleSet.ID())
								So(errg, ShouldBeNil)
								So(len(entries), ShouldEqual, len(discovers))
								So(entries[0].Status, ShouldEqual, set.Uploaded)
								So(entries[0].Attempts, ShouldEqual, jobRetries+1)
							}

							Convey("whereupon they can be manually retried", func() {
								manualRetry()
							})

							Convey("whereupon they can be manually retried even if not buried in the queue", func() {
								n := s.RemoveBuried(&BuriedFilter{
									User: exampleSet.Requester,
									Set:  exampleSet.Name,
								})
								So(n, ShouldEqual, 1)

								manualRetry()
							})

							Convey("whereupon they can be manually retried even if the set gets re-discovered", func() {
								err = client.TriggerDiscovery(exampleSet.ID())
								So(err, ShouldBeNil)

								ok := <-racCalled
								So(ok, ShouldBeTrue)

								entries, errg = client.GetFiles(exampleSet.ID())
								So(errg, ShouldBeNil)
								So(len(entries), ShouldEqual, len(discovers))
								So(entries[0].Status, ShouldEqual, set.Failed)
								So(entries[0].Attempts, ShouldEqual, jobRetries)
								So(entries[0].LastError, ShouldContainSubstring, transfer.ErrReadTimeout)
								So(entries[0].LastError, ShouldContainSubstring, entries[0].Path)

								manualRetry()
							})
						})
					})

					Convey("The system warns of possibly stuck uploads", func() {
						requests, errg := client.GetSomeUploadRequests()
						So(errg, ShouldBeNil)
						So(len(requests), ShouldEqual, len(discovers))

						slowDur := 1200 * time.Millisecond
						handler.MakePutSlow(requests[0].Remote, slowDur)

						p, d := makePutter(t, handler, requests, client)
						defer d()

						errCh := make(chan error)

						go func() {
							uploadStarts, uploadResults, skippedResults := p.Put()

							errCh <- client.SendPutResultsToServer(uploadStarts, uploadResults, skippedResults,
								minMBperSecondUploadSpeed, 100*time.Millisecond, slowDur*2, logger)
						}()

						<-time.After(600 * time.Millisecond)
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
						So(gotSet.Symlinks, ShouldEqual, 1)
						So(gotSet.Failed, ShouldEqual, 0)
						So(gotSet.Error, ShouldBeBlank)

						entries, errg = client.GetFiles(exampleSet.ID())
						So(errg, ShouldBeNil)
						So(len(entries), ShouldEqual, len(discovers))

						for n, entry := range entries {
							So(entry.Status, ShouldEqual, set.Uploaded)
							if n == 2 {
								So(entry.Type, ShouldEqual, set.Symlink)
							} else {
								So(entry.Type, ShouldEqual, set.Regular)
							}
							So(entry.Attempts, ShouldEqual, 1)
							So(entry.LastError, ShouldBeBlank)
						}
					})

					Convey("...but if it is beyond the max stuck time, it is killed", func() {
						testStart := time.Now()

						requests, errg := client.GetSomeUploadRequests()
						So(errg, ShouldBeNil)
						So(len(requests), ShouldEqual, len(discovers))

						slowDur := 10 * time.Second
						handler.MakePutSlow(requests[0].Remote, slowDur)

						p, d := makePutter(t, handler, requests, client)
						defer d()

						errCh := make(chan error)

						go func() {
							uploadStarts, uploadResults, skippedResults := p.Put()

							errCh <- client.SendPutResultsToServer(uploadStarts, uploadResults, skippedResults,
								minMBperSecondUploadSpeed, 1*time.Millisecond, 2*time.Millisecond, logger)
						}()

						err = <-errCh
						So(err, ShouldNotBeNil)
						So(err.Error(), ShouldContainSubstring, ErrKilledDueToStuck.Error())
						So(time.Since(testStart), ShouldBeLessThan, slowDur/2)

						entries, errg := client.GetFiles(exampleSet.ID())
						So(errg, ShouldBeNil)
						So(len(entries), ShouldEqual, len(discovers))

						firstEntry := entries[0]
						So(firstEntry.Status, ShouldEqual, set.Failed)
						So(firstEntry.Attempts, ShouldEqual, 1)
						So(firstEntry.LastError, ShouldEqual, transfer.ErrStuckTimeout)
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
								Data: &transfer.Request{Set: key},
								TTR:  ttr,
							}
						}

						_, _, err = s.queue.AddMany(context.Background(), ids)
						So(err, ShouldBeNil)

						s.numClients = 10
						So(s.numRequestsToReserve(), ShouldEqual, numRequests/s.numClients)

						numExtra := 200
						ids = make([]*queue.ItemDef, numExtra)

						for i := range ids {
							key := fmt.Sprintf("%d.extra", i)
							ids[i] = &queue.ItemDef{
								Key:  key,
								Data: &transfer.Request{Set: key},
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

				Convey("and add a set with a failing file that can be removed from the set during retries", func() {
					err = client.AddOrUpdateSet(exampleSet)
					So(err, ShouldBeNil)

					files, dirs, _, _ := createTestBackupFiles(t, localDir)
					files = []string{files[0], dirs[0]}

					err = client.MergeFiles(exampleSet.ID(), files)
					So(err, ShouldBeNil)

					err = client.TriggerDiscovery(exampleSet.ID())
					So(err, ShouldBeNil)

					ok := <-racCalled
					So(ok, ShouldBeTrue)

					requests, errg := client.GetSomeUploadRequests()
					So(errg, ShouldBeNil)
					So(len(requests), ShouldEqual, len(files))

					p, d := makePutter(t, handler, requests, client)
					defer d()

					uploadStarts, uploadResults, skippedResults := p.Put()

					err = client.SendPutResultsToServer(uploadStarts, uploadResults, skippedResults,
						minMBperSecondUploadSpeed, minTimeForUpload, maxStuckTime, logger)
					So(err, ShouldBeNil)

					gotSet, errg := client.GetSetByID(exampleSet.Requester, exampleSet.ID())
					So(errg, ShouldBeNil)
					So(gotSet.Uploaded, ShouldEqual, 1)
					So(gotSet.Failed, ShouldEqual, 1)

					err = s.db.RemoveFileEntry(exampleSet.ID(), dirs[0])
					So(err, ShouldBeNil)

					err = client.TriggerDiscovery(exampleSet.ID())
					So(err, ShouldBeNil)

					ok = <-racCalled
					So(ok, ShouldBeTrue)

					requests, errg = client.GetSomeUploadRequests()
					So(errg, ShouldBeNil)
					So(len(requests), ShouldEqual, 2)
					So(requests[0].Local, ShouldEqual, dirs[0])

					p, d = makePutter(t, handler, requests, client)
					defer d()

					uploadStarts, uploadResults, skippedResults = p.Put()

					err = client.SendPutResultsToServer(uploadStarts, uploadResults, skippedResults,
						minMBperSecondUploadSpeed, minTimeForUpload, maxStuckTime, logger)
					So(err, ShouldNotBeNil)

					items := s.queue.AllItems()
					So(len(items), ShouldEqual, 0)

					requests, errg = client.GetSomeUploadRequests()
					So(errg, ShouldBeNil)
					So(len(requests), ShouldEqual, 0)
				})

				Convey("and add a set with non-UTF8 chars in a filename and have the system process it", func() {
					err = client.AddOrUpdateSet(exampleSet)
					So(err, ShouldBeNil)

					path := filepath.Join(localDir, "F_G\xe9o.frm")
					internal.CreateTestFileOfLength(t, path, 1)

					err = client.MergeFiles(exampleSet.ID(), []string{path})
					So(err, ShouldBeNil)

					entries, errg := s.db.GetPureFileEntries(exampleSet.ID())
					So(errg, ShouldBeNil)
					So(len(entries), ShouldEqual, 1)
					So([]byte(entries[0].Path), ShouldResemble, []byte(path))

					err = client.TriggerDiscovery(exampleSet.ID())
					So(err, ShouldBeNil)

					ok := <-racCalled
					So(ok, ShouldBeTrue)

					putSetWithOneFile(t, handler, client, exampleSet, minMBperSecondUploadSpeed, logger)
				})

				Convey("and add a set with non-regular files and have the system skip them as abnormal", func() {
					err = client.AddOrUpdateSet(exampleSet)
					So(err, ShouldBeNil)

					fifoPath := filepath.Join(localDir, "fifo")
					err = syscall.Mkfifo(fifoPath, userPerms)
					So(err, ShouldBeNil)

					regPath := filepath.Join(localDir, "regular")
					internal.CreateTestFileOfLength(t, regPath, 1)

					err = client.MergeFiles(exampleSet.ID(), []string{fifoPath, regPath})
					So(err, ShouldBeNil)

					entries, errg := s.db.GetPureFileEntries(exampleSet.ID())
					So(errg, ShouldBeNil)
					So(len(entries), ShouldEqual, 2)
					So(entries[0].Path, ShouldEqual, fifoPath)
					So(entries[1].Path, ShouldEqual, regPath)

					err = client.TriggerDiscovery(exampleSet.ID())
					So(err, ShouldBeNil)

					ok := <-racCalled
					So(ok, ShouldBeTrue)

					gotSet, errg := client.GetSetByID(exampleSet.Requester, exampleSet.ID())
					So(errg, ShouldBeNil)
					So(gotSet.Status, ShouldEqual, set.PendingUpload)
					So(gotSet.NumFiles, ShouldEqual, 2)
					So(gotSet.Uploaded, ShouldEqual, 0)

					requests, errg := client.GetSomeUploadRequests()
					So(errg, ShouldBeNil)
					So(len(requests), ShouldEqual, 1)

					p, d := makePutter(t, handler, requests, client)
					defer d()

					uploadStarts, uploadResults, skippedResults := p.Put()

					err = client.SendPutResultsToServer(uploadStarts, uploadResults, skippedResults,
						minMBperSecondUploadSpeed, minTimeForUpload, 1*time.Hour, logger)
					So(err, ShouldBeNil)

					gotSet, err = client.GetSetByID(exampleSet.Requester, exampleSet.ID())
					So(err, ShouldBeNil)
					So(gotSet.Status, ShouldEqual, set.Complete)
					So(gotSet.NumFiles, ShouldEqual, 2)
					So(gotSet.Uploaded, ShouldEqual, 1)
					So(gotSet.Failed, ShouldEqual, 0)
					So(gotSet.Missing, ShouldEqual, 0)
					So(gotSet.Abnormal, ShouldEqual, 1)
				})

				Convey("and add a set with non-regular files in a directory and have the system ignore them", func() {
					err = client.AddOrUpdateSet(exampleSet)
					So(err, ShouldBeNil)

					setDir := filepath.Join(localDir, "setfiles")
					err = os.Mkdir(setDir, userPerms)
					So(err, ShouldBeNil)

					fifoPath := filepath.Join(setDir, "fifo")
					err = syscall.Mkfifo(fifoPath, userPerms)
					So(err, ShouldBeNil)

					regPath := filepath.Join(setDir, "regular")
					internal.CreateTestFileOfLength(t, regPath, 1)

					err = client.MergeDirs(exampleSet.ID(), []string{setDir})
					So(err, ShouldBeNil)

					err = client.TriggerDiscovery(exampleSet.ID())
					So(err, ShouldBeNil)

					ok := <-racCalled
					So(ok, ShouldBeTrue)

					gotSet, errg := client.GetSetByID(exampleSet.Requester, exampleSet.ID())
					So(errg, ShouldBeNil)
					So(gotSet.Status, ShouldEqual, set.PendingUpload)
					So(gotSet.NumFiles, ShouldEqual, 1)

					entries, errg := s.db.GetFileEntries(exampleSet.ID())
					So(errg, ShouldBeNil)
					So(len(entries), ShouldEqual, 1)
					So(entries[0].Path, ShouldEqual, regPath)
				})

				Convey("and add a set with non-UTF8 chars in a discovered directory and have the system process it", func() {
					err = client.AddOrUpdateSet(exampleSet)
					So(err, ShouldBeNil)

					dir := filepath.Join(localDir, "dir\xe9todiscover")

					path := filepath.Join(dir, "\xe9o.frm")
					internal.CreateTestFileOfLength(t, path, 1)

					err = client.MergeDirs(exampleSet.ID(), []string{dir})
					So(err, ShouldBeNil)

					err = client.TriggerDiscovery(exampleSet.ID())
					So(err, ShouldBeNil)

					ok := <-racCalled
					So(ok, ShouldBeTrue)

					entries, errg := s.db.GetFileEntries(exampleSet.ID())
					So(errg, ShouldBeNil)
					So(len(entries), ShouldEqual, 1)
					So([]byte(entries[0].Path), ShouldResemble, []byte(path))

					putSetWithOneFile(t, handler, client, exampleSet, minMBperSecondUploadSpeed, logger)
				})

				Convey("and add a set with files and retrieve an example one", func() {
					err = client.AddOrUpdateSet(exampleSet)
					So(err, ShouldBeNil)

					example, errg := client.GetExampleFile(exampleSet.ID())
					So(errg, ShouldBeNil)
					So(example, ShouldBeNil)

					pathExpected := filepath.Join(localDir, "file1.txt")
					internal.CreateTestFileOfLength(t, pathExpected, 1)

					pathOther := filepath.Join(localDir, "file2.txt")
					internal.CreateTestFileOfLength(t, pathOther, 1)

					err = client.MergeFiles(exampleSet.ID(), []string{pathExpected, pathOther})
					So(err, ShouldBeNil)

					entries, errg := s.db.GetPureFileEntries(exampleSet.ID())
					So(errg, ShouldBeNil)
					So(len(entries), ShouldEqual, 2)
					So([]byte(entries[0].Path), ShouldResemble, []byte(pathExpected))

					example, err = client.GetExampleFile(exampleSet.ID())
					So(err, ShouldBeNil)
					So(example.Path, ShouldEqual, pathExpected)
				})

				Convey("and enable database backups which trigger at appropriate times", func() {
					s.db.SetBackupPath(backupPath)
					s.db.SetMinimumTimeBetweenBackups(0)

					_, err = os.Stat(backupPath)
					So(err, ShouldNotBeNil)

					err = client.AddOrUpdateSet(exampleSet)
					So(err, ShouldBeNil)

					exists := internal.WaitForFile(t, backupPath)
					So(exists, ShouldBeTrue)

					stat, errS := os.Stat(backupPath)
					So(errS, ShouldBeNil)

					lastMod := stat.ModTime()

					pathToBackup := filepath.Join(localDir, "a.file")
					internal.CreateTestFileOfLength(t, pathToBackup, 1)

					err = client.MergeFiles(exampleSet.ID(), []string{pathToBackup})
					So(err, ShouldBeNil)

					err = client.TriggerDiscovery(exampleSet.ID())
					So(err, ShouldBeNil)

					ok := <-racCalled
					So(ok, ShouldBeTrue)

					changed := internal.WaitForFileChange(t, backupPath, lastMod)
					So(changed, ShouldBeTrue)

					stat, err = os.Stat(backupPath)
					So(err, ShouldBeNil)

					lastMod = stat.ModTime()

					putSetWithOneFile(t, handler, client, exampleSet, minMBperSecondUploadSpeed, logger)

					changed = internal.WaitForFileChange(t, backupPath, lastMod)
					So(changed, ShouldBeTrue)

					remoteBackupDir := filepath.Join(filepath.Dir(backupPath), "remoteBackup")

					err = os.Mkdir(remoteBackupDir, userPerms)
					So(err, ShouldBeNil)

					remoteBackupPath := filepath.Join(remoteBackupDir, "remoteDB")

					handler = internal.GetLocalHandler()

					s.EnableRemoteDBBackups(remoteBackupPath, handler)

					err = client.AddOrUpdateSet(exampleSet)
					So(err, ShouldBeNil)

					remoteBackupExists := internal.WaitForFile(t, remoteBackupPath)
					So(remoteBackupExists, ShouldBeTrue)
				})

				Convey("and add a set containing directories that can't be accessed, which is shown as a set warning", func() {
					err = client.AddOrUpdateSet(exampleSet)
					So(err, ShouldBeNil)

					setDir := filepath.Join(localDir, "perms_test")

					pathExpected := filepath.Join(setDir, "dir1", "file1.txt")
					internal.CreateTestFileOfLength(t, pathExpected, 1)
					pathExpected2 := filepath.Join(setDir, "dir2", "file2.txt")
					internal.CreateTestFileOfLength(t, pathExpected2, 1)
					pathExpected3 := filepath.Join(setDir, "dir3", "file3.txt")
					internal.CreateTestFileOfLength(t, pathExpected3, 1)

					err = os.Chmod(filepath.Dir(pathExpected2), 0)
					So(err, ShouldBeNil)
					err = os.Chmod(filepath.Dir(pathExpected3), 0)
					So(err, ShouldBeNil)

					defer func() {
						err = os.Chmod(filepath.Dir(pathExpected2), userPerms)
						So(err, ShouldBeNil)
						err = os.Chmod(filepath.Dir(pathExpected3), userPerms)
						So(err, ShouldBeNil)
					}()

					err = client.MergeDirs(exampleSet.ID(), []string{setDir})
					So(err, ShouldBeNil)

					err = client.TriggerDiscovery(exampleSet.ID())
					So(err, ShouldBeNil)

					ok := <-racCalled
					So(ok, ShouldBeTrue)

					gotSet, errg := client.GetSetByID(exampleSet.Requester, exampleSet.ID())
					So(errg, ShouldBeNil)

					entries, errg := client.GetFiles(exampleSet.ID())
					So(errg, ShouldBeNil)
					So(len(entries), ShouldEqual, 1)

					So(gotSet.Warning, ShouldContainSubstring, filepath.Dir(pathExpected2)+"/: permission denied")
					So(gotSet.Warning, ShouldContainSubstring, filepath.Dir(pathExpected3)+"/: permission denied")
				})

				Convey("and add a set with hardlinks defined directly", func() {
					err = client.AddOrUpdateSet(exampleSet)
					So(err, ShouldBeNil)

					path1 := filepath.Join(localDir, "file.link1")
					internal.CreateTestFileOfLength(t, path1, 1)

					path2 := filepath.Join(localDir, "file.link2")
					err = os.Link(path1, path2)
					So(err, ShouldBeNil)

					path3 := filepath.Join(localDir, "file.link3")
					err = os.Link(path1, path3)
					So(err, ShouldBeNil)

					err = client.MergeFiles(exampleSet.ID(), []string{path1, path2, path3})
					So(err, ShouldBeNil)

					Convey("without remote hardlink location set, hardlinks upload multiple times", func() {
						err = client.TriggerDiscovery(exampleSet.ID())
						So(err, ShouldBeNil)

						ok := <-racCalled
						So(ok, ShouldBeTrue)

						gotSet, errg := client.GetSetByID(exampleSet.Requester, exampleSet.ID())
						So(errg, ShouldBeNil)
						So(gotSet.Status, ShouldEqual, set.PendingUpload)
						So(gotSet.NumFiles, ShouldEqual, 3)
						So(gotSet.Uploaded, ShouldEqual, 0)
						So(gotSet.Hardlinks, ShouldEqual, 2)

						requests, errg := client.GetSomeUploadRequests()
						So(errg, ShouldBeNil)
						So(len(requests), ShouldEqual, 3)

						So(requests[0].Local, ShouldEqual, path1)
						So(requests[0].Hardlink, ShouldBeBlank)
						So(requests[1].Local, ShouldEqual, path2)
						So(requests[2].Local, ShouldEqual, path3)

						p, d := makePutter(t, handler, requests, client)
						defer d()

						uploadStarts, uploadResults, skippedResults := p.Put()

						err = client.SendPutResultsToServer(uploadStarts, uploadResults, skippedResults,
							minMBperSecondUploadSpeed, minTimeForUpload, 1*time.Hour, logger)
						So(err, ShouldBeNil)

						info, errs := os.Stat(requests[0].Remote)
						So(errs, ShouldBeNil)
						So(info.Size(), ShouldNotEqual, 0)
						info, errs = os.Stat(requests[1].Remote)
						So(errs, ShouldBeNil)
						So(info.Size(), ShouldNotEqual, 0)
						info, errs = os.Stat(requests[2].Remote)
						So(errs, ShouldBeNil)
						So(info.Size(), ShouldNotEqual, 0)
					})

					Convey("with remote hardlink location set only uploads hardlinks once", func() {
						hardlinksDir := filepath.Join(remoteDir, "mountpoints")
						s.SetRemoteHardlinkLocation(hardlinksDir)

						err = client.TriggerDiscovery(exampleSet.ID())
						So(err, ShouldBeNil)

						ok := <-racCalled
						So(ok, ShouldBeTrue)

						gotSet, errg := client.GetSetByID(exampleSet.Requester, exampleSet.ID())
						So(errg, ShouldBeNil)
						So(gotSet.Status, ShouldEqual, set.PendingUpload)
						So(gotSet.NumFiles, ShouldEqual, 3)
						So(gotSet.Uploaded, ShouldEqual, 0)
						So(gotSet.Hardlinks, ShouldEqual, 2)

						requests, errg := client.GetSomeUploadRequests()
						So(errg, ShouldBeNil)
						So(len(requests), ShouldEqual, 3)

						info, errs := os.Stat(path1)
						So(errs, ShouldBeNil)

						statt, ok := info.Sys().(*syscall.Stat_t)
						So(ok, ShouldBeTrue)

						inoStr := strconv.FormatUint(statt.Ino, 10)

						So(requests[0].Local, ShouldEqual, path1)
						So(requests[0].Hardlink, ShouldBeBlank)
						So(requests[1].Local, ShouldEqual, path2)
						So(requests[1].Hardlink, ShouldContainSubstring, "mountpoints")
						So(requests[1].Hardlink, ShouldContainSubstring, inoStr)
						So(requests[2].Local, ShouldEqual, path3)
						So(requests[2].Hardlink, ShouldContainSubstring, "mountpoints")
						So(requests[2].Hardlink, ShouldContainSubstring, inoStr)

						inodeFile := filepath.Join(hardlinksDir,
							path1,
							inoStr)
						So(requests[1].Hardlink, ShouldEqual, inodeFile)
						So(requests[2].Hardlink, ShouldEqual, inodeFile)

						for _, request := range requests {
							if request.Set != gotSet.Name {
								continue
							}

							request.Status = transfer.RequestStatusUploaded
							err = client.UpdateFileStatus(request)
							So(err, ShouldBeNil)
						}

						err = client.TriggerDiscovery(exampleSet.ID())
						So(err, ShouldBeNil)

						ok = <-racCalled
						So(ok, ShouldBeTrue)

						gotSet, err = client.GetSetByID(exampleSet.Requester, exampleSet.ID())
						So(err, ShouldBeNil)
						So(gotSet.Status, ShouldEqual, set.PendingUpload)
						So(gotSet.NumFiles, ShouldEqual, 3)
						So(gotSet.Uploaded, ShouldEqual, 0)
						So(gotSet.Hardlinks, ShouldEqual, 2)

						Convey("uploading hardlinks sets the hardlink metadata on an empty file"+
							" and uploads real data to inode file", func() {
							requests, errg := client.GetSomeUploadRequests()
							So(errg, ShouldBeNil)
							So(len(requests), ShouldEqual, 3)

							p, d := makePutter(t, handler, requests, client)
							defer d()

							uploadStarts, uploadResults, skippedResults := p.Put()

							err = client.SendPutResultsToServer(uploadStarts, uploadResults, skippedResults,
								minMBperSecondUploadSpeed, minTimeForUpload, 1*time.Hour, logger)
							So(err, ShouldBeNil)

							entries, errg := client.GetFiles(exampleSet.ID())
							So(errg, ShouldBeNil)
							So(len(entries), ShouldEqual, 3)
							So(entries[0].Status, ShouldEqual, set.Uploaded)
							So(entries[1].Status, ShouldEqual, set.Uploaded)
							So(entries[2].Status, ShouldEqual, set.Uploaded)

							transformer, errg := exampleSet.MakeTransformer()
							So(errg, ShouldBeNil)

							remote1, errg := transformer(path2)
							So(errg, ShouldBeNil)

							remoteMeta, errr := handler.GetMeta(remote1)
							So(errr, ShouldBeNil)
							So(remoteMeta, ShouldNotBeNil)
							So(remoteMeta[transfer.MetaKeyHardlink], ShouldEqual, path2)
							So(remoteMeta[transfer.MetaKeyRemoteHardlink], ShouldEqual, inodeFile)

							info, errs := os.Stat(remote1)
							So(errs, ShouldBeNil)
							So(info.Size(), ShouldEqual, 0)

							remote2, errg := transformer(path3)
							So(errg, ShouldBeNil)

							remoteMeta, err = handler.GetMeta(remote2)
							So(err, ShouldBeNil)
							So(remoteMeta, ShouldNotBeNil)
							So(remoteMeta[transfer.MetaKeyHardlink], ShouldEqual, path3)
							So(remoteMeta[transfer.MetaKeyRemoteHardlink], ShouldEqual, inodeFile)

							info, errs = os.Stat(remote2)
							So(errs, ShouldBeNil)
							So(info.Size(), ShouldEqual, 0)

							info, errs = os.Stat(inodeFile)
							So(errs, ShouldBeNil)
							So(info.Size(), ShouldEqual, 1)

							inodeMeta, errm := handler.GetMeta(inodeFile)
							So(errm, ShouldBeNil)
							So(inodeMeta, ShouldNotBeNil)
							So(inodeMeta[transfer.MetaKeyHardlink], ShouldEqual, path1)
							So(inodeMeta[transfer.MetaKeyRemoteHardlink], ShouldBeBlank)

							gotSet, err = client.GetSetByID(exampleSet.Requester, exampleSet.ID())
							So(err, ShouldBeNil)
							So(gotSet.Status, ShouldEqual, set.Complete)
							So(gotSet.NumFiles, ShouldEqual, 3)
							So(gotSet.Uploaded, ShouldEqual, 3)
							So(gotSet.Hardlinks, ShouldEqual, 2)
							So(gotSet.SizeTotal, ShouldEqual, 1)

							Convey("moving all files of an inode uploads hardlinks to new location", func() {
								path4 := filepath.Join(localDir, "file2.link1")
								err = os.Rename(path1, path4)
								So(err, ShouldBeNil)

								path5 := filepath.Join(localDir, "file2.link2")
								err = os.Rename(path2, path5)
								So(err, ShouldBeNil)

								path6 := filepath.Join(localDir, "file2.link3")
								err = os.Rename(path3, path6)
								So(err, ShouldBeNil)

								entries, err = client.GetFiles(exampleSet.ID())
								So(err, ShouldBeNil)

								for _, file := range entries {
									err = s.db.RemoveFileEntry(exampleSet.ID(), file.Path)
									So(err, ShouldBeNil)

									err = s.db.UpdateBasedOnRemovedEntry(exampleSet.ID(), file)
									So(err, ShouldBeNil)
								}

								err = client.MergeFiles(exampleSet.ID(), []string{path4, path5, path6})
								So(err, ShouldBeNil)

								err = client.TriggerDiscovery(exampleSet.ID())
								So(err, ShouldBeNil)

								ok := <-racCalled
								So(ok, ShouldBeTrue)

								gotSet, err = client.GetSetByID(exampleSet.Requester, exampleSet.ID())
								So(err, ShouldBeNil)
								So(gotSet.Status, ShouldEqual, set.PendingUpload)
								So(gotSet.NumFiles, ShouldEqual, 3)
								So(gotSet.Uploaded, ShouldEqual, 0)
								So(gotSet.Hardlinks, ShouldEqual, 2)

								requests2, errg := client.GetSomeUploadRequests()
								So(errg, ShouldBeNil)
								So(len(requests), ShouldEqual, 3)

								So(requests2[0].Local, ShouldEqual, path4)
								So(requests2[0].Hardlink, ShouldBeBlank)
								So(requests2[1].Local, ShouldEqual, path5)
								So(requests2[1].Hardlink, ShouldContainSubstring, "mountpoints")
								So(requests2[1].Hardlink, ShouldContainSubstring, inoStr)
								So(requests2[1].Hardlink, ShouldNotEqual, requests[1].Hardlink)
								So(requests2[2].Local, ShouldEqual, path6)
								So(requests2[2].Hardlink, ShouldContainSubstring, "mountpoints")
								So(requests2[2].Hardlink, ShouldContainSubstring, inoStr)
								So(requests2[2].Hardlink, ShouldNotEqual, requests[2].Hardlink)

								inodeFile = filepath.Join(hardlinksDir,
									path4,
									inoStr)
								So(requests2[1].Hardlink, ShouldEqual, inodeFile)
								So(requests2[2].Hardlink, ShouldEqual, inodeFile)
							})
						})
					})
				})

				Convey("and add a set with hardlinks in a directory which only uploads the file once", func() {
					err = client.AddOrUpdateSet(exampleSet)
					So(err, ShouldBeNil)

					hdir := filepath.Join(localDir, "hardlinks")
					err = os.Mkdir(hdir, userPerms)
					So(err, ShouldBeNil)

					path1 := filepath.Join(hdir, "file.link1")
					internal.CreateTestFileOfLength(t, path1, 1)

					path2 := filepath.Join(hdir, "file.link2")
					err = os.Link(path1, path2)
					So(err, ShouldBeNil)

					err = client.MergeDirs(exampleSet.ID(), []string{hdir})
					So(err, ShouldBeNil)

					err = client.TriggerDiscovery(exampleSet.ID())
					So(err, ShouldBeNil)

					ok := <-racCalled
					So(ok, ShouldBeTrue)

					gotSet, errg := client.GetSetByID(exampleSet.Requester, exampleSet.ID())
					So(errg, ShouldBeNil)
					So(gotSet.Status, ShouldEqual, set.PendingUpload)
					So(gotSet.NumFiles, ShouldEqual, 2)
					So(gotSet.Uploaded, ShouldEqual, 0)
					So(gotSet.Hardlinks, ShouldEqual, 1)
				})

				Convey("and add a set with previously added moved files, which are not treated as hardlinks", func() {
					err = client.AddOrUpdateSet(exampleSet)
					So(err, ShouldBeNil)

					dir := filepath.Join(localDir, "1")
					err = os.Mkdir(dir, userPerms)
					So(err, ShouldBeNil)

					path1 := filepath.Join(dir, "file1")
					internal.CreateTestFileOfLength(t, path1, 1)

					err = client.MergeDirs(exampleSet.ID(), []string{dir})
					So(err, ShouldBeNil)

					err = client.TriggerDiscovery(exampleSet.ID())
					So(err, ShouldBeNil)

					ok := <-racCalled
					So(ok, ShouldBeTrue)

					path2 := filepath.Join(dir, "file2")
					err = os.Rename(path1, path2)
					So(err, ShouldBeNil)

					err = client.AddOrUpdateSet(exampleSet)
					So(err, ShouldBeNil)

					err = client.TriggerDiscovery(exampleSet.ID())
					So(err, ShouldBeNil)

					ok = <-racCalled
					So(ok, ShouldBeTrue)

					gotSet, err := client.GetSetByID(exampleSet.Requester, exampleSet.ID())
					So(err, ShouldBeNil)
					So(gotSet.Status, ShouldEqual, set.PendingUpload)
					So(gotSet.NumFiles, ShouldEqual, 2)
					So(gotSet.Uploaded, ShouldEqual, 0)
					So(gotSet.Hardlinks, ShouldEqual, 0)
				})
			})
		})
	})
}

func TestServerHelperFunctions(t *testing.T) {
	Convey("With a PTrie and a path", t, func() {
		entryPath := "/path/to"

		pathsForTree := []string{"/path/", "/some/path/to/myF", "/path/toFile", "/path/to/file"}

		tree := ptrie.New[bool]()
		for _, path := range pathsForTree {
			err := tree.Put([]byte(path), true)
			So(err, ShouldBeNil)
		}

		Convey("getChildEntriesFromPTrie returns only children paths", func() {
			paths := getChildPathsFromPTrie(entryPath, tree)
			So(paths, ShouldHaveLength, 1)
			So(paths[0], ShouldEqual, "/path/to/file")

			paths = getChildPathsFromPTrie(entryPath+"/", tree)
			So(paths, ShouldHaveLength, 1)
		})

		Convey("isDirentRemovedFromSet works for dirs", func() {
			dirent := set.Dirent{
				Path: entryPath,
				Mode: os.ModeDir,
			}

			isRemoved, match := isDirentRemovedFromSet(&dirent, tree)
			So(isRemoved, ShouldBeTrue)
			So(match, ShouldEqual, "/path/")
		})

		Convey("isDirentRemovedFromSet works for files", func() {
			dirent := set.Dirent{
				Path: "/some/path/to/myFile",
			}

			isRemoved, _ := isDirentRemovedFromSet(&dirent, tree)
			So(isRemoved, ShouldBeFalse)
		})
	})
}

func TestDiscoveryCoordinator(t *testing.T) {
	Convey("With a new DiscoveryCoordinator", t, func() {
		dc := newDiscoveryCoordinator()

		sid := "1"

		var remCount atomic.Int32

		var discoveryFinished atomic.Bool

		numberOfRemovals := 10
		removals := make(chan bool, numberOfRemovals)
		discoveryStarted := make(chan bool)
		finished := make(chan bool)

		mockDiscovery := func(sid string) {
			dc.StartDiscovery(sid)
			close(discoveryStarted)

			time.Sleep(500 * time.Millisecond)

			discoveryFinished.Store(true)

			dc.DiscoveryHappened(sid)
		}

		mockRemoval := func(sid string) {
			dc.WillRemove(sid)

			for range numberOfRemovals {
				_ = dc.WaitForDiscovery(sid)

				time.Sleep(10 * time.Millisecond)

				select {
				case <-finished:
					return
				default:
					remCount.Add(1)
					removals <- true

					dc.AllowDiscovery(sid)
				}
			}

			dc.RemovalDone(sid)
		}

		Convey("You can start discovery and then run a removal before discovery finishes", func() {
			go mockDiscovery(sid)

			<-discoveryStarted

			go mockRemoval(sid)

			for {
				if discoveryFinished.Load() {
					break
				}

				So(remCount.Load(), ShouldEqual, 0)
				time.Sleep(100 * time.Millisecond)
			}

			<-removals

			So(remCount.Load(), ShouldNotEqual, 0)

			close(finished)
			clearChannel(removals)
		})

		Convey("You can start removals and then trigger discovery which will get priority", func() {
			go mockRemoval(sid)

			<-removals

			go mockDiscovery(sid)

			<-discoveryStarted

			removalsCompletedBeforeDiscovery := remCount.Load()

			clearChannel(removals)

			for {
				if discoveryFinished.Load() {
					break
				}

				So(remCount.Load(), ShouldEqual, removalsCompletedBeforeDiscovery)
				time.Sleep(100 * time.Millisecond)
			}

			<-removals

			So(remCount.Load(), ShouldBeGreaterThan, removalsCompletedBeforeDiscovery)

			close(finished)
			clearChannel(removals)
		})
	})
}

func clearChannel(ch chan bool) {
	for len(ch) > 0 {
		<-ch
	}
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
		internal.CreateTestFileOfLength(t, path, i+1)
	}

	for i, path := range discovers {
		internal.CreateTestFileOfLength(t, path, i+1)
	}

	symlinkPath := filepath.Join(dirs[0], "symlink")
	if err := os.Symlink(files[0], symlinkPath); err != nil {
		t.Fatalf("failed to create symlink: %s", err)
	}

	discovers = append(discovers, symlinkPath)

	return files, dirs, discovers, symlinkPath
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
		internal.CreateTestFileOfLength(t, path, 1)
	}

	return paths
}

func makePutter(t *testing.T, handler transfer.Handler, requests []*transfer.Request,
	client *Client) (*transfer.Putter, func()) {
	t.Helper()

	p, errp := transfer.New(handler, requests)
	So(errp, ShouldBeNil)

	d := func() {
		p.Cleanup()
	}

	err := client.StartingToCreateCollections()
	So(err, ShouldBeNil)

	qs, err := client.GetQueueStatus()
	So(err, ShouldBeNil)
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

// putSetWithOneFile tests that we can successfully upload a set with 1 file in
// it.
func putSetWithOneFile(t *testing.T, handler transfer.Handler, client *Client,
	exampleSet *set.Set, minMBperSecondUploadSpeed float64, logger log15.Logger) {
	t.Helper()

	gotSet, err := client.GetSetByID(exampleSet.Requester, exampleSet.ID())
	So(err, ShouldBeNil)
	So(gotSet.Status, ShouldEqual, set.PendingUpload)
	So(gotSet.NumFiles, ShouldEqual, 1)
	So(gotSet.Uploaded, ShouldEqual, 0)

	requests, err := client.GetSomeUploadRequests()
	So(err, ShouldBeNil)
	So(len(requests), ShouldEqual, 1)

	p, d := makePutter(t, handler, requests, client)
	defer d()

	uploadStarts, uploadResults, skippedResults := p.Put()

	err = client.SendPutResultsToServer(uploadStarts, uploadResults, skippedResults,
		minMBperSecondUploadSpeed, minTimeForUpload, 1*time.Hour, logger)
	So(err, ShouldBeNil)

	gotSet, err = client.GetSetByID(exampleSet.Requester, exampleSet.ID())
	So(err, ShouldBeNil)
	So(gotSet.Status, ShouldEqual, set.Complete)
	So(gotSet.NumFiles, ShouldEqual, 1)
	So(gotSet.Uploaded, ShouldEqual, 1)
}

func createRemoteHardlink(t *testing.T, handler remove.Handler, lPath, rPath,
	filePath, inodePath string, set *set.Set) {
	t.Helper()

	hardlinkMeta := map[string]string{
		transfer.MetaKeySets:           set.Name,
		transfer.MetaKeyRequester:      set.Requester,
		transfer.MetaKeyHardlink:       "hardlink",
		transfer.MetaKeyRemoteHardlink: inodePath,
	}

	createRemoteObject(t, handler, hardlinkMeta, rPath)

	err := os.Link(filePath, lPath)
	So(err, ShouldBeNil)
}

func createRemoteObject(t *testing.T, handler remove.Handler, meta map[string]string, rPath string) {
	t.Helper()

	internal.CreateTestFileOfLength(t, rPath, 1)

	err := handler.AddMeta(rPath, meta)
	So(err, ShouldBeNil)
}

func waitForDiscovery(t *testing.T, client *Client, given *set.Set) {
	t.Helper()

	discovered := given.LastDiscovery

	internal.RetryUntilWorksCustom(t, func() error { //nolint:errcheck
		tickerSet, errg := client.GetSetByID(given.Requester, given.ID())
		So(errg, ShouldBeNil)

		if tickerSet.LastDiscovery.After(discovered) {
			return nil
		}

		return errNotDiscovered
	}, given.MonitorTime*2, given.MonitorTime/10)
}

func waitForRemovals(t *testing.T, client *Client, given *set.Set) {
	t.Helper()

	internal.RetryUntilWorksCustom(t, func() error { //nolint:errcheck
		tickerSet, errg := client.GetSetByID(given.Requester, given.ID())
		So(errg, ShouldBeNil)

		if tickerSet.NumObjectsRemoved == tickerSet.NumObjectsToBeRemoved {
			return nil
		}

		return errNotFinishedRemoving
	}, time.Second*10, time.Millisecond*100)
}

func makeGivenSetComplete(numExpectedRequests int, setName string, client *Client) {
	for i := range numExpectedRequests/100 + 1 {
		changeSetFilesStatus(min(100, numExpectedRequests-100*i), setName, client, transfer.RequestStatusUploaded)
	}
}

func changeSetFilesStatus(numExpectedRequests int, setName string, client *Client, status transfer.RequestStatus) {
	requests, errg := client.GetSomeUploadRequests()
	So(errg, ShouldBeNil)
	So(len(requests), ShouldEqual, numExpectedRequests)

	for _, request := range requests {
		if request.Set != setName {
			continue
		}

		request.Status = status
		err := client.UpdateFileStatus(request)
		So(err, ShouldBeNil)
	}
}
