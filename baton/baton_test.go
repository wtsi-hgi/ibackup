/*******************************************************************************
 * Copyright (c) 2025 Genome Research Ltd.
 *
 * Author: Rosie Kern <rk18@sanger.ac.uk>
 * Author: Iaroslav Popov <ip13@sanger.ac.uk>
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

package baton

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/ibackup/baton/meta"
	"github.com/wtsi-hgi/ibackup/errs"
	"github.com/wtsi-hgi/ibackup/internal"
	"github.com/wtsi-hgi/ibackup/internal/testutil"
	ex "github.com/wtsi-npg/extendo/v3"
)

var testStartTime time.Time //nolint:gochecknoglobals

var icmd *testutil.ICommander //nolint:gochecknoglobals

var (
	errExpectedStatToFindUploadedObject = errors.New("expected Stat to find uploaded object")
	errTransferStuck                    = errors.New("transfer did not return")
)

func countReplicates(reps []ex.Replicate) (int, int) {
	good, bad := 0, 0

	for _, r := range reps {
		if r.Valid {
			good++
		} else {
			bad++
		}
	}

	return good, bad
}

func TestBaton(t *testing.T) {
	testStartTime = time.Now()

	h, errgbh := GetBatonHandler()
	if errgbh != nil {
		t.Logf("GetBatonHandler error: %s", errgbh)
		SkipConvey("Skipping baton tests since couldn't find baton", t, func() {})

		return
	}

	remotePath := testutil.RequireIRODSTestCollection(t)
	if remotePath == "" {
		SkipConvey("Skipping baton tests since IBACKUP_TEST_COLLECTION is not defined", t, func() {})

		return
	}

	icmd = testutil.NewIcommander(t)
	if icmd == nil {
		t.Skip("skipping baton tests since iCommands are unavailable")
	}

	localPath := t.TempDir()

	Convey("Given a baton handler", t, func() {
		meta := map[string]string{
			"ibackup:test:a": "1",
			"ibackup:test:b": "2",
		}

		Convey("And a local file", func() {
			file1local := filepath.Join(localPath, "file1")
			file1remote := filepath.Join(remotePath, "file1")

			internal.CreateTestFileOfLength(t, file1local, 1)

			Convey("You can stat a file not in iRODS", func() {
				exists, _, errs := h.Stat(file1remote)
				So(errs, ShouldBeNil)
				So(exists, ShouldBeFalse)
			})

			Convey("You can get replica counts for a file not in iRODS", func() {
				exists, good, bad, err := h.ReplicaCounts(file1remote)
				So(err, ShouldBeNil)
				So(exists, ShouldBeFalse)
				So(good, ShouldEqual, 0)
				So(bad, ShouldEqual, 0)
			})

			Convey("You can try to remove the file from iRODS and receive an error", func() {
				err := h.RemoveFile(file1remote)
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldContainSubstring, "does not exist")
			})

			Convey("You can put the file in iRODS", func() {
				err := h.Put(file1local, file1remote)
				So(err, ShouldBeNil)

				So(isObjectInIRODS(remotePath, "file1"), ShouldBeTrue)

				Convey("And you can get its replica counts", func() {
					exists, good, bad, errr := h.ReplicaCounts(file1remote)
					So(errr, ShouldBeNil)
					So(exists, ShouldBeTrue)

					it, existsIt, errIt := h.listItemWithReplicates(file1remote)
					So(errIt, ShouldBeNil)
					So(existsIt, ShouldBeTrue)
					So(len(it.IReplicates), ShouldBeGreaterThan, 0)

					expectedGood, expectedBad := countReplicates(it.IReplicates)
					So(good, ShouldEqual, expectedGood)
					So(bad, ShouldEqual, expectedBad)

					exists2, good2, bad2, errr2 := h.ReplicaCounts(file1remote)
					So(errr2, ShouldBeNil)
					So(exists2, ShouldBeTrue)
					So(good2, ShouldEqual, good)
					So(bad2, ShouldEqual, bad)
				})

				Convey("And putting a new file in the same location will overwrite existing object", func() {
					file2local := filepath.Join(localPath, "file2")
					sizeOfFile2 := 10

					internal.CreateTestFileOfLength(t, file2local, sizeOfFile2)

					err = h.Put(file2local, file1remote)
					So(err, ShouldBeNil)

					So(getSizeOfObject(file1remote), ShouldEqual, sizeOfFile2)
				})

				Convey("And then you can add metadata to it", func() {
					errm := h.AddMeta(file1remote, meta)
					So(errm, ShouldBeNil)

					So(getRemoteMeta(file1remote), ShouldContainSubstring, "ibackup:test:a")
					So(getRemoteMeta(file1remote), ShouldContainSubstring, "ibackup:test:b")
				})

				Convey("And given metadata on the file in iRODS", func() {
					for k, v := range meta {
						addRemoteMeta(file1remote, k, v)
					}

					Convey("You can get the metadata from a file in iRODS", func() {
						fileMeta, errm := h.GetMeta(file1remote)
						So(errm, ShouldBeNil)

						compareMetasWithSize(t, fileMeta, meta, 1)

						Convey("And you can close the put and meta clients", func() {
							h.Cleanup()

							So(h.AllClientsStopped(), ShouldBeTrue)
						})
					})

					Convey("You can stat a file and get its metadata", func() {
						exists, fileMeta, errm := h.Stat(file1remote)
						So(errm, ShouldBeNil)
						So(exists, ShouldBeTrue)
						compareMetasWithSize(t, fileMeta, meta, 0)
					})

					Convey("You can query if files contain specific metadata", func() {
						files, errq := h.QueryMeta(remotePath, map[string]string{"ibackup:test:a": "1"})
						So(errq, ShouldBeNil)

						So(len(files), ShouldEqual, 1)
						So(files, ShouldContain, file1remote)

						files, errq = h.QueryMeta(remotePath, map[string]string{"ibackup:test:a": "2"})
						So(errq, ShouldBeNil)

						So(len(files), ShouldEqual, 0)
					})

					Convey("You can remove specific metadata from a file in iRODS", func() {
						errm := h.RemoveMeta(file1remote, map[string]string{"ibackup:test:a": "1"})
						So(errm, ShouldBeNil)

						fileMeta, errm := h.GetMeta(file1remote)
						So(errm, ShouldBeNil)

						compareMetasWithSize(t, fileMeta, map[string]string{"ibackup:test:b": "2"}, 1)
					})
				})

				Convey("And then you can remove it from iRODS", func() {
					err = h.RemoveFile(file1remote)
					So(err, ShouldBeNil)

					So(isObjectInIRODS(remotePath, "file1"), ShouldBeFalse)

					Convey("And you can close the put and remove clients", func() {
						h.Cleanup()

						So(h.AllClientsStopped(), ShouldBeTrue)
					})
				})
			})
		})

		Convey("You can open collection clients and put an empty dir in iRODS", func() {
			dir1remote := filepath.Join(remotePath, "dir1")
			err := h.EnsureCollection(dir1remote)
			So(err, ShouldBeNil)

			So(h.collPool.IsOpen(), ShouldBeTrue)

			for _, client := range h.collClients {
				So(client.IsRunning(), ShouldBeTrue)
			}

			So(isObjectInIRODS(remotePath, "dir1"), ShouldBeTrue)

			Convey("Then you can close the collection clients", func() {
				err = h.CollectionsDone()
				So(err, ShouldBeNil)

				So(h.collPool.IsOpen(), ShouldBeFalse)
				So(len(h.collClients), ShouldEqual, 0)

				Convey("And then you can remove the dir from iRODS", func() {
					err = h.RemoveDir(dir1remote)
					So(err, ShouldBeNil)

					So(isObjectInIRODS(remotePath, "dir1"), ShouldBeFalse)

					Convey("And you can close the remove and collections clients", func() {
						h.Cleanup()

						So(h.AllClientsStopped(), ShouldBeTrue)
					})
				})
			})
		})
	})
}

func TestBatonConcurrentClientInit(t *testing.T) {
	_, errgbh := GetBatonHandler()
	if errgbh != nil {
		t.Logf("GetBatonHandler error: %s", errgbh)
		SkipConvey("Skipping baton concurrency test since couldn't find baton", t, func() {})

		return
	}

	remotePath := testutil.RequireIRODSTestCollection(t)
	if remotePath == "" {
		SkipConvey("Skipping baton concurrency test since IBACKUP_TEST_COLLECTION is not defined", t, func() {})

		return
	}

	icmd = testutil.NewIcommander(t)
	if icmd == nil {
		t.Skip("skipping baton concurrency test since iCommands are unavailable")
	}

	Convey("Concurrent Stat is safe during lazy client init", t, func() {
		localPath := t.TempDir()
		fileLocal := filepath.Join(localPath, "file")
		fileRemote := filepath.Join(remotePath, "file")

		internal.CreateTestFileOfLength(t, fileLocal, 1)

		hPut, err := GetBatonHandler()
		So(err, ShouldBeNil)

		err = hPut.Put(fileLocal, fileRemote)
		So(err, ShouldBeNil)

		hPut.Cleanup()

		h, err := GetBatonHandler()
		So(err, ShouldBeNil)
		Reset(func() {
			h.Cleanup()
		})

		start := make(chan struct{})

		const goroutines = 32

		var wg sync.WaitGroup
		wg.Add(goroutines)

		errCh := make(chan error, goroutines)

		for range goroutines {
			go func() {
				defer wg.Done()

				<-start

				exists, _, err := h.Stat(fileRemote)
				if err != nil {
					errCh <- err

					return
				}

				if !exists {
					errCh <- errExpectedStatToFindUploadedObject
				}
			}()
		}

		close(start)
		wg.Wait()
		close(errCh)

		var errs []error
		for err := range errCh {
			errs = append(errs, err)
		}

		So(errs, ShouldBeEmpty)
	})

	Convey("Cleanup is safe during lazy client init, leaving a usable handler", t, func() {
		localPath := t.TempDir()
		fileLocal := filepath.Join(localPath, "file")
		fileRemote := filepath.Join(remotePath, "file")

		internal.CreateTestFileOfLength(t, fileLocal, 1)

		hPut, err := GetBatonHandler()
		So(err, ShouldBeNil)

		err = hPut.Put(fileLocal, fileRemote)
		So(err, ShouldBeNil)

		hPut.Cleanup()

		h, err := GetBatonHandler()
		So(err, ShouldBeNil)
		Reset(func() {
			h.Cleanup()
		})

		start := make(chan struct{})

		var wg sync.WaitGroup

		wg.Go(func() {
			<-start

			h.Stat(fileRemote) //nolint:errcheck
		})

		wg.Go(func() {
			<-start

			h.Cleanup()
		})

		close(start)
		wg.Wait()

		exists, _, err := h.Stat(fileRemote)
		So(err, ShouldBeNil)
		So(exists, ShouldBeTrue)

		h.Cleanup()
		So(h.AllClientsStopped(), ShouldBeTrue)
	})

	Convey("Collection clients replaced after a failed MkDir can be checked concurrently", t, func() {
		localPath := t.TempDir()
		fileLocal := filepath.Join(localPath, "file")
		fileRemote := filepath.Join(remotePath, "notadir")

		internal.CreateTestFileOfLength(t, fileLocal, 1)

		h, err := GetBatonHandler()
		So(err, ShouldBeNil)
		Reset(func() {
			h.Cleanup()
		})

		So(h.Put(fileLocal, fileRemote), ShouldBeNil)

		ensureDone := make(chan error, 1)

		go func() {
			ensureDone <- h.EnsureCollection(filepath.Join(fileRemote, "sub"))
		}()

		var ensureErr error

	poll:
		for {
			select {
			case ensureErr = <-ensureDone:
				break poll
			default:
				h.AllClientsStopped()
			}
		}

		So(ensureErr, ShouldNotBeNil)
		So(h.CollectionsDone(), ShouldBeNil)
	})

	Convey("Concurrent EnsureCollection callers each get their own collection's result", t, func() {
		localPath := t.TempDir()
		fileLocal := filepath.Join(localPath, "file")
		fileRemote := filepath.Join(remotePath, "notadir-own-result")
		failColl := filepath.Join(fileRemote, "sub")
		okColl := filepath.Join(remotePath, "ensure-own-result")

		internal.CreateTestFileOfLength(t, fileLocal, 1)

		h, err := GetBatonHandler()
		So(err, ShouldBeNil)
		Reset(func() {
			h.Cleanup()
		})

		So(h.Put(fileLocal, fileRemote), ShouldBeNil)

		failErrCh := make(chan error, 1)

		go func() {
			failErrCh <- h.EnsureCollection(failColl)
		}()

		// Let the failing call reach a worker and start waiting for its
		// result; its MkDir retries outlast the rest of this test.
		time.Sleep(time.Second)

		okErrCh := make(chan error, 1)

		go func() {
			okErrCh <- h.EnsureCollection(okColl)
		}()

		var (
			okReturned, failReturnedFirst bool
			okErr                         error
		)

		select {
		case okErr = <-okErrCh:
			okReturned = true
		case <-failErrCh:
			failReturnedFirst = true
		case <-time.After(operationTimeout / 6):
		}

		So(failReturnedFirst, ShouldBeFalse)
		So(okReturned, ShouldBeTrue)
		So(okErr, ShouldBeNil)

		_, err = icmd.ILS(okColl)
		So(err, ShouldBeNil)

		h.Cleanup()

		So(<-failErrCh, ShouldNotBeNil)
	})

	Convey("Cleanup during concurrent EnsureCollection calls makes them return, leaving a usable handler", t, func() {
		parent := filepath.Join(remotePath, "ensure-cleanup")

		h, err := GetBatonHandler()
		So(err, ShouldBeNil)
		Reset(func() {
			h.Cleanup()
		})

		const callers = 32

		ensureErrs := make([]error, callers)
		firstDone := make(chan struct{})

		var (
			once sync.Once
			wg   sync.WaitGroup
		)

		for i := range callers {
			wg.Go(func() {
				ensureErrs[i] = h.EnsureCollection(filepath.Join(parent, fmt.Sprintf("d%02d", i)))

				once.Do(func() { close(firstDone) })
			})
		}

		<-firstDone
		h.Cleanup()

		callersReturned := make(chan struct{})

		go func() {
			wg.Wait()
			close(callersReturned)
		}()

		var returnedPromptly bool

		select {
		case <-callersReturned:
			returnedPromptly = true
		case <-time.After(operationTimeout / 6):
		}

		So(returnedPromptly, ShouldBeTrue)

		So(h.EnsureCollection(parent), ShouldBeNil)

		output, err := icmd.ILS(parent)
		So(err, ShouldBeNil)

		// Each caller's nil result means its own collection was made, and any
		// error is the stopped error for its own collection.
		var nilButNotMade, notOwnStopped int

		for i, ensureErr := range ensureErrs {
			coll := filepath.Join(parent, fmt.Sprintf("d%02d", i))

			switch {
			case ensureErr == nil:
				if !strings.Contains(string(output), coll+"\n") {
					nilButNotMade++
				}
			case !errors.Is(ensureErr, errs.PathError{Msg: ErrCollectionsStopped, Path: coll}):
				notOwnStopped++
			}
		}

		So(nilButNotMade, ShouldEqual, 0)
		So(notOwnStopped, ShouldEqual, 0)
		So(h.CollectionsDone(), ShouldBeNil)

		// Collection creation interrupted by the Cleanup must not still be
		// retrying, now that CollectionsDone() has discarded the clients.
		time.Sleep(2 * operationMinBackoff)
	})

	Convey("EnsureCollection after Cleanup succeeds without retrying", t, func() {
		coll := filepath.Join(remotePath, "ensure-after-cleanup")

		h, err := GetBatonHandler()
		So(err, ShouldBeNil)
		Reset(func() {
			h.Cleanup()
		})

		So(h.EnsureCollection(coll), ShouldBeNil)

		h.Cleanup()

		// The collection exists, so only a failed attempt and its retry
		// backoff, not a slow iRODS mkdir, could take this long.
		start := time.Now()

		So(h.EnsureCollection(coll), ShouldBeNil)
		So(time.Since(start), ShouldBeLessThan, operationMinBackoff)
	})

	Convey("CollectionsDone without an earlier EnsureCollection works", t, func() {
		h, err := GetBatonHandler()
		So(err, ShouldBeNil)
		Reset(func() {
			h.Cleanup()
		})

		So(func() { err = h.CollectionsDone() }, ShouldNotPanic)
		So(err, ShouldBeNil)
	})

	Convey("GetClientsFromPoolConcurrently failing partway leaves no started client running", t, func() {
		h, err := GetBatonHandler()
		So(err, ShouldBeNil)

		// A pool that can only make 1 client lets 1 Get succeed while the
		// other times out.
		params := ex.DefaultClientPoolParams
		params.MaxSize = 1
		pool := ex.NewClientPool(params, "")
		Reset(pool.Close)

		before := runningBatonDoChildren(t)

		_, err = h.GetClientsFromPoolConcurrently(pool, 2)
		So(err, ShouldNotBeNil)

		var started []int

		for pid := range runningBatonDoChildren(t) {
			if !before[pid] {
				started = append(started, pid)
			}
		}

		So(started, ShouldBeEmpty)
	})

	Convey("Failing to connect leaves no client pool checking for clients", t, func() {
		h, err := GetBatonHandler()
		So(err, ShouldBeNil)
		Reset(h.Cleanup)

		before := poolCheckerGoroutines()

		withoutBatonDo(t)

		Convey("when making a put client", func() {
			So(h.Put(os.DevNull, filepath.Join(remotePath, "no-baton")), ShouldNotBeNil)
			So(newPoolCheckersAfterSettling(before), ShouldBeEmpty)
		})

		Convey("when making collection clients", func() {
			So(h.EnsureCollection(remotePath), ShouldNotBeNil)
			So(newPoolCheckersAfterSettling(before), ShouldBeEmpty)
		})
	})

	Convey("Failing to replace a collection client leaves no client pool checking for clients", t, func() {
		localPath := t.TempDir()
		fileLocal := filepath.Join(localPath, "file")
		fileRemote := filepath.Join(remotePath, "notadir-no-baton")

		internal.CreateTestFileOfLength(t, fileLocal, 1)

		h, err := GetBatonHandler()
		So(err, ShouldBeNil)
		Reset(h.Cleanup)

		So(h.Put(fileLocal, fileRemote), ShouldBeNil)
		So(h.EnsureCollection(remotePath), ShouldBeNil)

		// Our open collection client pool must be seen, else nothing would be.
		before := poolCheckerGoroutines()
		So(before, ShouldNotBeEmpty)

		withoutBatonDo(t)

		// MkDir under a data object fails, so a new client is tried for; once
		// the retry backoff is sleeping, that has been tried and failed.
		ensureDone := make(chan error, 1)

		go func() {
			ensureDone <- h.EnsureCollection(filepath.Join(fileRemote, "sub"))
		}()

		So(waitForGoroutineIn("backoff/time.(*Sleeper).Sleep", 30*time.Second), ShouldBeTrue)

		h.Cleanup()
		So(<-ensureDone, ShouldNotBeNil)

		So(newPoolCheckersAfterSettling(before), ShouldBeEmpty)
	})

	Convey("GetMeta racing a concurrent Cleanup returns instead of blocking forever", t, func() {
		localPath := t.TempDir()
		fileLocal := filepath.Join(localPath, "file")
		fileRemote := filepath.Join(remotePath, "getmeta-cleanup")

		internal.CreateTestFileOfLength(t, fileLocal, 1)

		h, err := GetBatonHandler()
		So(err, ShouldBeNil)
		Reset(func() {
			h.Cleanup()
		})

		So(h.Put(fileLocal, fileRemote), ShouldBeNil)

		// extendo accepts a request on a client whose Stop() has begun but
		// whose baton-do has not yet exited, then never sends it; a GetMeta
		// just after a concurrent Cleanup usually lands in that window, and
		// can then only return by timing out. A short timeout keeps this
		// quick; the extra wait covers GetMeta making a new client.
		const (
			attempts  = 10
			opTimeout = 2 * time.Second
			bound     = opTimeout + operationMinBackoff
		)

		h.opTimeout = opTimeout

		var stuck, timedOut bool

		for i := range attempts {
			_, err = h.GetMeta(fileRemote)
			So(err, ShouldBeNil)

			cleanupDone := make(chan struct{})

			go func() {
				h.Cleanup()
				close(cleanupDone)
			}()

			time.Sleep(time.Duration(i%3) * time.Millisecond)

			metaDone := make(chan error, 1)

			go func() {
				_, errm := h.GetMeta(fileRemote)
				metaDone <- errm
			}()

			select {
			case errm := <-metaDone:
				timedOut = errm != nil && strings.Contains(errm.Error(), ErrOperationTimeout)
			case <-time.After(bound):
				stuck = true
			}

			<-cleanupDone

			if stuck || timedOut {
				break
			}
		}

		So(stuck, ShouldBeFalse)

		if !timedOut {
			t.Logf("GetMeta never landed in the stopping-client window in %d attempts", attempts)
		}

		h.opTimeout = operationTimeout

		_, err = h.GetMeta(fileRemote)
		So(err, ShouldBeNil)
	})

	Convey("Transfers racing a concurrent Cleanup return instead of blocking forever", t, func() {
		localPath := t.TempDir()
		fileLocal := filepath.Join(localPath, "file")
		fileGot := filepath.Join(localPath, "got")
		fileRemote := filepath.Join(remotePath, "transfer-cleanup")

		internal.CreateTestFileOfLength(t, fileLocal, 1)

		h, err := GetBatonHandler()
		So(err, ShouldBeNil)
		Reset(func() {
			h.Cleanup()
		})

		So(h.Put(fileLocal, fileRemote), ShouldBeNil)

		// As for GetMeta above, a request on a client whose Stop() has begun
		// is never sent. Transfers aren't timed out, since they can be long,
		// so must instead notice that their client stopped.
		raceCleanup := func(op func() error) {
			const (
				attempts = 10
				bound    = 20 * time.Second
			)

			var stuck, failed bool

			for i := range attempts {
				So(op(), ShouldBeNil)

				cleanupDone := make(chan struct{})

				go func() {
					h.Cleanup()
					close(cleanupDone)
				}()

				time.Sleep(time.Duration(i%3) * time.Millisecond)

				opDone := make(chan error, 1)

				go func() {
					opDone <- op()
				}()

				select {
				case erro := <-opDone:
					failed = erro != nil
				case <-time.After(bound):
					stuck = true
				}

				<-cleanupDone

				if stuck || failed {
					break
				}
			}

			So(stuck, ShouldBeFalse)

			if !failed {
				t.Logf("transfer never landed in the stopping-client window in %d attempts", attempts)
			}

			So(op(), ShouldBeNil)
		}

		Convey("Put", func() {
			raceCleanup(func() error { return h.Put(fileLocal, fileRemote) })
		})

		Convey("Get", func() {
			raceCleanup(func() error { return h.Get(fileGot, fileRemote) })
		})
	})
}

func TestUploadRetry(t *testing.T) {
	path := os.Getenv("PATH")

	Convey("With a pseudo baton script", t, func() {
		l, err := net.Listen("tcp", "127.0.0.1:0") //nolint:noctx
		So(err, ShouldBeNil)
		Reset(func() { l.Close() })

		dir := t.TempDir()

		port := strconv.Itoa(l.Addr().(*net.TCPAddr).Port) //nolint:forcetypeassert,errcheck

		source := []byte(`#!/bin/bash
exec 3<>/dev/tcp/127.0.0.1/` + port + `;
cat <&3 &
declare PID=$!;
cat >&3;
sleep ${IBACKUP_TEST_BATON_LINGER:-0};
kill $PID;
exec 3>&-;`)

		So(os.WriteFile(filepath.Join(dir, "baton-do"), source, 0700), ShouldBeNil) //nolint:gosec

		// ex.FindBaton() returns the last baton-do in PATH, so append ours.
		t.Setenv("PATH", path+":"+dir)

		h, err := GetBatonHandler()
		So(err, ShouldBeNil)

		Reset(h.Cleanup)

		bh := make(chan func(*ex.Envelope), 1)

		Reset(func() { close(bh) })

		go func() {
			c, errr := l.Accept()
			if errr != nil {
				return
			}

			defer c.Close()

			dec := json.NewDecoder(c)
			enc := json.NewEncoder(c)

			for fn := range bh {
				var env ex.Envelope

				dec.Decode(&env) //nolint:errcheck
				fn(&env)
				enc.Encode(&env) //nolint:errcheck
			}
		}()

		Convey("You can 'upload' files", func() {
			bh <- func(e *ex.Envelope) {
				e.Result = &ex.ResultWrapper{Item: &e.Target}
			}

			So(h.Put("/some/local/file", "/some/remote/file"), ShouldBeNil)

			bh <- func(e *ex.Envelope) {
				e.ErrorMsg = &ex.ErrorMsg{Code: 123, Message: "bad"}
			}

			err = h.Put("/some/local/file", "/some/remote/file")
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "put operation failed: bad code: 123")
		})

		Convey("An upload erroring due to a space issue is retried after deleting", func() {
			bh <- func(e *ex.Envelope) {
				e.ErrorMsg = &ex.ErrorMsg{Code: errSysCopyLen, Message: "SYS_COPY_LEN_ERR"}

				bh <- func(e *ex.Envelope) {
					e.Result = &ex.ResultWrapper{Item: &e.Target}

					bh <- func(e *ex.Envelope) {
						e.Result = &ex.ResultWrapper{Item: &e.Target}
					}
				}
			}

			So(h.Put("/some/local/file", "/some/remote/file"), ShouldBeNil)
		})

		Convey("Errors are correctly returned when retrying an upload", func() {
			bh <- func(e *ex.Envelope) {
				e.ErrorMsg = &ex.ErrorMsg{Code: errSysCopyLen, Message: "SYS_COPY_LEN_ERR"}

				bh <- func(e *ex.Envelope) {
					e.ErrorMsg = &ex.ErrorMsg{Code: 1234, Message: "BAD"}
				}
			}

			err = h.Put("/some/local/file", "/some/remote/file")
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "remove operation failed: BAD code: 1234")

			bh <- func(e *ex.Envelope) {
				e.ErrorMsg = &ex.ErrorMsg{Code: errSysCopyLen, Message: "SYS_COPY_LEN_ERR"}

				bh <- func(e *ex.Envelope) {
					e.Result = &ex.ResultWrapper{Item: &e.Target}

					bh <- func(e *ex.Envelope) {
						e.ErrorMsg = &ex.ErrorMsg{Code: 12345, Message: "BAD"}
					}
				}
			}

			err = h.Put("/some/local/file", "/some/remote/file")
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "put operation failed: BAD code: 12345")
		})

		Convey("Transfers whose client is stopping as they start return an error", func() {
			// Like a real baton-do finishing its work, ours lingers after its
			// stdin is closed, so extendo still thinks the client is running
			// after Stop() has stopped it reading requests.
			linger := os.Getenv("IBACKUP_TEST_BATON_LINGER")

			So(os.Setenv("IBACKUP_TEST_BATON_LINGER", "1"), ShouldBeNil)
			Reset(func() { os.Setenv("IBACKUP_TEST_BATON_LINGER", linger) }) //nolint:errcheck,usetesting

			stopWhileReplying := func(e *ex.Envelope) {
				e.Result = &ex.ResultWrapper{Item: &e.Target}

				go h.Cleanup()

				time.Sleep(500 * time.Millisecond)
			}

			returnsWithin := func(op func() error) error {
				errCh := make(chan error, 1)

				go func() { errCh <- op() }()

				select {
				case erro := <-errCh:
					return erro
				case <-time.After(20 * time.Second):
					return errTransferStuck
				}
			}

			put := func() error { return h.Put("/some/local/file", "/some/remote/file") }

			Convey("for Put", func() {
				bh <- stopWhileReplying

				So(put(), ShouldBeNil)

				err = returnsWithin(put)
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldContainSubstring, ErrClientStopped)
			})

			Convey("for Get", func() {
				bh <- stopWhileReplying

				So(put(), ShouldBeNil)

				err = returnsWithin(func() error {
					return h.Get(filepath.Join(t.TempDir(), "file"), "/some/remote/file")
				})
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldContainSubstring, ErrClientStopped)
			})

			Convey("for the Put retried after a SYS_COPY_LEN_ERR", func() {
				bh <- func(e *ex.Envelope) {
					e.ErrorMsg = &ex.ErrorMsg{Code: errSysCopyLen, Message: "SYS_COPY_LEN_ERR"}

					bh <- stopWhileReplying
				}

				err = returnsWithin(put)
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldContainSubstring, ErrClientStopped)
			})
		})
	})
}

func isObjectInIRODS(remotePath, name string) bool {
	output, err := icmd.ILS(remotePath)
	So(err, ShouldBeNil)

	return strings.Contains(string(output), name)
}

func getRemoteMeta(path string) string {
	output, err := icmd.IMETA("ls", "-d", path)
	So(err, ShouldBeNil)

	return string(output)
}

func addRemoteMeta(path, key, val string) {
	output, err := icmd.IMETA("add", "-d", path, key, val)
	if strings.Contains(string(output), "CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME") {
		return
	}

	So(err, ShouldBeNil)
}

func getSizeOfObject(path string) int {
	output, err := icmd.ILS("-l", path)
	So(err, ShouldBeNil)

	cols := strings.Fields(string(output))
	size, err := strconv.Atoi(cols[3])
	So(err, ShouldBeNil)

	return size
}

func compareMetasWithSize(t *testing.T, remote, expected map[string]string, size int64) {
	t.Helper()

	now := time.Now()

	var mtime, ctime time.Time

	So(ctime.UnmarshalText([]byte(remote[meta.MetaKeyRemoteCtime])), ShouldBeNil)
	So(mtime.UnmarshalText([]byte(remote[meta.MetaKeyRemoteMtime])), ShouldBeNil)
	So(mtime, ShouldHappenBetween, testStartTime, now)
	So(ctime, ShouldHappenBetween, testStartTime, now)

	delete(remote, meta.MetaKeyRemoteCtime)
	delete(remote, meta.MetaKeyRemoteMtime)

	expected[meta.MetaKeyRemoteSize] = strconv.FormatInt(size, 10)

	So(remote, ShouldResemble, expected)
}

// runningBatonDoChildren returns the pids of our running (not zombie) baton-do
// child processes.
func runningBatonDoChildren(t *testing.T) map[int]bool {
	t.Helper()

	statPaths, err := filepath.Glob("/proc/[0-9]*/stat")
	So(err, ShouldBeNil)

	ppid := strconv.Itoa(os.Getpid())
	pids := make(map[int]bool)

	for _, statPath := range statPaths {
		stat, errr := os.ReadFile(statPath)
		if errr != nil {
			continue
		}

		// Format: pid (comm) state ppid ...
		pidStr, rest, _ := strings.Cut(string(stat), " (")
		comm, rest, _ := strings.Cut(rest, ") ")
		fields := strings.Fields(rest)

		if comm != "baton-do" || len(fields) < 2 || fields[0] == "Z" || fields[1] != ppid {
			continue
		}

		pid, errc := strconv.Atoi(pidStr)
		So(errc, ShouldBeNil)

		pids[pid] = true
	}

	return pids
}

// withoutBatonDo makes starting baton-do fail until the current Convey ends, by
// emptying PATH, and makes client pools check their clients often, so a closed
// pool's checking goroutine exits quickly.
func withoutBatonDo(t *testing.T) {
	t.Helper()

	path := os.Getenv("PATH")
	freq := ex.DefaultClientPoolParams.CheckClientFreq

	So(os.Setenv("PATH", t.TempDir()), ShouldBeNil)

	ex.DefaultClientPoolParams.CheckClientFreq = 10 * time.Millisecond

	Reset(func() {
		os.Setenv("PATH", path) //nolint:errcheck,usetesting

		ex.DefaultClientPoolParams.CheckClientFreq = freq
	})
}

// newPoolCheckersAfterSettling returns the ids of pool checker goroutines not
// in before that are still running after giving them time to exit.
func newPoolCheckersAfterSettling(before map[string]bool) []string {
	var running []string

	for range 100 {
		running = nil

		for id := range poolCheckerGoroutines() {
			if !before[id] {
				running = append(running, id)
			}
		}

		if len(running) == 0 {
			break
		}

		time.Sleep(10 * time.Millisecond)
	}

	return running
}

// poolCheckerGoroutines returns the ids of the goroutines running an extendo
// client pool's client checker, which runs until its pool is closed.
func poolCheckerGoroutines() map[string]bool {
	ids := make(map[string]bool)

	for _, stack := range goroutineStacks() {
		if strings.Contains(stack, "extendo/v3.(*ClientPool).checkClients") {
			id, _, _ := strings.Cut(strings.TrimPrefix(stack, "goroutine "), " ")
			ids[id] = true
		}
	}

	return ids
}

// waitForGoroutineIn waits until some goroutine's stack contains the given
// function, returning false if that doesn't happen within the timeout.
func waitForGoroutineIn(function string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		for _, stack := range goroutineStacks() {
			if strings.Contains(stack, function) {
				return true
			}
		}

		time.Sleep(10 * time.Millisecond)
	}

	return false
}

func goroutineStacks() []string {
	buf := make([]byte, 1<<20)

	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			return strings.Split(string(buf[:n]), "\n\n")
		}

		buf = make([]byte, 2*len(buf))
	}
}
