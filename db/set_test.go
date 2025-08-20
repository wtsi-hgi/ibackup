/*******************************************************************************
 * Copyright (c) 2025 Genome Research Ltd.
 *
 * Author: Michael Woolnough <mw31@sanger.ac.uk>
 * Author: Rosie Kern <rk18@sanger.ac.uk>
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

package db

import (
	"fmt"
	"iter"
	"slices"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestSet(t *testing.T) {
	Convey("With a new database", t, func() {
		d := createTestDatabase(t)

		Convey("You cannot add a set with an invalid name", func() {
			set := &Set{
				Name:        "",
				Requester:   "me",
				Transformer: complexTransformer,
			}

			So(d.CreateSet(set), ShouldEqual, ErrInvalidSetName)

			set.Name = "\x00mySet"

			So(d.CreateSet(set), ShouldEqual, ErrInvalidSetName)
		})

		Convey("You cannot add a set with an invalid requester", func() {
			set := &Set{
				Name:        "mySet",
				Requester:   "",
				Transformer: complexTransformer,
			}

			So(d.CreateSet(set), ShouldEqual, ErrInvalidRequesterName)

			set.Requester = "\x00me"

			So(d.CreateSet(set), ShouldEqual, ErrInvalidRequesterName)
		})

		Convey("You can add and retrieve Sets", func() {
			setA := &Set{
				Name:        "mySet",
				Requester:   "me",
				Transformer: complexTransformer,
				Description: "my first set",
			}

			setB := &Set{
				Name:        "my2ndSet",
				Requester:   "me",
				Transformer: simpleTransformer,
				Description: "my second set",
			}

			setC := &Set{
				Name:        "mySet",
				Requester:   "you",
				Transformer: simpleTransformer,
				Description: "my first set",
				Reason:      Quarantine,
				ReviewDate:  time.Now().Truncate(time.Second).In(time.UTC),
				DeleteDate:  time.Now().Truncate(time.Second).In(time.UTC).Add(time.Hour),
				Metadata: Metadata{
					"some-extra-meta": "some-value",
				},
			}

			So(d.CreateSet(setA), ShouldBeNil)
			So(d.CreateSet(setA), ShouldNotBeNil)
			So(d.CreateSet(setB), ShouldBeNil)
			So(d.CreateSet(setC), ShouldBeNil)

			got := d.getSetFromDB(setA)
			So(got, ShouldResemble, setA)

			got = d.getSetFromDB(setB)
			So(got, ShouldResemble, setB)

			got = d.getSetFromDB(setC)
			So(got, ShouldResemble, setC)

			So(collectIter(t, d.GetSetsByRequester("me")), ShouldResemble, []*Set{setA, setB})
			So(collectIter(t, d.GetSetsByRequester("you")), ShouldResemble, []*Set{setC})
			So(collectIter(t, d.GetAllSets()), ShouldResemble, []*Set{setA, setB, setC})

			Convey("And you can delete added empty sets", func() {
				So(d.DeleteSet(setA), ShouldBeNil)

				So(collectIter(t, d.GetAllSets()), ShouldResemble, []*Set{setB, setC})

				So(d.DeleteSet(setC), ShouldBeNil)
				So(collectIter(t, d.GetAllSets()), ShouldResemble, []*Set{setB})
			})

			var err error

			Convey("A set can be hidden and unhidden", func() {
				So(setB.Hidden, ShouldBeFalse)

				So(d.SetSetHidden(setB), ShouldBeNil)
				So(setB.Hidden, ShouldBeTrue)

				got = d.getSetFromDB(setB)
				So(got.Hidden, ShouldBeTrue)

				So(d.SetSetVisible(setB), ShouldBeNil)
				So(setB.Hidden, ShouldBeFalse)

				got = d.getSetFromDB(setB)
				So(got.Hidden, ShouldBeFalse)
			})

			Convey("A set can be made readonly", func() {
				So(setB.IsReadonly(), ShouldBeFalse)
				So(d.SetSetReadonly(setB), ShouldBeNil)
				So(setB.IsReadonly(), ShouldBeTrue)

				got = d.getSetFromDB(setB)
				So(got.IsReadonly(), ShouldBeTrue)

				Convey("A readonly set cannot be changed", func() {
					So(d.SetSetHidden(setB), ShouldEqual, ErrReadonlySet)
					So(d.SetSetWarning(setB), ShouldEqual, ErrReadonlySet)
					So(d.SetSetError(setB), ShouldEqual, ErrReadonlySet)
					So(d.SetSetDiscoveryStarted(setB), ShouldEqual, ErrReadonlySet)
					So(d.DeleteSet(setB), ShouldEqual, ErrReadonlySet)
					So(d.CompleteDiscovery(setB, slices.Values([]*File{}), noSeq[*File]), ShouldEqual, ErrReadonlySet)
				})

				So(d.SetSetModifiable(setB), ShouldBeNil)
				So(setB.IsReadonly(), ShouldBeFalse)

				got = d.getSetFromDB(setB)
				So(got.IsReadonly(), ShouldBeFalse)
			})

			Convey("And given files added to a set", func() {
				files := slices.Collect(genFiles(5))
				files = slices.Concat(files, slices.Collect(setFileType(genFiles(4), Abnormal)))
				files = slices.Concat(files, slices.Collect(setFileType(genFiles(2), Symlink)))

				for _, file := range files {
					file.setID = setB.id
				}

				checkSetCounts := func(set *Set) {
					So(set.NumFiles, ShouldEqual, 11)
					So(set.SizeTotal, ShouldEqual, 1100)
					So(set.Symlinks, ShouldEqual, 3)
					// So(set.Hardlinks, ShouldEqual, 1)
					So(set.Abnormal, ShouldEqual, 4)
					So(set.Missing, ShouldEqual, 0)
					So(set.Orphaned, ShouldEqual, 0)
					So(set.Uploaded, ShouldEqual, 0)
					So(set.Replaced, ShouldEqual, 0)
				}

				setB = d.getSetFromDB(setB)
				So(setB.Status, ShouldEqual, PendingDiscovery)
				So(setB.NumFiles, ShouldEqual, 0)
				So(d.CompleteDiscovery(setB, slices.Values(files), noSeq[*File]), ShouldBeNil)

				setB = d.getSetFromDB(setB)
				So(setB.Status, ShouldEqual, PendingUpload)

				checkSetCounts(setB)

				Convey("You cannot delete sets containing files", func() {
					So(d.DeleteSet(setB), ShouldNotBeNil)

					So(collectIter(t, d.GetAllSets()), ShouldResemble, []*Set{setA, setB, setC})
				})

				Convey("And given a second set with two of the same files, one having different content", func() {
					So(d.clearQueue(), ShouldBeNil)

					fileToBeReplaced := *files[0]
					fileToBeReplaced.Size += 100

					cFiles := []*File{&fileToBeReplaced, files[1]}

					So(d.CompleteDiscovery(setC, slices.Values(cFiles), noSeq[*File]), ShouldBeNil)

					Convey("The total size for the original set will be changed", func() {
						setB = d.getSetFromDB(setB)
						So(setB.SizeTotal, ShouldEqual, 1200)
					})

					Convey("The second set will have different counts once the queue is cleared", func() {
						process, errr := d.RegisterProcess()
						So(errr, ShouldBeNil)

						So(d.TaskComplete(collectIter(t, d.ReserveTasks(process, 1))[0]), ShouldBeNil)

						task := collectIter(t, d.ReserveTasks(process, 1))[0]
						task.Skipped = true

						So(d.TaskComplete(task), ShouldBeNil)
						So(d.clearQueue(), ShouldBeNil)

						setC = d.getSetFromDB(setC)
						So(setC.NumFiles, ShouldEqual, 2)
						So(setC.Replaced, ShouldEqual, 1)
						So(setC.Skipped, ShouldEqual, 1)
						So(setC.SizeTotal, ShouldEqual, 300)
						So(setC.SizeUploaded, ShouldEqual, 200)
					})
				})

				Convey("If you change the size of files and complete discovery again", func() {
					So(d.clearQueue(), ShouldBeNil)

					setB = d.getSetFromDB(setB)
					So(setB.NumFiles, ShouldEqual, 11)
					So(setB.SizeTotal, ShouldEqual, 1100)
					So(setB.SizeTotal, ShouldEqual, 1100)

					files[0].Size += 100
					files[1].Size += 200 // same inode as files[2]

					So(d.CompleteDiscovery(setB, slices.Values(files[:2]), noSeq[*File]), ShouldBeNil)

					Convey("The size is updated for all files that share the same inode", func() {
						setB = d.getSetFromDB(setB)
						So(setB.NumFiles, ShouldEqual, 11)
						So(setB.SizeTotal, ShouldEqual, 1600)

						So(d.clearQueue(), ShouldBeNil)

						setB = d.getSetFromDB(setB)
						So(setB.SizeUploaded, ShouldEqual, 500)
					})
				})

				Convey("You can change the inode, size, status and type of a file and set counts will be updated correctly", func() {
					So(d.clearQueue(), ShouldBeNil)

					files[1].Status = StatusMissing

					So(d.CompleteDiscovery(setB, slices.Values(files[:2]), noSeq[*File]), ShouldBeNil)
					So(d.clearQueue(), ShouldBeNil)

					setB = d.getSetFromDB(setB)
					So(setB.NumFiles, ShouldEqual, 11)
					So(setB.SizeTotal, ShouldEqual, 1100)
					So(setB.Symlinks, ShouldEqual, 3)
					So(setB.Orphaned, ShouldEqual, 1)

					files[1].Size += 200
					files[1].Inode = 33333333
					files[1].Type = Symlink
					files[1].Status = StatusNone

					So(d.CompleteDiscovery(setB, slices.Values(files[:2]), noSeq[*File]), ShouldBeNil)

					setB = d.getSetFromDB(setB)
					So(setB.NumFiles, ShouldEqual, 11)
					So(setB.SizeTotal, ShouldEqual, 1300)
					So(setB.Symlinks, ShouldEqual, 4)
					So(setB.Orphaned, ShouldEqual, 0)
				})

				Convey("You can change the inode for a file and the inode counts will be correct", func() {
					So(d.clearQueue(), ShouldBeNil)

					setB = d.getSetFromDB(setB)
					So(setB.NumFiles, ShouldEqual, 11)
					So(setB.SizeTotal, ShouldEqual, 1100)

					files[0].Size += 200
					files[0].Inode = 33333333

					So(len(collectIter(t, d.listInodes(t))), ShouldEqual, 9)

					So(d.CompleteDiscovery(setB, slices.Values(files[:1]), noSeq[*File]), ShouldBeNil)

					setB = d.getSetFromDB(setB)
					So(setB.NumFiles, ShouldEqual, 11)
					So(setB.SizeTotal, ShouldEqual, 1300)

					So(d.clearQueue(), ShouldBeNil)

					So(len(collectIter(t, d.listInodes(t))), ShouldEqual, 9)
				})

				Convey("You can change a file's inode to one already in the database, and counts update correctly", func() {
					So(d.clearQueue(), ShouldBeNil)

					setB = d.getSetFromDB(setB)
					So(setB.NumFiles, ShouldEqual, 11)
					So(setB.SizeTotal, ShouldEqual, 1100)

					files[0].Size += 200
					files[0].Inode = files[4].Inode

					So(len(collectIter(t, d.listInodes(t))), ShouldEqual, 9)

					So(d.CompleteDiscovery(setB, slices.Values(files[:1]), noSeq[*File]), ShouldBeNil)

					setB = d.getSetFromDB(setB)
					So(setB.NumFiles, ShouldEqual, 11)
					So(setB.SizeTotal, ShouldEqual, 1500)
					
					So(d.clearQueue(), ShouldBeNil)

					So(len(collectIter(t, d.listInodes(t))), ShouldEqual, 8)
				})

				Convey("If one of the files is found missing before rediscovery", func() {
					files[0].Status = StatusMissing

					So(d.CompleteDiscovery(setB, slices.Values(files), noSeq[*File]), ShouldBeNil)
					So(d.clearQueue(), ShouldBeNil)

					Convey("The set counts it as orphaned", func() {
						setB = d.getSetFromDB(setB)
						So(setB.Missing, ShouldEqual, 0)
						So(setB.Orphaned, ShouldEqual, 1)
					})
				})

				Convey("You can change the transformer on a set", func() {
					discovery := &Discover{Path: "/some/path", Type: DiscoverFODN}

					So(d.AddSetDiscovery(setB, discovery), ShouldBeNil)
					So(d.CompleteDiscovery(setB, slices.Values(files), noSeq[*File]), ShouldBeNil)
					So(d.clearQueue(), ShouldBeNil)

					setB.Transformer, err = NewTransformer("prefix=/some/:/other/remote/", `^/some/`, "/other/remote/")
					So(err, ShouldBeNil)

					oldID := setB.id

					So(d.SetSetTransformer(setB), ShouldBeNil)

					updatedSet := d.getSetFromDB(setB)
					So(updatedSet.Transformer, ShouldResemble, setB.Transformer)

					Convey("Which updates the transformer for each file in the set", func() {
						for _, file := range files {
							file.id += 22
							file.setID = setB.id
							file.RemotePath = "/other" + file.RemotePath
						}

						So(collectIter(t, d.GetSetFiles(updatedSet)), ShouldResemble, files)
						So(collectIter(t, d.GetSetDiscovery(updatedSet)), ShouldResemble, []*Discover{discovery})

						checkSetCounts(updatedSet)
					})

					Convey("Which creates and moves files to a new set, with the old set having a changed requester", func() {
						oldSetRequester := fmt.Sprintf("\x00%d\x00%s", oldID, setB.Requester)

						_, err = scanSet(d.db.QueryRow(getSetByNameRequester, setB.Name, oldSetRequester))
						So(err, ShouldBeNil)

						Convey("The old set is removed once the queued items are dealt with", func() {
							So(d.clearQueue(), ShouldBeNil)

							_, err = scanSet(d.db.QueryRow(getSetByNameRequester, setB.Name, oldSetRequester))
							So(err, ShouldNotBeNil)
						})
					})
				})

				Convey("And given a removal that's still running", func() {
					So(d.RemoveSetFiles(setB, slices.Values(files[:6])), ShouldBeNil)

					setB = d.getSetFromDB(setB)
					So(setB.NumObjectsRemoved, ShouldEqual, 0)
					So(setB.NumObjectsToBeRemoved, ShouldEqual, 6)
					So(setB.SizeRemoved, ShouldEqual, 0)
					So(setB.NumFiles, ShouldEqual, 11)

					process, errr := d.RegisterProcess()
					So(errr, ShouldBeNil)

					for n := range 3 {
						var task *Task

						for {
							if task = collectIter(t, d.ReserveTasks(process, 1))[0]; task.Type == QueueRemoval {
								break
							}

							So(d.TaskComplete(task), ShouldBeNil)
						}

						So(task.Type, ShouldEqual, QueueRemoval)
						So(d.TaskComplete(task), ShouldBeNil)

						setB = d.getSetFromDB(setB)
						So(setB.NumObjectsRemoved, ShouldEqual, n+1)
						So(setB.NumObjectsToBeRemoved, ShouldEqual, 6)
						So(setB.SizeRemoved, ShouldEqual, (n+1)*100)
					}

					So(setB.NumFiles, ShouldEqual, 8)
					So(setB.SizeTotal, ShouldEqual, 800)

					Convey("If discovery starts on a different file, removal queue is unaffected", func() {
						newFileSlice := slices.Collect(genFiles(1))
						newFileSlice[0].setID = setB.id

						So(d.CompleteDiscovery(setB, slices.Values(newFileSlice), noSeq[*File]), ShouldBeNil)

						setB = d.getSetFromDB(setB)
						So(setB.NumObjectsRemoved, ShouldEqual, 3)
						So(setB.NumObjectsToBeRemoved, ShouldEqual, 6)
						So(setB.SizeRemoved, ShouldEqual, 300)
						So(setB.SizeTotal, ShouldEqual, 900)
						So(setB.NumFiles, ShouldEqual, 9)

						Convey("And when tasks complete, counts are correct", func() {
							So(d.clearQueue(), ShouldBeNil)

							setB = d.getSetFromDB(setB)
							So(setB.NumObjectsRemoved, ShouldEqual, 6)
							So(setB.NumObjectsToBeRemoved, ShouldEqual, 6)
							So(setB.SizeRemoved, ShouldEqual, 600)
							So(setB.NumFiles, ShouldEqual, 6)
							So(setB.SizeUploaded, ShouldEqual, 600)
							So(setB.SizeTotal, ShouldEqual, 600)
						})
					})

					Convey("If discovery starts on a file queued to be removed, it's removed from remove queue", func() {
						So(d.CompleteDiscovery(setB, slices.Values(files[4:5]), noSeq[*File]), ShouldBeNil)

						setB = d.getSetFromDB(setB)
						So(setB.NumObjectsRemoved, ShouldEqual, 3)
						So(setB.NumObjectsToBeRemoved, ShouldEqual, 5)
						So(setB.SizeRemoved, ShouldEqual, 300)
						So(setB.NumFiles, ShouldEqual, 8)
						So(setB.SizeTotal, ShouldEqual, 800)

						Convey("And if you add more files to be removed, the counts are updated", func() {
							So(d.RemoveSetFiles(setB, slices.Values(files[4:5])), ShouldBeNil)

							setB = d.getSetFromDB(setB)
							So(setB.NumObjectsRemoved, ShouldEqual, 3)
							So(setB.NumObjectsToBeRemoved, ShouldEqual, 6)
							So(setB.SizeRemoved, ShouldEqual, 300)
							So(setB.NumFiles, ShouldEqual, 8)
							So(setB.SizeTotal, ShouldEqual, 800)
						})
					})
				})

				Convey("You can remove all files", func() {
					files := collectIter(t, d.GetSetFiles(setB))
					So(d.RemoveSetFiles(setB, slices.Values(files)), ShouldBeNil)

					setB, err = d.GetSet("my2ndSet", "me")
					So(err, ShouldBeNil)
					So(setB.NumFiles, ShouldEqual, 11)

					So(d.clearQueue(), ShouldBeNil)

					setB = d.getSetFromDB(setB)
					So(setB.Status, ShouldEqual, PendingDiscovery)
					So(setB.NumFiles, ShouldEqual, 0)
					So(setB.SizeTotal, ShouldEqual, 0)
					So(setB.Symlinks, ShouldEqual, 0)
					So(setB.Hardlinks, ShouldEqual, 0)

					Convey("And you can still add files, which updates counts", func() {
						So(d.CompleteDiscovery(setB, genFiles(5), noSeq[*File]), ShouldBeNil)

						setB = d.getSetFromDB(setB)
						So(setB.Status, ShouldEqual, PendingUpload)
						So(setB.NumFiles, ShouldEqual, 5)
						So(setB.SizeTotal, ShouldEqual, 500)
						So(setB.SizeUploaded, ShouldEqual, 0)
						So(setB.Symlinks, ShouldEqual, 1)
						So(setB.Hardlinks, ShouldEqual, 1)
						So(setB.Missing, ShouldEqual, 0)
						So(setB.Orphaned, ShouldEqual, 0)
					})
				})
			})

			Convey("Once all upload tasks have been attempted, the Last* fields are set", func() {
				So(d.CompleteDiscovery(setB, genFiles(2), noSeq[*File]), ShouldBeNil)
				So(d.CompleteDiscovery(setB, setFileStatus(genFiles(1), StatusMissing), noSeq[*File]), ShouldBeNil)

				process, err := d.RegisterProcess()
				So(err, ShouldBeNil)

				now := time.Now().Truncate(time.Second)

				for range 2 {
					setB, err = d.GetSet(setB.Name, setB.Requester)
					So(err, ShouldBeNil)
					So(setB.LastCompleted, ShouldBeZeroValue)
					So(setB.LastCompletedCount, ShouldEqual, 0)
					So(setB.LastCompletedSize, ShouldEqual, 0)
					So(d.TaskComplete(collectIter(t, d.ReserveTasks(process, 1))[0]), ShouldBeNil)
				}

				setB, err = d.GetSet(setB.Name, setB.Requester)
				So(err, ShouldBeNil)
				So(setB.LastCompletedCount, ShouldEqual, 3)
				So(setB.LastCompletedSize, ShouldEqual, 300)
				So(setB.LastCompleted, ShouldHappenOnOrAfter, now)
			})
		})
	})
}

func (d *DB) getSetFromDB(set *Set) *Set {
	dbSet, err := d.GetSet(set.Name, set.Requester)
	So(err, ShouldBeNil)

	return dbSet
}

func collectIter[T any](t *testing.T, i *IterErr[T]) []T {
	t.Helper()

	var vs []T

	So(i.ForEach(func(item T) error {
		vs = append(vs, item)

		return nil
	}), ShouldBeNil)

	return vs
}

func setFileType(files iter.Seq[*File], ftype FileType) iter.Seq[*File] {
	return func(yield func(*File) bool) {
		for file := range files {
			file.Type = ftype

			if !yield(file) {
				break
			}
		}
	}
}

func setFileStatus(files iter.Seq[*File], status FileStatus) iter.Seq[*File] {
	return func(yield func(*File) bool) {
		for file := range files {
			file.Status = status

			if !yield(file) {
				break
			}
		}
	}
}
