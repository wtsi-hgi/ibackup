/*******************************************************************************
 * Copyright (c) 2025 Genome Research Ltd.
 *
 * Author: Michael Woolnough <mw31@sanger.ac.uk>
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
				Reason:      "in case it gets deleted",
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

			got, err := d.GetSet("mySet", "me")
			So(err, ShouldBeNil)
			So(got, ShouldResemble, setA)

			got, err = d.GetSet("my2ndSet", "me")
			So(err, ShouldBeNil)
			So(got, ShouldResemble, setB)

			got, err = d.GetSet("mySet", "you")
			So(err, ShouldBeNil)
			So(got, ShouldResemble, setC)

			So(collectIter(t, d.GetSetsByRequester("me")), ShouldResemble, []*Set{setA, setB})
			So(collectIter(t, d.GetSetsByRequester("you")), ShouldResemble, []*Set{setC})
			So(collectIter(t, d.GetAllSets()), ShouldResemble, []*Set{setA, setB, setC})

			So(d.DeleteSet(setA), ShouldBeNil)

			So(collectIter(t, d.GetAllSets()), ShouldResemble, []*Set{setB, setC})

			So(d.DeleteSet(setC), ShouldBeNil)
			So(collectIter(t, d.GetAllSets()), ShouldResemble, []*Set{setB})

			Convey("A set can be hidden and unhidden", func() {
				So(setB.Hidden, ShouldBeFalse)
				So(d.SetSetHidden(setB), ShouldBeNil)
				So(setB.Hidden, ShouldBeTrue)

				got, err = d.GetSet("my2ndSet", "me")
				So(err, ShouldBeNil)
				So(got.Hidden, ShouldBeTrue)

				So(d.SetSetVisible(setB), ShouldBeNil)
				So(setB.Hidden, ShouldBeFalse)

				got, err = d.GetSet("my2ndSet", "me")
				So(err, ShouldBeNil)
				So(got.Hidden, ShouldBeFalse)
			})

			Convey("A set can be made readonly", func() {
				So(setB.IsReadonly(), ShouldBeFalse)
				So(d.SetSetReadonly(setB), ShouldBeNil)
				So(setB.IsReadonly(), ShouldBeTrue)

				got, err = d.GetSet("my2ndSet", "me")
				So(err, ShouldBeNil)
				So(got.IsReadonly(), ShouldBeTrue)

				Convey("A readonly set cannot be changed", func() {
					So(d.SetSetHidden(setB), ShouldEqual, ErrReadonlySet)
					So(d.SetSetWarning(setB), ShouldEqual, ErrReadonlySet)
					So(d.SetSetError(setB), ShouldEqual, ErrReadonlySet)
					So(d.SetSetDicoveryStarted(setB), ShouldEqual, ErrReadonlySet)
					So(d.SetSetDicoveryCompleted(setB), ShouldEqual, ErrReadonlySet)
					So(d.DeleteSet(setB), ShouldEqual, ErrReadonlySet)
					So(d.SetSetFiles(setB, slices.Values([]*File{}), noSeq[*File]), ShouldEqual, ErrReadonlySet)
				})

				So(d.SetSetModifiable(setB), ShouldBeNil)
				So(setB.IsReadonly(), ShouldBeFalse)

				got, err = d.GetSet("my2ndSet", "me")
				So(err, ShouldBeNil)
				So(got.IsReadonly(), ShouldBeFalse)
			})

			Convey("You can change the transformer on a set", func() {
				files := slices.Collect(genFiles(5))

				discovery := &Discover{Path: "/some/path", Type: DiscoverFODN}

				So(d.AddSetDiscovery(setB, discovery), ShouldBeNil)
				So(d.SetSetFiles(setB, slices.Values(files), noSeq[*File]), ShouldBeNil)
				So(d.clearQueue(), ShouldBeNil)

				setB.Transformer, err = NewTransformer("prefix=/some/:/other/remote/", `^/some/`, "/other/remote/")
				So(err, ShouldBeNil)

				oldID := setB.id

				So(d.SetSetTransformer(setB), ShouldBeNil)

				for _, file := range files {
					file.id += 5
					file.RemotePath = "/other" + file.RemotePath
				}

				So(collectIter(t, d.GetSetFiles(setB)), ShouldResemble, files)
				So(collectIter(t, d.GetSetDiscovery(setB)), ShouldResemble, []*Discover{discovery})

				Convey("The old set is removed once the queued items are dealt with", func() {
					requester := fmt.Sprintf("\x00%d\x00%s", oldID, setB.Requester)

					_, err := scanSet(d.db.QueryRow(getSetByNameRequester, setB.Name, requester))
					So(err, ShouldBeNil)
					So(d.clearQueue(), ShouldBeNil)

					_, err = scanSet(d.db.QueryRow(getSetByNameRequester, setB.Name, requester))
					So(err, ShouldNotBeNil)
				})
			})

			Convey("Adding/Removing files to a set updates the set file numbers", func() {
				setB, err := d.GetSet("my2ndSet", "me")
				So(err, ShouldBeNil)
				So(setB.NumFiles, ShouldEqual, 0)
				So(d.SetSetFiles(setB, genFiles(5), noSeq[*File]), ShouldBeNil)
				So(d.SetSetFiles(setB, setFileType(genFiles(4), Abnormal), noSeq[*File]), ShouldBeNil)
				So(d.SetSetFiles(setB, setFileType(genFiles(2), Symlink), noSeq[*File]), ShouldBeNil)

				setB, err = d.GetSet("my2ndSet", "me")
				So(err, ShouldBeNil)
				So(setB.NumFiles, ShouldEqual, 11)
				So(setB.SizeTotal, ShouldEqual, 1100)
				So(setB.Symlinks, ShouldEqual, 3)
				So(setB.Hardlinks, ShouldEqual, 1)
				So(setB.Abnormal, ShouldEqual, 4)
				So(setB.Missing, ShouldEqual, 0)
				So(setB.Orphaned, ShouldEqual, 0)

				files := d.GetSetFiles(setB)
				So(d.RemoveSetFiles(files.Iter), ShouldBeNil)
				So(files.Error, ShouldBeNil)

				setB, err = d.GetSet("my2ndSet", "me")
				So(err, ShouldBeNil)
				So(setB.NumFiles, ShouldEqual, 11)
				So(d.clearQueue(), ShouldBeNil)

				setB, err = d.GetSet("my2ndSet", "me")
				So(err, ShouldBeNil)

				So(setB.NumFiles, ShouldEqual, 0)
				So(setB.SizeTotal, ShouldEqual, 0)
				So(setB.Symlinks, ShouldEqual, 0)
				So(setB.Hardlinks, ShouldEqual, 0)

				So(d.SetSetFiles(setB, setFileStatus(genFiles(5), StatusMissing), noSeq[*File]), ShouldBeNil)

				setB, err = d.GetSet("my2ndSet", "me")
				So(err, ShouldBeNil)
				So(setB.NumFiles, ShouldEqual, 5)
				So(setB.SizeTotal, ShouldEqual, 500)
				So(setB.SizeUploaded, ShouldEqual, 0)
				So(setB.Symlinks, ShouldEqual, 1)
				So(setB.Hardlinks, ShouldEqual, 1)
				So(setB.Missing, ShouldEqual, 5)
				So(setB.Orphaned, ShouldEqual, 0)

				filePrefix--

				So(d.CreateSet(setC), ShouldBeNil)
				So(d.SetSetFiles(setC, genFiles(2), noSeq[*File]), ShouldBeNil)
				So(d.clearQueue(), ShouldBeNil)

				setB, err = d.GetSet(setB.Name, setB.Requester)
				So(err, ShouldBeNil)
				So(setB.Missing, ShouldEqual, 3)
				So(setB.Orphaned, ShouldEqual, 2)
				So(setB.SizeUploaded, ShouldEqual, 0)

				setC, err = d.GetSet(setC.Name, setC.Requester)
				So(err, ShouldBeNil)
				So(setC.SizeUploaded, ShouldEqual, 200)

				So(d.SetSetDicoveryCompleted(setC), ShouldBeNil)

				cFiles := slices.Collect(genFiles(12))

				So(d.SetSetFiles(setC, slices.Values(cFiles), noSeq[*File]), ShouldBeNil)

				setC, err = d.GetSet(setC.Name, setC.Requester)
				So(err, ShouldBeNil)
				So(setC.SizeUploaded, ShouldEqual, 0)

				process, err := d.RegisterProcess()
				So(err, ShouldBeNil)

				So(d.TaskComplete(collectIter(t, d.ReserveTasks(process, 1))[0]), ShouldBeNil)

				setC, err = d.GetSet(setC.Name, setC.Requester)
				So(err, ShouldBeNil)
				So(setC.Uploaded, ShouldEqual, 1)
				So(setC.Replaced, ShouldEqual, 0)
				So(setC.Skipped, ShouldEqual, 0)
				So(setC.SizeUploaded, ShouldEqual, 100)

				So(d.TaskSkipped(collectIter(t, d.ReserveTasks(process, 1))[0]), ShouldBeNil)

				setC, err = d.GetSet(setC.Name, setC.Requester)
				So(err, ShouldBeNil)
				So(setC.Uploaded, ShouldEqual, 1)
				So(setC.Replaced, ShouldEqual, 0)
				So(setC.Skipped, ShouldEqual, 1)
				So(setC.SizeUploaded, ShouldEqual, 100)

				So(d.clearQueue(), ShouldBeNil)

				setC, err = d.GetSet(setC.Name, setC.Requester)
				So(err, ShouldBeNil)
				So(setC.Uploaded, ShouldEqual, 11)
				So(setC.Replaced, ShouldEqual, 0)
				So(setC.Skipped, ShouldEqual, 1)
				So(setC.SizeUploaded, ShouldEqual, 1100)

				filePrefix -= 2

				So(d.SetSetFiles(setC, genFiles(1), noSeq[*File]), ShouldBeNil)

				filePrefix++

				setC, err = d.GetSet(setC.Name, setC.Requester)
				So(err, ShouldBeNil)
				So(setC.Replaced, ShouldEqual, 0)

				So(d.clearQueue(), ShouldBeNil)

				setC, err = d.GetSet(setC.Name, setC.Requester)
				So(err, ShouldBeNil)
				So(setC.Uploaded, ShouldEqual, 10)
				So(setC.Replaced, ShouldEqual, 1)
				So(setC.Skipped, ShouldEqual, 1)
				So(setC.SizeUploaded, ShouldEqual, 1200)
				So(setC.NumObjectsRemoved, ShouldEqual, 0)
				So(setC.NumObjectsToBeRemoved, ShouldEqual, 0)
				So(setC.SizeRemoved, ShouldEqual, 0)

				So(d.RemoveSetFiles(slices.Values(cFiles[:1])), ShouldBeNil)

				setC, err = d.GetSet(setC.Name, setC.Requester)
				So(err, ShouldBeNil)
				So(setC.NumObjectsRemoved, ShouldEqual, 0)
				So(setC.NumObjectsToBeRemoved, ShouldEqual, 1)
				So(setC.SizeRemoved, ShouldEqual, 0)
				So(d.clearQueue(), ShouldBeNil)

				setC, err = d.GetSet(setC.Name, setC.Requester)
				So(err, ShouldBeNil)
				So(setC.NumObjectsRemoved, ShouldEqual, 1)
				So(setC.NumObjectsToBeRemoved, ShouldEqual, 1)
				So(setC.SizeRemoved, ShouldEqual, 100)

				So(d.RemoveSetFiles(slices.Values(cFiles[1:7])), ShouldBeNil)

				setC, err = d.GetSet(setC.Name, setC.Requester)
				So(err, ShouldBeNil)
				So(setC.NumObjectsRemoved, ShouldEqual, 0)
				So(setC.NumObjectsToBeRemoved, ShouldEqual, 6)
				So(setC.SizeRemoved, ShouldEqual, 0)

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

					setC, err = d.GetSet(setC.Name, setC.Requester)
					So(err, ShouldBeNil)
					So(setC.NumObjectsRemoved, ShouldEqual, n+1)
					So(setC.NumObjectsToBeRemoved, ShouldEqual, 6)
					So(setC.SizeRemoved, ShouldEqual, (n+1)*100)
				}

				So(d.SetSetFiles(setC, slices.Values(cFiles[4:5]), noSeq[*File]), ShouldBeNil)

				setC, err = d.GetSet(setC.Name, setC.Requester)
				So(err, ShouldBeNil)
				So(setC.NumObjectsRemoved, ShouldEqual, 3)
				So(setC.NumObjectsToBeRemoved, ShouldEqual, 5)
				So(setC.SizeRemoved, ShouldEqual, 300)

				So(d.RemoveSetFiles(slices.Values(cFiles[4:5])), ShouldBeNil)

				setC, err = d.GetSet(setC.Name, setC.Requester)
				So(err, ShouldBeNil)
				So(setC.NumObjectsRemoved, ShouldEqual, 3)
				So(setC.NumObjectsToBeRemoved, ShouldEqual, 6)
				So(setC.SizeRemoved, ShouldEqual, 300)
			})
		})
	})
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
