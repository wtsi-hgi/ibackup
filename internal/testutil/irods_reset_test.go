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

package testutil

import (
	"errors"
	"strings"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

const ilsCommand = "ils"

var errFakeICommand = errors.New("fake iCommand failure")

// fakeICommands records the iCommands it is asked to run, failing those named
// in fail, and answering ils with ilsOut.
type fakeICommands struct {
	ilsOut string
	fail   map[string]bool
	ran    []string
}

func (f *fakeICommands) run(command string, args []string) ([]byte, error) {
	f.ran = append(f.ran, command+" "+strings.Join(args, " "))

	if f.fail[command] {
		return []byte(command + " output"), errFakeICommand
	}

	if command == ilsCommand {
		return []byte(f.ilsOut), nil
	}

	return nil, nil
}

func (f *fakeICommands) ILS(args ...string) ([]byte, error) { return f.run(ilsCommand, args) }

func (f *fakeICommands) IRM(args ...string) ([]byte, error) { return f.run("irm", args) }

func (f *fakeICommands) IMKDIR(args ...string) ([]byte, error) { return f.run("imkdir", args) }

func TestResetIRODSCollection(t *testing.T) {
	const coll = "/zone/home/test/coll"

	Convey("Resetting a collection that lists", t, func() {
		f := &fakeICommands{}

		Convey("removes its data objects and subcollections, keeping the collection", func() {
			f.ilsOut = coll + ":\n  file1\n  file 2\n  C- " + coll + "/sub\n"

			So(resetIRODSCollection(f, coll), ShouldBeNil)
			So(f.ran, ShouldResemble, []string{
				"ils " + coll,
				"irm -rf " + coll + "/file1 " + coll + "/file 2 " + coll + "/sub",
			})
		})

		Convey("does nothing more when it is empty", func() {
			f.ilsOut = coll + ":\n"

			So(resetIRODSCollection(f, coll), ShouldBeNil)
			So(f.ran, ShouldResemble, []string{"ils " + coll})
		})

		Convey("fails if its contents can't be removed", func() {
			f.ilsOut = coll + ":\n  file1\n"
			f.fail = map[string]bool{"irm": true}

			err := resetIRODSCollection(f, coll)
			So(err, ShouldWrap, errFakeICommand)
			So(err.Error(), ShouldContainSubstring, "irm output")
		})
	})

	Convey("Resetting a collection that can't be listed", t, func() {
		f := &fakeICommands{fail: map[string]bool{"ils": true}}

		Convey("recreates it", func() {
			So(resetIRODSCollection(f, coll), ShouldBeNil)
			So(f.ran, ShouldResemble, []string{"ils " + coll, "irm -rf " + coll, "imkdir -p " + coll})
		})

		Convey("fails if it can't be removed", func() {
			f.fail["irm"] = true

			err := resetIRODSCollection(f, coll)
			So(err, ShouldWrap, errFakeICommand)
			So(err.Error(), ShouldContainSubstring, "irm -rf failed")
			So(f.ran, ShouldResemble, []string{"ils " + coll, "irm -rf " + coll})
		})

		Convey("fails if it can't be created", func() {
			f.fail["imkdir"] = true

			err := resetIRODSCollection(f, coll)
			So(err, ShouldWrap, errFakeICommand)
			So(err.Error(), ShouldContainSubstring, "imkdir -p failed")
		})
	})
}
