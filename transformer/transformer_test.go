/*******************************************************************************
 * Copyright (c) 2025 Genome Research Ltd.
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

package transformer

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestTransformers(t *testing.T) {
	Convey("A transformer transforms paths", t, func() {
		So(Register(
			"complex",
			`^/mount/(point[^/]+)(/[^/]*)+?/(projects|teams|users)(_v2)?/([^/]+)/`,
			"/backup/$3/$5/$1$4/",
		), ShouldBeNil)

		for _, test := range [...]struct {
			Name        string
			Transformer string
			Input       string
			Valid       bool
			Output      string
		}{
			{
				"A simple path transformation",
				"prefix=/some/:/remote/",
				"/some/path/some/file",
				true,
				"/remote/path/some/file",
			},
			{
				"A simple path transformation that should fail but doesn't",
				"prefix=/some/:/remote/",
				"/dome/path/some/file",
				true,
				"/remote/dome/path/some/file",
			},
			{
				"A complex path transformation",
				"complex",
				"/mount/point_1/something/projects_v2/awesome/file",
				true,
				"/backup/projects/awesome/point_1_v2/file",
			},
			{
				"A complex path transformation that fails",
				"complex",
				"/some/other/path/some/file",
				false,
				"",
			},
		} {
			Convey(test.Name, func() {
				transformed, err := Transform(test.Transformer, test.Input)
				if test.Valid {
					So(err, ShouldBeNil)
				} else {
					So(err, ShouldNotBeNil)
				}

				So(transformed, ShouldEqual, test.Output)
			})
		}
	})
}
