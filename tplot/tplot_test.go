/*******************************************************************************
* Copyright (c) 2023 Genome Research Ltd.
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

package tplot

import (
	"bytes"
	"os/exec"
	"strings"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestTPlot(t *testing.T) {
	Convey("Given a TPlotter and some data", t, func() {
		tp := New()
		So(tp, ShouldNotBeNil)

		var buf bytes.Buffer
		tp.output = &buf

		data := NewData("Title")
		data.Add("A", 0.123)
		data.Add("B", 1.123)
		data.Add("C", 10.123)
		data.Add("D", 100.123)

		exe, err := exec.LookPath(youplot)
		if err == nil {
			Convey("With YouPlot you can fancy-plot the data to the terminal", func() {
				So(exe, ShouldNotBeBlank)
				So(exe, ShouldEqual, tp.youPlotExe)

				err = tp.Plot(data)
				So(err, ShouldBeNil)
				expected := `

				Title
  ┌                                        ┐
A ┤ 0.123
B ┤ 1.12
C ┤■■■ 10.1
D ┤■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■ 100.0
  └                                        ┘
`
				actual := buf.String()

				expected = strings.ReplaceAll(expected, " ", "")
				actual = strings.ReplaceAll(actual, " ", "")
				expected = strings.ReplaceAll(expected, "\t", "")
				actual = strings.ReplaceAll(actual, "\t", "")

				So(actual, ShouldEqual, expected)
			})
		} else {
			SkipConvey("Can't test fancy plotting without YouPlot installed", func() {})
		}

		tp.youPlotExe = ""
		Convey("Without YouPlot you can just print the data to the terminal", func() {
			err = tp.Plot(data)
			So(err, ShouldBeNil)
			So(buf.String(), ShouldEqual, "\nTitle:\n A:\t0.123\n B:\t1.12\n C:\t10.1\n D:\t100\n")
		})
	})
}
