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

// package tplot is for plotting data to the terminal in barplots if YouPlot
// is installed, or just as plain text if not.
package tplot

import (
	"fmt"
	"io"
	"os"
	"os/exec"
)

const (
	youplot     = "youplot"
	tinyValue   = 1
	tinyValueDP = 3
	lowValue    = 10
	lowValueDP  = 2
	midValue    = 100
	midValueDP  = 1
	highValueDP = 0
)

type datum struct {
	x, y string
}

type Data struct {
	title string
	data  []*datum
}

func NewData(title string) *Data {
	return &Data{
		title: title,
	}
}

func (d *Data) Add(x string, y float64) {
	var dp int

	switch {
	case y < tinyValue:
		dp = tinyValueDP
	case y < lowValue:
		dp = lowValueDP
	case y < midValue:
		dp = midValueDP
	default:
		dp = highValueDP
	}

	format := "%" + fmt.Sprintf(".%df", dp)

	xy := &datum{
		x: x,
		y: fmt.Sprintf(format, y),
	}

	d.data = append(d.data, xy)
}

type TPlotter struct {
	youPlotExe string
	output     io.Writer
}

func New() *TPlotter {
	exe, _ := exec.LookPath(youplot) //nolint:errcheck

	return &TPlotter{
		youPlotExe: exe,
		output:     os.Stdout,
	}
}

func (t *TPlotter) Plot(data *Data) error {
	if t.youPlotExe == "" {
		t.simplePlot(data)

		return nil
	}

	return t.fancyPlot(data)
}

func (t *TPlotter) simplePlot(data *Data) {
	fmt.Fprintf(t.output, "\n%s:\n", data.title)

	for _, xy := range data.data {
		fmt.Fprintf(t.output, " %s:\t%s\n", xy.x, xy.y)
	}
}

func (t *TPlotter) fancyPlot(data *Data) error {
	fmt.Fprintf(t.output, "\n\n")

	cmd := exec.Command(t.youPlotExe, "bar", "-t", data.title) //nolint:gosec
	cmd.Stdout = t.output
	cmd.Stderr = t.output

	w, err := cmd.StdinPipe()
	if err != nil {
		return err
	}

	err = cmd.Start()
	if err != nil {
		return err
	}

	for _, datum := range data.data {
		_, err = w.Write([]byte(fmt.Sprintf("%s\t%s\n", datum.x, datum.y)))
		if err != nil {
			return err
		}
	}

	err = w.Close()
	if err != nil {
		return err
	}

	return cmd.Wait()
}
