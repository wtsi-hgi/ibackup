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

package cmd

import (
	"fmt"
	"os"

	//nolint:misspell
	"github.com/dustin/go-humanize"
	"github.com/spf13/cobra"
	"github.com/wtsi-hgi/ibackup/set"
	"github.com/wtsi-hgi/ibackup/tplot"
)

const bySetMax = 20

// options for this cmd.
var summaryDB string

// summaryCmd represents the summary command.
var summaryCmd = &cobra.Command{
	Use:   "summary",
	Short: "Get a summary of backed up sets",
	Long: `Get a summary of backed up sets.

The user who started the server can use this sub-command to summarise what has
been backed up (following 'ibackup add' calls).
 
The --database option should be the path to the local backup of the ibackup
database, defaulting to the value of the IBACKUP_LOCAL_DB_BACKUP_PATH
environmental variable.

The output will look nicer if you have https://github.com/red-data-tools/YouPlot
installed.
 `,
	Run: func(cmd *cobra.Command, args []string) {
		err := summary(summaryDB)
		if err != nil {
			die(err)
		}
	},
}

func init() {
	RootCmd.AddCommand(summaryCmd)

	summaryCmd.Flags().StringVarP(&summaryDB, "database", "d",
		os.Getenv("IBACKUP_LOCAL_DB_BACKUP_PATH"), "path to ibackup database file")
}

func summary(dbPath string) error {
	db, err := set.NewRO(dbPath)
	if err != nil {
		return err
	}

	sets, err := db.GetAll()
	if err != nil {
		return err
	}

	usage := set.UsageSummary(sets)

	cliPrint("Total size: %s\nTotal files: %s\n",
		humanize.IBytes(usage.Total.Size), humanize.Comma(int64(usage.Total.Number)))

	tp := tplot.New()

	err = plotSANs(tp, usage.ByRequester, 0, "By user (TB backed up)")
	if err != nil {
		return err
	}

	err = plotSANs(tp, usage.BySet, bySetMax, fmt.Sprintf("Largest %d sets (TB)", bySetMax))
	if err != nil {
		return err
	}

	return plotSANs(tp, usage.ByMonth, 0, "Backed up (TB) each month")
}

func plotSANs(tp *tplot.TPlotter, sans []*set.SizeAndNumber, max int, title string) error {
	data := tplot.NewData(title)

	for i, san := range sans {
		data.Add(san.For, san.SizeTiB())

		if max > 0 && i == max-1 {
			break
		}
	}

	return tp.Plot(data)
}
