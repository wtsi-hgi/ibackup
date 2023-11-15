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
	"slices"
	"sort"

	//nolint:misspell
	"github.com/dustin/go-humanize"
	"github.com/spf13/cobra"
	"github.com/wtsi-hgi/ibackup/set"
	"github.com/wtsi-hgi/ibackup/tplot"
)

const (
	setsBucket              = "sets"
	subBucketPrefix         = "~!~"
	dbOpenMode              = 0600
	bySetMax                = 20
	bytesInTiB      float64 = 1024 * 1024 * 1024 * 1024
	youplot                 = "youplot"
)

// options for this cmd.
var summaryDB string

// summaryCmd represents the summary command.
var summaryCmd = &cobra.Command{
	Use:   "summary",
	Short: "Get a summary of backed up sets",
	Long: `Get a summary of backed up sets.

The user who started the server can use this sub-command to summarise what has
been backed up (follwing 'ibackup add' calls).
 
The --database option should be the path to the local backup of the ibackup
database, defaulting to the value of the IBACKUP_LOCAL_DB_BACKUP_PATH
environmental variable.

The output will look nicer if you have https://github.com/red-data-tools/YouPlot
installed.
 `,
	Run: func(cmd *cobra.Command, args []string) {
		err := summary(summaryDB)
		if err != nil {
			die(err.Error())
		}
	},
}

func init() {
	RootCmd.AddCommand(summaryCmd)

	summaryCmd.Flags().StringVarP(&summaryDB, "database", "d",
		os.Getenv("IBACKUP_LOCAL_DB_BACKUP_PATH"), "path to ibackup database file")
}

type sizeCount struct {
	size  uint64
	count uint64
}

func (s *sizeCount) add(set *set.Set) {
	s.size += set.SizeFiles
	s.count += set.NumFiles
}

// sizeTB returns size in TiB.
func (s *sizeCount) sizeTB() float64 {
	return float64(s.size) / bytesInTiB
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

	totalSizeCount := &sizeCount{}
	byUser := make(map[string]*sizeCount)
	bySet := make(map[string]*sizeCount)
	byMonth := make(map[string]*sizeCount)

	for _, set := range sets {
		totalSizeCount.add(set)

		userSC, ok := byUser[set.Requester]
		if !ok {
			byUser[set.Requester] = &sizeCount{}
			userSC = byUser[set.Requester]
		}

		userSC.add(set)

		sc := &sizeCount{}
		sc.add(set)
		bySet[set.Requester+"."+set.Name] = sc

		if !set.LastCompleted.IsZero() {
			month := fmt.Sprintf("%d/%02d", set.LastCompleted.Year(), int(set.LastCompleted.Month()))

			monthSC, ok := byMonth[month]
			if !ok {
				byMonth[month] = &sizeCount{}
				monthSC = byMonth[month]
			}

			monthSC.add(set)
		}
	}

	cliPrint("Total size: %s\nTotal files: %s\n",
		humanize.Bytes(totalSizeCount.size), humanize.Comma(int64(totalSizeCount.count)))

	tp := tplot.New()

	err = sortAndPlotSCmap(tp, byUser, 0, "By user (TB backed up)")
	if err != nil {
		return err
	}

	err = sortAndPlotSCmap(tp, bySet, bySetMax, fmt.Sprintf("Largest %d sets (TB)", bySetMax))
	if err != nil {
		return err
	}

	return plotUsageOverTime(tp, byMonth)
}

func sortAndPlotSCmap(tp *tplot.TPlotter, scMap map[string]*sizeCount, max int, title string) error {
	keys := make([]string, 0, len(scMap))
	for user := range scMap {
		keys = append(keys, user)
	}

	sort.Slice(keys, func(i, j int) bool {
		return scMap[keys[i]].size > scMap[keys[j]].size
	})

	data := tplot.NewData(title)

	for i, key := range keys {
		if scMap[key].size == 0 {
			continue
		}

		data.Add(key, scMap[key].sizeTB())

		if max > 0 && i == max-1 {
			break
		}
	}

	return tp.Plot(data)
}

func plotUsageOverTime(tp *tplot.TPlotter, byMonth map[string]*sizeCount) error {
	months := make([]string, 0, len(byMonth))
	data := tplot.NewData("Backed up (TB) each month")

	for month := range byMonth {
		months = append(months, month)
	}

	slices.Sort(months)

	for _, month := range months {
		data.Add(month, byMonth[month].sizeTB())
	}

	return tp.Plot(data)
}
