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
	"math"
	"slices"
	"sort"
	"strings"

	//nolint:misspell
	"github.com/dustin/go-humanize"
	ui "github.com/gizak/termui/v3"
	"github.com/gizak/termui/v3/widgets"
	"github.com/spf13/cobra"
	"github.com/ugorji/go/codec"
	"github.com/wtsi-hgi/ibackup/set"
	bolt "go.etcd.io/bbolt"
)

const (
	setsBucket              = "sets"
	subBucketPrefix         = "~!~"
	dbOpenMode              = 0600
	bySetMax                = 20
	bytesInTiB      float64 = 1024 * 1024 * 1024 * 1024
)

// summaryCmd represents the summary command.
var summaryCmd = &cobra.Command{
	Use:   "summary",
	Short: "Get a summary of backed up sets",
	Long: `Get a summary of backed up sets.
 
 Having used 'ibackup add' to add the details of one or more backup sets, use
 this command to summarise what has been backed up.
 
 This will only work for the user who started the ibackup server, and you need
 to supply the path to the server's database (or its backup).
 `,
	Run: func(cmd *cobra.Command, args []string) {
		ensureURLandCert()

		if len(args) != 1 {
			die("you must supply the path to an ibackup database")
		}

		err := summary(args[0])
		if err != nil {
			die(err.Error())
		}
	},
}

func init() {
	RootCmd.AddCommand(summaryCmd)
}

type sizeCount struct {
	size  uint64
	count uint64
}

func (s *sizeCount) add(set *set.Set) {
	s.size += set.SizeFiles
	s.count += set.NumFiles
}

func summary(dbPath string) error {
	boltDB, err := bolt.Open(dbPath, dbOpenMode, &bolt.Options{
		ReadOnly: true,
	})
	if err != nil {
		return err
	}

	ch := new(codec.BincHandle)
	totalSizeCount := &sizeCount{}
	byUser := make(map[string]*sizeCount)
	bySet := make(map[string]*sizeCount)
	byMonth := make(map[string]*sizeCount)

	err = boltDB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(setsBucket))

		return b.ForEach(func(k, v []byte) error {
			if strings.HasPrefix(string(k), subBucketPrefix) {
				return nil
			}

			set := decodeSet(v, ch)
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
				month := fmt.Sprintf("%d/%d", set.LastCompleted.Year(), int(set.LastCompleted.Month()))

				monthSC, ok := byMonth[month]
				if !ok {
					byMonth[month] = &sizeCount{}
					monthSC = byMonth[month]
				}

				monthSC.add(set)
			}

			return nil
		})
	})
	if err != nil {
		return err
	}

	cliPrint("Total size: %s\nTotal files: %s\n",
		humanize.Bytes(totalSizeCount.size), humanize.Comma(int64(totalSizeCount.count)))

	cliPrint("\nBy user:\n")
	sortAndPrintSCmap(byUser, 0)

	cliPrint("\nLargest %d sets:\n", bySetMax)
	sortAndPrintSCmap(bySet, bySetMax)

	return plotUsageOverTime(byMonth)
}

func decodeSet(v []byte, ch codec.Handle) *set.Set {
	dec := codec.NewDecoderBytes(v, ch)

	var set *set.Set

	dec.MustDecode(&set)

	return set
}

func sortAndPrintSCmap(scMap map[string]*sizeCount, max int) {
	keys := make([]string, 0, len(scMap))
	for user := range scMap {
		keys = append(keys, user)
	}

	sort.Slice(keys, func(i, j int) bool {
		return scMap[keys[i]].size > scMap[keys[j]].size
	})

	for i, key := range keys {
		cliPrint(" %s:\t%s\n", key, humanize.Bytes(scMap[key].size))

		if max > 0 && i == max-1 {
			break
		}
	}
}

func plotUsageOverTime(byMonth map[string]*sizeCount) error {
	if err := ui.Init(); err != nil {
		return err
	}
	defer ui.Close()

	months := make([]string, 0, len(byMonth))
	monthUsage := make([]float64, len(byMonth))

	for month := range byMonth {
		months = append(months, month)
	}

	slices.Sort(months)

	for i, month := range months {
		monthUsage[i] = math.Round(float64(byMonth[month].size) / bytesInTiB)
	}

	bc := widgets.NewBarChart()
	bc.Data = monthUsage
	bc.Labels = months
	bc.Title = "Usage Over Time"
	// bc.SetRect(5, 5, 100, 25)

	bc.BarWidth = 9
	bc.BarColors = []ui.Color{ui.ColorRed}
	bc.LabelStyles = []ui.Style{ui.NewStyle(ui.ColorBlue)}
	// bc.NumStyles = []ui.Style{ui.NewStyle(ui.ColorYellow)}

	// ui.Render(bc)

	grid := ui.NewGrid()
	termWidth, termHeight := ui.TerminalDimensions()
	grid.SetRect(0, 0, termWidth, termHeight)

	grid.Set(
		ui.NewRow(1.0,
			ui.NewCol(1.0, bc),
		),
	)

	ui.Render(grid)

	return nil
}
