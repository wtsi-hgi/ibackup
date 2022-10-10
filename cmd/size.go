/*******************************************************************************
 * Copyright (c) 2022 Genome Research Ltd.
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
	"bufio"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/spf13/cobra"
	"github.com/wtsi-ssg/wrstat/pathsize"
)

// sizeCmd represents the size command.
var sizeCmd = &cobra.Command{
	Use:   "size",
	Short: "size",
	Long:  `size.`,
	Run: func(cmd *cobra.Command, args []string) {
		dbDir := args[0]
		queryPath := args[1]

		if dbDir == "" || queryPath == "" {
			die("provide db dir and query path")
		}

		file, err := os.Open(queryPath)
		if err != nil {
			die("%s", err)
		}
		defer file.Close()

		var paths []string

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			paths = append(paths, "/lustre/scratch123/hgi/mdt1/projects/ddd/users/hcm/"+scanner.Text())
		}

		entries, err := os.ReadDir(dbDir)
		if err != nil {
			die("%s", err)
		}

		dbPaths := make([]string, len(entries))

		for i, entry := range entries {
			dbPaths[i] = filepath.Join(dbDir, entry.Name())
		}

		t := time.Now()

		db := pathsize.NewDB(dbPaths...)
		defer db.Close()

		if err := db.Open(); err != nil {
			die("%s", err)
		}

		sort.Strings(paths)

		files, size := 0, uint64(0)

		for _, path := range paths {
			if s, ok := db.SizeOf(path); ok {
				files++
				size += s
			}
		}

		info("SizeOf took %s", time.Since(t))
		info("files: %d, size: %d", files, size)

		t = time.Now()
		files, size = 0, uint64(0)

		for _, path := range paths {
			if info, err := os.Stat(path); err == nil {
				files++
				size += uint64(info.Size())
			}
		}

		info("Stat took %s", time.Since(t))
		info("files: %d, size: %d", files, size)

		// pss := db.GetChildren(queryPath)

		// if len(pss) == 0 {
		// 	warn("path not found")

		// 	return
		// }

		// info("GetChildren took %s", time.Since(t))

		// for _, ps := range pss {
		// 	// info("%s: %d", ps.Path, ps.Size)
		// 	files++
		// 	size += ps.Size
		// }

		// info("files: %d, size: %d", files, size)

		// files, size = 0, uint64(0)

		// fn := func(path string, d fs.DirEntry, err error) error {
		// 	if err != nil {
		// 		return err
		// 	}

		// 	if d.Type().IsRegular() {
		// 		info, err := d.Info()
		// 		if err != nil {
		// 			return err
		// 		}

		// 		files++
		// 		size += uint64(info.Size())
		// 	}

		// 	return nil
		// }

		// t = time.Now()

		// if err := filepath.WalkDir(queryPath, fn); err != nil {
		// 	die("%s", err)
		// }

		// info("walk took %s", time.Since(t))
		// info("files: %d, size: %d", files, size)
	},
}

func init() {
	RootCmd.AddCommand(sizeCmd)
}
