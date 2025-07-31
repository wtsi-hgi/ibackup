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
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	_ "modernc.org/sqlite"
)

func createTestDatabase(t *testing.T) *DB {
	t.Helper()

	var sdriver, uri string

	if p := os.Getenv("IBACKUP_MYSQL_URL"); p != "" { //nolint:nestif
		sdriver = "mysql"
		uri = p + "?parseTime=true"

		So(dropTables(p), ShouldBeNil)
	} else {
		sdriver = "sqlite"
		oldTmp := os.Getenv("TMPDIR")

		if _, err := os.Stat("/dev/shm"); err == nil {
			os.Setenv("TMPDIR", "/dev/shm")
		}

		uri = filepath.Join(t.TempDir(), "db?journal_mode=WAL")

		os.Setenv("TMPDIR", oldTmp)
	}

	d, err := Init(sdriver, uri)
	So(err, ShouldBeNil)

	if sdriver == "sqlite" {
		d.execReturningRowID = func(tx *sql.Tx, sqlstr string, params ...any) (int64, error) {
			var id int64

			err := tx.QueryRow(sqlstr, params...).Scan(&id)
			if errors.Is(err, sql.ErrNoRows) {
				return 0, nil
			}

			return id, err
		}
	}

	Reset(func() { d.Close() })

	return d
}

func dropTables(uri string) error {
	db, err := sql.Open("mysql", uri)
	if err != nil {
		return err
	}

	for _, table := range [...]string{"queue", "processes", "localFiles",
		"remoteFiles", "hardlinks", "toDiscover",
		"sets", "transformers"} {
		if _, err = db.Exec("DROP TABLE IF EXISTS `" + table + "`;"); err != nil {
			return err
		}
	}

	return nil
}
