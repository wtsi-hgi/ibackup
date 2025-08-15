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

package testdb

import (
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"unsafe"

	. "github.com/smartystreets/goconvey/convey" //nolint:revive,stylecheck
	"github.com/wtsi-hgi/ibackup/db"
	"modernc.org/sqlite"
)

type dbe struct {
	db.DBRO

	execReturningRowID func(tx *sql.Tx, sql string, params ...any) (int64, error)
	isAlreadyExists    func(error) bool
	isForeignKeyError  func(error) bool
}

func CreateTestDatabase(t *testing.T) *db.DB { //nolint:funlen
	t.Helper()

	oldTmp := os.Getenv("TMPDIR")

	if _, err := os.Stat("/dev/shm"); err == nil {
		os.Setenv("TMPDIR", "/dev/shm")
	}

	os.Setenv("TMPDIR", oldTmp) //nolint:tenv

	d, err := db.Init("sqlite", filepath.Join(t.TempDir(), "db?journal_mode=WAL&_pragma=foreign_keys(1)"))
	So(err, ShouldBeNil)

	(*dbe)(unsafe.Pointer(d)).execReturningRowID = func(tx *sql.Tx, sqlstr string, params ...any) (int64, error) {
		var id int64

		err := tx.QueryRow(sqlstr, params...).Scan(&id)
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil
		}

		return id, err
	}

	(*dbe)(unsafe.Pointer(d)).isAlreadyExists = func(err error) bool {
		var sqlerr *sqlite.Error

		if errors.As(err, &sqlerr) {
			return sqlerr.Code() == 2067 //nolint:mnd
		}

		return false
	}

	(*dbe)(unsafe.Pointer(d)).isForeignKeyError = func(err error) bool {
		var sqlerr *sqlite.Error

		if errors.As(err, &sqlerr) {
			return sqlerr.Code() == 787 //nolint:mnd
		}

		return false
	}

	Reset(func() { d.Close() })

	return d
}

func DoTasks(t *testing.T, d *db.DB) {
	t.Helper()

	process, err := d.RegisterProcess()
	So(err, ShouldBeNil)

	const numTasks = 1024

	for {
		var tasks []*db.Task

		So(d.ReserveTasks(process, numTasks).ForEach(func(task *db.Task) error {
			tasks = append(tasks, task)

			return nil
		}), ShouldBeNil)

		if len(tasks) == 0 {
			break
		}

		for _, task := range tasks {
			So(d.TaskComplete(task), ShouldBeNil)
		}
	}
}
