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

import "database/sql"

type DBRO struct { //nolint:revive
	db *sql.DB
}

type DB struct {
	DBRO

	execReturningRowID func(tx *sql.Tx, sql string, params ...any) (int64, error)
	isAlreadyExists    func(error) bool
	isForeignKeyError  func(error) bool
}

func InitRO(driver, connection string) (*DBRO, error) {
	db, err := sql.Open(driver, connection)
	if err != nil {
		return nil, err
	}

	return &DBRO{db: db}, nil
}

func Init(driver, connection string) (*DB, error) {
	db, err := sql.Open(driver, connection)
	if err != nil {
		return nil, err
	}

	d := &DB{
		DBRO:               DBRO{db: db},
		execReturningRowID: execReturningRowID,
		isAlreadyExists:    isAlreadyExists,
		isForeignKeyError:  isForeignKeyError,
	}

	if err = d.initTables(); err != nil {
		return nil, err
	}

	return d, nil
}

func (d *DB) initTables() error {
	for _, table := range tables {
		if _, err := d.db.Exec(table); err != nil {
			return err
		}
	}

	return nil
}

func (d *DB) exec(sql string, params ...any) error {
	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	if _, err = tx.Exec(sql, params...); err != nil {
		return err
	}

	return tx.Commit()
}

func (d *DBRO) Close() error {
	return d.db.Close()
}
