package db

import (
	"database/sql"

	_ "github.com/go-sql-driver/mysql" //
)

type DBRO struct { //nolint:revive
	db *sql.DB
}

type DB struct {
	DBRO

	execReturningRowID func(tx *sql.Tx, sql string, params ...any) (int64, error)
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

	d := &DB{DBRO: DBRO{db: db}, execReturningRowID: execReturningRowID}

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
