package db

import (
	"database/sql"
)

type DBRO struct { //nolint:revive
	db *sql.DB
}

type DB struct {
	DBRO
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

	d := &DB{DBRO: DBRO{db: db}}

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

	_, err = tx.Exec(sql, params...)
	if err != nil {
		return err
	}

	return tx.Commit()
}
