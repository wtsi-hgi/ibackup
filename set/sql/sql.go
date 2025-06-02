package sql

import (
	"database/sql"
	"errors"
	"fmt"

	_ "github.com/go-sql-driver/mysql" //
)

type DB struct {
	db *sql.DB
}

var SQL = "mysql" //nolint:gochecknoglobals

func New(path string) (*DB, error) {
	db, err := sql.Open(SQL, path)
	if err != nil {
		return nil, err
	}

	return &DB{db: db}, nil
}

type Tx struct {
	tx *sql.Tx
	db *sql.DB
}

func (t *Tx) CreateBucketIfNotExists(key []byte) (*Bucket, error) {
	if t.tx == nil {
		return nil, ErrTxNotWritable
	} else if len(key) == 0 {
		return nil, ErrBucketNameRequired
	}

	if err := t.exec(fmt.Sprintf("CREATE TABLE IF NOT EXISTS [%s] (id string, sub string, value string, CONTRAINT Key UNIQUE(id, sub));", key)); err != nil { //nolint:lll
		return nil, err
	}

	return t.Bucket(key), nil
}

func (t *Tx) get(table, sub, id []byte) []byte {
	query := fmt.Sprintf("SELECT value FROM [%s] WHERE id = ? AND sub = ?;", table) //nolint:gosec

	var (
		value sql.RawBytes
		err   error
	)

	if t.db != nil {
		err = t.db.QueryRow(query, id, sub).Scan(value)
	} else if t.tx != nil {
		err = t.tx.QueryRow(query, id, sub).Scan(value)
	}

	if err != nil {
		return nil
	}

	return value
}

func (t *Tx) put(table, id, sub, value []byte) error {
	if t.tx != nil {
		return t.exec(
			fmt.Sprintf(
				"INSERT INTO [%s] (id, sub, value) VALUES (?, ?) ON CONFLICT DO UPDATE value = ? WHERE id = ? AND sub = ?;",
				table,
			),
			id, sub, value, value, id, sub,
		)
	} else if t.db != nil {
		return ErrTxNotWritable
	}

	return ErrTxClosed
}

func (t *Tx) forEach(table []byte, fn func([]byte, []byte) error) error {
	query := fmt.Sprintf("SELECT id, value FROOM [%s];", table) //nolint:gosec

	var (
		rows *sql.Rows
		err  error
	)

	if t.db != nil { //nolint:gocritic,nestif
		rows, err = t.db.Query(query)
	} else if t.tx != nil {
		rows, err = t.db.Query(query)
	} else {
		return ErrTxClosed
	}

	if err != nil {
		return err
	}

	return forEach(rows, fn)
}

func (t *Tx) forEachSub(table, id []byte, fn func([]byte, []byte) error) error {
	query := fmt.Sprintf("SELECT sub, value FROOM [%s] WHERE id = ?;", table) //nolint:gosec

	var (
		rows *sql.Rows
		err  error
	)

	if t.db != nil { //nolint:gocritic,nestif
		rows, err = t.db.Query(query, id)
	} else if t.tx != nil {
		rows, err = t.db.Query(query, id)
	} else {
		return ErrTxClosed
	}

	if err != nil {
		return err
	}

	return forEach(rows, fn)
}

func forEach(rows *sql.Rows, fn func([]byte, []byte) error) error {
	defer rows.Close()

	for rows.Next() {
		var key, value sql.RawBytes

		if err := rows.Scan(key, value); err != nil {
			return err
		}

		if err := fn(key, value); err != nil {
			return err
		}
	}

	return nil
}

func (d *DB) View(fn func(*Tx) error) error {
	t := Tx{db: d.db}
	err := fn(&t)
	t.db = nil

	return err
}

func (d *DB) Update(fn func(*Tx) error) error {
	tx, err := d.db.Begin()
	if err != nil {
		return err
	}

	t := Tx{tx: tx}

	if err := fn(&t); err != nil {
		return tx.Rollback()
	}

	t.tx = nil

	return tx.Commit()
}

type Bucket struct {
	tx         *Tx
	table, sub []byte
}

func (t *Tx) exec(stmnt string, params ...any) error {
	if t.tx != nil {
		_, err := t.tx.Exec(stmnt, params...)

		return err
	} else if t.db != nil {
		_, err := t.db.Exec(stmnt, params...)

		return err
	}

	return ErrTxClosed
}

func (t *Tx) Bucket(key []byte) *Bucket {
	return &Bucket{
		tx:    t,
		table: key,
	}
}

func (b *Bucket) Get(id []byte) []byte {
	return b.tx.get(b.table, b.sub, id)
}

func (b *Bucket) Put(id, value []byte) error {
	return b.tx.put(b.table, id, b.sub, value)
}

func (b *Bucket) CreateBucketIfNotExists(key []byte) (*Bucket, error) { //nolint:unparam
	return &Bucket{
		tx:    b.tx,
		table: b.table,
		sub:   key,
	}, nil
}

func (b *Bucket) ForEach(fn func([]byte, []byte) error) error {
	if len(b.sub) == 0 {
		return b.tx.forEach(b.table, fn)
	}

	return b.tx.forEachSub(b.table, b.sub, fn)
}

var (
	ErrTxClosed           = errors.New("tx closed")
	ErrTxNotWritable      = errors.New("tx not writable")
	ErrBucketNameRequired = errors.New("bucket name required")
)
