package sql

import (
	"database/sql"
	"errors"
	"fmt"
	"io"
	"strconv"
	"unsafe"

	"github.com/klauspost/pgzip"
	"github.com/wtsi-hgi/ibackup/set/db"
)

type DB struct {
	db       *sql.DB
	readonly bool
	tables   map[string]struct{}
}

func New(driver, path string, readonly bool) (db.DB, error) { //nolint:ireturn
	db, err := sql.Open(driver, path)
	if err != nil {
		return nil, err
	}

	return &DB{db: db, readonly: readonly, tables: make(map[string]struct{})}, nil
}

func (d *DB) View(fn func(db.Tx) error) error {
	t := Tx{db: d.db, tables: d.tables}
	err := fn(&t)
	t.db = nil

	return err
}

func (d *DB) Update(fn func(db.Tx) error) error {
	if d.readonly {
		return ErrReadOnly
	}

	tx, err := d.db.Begin()
	if err != nil {
		return err
	}

	defer tx.Rollback() //nolint:errcheck

	t := Tx{tx: tx, tables: d.tables}

	if err := fn(&t); err != nil {
		return err
	}

	t.tx = nil

	return tx.Commit()
}

func (d *DB) Close() error {
	return d.db.Close()
}

type Tx struct {
	tx     *sql.Tx
	db     *sql.DB
	tables map[string]struct{}
}

func (t *Tx) CreateBucketIfNotExists(key []byte) (db.Bucket, error) { //nolint:ireturn
	if t.tx == nil {
		return nil, ErrTxNotWritable
	} else if len(key) == 0 {
		return nil, ErrBucketNameRequired
	}

	t.tables[string(key)] = struct{}{}

	if err := t.exec(fmt.Sprintf("CREATE TABLE IF NOT EXISTS [%s]"+
		" (id string, sub string, value string, UNIQUE(id, sub));", key)); err != nil {
		return nil, err
	}

	return t.Bucket(key), nil
}

func (t *Tx) queryRows(sql string, args ...any) (*sql.Rows, error) {
	if t.db != nil {
		return t.db.Query(sql, args...)
	} else if t.tx != nil {
		return t.tx.Query(sql, args...)
	}

	return nil, ErrTxClosed
}

func (t *Tx) get(table, sub, id []byte) []byte {
	query := fmt.Sprintf("SELECT value FROM [%s] WHERE sub = ? AND id = ? ORDER BY id ASC;", table) //nolint:gosec

	var (
		value []byte
		err   error
	)

	if t.db != nil {
		err = t.db.QueryRow(query, sub, id).Scan(&value)
	} else if t.tx != nil {
		err = t.tx.QueryRow(query, sub, id).Scan(&value)
	}

	if err != nil {
		return nil
	}

	return value
}

func (t *Tx) put(table, sub, id, value []byte) error {
	if t.tx != nil {
		return t.exec(
			fmt.Sprintf(
				"REPLACE INTO [%s] (sub, id, value) VALUES (?, ?, ?);",
				table,
			),
			sub, id, value,
		)
	} else if t.db != nil {
		return ErrTxNotWritable
	}

	return ErrTxClosed
}

func (t *Tx) delete(table, sub, id []byte) error {
	if t.tx != nil {
		return t.exec(
			fmt.Sprintf(
				"DELETE FROM [%s] WHERE sub = ? AND id = ?;",
				table,
			),
			sub, id,
		)
	} else if t.db != nil {
		return ErrTxNotWritable
	}

	return ErrTxClosed
}

func (t *Tx) deleteSub(table, sub []byte) error {
	if t.tx != nil {
		return t.exec(
			fmt.Sprintf(
				"DELETE FROM [%s] WHERE sub = ?;",
				table,
			),
			sub,
		)
	} else if t.db != nil {
		return ErrTxNotWritable
	}

	return ErrTxClosed
}

func (t *Tx) forEach(table, sub []byte) (*sql.Rows, error) {
	return t.queryRows(
		fmt.Sprintf("SELECT id, value FROM [%s] WHERE sub = ? ORDER BY id ASC;", table),
		sub,
	)
}

func (t *Tx) forEachStarting(table, sub, starting []byte) (*sql.Rows, error) {
	return t.queryRows(
		fmt.Sprintf("SELECT id, value FROM [%s] WHERE sub = ? AND id >= ? ORDER BY id ASC;", table),
		sub, starting,
	)
}

func (t *Tx) nextSequence(table []byte) (uint64, error) {
	query := fmt.Sprintf( //nolint:gosec
		"SELECT id FROM [%[1]s] WHERE LENGTH(id) == ( SELECT MAX(LENGTH(id)) FROM [%[1]s] ) ORDER BY id DESC LIMIT 1",
		table,
	)

	var row *sql.Row

	if t.db != nil { //nolint:gocritic,nestif
		row = t.db.QueryRow(query)
	} else if t.tx != nil {
		row = t.tx.QueryRow(query)
	} else {
		return 0, ErrTxClosed
	}

	var curr []byte

	err := row.Scan(&curr)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			err = nil
		}

		return 0, err
	}

	c, err := strconv.ParseUint(unsafe.String(unsafe.SliceData(curr), len(curr)), 10, 64)
	if err != nil {
		return 0, err
	}

	return c + 1, nil
}

type stickyWriter struct {
	Writer io.Writer
	n      int64
	err    error
}

func (s *stickyWriter) Write(p []byte) (int, error) {
	if s.err != nil {
		return 0, s.err
	}

	n, err := s.Writer.Write(p)
	s.n += int64(n)
	s.err = err

	return n, err
}

func (t *Tx) WriteTo(w io.Writer) (int64, error) {
	sw := stickyWriter{Writer: w}

	gw := pgzip.NewWriter(&sw)
	defer gw.Close()

	for table := range t.tables {
		if err := t.writeTable(gw, table); err != nil {
			return sw.n, err
		}
	}

	return sw.n, sw.err
}

const rowsPerInsert = 1000

func (t *Tx) writeTable(w io.Writer, table string) error {
	rows, err := t.queryRows(fmt.Sprintf("SELECT sub, id, value FROM [%s]", table))
	if err != nil {
		return err
	}

	fmt.Fprintf(w, "DROP TABLE [%[1]s]; CREATE TABLE [%[1]s]"+
		" (id string, sub string, value string, UNIQUE(id, sub));\n", table)

	if err := printRows(w, table, rows); err != nil {
		return err
	}

	return rows.Close()
}

func printRows(w io.Writer, table string, rows *sql.Rows) error { //nolint:gocognit
	n := 0

	for rows.Next() {
		if n%rowsPerInsert == 0 { //nolint:nestif
			if n > 0 {
				io.WriteString(w, ";\n") //nolint:errcheck
			}

			fmt.Fprintf(w, "INSERT INTO [%s] (sub, id, value) VALUES\n", table)
		} else {
			io.WriteString(w, ",\n") //nolint:errcheck
		}

		n++

		var id, sub, value []byte

		if err := rows.Scan(&sub, &id, &value); err != nil {
			return err
		}

		fmt.Fprintf(w, "(%q, %q, %q)", sub, id, value)
	}

	if n > 0 {
		io.WriteString(w, ";") //nolint:errcheck
	}

	return nil
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

func (t *Tx) Bucket(key []byte) db.Bucket { //nolint:ireturn
	return &Bucket{
		tx:    t,
		table: key,
		sub:   []byte{},
	}
}

func (b *Bucket) Get(id []byte) []byte {
	return b.tx.get(b.table, b.sub, id)
}

func (b *Bucket) Put(id, value []byte) error {
	return b.tx.put(b.table, b.sub, id, value)
}

func (b *Bucket) CreateBucketIfNotExists(key []byte) (db.Bucket, error) { //nolint:ireturn
	return &Bucket{
		tx:    b.tx,
		table: b.table,
		sub:   key,
	}, nil
}

func (b *Bucket) CreateBucket(key []byte) (db.Bucket, error) { //nolint:ireturn
	return &Bucket{
		tx:    b.tx,
		table: b.table,
		sub:   key,
	}, nil
}

func (b *Bucket) Bucket(key []byte) db.Bucket { //nolint:ireturn
	return &Bucket{
		tx:    b.tx,
		table: b.table,
		sub:   key,
	}
}

func (b *Bucket) Delete(id []byte) error {
	return b.tx.delete(b.table, b.sub, id)
}

func (b *Bucket) DeleteBucket(sub []byte) error {
	return b.tx.deleteSub(b.table, sub)
}

func (b *Bucket) NextSequence() (uint64, error) {
	return b.tx.nextSequence(b.table)
}

func (b *Bucket) ForEach(fn func([]byte, []byte) error) error {
	rows, err := b.tx.forEach(b.table, b.sub)
	if err != nil {
		return err
	}

	defer rows.Close()

	for rows.Next() {
		var key, value sql.RawBytes

		if err := rows.Scan(&key, &value); err != nil {
			return err
		}

		if err := fn(key, value); err != nil {
			return err
		}
	}

	return nil
}

func (b *Bucket) Cursor() db.Cursor { //nolint:ireturn
	return &Cursor{bucket: b}
}

func (b *Bucket) getAll(starting []byte) *sql.Rows {
	var rows *sql.Rows

	if starting == nil {
		rows, _ = b.tx.forEach(b.table, b.sub) //nolint:errcheck
	} else {
		rows, _ = b.tx.forEachStarting(b.table, b.sub, starting) //nolint:errcheck
	}

	return rows
}

type Cursor struct {
	bucket *Bucket
	rows   *sql.Rows
}

func (c *Cursor) First() ([]byte, []byte) {
	return c.resetRows(nil)
}

func (c *Cursor) resetRows(key []byte) ([]byte, []byte) {
	if c.rows != nil && c.rows.Close() != nil {
		return nil, nil
	}

	c.rows = c.bucket.getAll(key)

	return c.Next()
}

func (c *Cursor) Next() ([]byte, []byte) {
	if c.rows == nil || !c.rows.Next() {
		return nil, nil
	}

	var key, value []byte

	if c.rows.Scan(&key, &value) != nil {
		return nil, nil
	}

	return key, value
}

func (c *Cursor) Seek(key []byte) ([]byte, []byte) {
	return c.resetRows(key)
}

var (
	ErrTxClosed           = errors.New("tx closed")
	ErrTxNotWritable      = errors.New("tx not writable")
	ErrBucketNameRequired = errors.New("bucket name required")
	ErrReadOnly           = errors.New("readonly database")
)
