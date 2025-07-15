package db

import (
	"database/sql"
	"errors"
	"os"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	_ "modernc.org/sqlite"
)

func createTestDatabase(t *testing.T) *DB {
	t.Helper()

	var sdriver, uri string

	if p := os.Getenv("IBACKUP_MYSQL_URL"); p != "" {
		sdriver = "mysql"
		uri = p + "?parseTime=true"

		So(dropTables(p), ShouldBeNil)
	} else {
		sdriver = "sqlite"
		uri = ":memory:"
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

	return d
}

func dropTables(uri string) error {
	db, err := sql.Open("mysql", uri)
	if err != nil {
		return err
	}

	for _, table := range [...]string{"queue", "localFiles",
		"remoteFiles", "hardlinks", "toDiscover",
		"sets", "transformers"} {
		if _, err = db.Exec("DROP TABLE IF EXISTS `" + table + "`;"); err != nil {
			return err
		}
	}

	return nil
}
