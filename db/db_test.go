package db

import (
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"os"
	"testing"
	"unsafe"

	. "github.com/smartystreets/goconvey/convey"
	"modernc.org/sqlite"
)

func init() { //nolint:gochecknoinits
	sqlite.MustRegisterScalarFunction("SHA2", 2, func(_ *sqlite.FunctionContext, a []driver.Value) (driver.Value, error) {
		v := a[0].(string) //nolint:errcheck,forcetypeassert

		return fmt.Sprintf("%X", sha256.Sum256(unsafe.Slice(unsafe.StringData(v), len(v)))), nil
	})
}

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
		d.execReturningRowID = func(tx *sql.Tx, sql string, params ...any) (int64, error) {
			var id int64

			err := tx.QueryRow(sql, params...).Scan(&id)

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

	for _, table := range [...]string{"uploads", "localFiles",
		"remoteFiles", "hardlinks", "toDiscover",
		"sets", "transformers"} {
		if _, err = db.Exec("DROP TABLE `" + table + "`;"); err != nil {
			return err
		}
	}

	return nil
}
