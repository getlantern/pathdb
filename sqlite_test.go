package pathdb

import (
	"database/sql"

	_ "github.com/mattn/go-sqlite3"

	"github.com/getlantern/pathdb/minisql"
)

func newSQLiteImpl(file string) (minisql.DB, error) {
	db, err := sql.Open("sqlite3", file)
	if err != nil {
		return nil, err
	}
	return &minisql.DBAdapter{DB: db}, nil
}
