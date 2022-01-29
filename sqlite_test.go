package pathdb

import (
	"database/sql"

	"github.com/getlantern/pathdb/minisql"
	_ "github.com/mattn/go-sqlite3"
)

func newSQLiteImpl(file string) (minisql.DB, error) {
	db, err := sql.Open("sqlite3", file)
	if err != nil {
		return nil, err
	}
	return &minisql.DBAdapter{db}, nil
}
