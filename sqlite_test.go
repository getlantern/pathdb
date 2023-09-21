package pathdb

import (
	"database/sql"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"

	"github.com/getlantern/pathdb/minisql"
)

func newSQLiteImpl(t *testing.T) minisql.DB {
	tmpDir := t.TempDir()
	db, err := sql.Open("sqlite3", filepath.Join(tmpDir, "test.db"))
	require.NoError(t, err)
	return &minisql.DBAdapter{DB: db}
}
