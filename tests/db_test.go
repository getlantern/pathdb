package tests

import (
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/getlantern/pathdb/minisql"
	"github.com/getlantern/pathdb/testsupport"
)

func TestDB(t *testing.T) {
	t.Run("TestTransactions", func(t *testing.T) {
		testsupport.TestTransactions(t, newSQLiteImpl(t))
	})
	t.Run("TestSubscriptions", func(t *testing.T) {
		testsupport.TestSubscriptions(t, newSQLiteImpl(t))
	})
	t.Run("TestSubscribeToInitialDetails", func(t *testing.T) {
		testsupport.TestSubscribeToInitialDetails(t, newSQLiteImpl(t))
	})
	t.Run("TestDetailSubscriptionModifyDetails", func(t *testing.T) {
		testsupport.TestDetailSubscriptionModifyDetails(t, newSQLiteImpl(t))
	})
	t.Run("TestDetailSubscriptionModifyIndex", func(t *testing.T) {
		testsupport.TestDetailSubscriptionModifyIndex(t, newSQLiteImpl(t))
	})
	t.Run("TestList", func(t *testing.T) {
		testsupport.TestList(t, newSQLiteImpl(t))
	})
	t.Run("TestSearch", func(t *testing.T) {
		testsupport.TestSearch(t, newSQLiteImpl(t))
	})
	t.Run("TestSearchChinese", func(t *testing.T) {
		testsupport.TestSearchChinese(t, newSQLiteImpl(t))
	})
}

func newSQLiteImpl(t *testing.T) minisql.DB {
	tmpDir := t.TempDir()
	db, err := sql.Open("sqlite3", filepath.Join(tmpDir, "test.db"))
	require.NoError(t, err)
	return &minisql.DBAdapter{DB: db}
}
