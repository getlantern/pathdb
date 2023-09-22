package tests

import (
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/getlantern/pathdb/minisql"
)

func TestDB(t *testing.T) {
	t.Run("TestTransactions", func(t *testing.T) {
		TestTransactions(t, newSQLiteImpl(t))
	})
	t.Run("TestSubscriptions", func(t *testing.T) {
		TestSubscriptions(t, newSQLiteImpl(t))
	})
	t.Run("TestSubscribeToInitialDetails", func(t *testing.T) {
		TestSubscribeToInitialDetails(t, newSQLiteImpl(t))
	})
	t.Run("TestDetailSubscriptionModifyDetails", func(t *testing.T) {
		TestDetailSubscriptionModifyDetails(t, newSQLiteImpl(t))
	})
	t.Run("TestDetailSubscriptionModifyIndex", func(t *testing.T) {
		TestDetailSubscriptionModifyIndex(t, newSQLiteImpl(t))
	})
	t.Run("TestList", func(t *testing.T) {
		TestList(t, newSQLiteImpl(t))
	})
	t.Run("TestSearch", func(t *testing.T) {
		TestSearch(t, newSQLiteImpl(t))
	})
	t.Run("TestSearchChinese", func(t *testing.T) {
		TestSearchChinese(t, newSQLiteImpl(t))
	})
}

func newSQLiteImpl(t *testing.T) minisql.DB {
	tmpDir := t.TempDir()
	db, err := sql.Open("sqlite3", filepath.Join(tmpDir, "test.db"))
	require.NoError(t, err)
	return &minisql.DBAdapter{DB: db}
}
