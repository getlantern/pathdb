package pathdb

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDB(t *testing.T) {
	file, err := ioutil.TempFile("", "")
	require.NoError(t, err)
	defer os.Remove(file.Name())
	sl, err := newSQLiteImpl(file.Name())
	require.NoError(t, err)
	db, err := NewDB(sl, "test")
	require.NoError(t, err)

	// put and commit something
	tx, err := db.Begin()
	require.NoError(t, err)
	err = tx.Put("path", "hello world", "hello world full text")
	require.NoError(t, err)
	err = tx.Commit()
	require.NoError(t, err)
	result, err := db.Get("path")
	require.NoError(t, err)
	require.Equal(t, "hello world", result)

	// put and rollback something
	tx, err = db.Begin()
	require.NoError(t, err)
	err = tx.Delete("path")
	require.NoError(t, err)
	result, err = tx.Get("path")
	require.NoError(t, err)
	require.Nil(t, result, "delete should be reflected in scope of ongoing transaction")
	err = tx.Rollback()
	require.NoError(t, err)
	result, err = db.Get("path")
	require.NoError(t, err)
	require.Equal(t, "hello world", result, "delete should have been rolled back")
}
