package pathdb

import (
	"errors"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

var errTest = errors.New("test error")

func TestDB(t *testing.T) {
	file, err := ioutil.TempFile("", "")
	require.NoError(t, err)
	defer os.Remove(file.Name())
	sl, err := newSQLiteImpl(file.Name())
	require.NoError(t, err)
	db, err := NewDB(sl, "test")
	require.NoError(t, err)

	// put and commit something
	err = Mutate(db, func(tx TX) error {
		err := Put(tx, "path", "hello world", "hello world full text")
		require.NoError(t, err)
		return nil
	})
	require.NoError(t, err)

	// make sure it can be read
	result, err := Get[string](db, "path")
	require.NoError(t, err)
	require.Equal(t, "hello world", result)

	// put and rollback something
	err = Mutate(db, func(tx TX) error {
		err = Delete(tx, "path")
		require.NoError(t, err)
		result, err := Get[string](tx, "path")
		require.NoError(t, err)
		require.Empty(t, result, "delete should be reflected in scope of ongoing transaction")
		return errTest
	})
	require.Equal(t, errTest, err)

	result, err = Get[string](db, "path")
	require.NoError(t, err)
	require.Equal(t, "hello world", result, "delete should have been rolled back")
}
