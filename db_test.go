package pathdb

import (
	"errors"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

var errTest = errors.New("test error")

func TestTransactions(t *testing.T) {
	withDB(t, func(db DB) {
		// put and commit something
		err := Mutate(db, func(tx TX) error {
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
	})
}

func TestList(t *testing.T) {
	withDB(t, func(db DB) {
		err := Mutate(db, func(tx TX) error {
			return PutAll(tx, map[string]string{
				"/messages/c": "Message C",
				"/messages/d": "Message D",
				"/messages/a": "Message A",
				"/messages/b": "Message B",
			})
		})
		require.NoError(t, err)

		results, err := List[string](db, &Query{path: "/messages/%"}, nil)
		require.NoError(t, err)
		require.EqualValues(t, []*PathAndValue[string]{
			{"/messages/a", "Message A"},
			{"/messages/b", "Message B"},
			{"/messages/c", "Message C"},
			{"/messages/d", "Message D"},
		}, results)

		results, err = List[string](db, &Query{path: "/messages/%", reverseSort: true}, nil)
		require.NoError(t, err)
		require.EqualValues(t, []*PathAndValue[string]{
			{"/messages/d", "Message D"},
			{"/messages/c", "Message C"},
			{"/messages/b", "Message B"},
			{"/messages/a", "Message A"},
		}, results)

		rresults, err := RList[string](db, &Query{path: "/messages/%"}, nil)
		require.NoError(t, err)
		require.EqualValues(t, []*PathAndValue[*Raw[string]]{
			{"/messages/a", unloadedRaw(db.getSerde(), "Message A")},
			{"/messages/b", unloadedRaw(db.getSerde(), "Message B")},
			{"/messages/c", unloadedRaw(db.getSerde(), "Message C")},
			{"/messages/d", unloadedRaw(db.getSerde(), "Message D")},
		}, rresults)
	})
}

func withDB(t *testing.T, fn func(db DB)) {
	file, err := ioutil.TempFile("", "")
	require.NoError(t, err)
	defer panicOnError(os.Remove(file.Name()))
	sl, err := newSQLiteImpl(file.Name())
	require.NoError(t, err)
	db, err := NewDB(sl, "test")
	require.NoError(t, err)
	fn(db)
}

func panicOnError(err error) {
	if err != nil {
		panic(err)
	}
}
