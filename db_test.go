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
			require.NoError(t, Put(tx, "path", "hello world", "hello world full text"))
			return nil
		})
		require.NoError(t, err)

		// make sure it can be read
		result, err := Get[string](db, "path")
		require.NoError(t, err)
		require.Equal(t, "hello world", result)

		// put and rollback something
		err = Mutate(db, func(tx TX) error {
			require.NoError(t, Delete(tx, "path"))
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

		results, err := List[string](db, &QueryParams{path: "/messages/%"})
		require.NoError(t, err)
		require.EqualValues(t, []*Item[string]{
			{"/messages/a", "Message A"},
			{"/messages/b", "Message B"},
			{"/messages/c", "Message C"},
			{"/messages/d", "Message D"},
		}, results)

		results, err = List[string](db, &QueryParams{path: "/messages/%", reverseSort: true})
		require.NoError(t, err)
		require.EqualValues(t, []*Item[string]{
			{"/messages/d", "Message D"},
			{"/messages/c", "Message C"},
			{"/messages/b", "Message B"},
			{"/messages/a", "Message A"},
		}, results)

		rresults, err := RList[string](db, &QueryParams{path: "/messages/%"})
		require.NoError(t, err)
		require.EqualValues(t, []*Item[*Raw[string]]{
			{"/messages/a", unloadedRaw(db.getSerde(), "Message A")},
			{"/messages/b", unloadedRaw(db.getSerde(), "Message B")},
			{"/messages/c", unloadedRaw(db.getSerde(), "Message C")},
			{"/messages/d", unloadedRaw(db.getSerde(), "Message D")},
		}, rresults)
	})
}

func TestSearch(t *testing.T) {
	withDB(t, func(db DB) {
		err := Mutate(db, func(tx TX) error {
			require.NoError(t, Put(tx, "/messages/c", "Message C blah blah", "Message C blah blah"))
			require.NoError(t, Put(tx, "/messages/d", "Message D blah blah blah", "Message D blah blah blah"))
			require.NoError(t, Put(tx, "/messages/a", "Message A blah", "Message A blah"))
			require.NoError(t, Put(tx, "/messages/b", "Message B", "Message B"))
			return nil
		})
		require.NoError(t, err)

		results, err := List[string](db, &QueryParams{path: "/messages/%"})
		require.NoError(t, err)
		require.EqualValues(t, []*Item[string]{
			{"/messages/a", "Message A blah"},
			{"/messages/b", "Message B"},
			{"/messages/c", "Message C blah blah"},
			{"/messages/d", "Message D blah blah blah"},
		}, results)

		// test prefix match with highlighting
		searchResults, err := Search[string](
			db,
			&QueryParams{path: "/%"},
			&SearchParams{search: "bla*", numTokens: 7},
		)
		require.NoError(t, err)
		require.EqualValues(t, []*SearchResult[string]{
			{Item[string]{"/messages/d", "Message D blah blah blah"}, "...*bla*h *bla*h..."},
			{Item[string]{"/messages/c", "Message C blah blah"}, "...*bla*h *bla*h"},
			{Item[string]{"/messages/a", "Message A blah"}, "...ge A *bla*h"},
		}, searchResults)

		// now delete
		err = Mutate(db, func(tx TX) error {
			// delete an entry including the full text index
			require.NoError(t, Delete(tx, "/messages/d"))
			// add the entry back without full-text indexing to make sure it doesn't show up in results
			require.NoError(t, Put(tx, "/messages/d", "Message D blah blah blah", ""))
			// delete another entry without deleting the full text index
			require.NoError(t, Delete(tx, "/messages/c"))
			return nil
		})
		require.NoError(t, err)

		searchResults, err = Search[string](
			db,
			&QueryParams{path: "/%"},
			&SearchParams{search: "blah", numTokens: 1},
		)
		require.NoError(t, err)
		require.EqualValues(t, []*SearchResult[string]{
			{Item[string]{"/messages/a", "Message A blah"}, "...*bla*..."},
		}, searchResults)

		// now update
		err = Mutate(db, func(tx TX) error {
			require.NoError(t, Put(tx, "/messages/a", "Message A is different now", "Message A is different now"))
			return nil
		})

		searchResults, err = Search[string](
			db,
			&QueryParams{path: "/%"},
			&SearchParams{search: "blah"},
		)
		require.NoError(t, err)
		require.Empty(t, searchResults)

		searchResults, err = Search[string](
			db,
			&QueryParams{path: "/%"},
			&SearchParams{search: "diff"},
		)
		require.NoError(t, err)
		require.EqualValues(t, []*SearchResult[string]{
			{Item[string]{"/messages/a", "Message A is different now"}, "Message A is *diff*erent now"},
		}, searchResults)
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
