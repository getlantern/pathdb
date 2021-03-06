package pathdb

import (
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

var errTest = errors.New("test error")

func TestTransactions(t *testing.T) {

	withDB(t, func(db DB) {
		// put and commit something
		err := Mutate(db, func(tx TX) error {
			require.NoError(t, Put(tx, "path", "hello world", ""))
			didPut, err := PutIfAbsent(tx, "path", "hello overwritten world", "")
			require.NoError(t, err)
			require.False(t, didPut, "should not have put new value for path")
			didPut, err = PutIfAbsent(tx, "path2", "hello other world", "")
			require.NoError(t, err)
			require.True(t, didPut, "should have put value for new path2")
			existing, err := GetOrPut(tx, "path", "hello other overwritten world", "")
			require.NoError(t, err)
			require.EqualValues(t, "hello world", existing, "should have gotten existing value at path")
			return nil
		})
		require.NoError(t, err)

		// make sure it can be read
		require.Equal(t, "hello world", get[string](t, db, "path"))
		require.Equal(t, unloadedRaw(db.getSerde(), "hello world"), rget[string](t, db, "path"))
		require.Equal(t, unloadedRaw(db.getSerde(), "hello other world"), rget[string](t, db, "path2"))

		// delete and rollback something
		err = Mutate(db, func(tx TX) error {
			require.NoError(t, Delete(tx, "path"))
			require.Empty(t, get[string](t, tx, "path"), "delete should be reflected in scope of ongoing transaction")
			return errTest
		})
		require.Equal(t, errTest, err)
		require.Equal(t, "hello world", get[string](t, db, "path"), "delete should have been rolled back")

		// delete through put
		err = Mutate(db, func(tx TX) error {
			require.NoError(t, Put[interface{}](tx, "path", nil, ""))
			require.Empty(t, get[string](t, tx, "path"), "delete should be reflected in scope of ongoing transaction")
			return errTest
		})
		require.Equal(t, errTest, err)
		require.Equal(t, "hello world", get[string](t, db, "path"), "delete should have been rolled back")
	})
}

func TestSubscriptions(t *testing.T) {
	withDB(t, func(db DB) {
		var lastCS *ChangeSet[string]

		Subscribe(db, &Subscription[string]{
			ID:           "s1",
			PathPrefixes: []string{"p%"},
			OnUpdate: func(cs *ChangeSet[string]) error {
				lastCS = cs
				return nil
			},
		})

		// put and commit something
		err := Mutate(db, func(tx TX) error {
			require.NoError(t, Put(tx, "p1", "0", ""), "initial value for p1")
			require.NoError(t, Put(tx, "p1", "1", ""), "update p1")
			require.NoError(t, Put(tx, "p2", "2", ""), "path which will be deleted")
			require.NoError(t, Delete(tx, "p2"))
			require.NoError(t, Put(tx, "p3", "3", ""), "path which will be deleted but later added")
			require.NoError(t, Delete(tx, "p3"))
			require.NoError(t, PutRaw(tx, "p3", unloadedRaw(db.getSerde(), "3"), ""), "path re-added")
			require.NoError(t, Delete(tx, "p4"), "delete non-existent path")
			require.NoError(t, Put(tx, "a1", "1", ""), "add path to which we're not subscribing")
			require.NoError(t, Put(tx, "a2", "2", ""), "add path to which we're not subscribing which we'll delete")
			require.NoError(t, Delete(tx, "a2"), "delete path to which we're not subscribing")
			return nil
		})
		require.NoError(t, err)

		// make sure subscriber was notified
		require.EqualValues(t,
			&ChangeSet[string]{
				Updates: map[string]*Item[*Raw[string]]{
					"p1": {"p1", "", loadedRaw(db.getSerde(), "1")},
					"p3": {"p3", "", unloadedRaw(db.getSerde(), "3")},
				},
				Deletes: map[string]bool{"p2": true, "p4": true},
			}, lastCS)

		// unsubscribe
		lastCS = nil
		Unsubscribe(db, "s1")

		err = Mutate(db, func(tx TX) error {
			require.NoError(t, Put(tx, "p0", "0", ""), "value that should be picked up by any subscriber")
			return nil
		})
		require.NoError(t, err)
		require.Nil(t, lastCS)

		// subscribe and request initial
		Subscribe(db, &Subscription[string]{
			ID:             "s2",
			PathPrefixes:   []string{"p%"},
			ReceiveInitial: true,
			OnUpdate: func(cs *ChangeSet[string]) error {
				lastCS = cs
				return nil
			},
		})
		require.EqualValues(t,
			&ChangeSet[string]{
				Updates: map[string]*Item[*Raw[string]]{
					"p0": {"p0", "", unloadedRaw(db.getSerde(), "0")},
					"p1": {"p1", "", unloadedRaw(db.getSerde(), "1")},
					"p3": {"p3", "", unloadedRaw(db.getSerde(), "3")},
				},
			}, lastCS)
	})
}

func TestSubscribeToInitialDetails(t *testing.T) {
	testDetailSubscription(
		t,
		true,
		func(db DB) *ChangeSet[int64] {
			return &ChangeSet[int64]{
				Updates: map[string]*Item[*Raw[int64]]{
					"/index/1": {"/index/1", "/detail/1", unloadedRaw(db.getSerde(), int64(1))},
					"/index/2": {"/index/2", "/detail/2", unloadedRaw(db.getSerde(), int64(2))},
				},
			}
		},
		func(tx TX) {
			// change nothing
		},
	)
}

func TestDetailSubscriptionModifyDetails(t *testing.T) {
	testDetailSubscription(
		t,
		false,
		func(db DB) *ChangeSet[int64] {
			return &ChangeSet[int64]{
				Updates: map[string]*Item[*Raw[int64]]{
					"/index/1": {"/index/1", "/detail/1", loadedRaw(db.getSerde(), int64(11))},
				},
				Deletes: map[string]bool{"/index/2": true},
			}
		},
		func(tx TX) {
			require.NoError(t, Put(tx, "/detail/1", int64(11), ""))
			require.NoError(t, Delete(tx, "/detail/2"))
		},
	)
}

func TestDetailSubscriptionModifyIndex(t *testing.T) {
	testDetailSubscription(
		t,
		false,
		func(db DB) *ChangeSet[int64] {
			return &ChangeSet[int64]{
				Updates: map[string]*Item[*Raw[int64]]{
					"/index/1": {"/index/1", "/detail/2", unloadedRaw(db.getSerde(), int64(2))},
					"/index/3": {"/index/3", "/detail/3", loadedRaw(db.getSerde(), int64(3))},
				},
				Deletes: map[string]bool{"/index/2": true},
			}
		},
		func(tx TX) {
			require.NoError(t, Put(tx, "/index/1", "/detail/2", ""))
			require.NoError(t, Put(tx, "/detail/3", int64(3), ""))
			require.NoError(t, Delete(tx, "/index/2"))
		},
	)
}

func testDetailSubscription(
	t *testing.T,
	receiveInitial bool,
	expected func(db DB) *ChangeSet[int64],
	update func(tx TX),
) {
	withDB(t, func(db DB) {
		// put some initial values
		err := Mutate(db, func(tx TX) error {
			require.NoError(t, Put(tx, "/detail/1", int64(1), ""))
			require.NoError(t, Put(tx, "/detail/2", int64(2), ""))
			require.NoError(t, Put(tx, "/index/1", "/detail/1", ""))
			require.NoError(t, Put(tx, "/index/2", "/detail/2", ""))
			require.NoError(t, Put(tx, "/index/3", "/detail/3", "index entry to non-existent detail"))
			return nil
		})
		require.NoError(t, err)

		var lastCS *ChangeSet[int64]
		s := &Subscription[int64]{
			ID:             fmt.Sprintf("%d", rand.Int()),
			PathPrefixes:   []string{"/index/%"},
			ReceiveInitial: receiveInitial,
			JoinDetails:    true,
			OnUpdate: func(cs *ChangeSet[int64]) error {
				lastCS = cs
				return nil
			},
		}
		Subscribe(db, s)
		defer Unsubscribe(db, s.ID)
		if update != nil {
			Mutate(db, func(tx TX) error {
				update(tx)
				return nil
			})
		}
		require.EqualValues(t, expected(db), lastCS, "lastCS should equal expected")
	})
}

func TestList(t *testing.T) {
	withDB(t, func(db DB) {
		err := Mutate(db, func(tx TX) error {
			return PutAll(tx, map[string]string{
				"/contacts/32af234asdf324":                         "That Person",
				"/contacts/32af234asdf324/messages_by_timestamp/1": "/messages/c",
				"/contacts/32af234asdf324/messages_by_timestamp/2": "/messages/a",
				"/contacts/32af234asdf324/messages_by_timestamp/3": "/messages/b",
				"/contacts/32af234asdf324/messages_by_timestamp/4": "/messages/e", // this one doesn't exist
				"/messages/c": "Message C",
				"/messages/d": "Message D", // this one isn't referenced by messages_by_timestamp
				"/messages/a": "Message A",
				"/messages/b": "Message B",
			})
		})
		require.NoError(t, err)

		require.EqualValues(t, "That Person", get[string](t, db, "/contacts/32af234asdf324"))

		require.EqualValues(t, []*Item[string]{
			{"/messages/a", "", "Message A"},
			{"/messages/b", "", "Message B"},
			{"/messages/c", "", "Message C"},
			{"/messages/d", "", "Message D"},
		}, list[string](t, db, &QueryParams{path: "/messages/%"}),
			"items should be ordered ascending by path",
		)

		require.EqualValues(t, []*Item[string]{
			{"/messages/d", "", "Message D"},
			{"/messages/c", "", "Message C"},
			{"/messages/b", "", "Message B"},
			{"/messages/a", "", "Message A"},
		}, list[string](t, db, &QueryParams{path: "/messages/%", reverseSort: true}),
			"items should be ordered descending by path",
		)

		require.EqualValues(t, []*Item[*Raw[string]]{
			{"/messages/a", "", unloadedRaw(db.getSerde(), "Message A")},
			{"/messages/b", "", unloadedRaw(db.getSerde(), "Message B")},
			{"/messages/c", "", unloadedRaw(db.getSerde(), "Message C")},
			{"/messages/d", "", unloadedRaw(db.getSerde(), "Message D")},
		}, rlist[string](t, db, &QueryParams{path: "/messages/%"}),
			"should be able to retrieve raw items",
		)

		require.EqualValues(t, []*Item[string]{
			{"/contacts/32af234asdf324/messages_by_timestamp/3", "/messages/b", "Message B"},
			{"/contacts/32af234asdf324/messages_by_timestamp/2", "/messages/a", "Message A"},
			{"/contacts/32af234asdf324/messages_by_timestamp/1", "/messages/c", "Message C"},
		}, list[string](t, db, &QueryParams{
			path:        "/contacts/32af234asdf324/messages_by_timestamp/%",
			start:       0,
			count:       10,
			joinDetails: true,
			reverseSort: true,
		}),
			"wildcard detail query should return the right items",
		)

		require.EqualValues(t, []*Item[string]{
			{"/contacts/32af234asdf324/messages_by_timestamp/2", "/messages/a", "Message A"},
		}, list[string](t, db, &QueryParams{
			path:        "/contacts/32af234asdf324/messages_by_timestamp/2",
			start:       0,
			count:       10,
			joinDetails: true,
			reverseSort: true,
		}),
			"specific detail query should return the right items",
		)

		require.EqualValues(t, []*Item[string]{
			{"/contacts/32af234asdf324/messages_by_timestamp/2", "/messages/a", "Message A"},
		}, list[string](t, db, &QueryParams{
			path:        "/contacts/32af234asdf324/messages_by_timestamp/%",
			start:       1,
			count:       1,
			joinDetails: true,
		}),
			"detail query respects start and count",
		)

		require.EqualValues(t, []string{
			"/messages/b",
		}, listPaths(t, db, &QueryParams{
			path:  "/messages/%",
			start: 1,
			count: 1,
		}),
			"path query respects start and count",
		)
	})
}

func TestSearch(t *testing.T) {
	withDB(t, func(db DB) {
		err := Mutate(db, func(tx TX) error {
			require.NoError(t, Put(tx, "/messages/c", "Message C blah blah", "Message C blah blah"))
			require.NoError(t, Put(tx, "/messages/d", "Message D blah blah blah", "Message D blah blah blah"))
			require.NoError(t, Put(tx, "/messages/a", "Message A blah", "Message A blah"))
			require.NoError(t, Put(tx, "/messages/b", "Message B", "Message B"))
			return PutAll(tx, map[string]string{
				"/linktomessage/1": "/messages/d",
				"/linktomessage/2": "/messages/c",
				"/linktomessage/3": "/messages/b",
				"/linktomessage/4": "/messages/a",
			})
		})
		require.NoError(t, err)

		require.EqualValues(t, []*Item[string]{
			{"/messages/a", "", "Message A blah"},
			{"/messages/b", "", "Message B"},
			{"/messages/c", "", "Message C blah blah"},
			{"/messages/d", "", "Message D blah blah blah"},
		}, list[string](t, db, &QueryParams{path: "/messages/%"}))

		require.EqualValues(t, []*SearchResult[string]{
			{Item[string]{"/messages/d", "", "Message D blah blah blah"}, "...*bla*h *bla*h..."},
			{Item[string]{"/messages/c", "", "Message C blah blah"}, "...*bla*h *bla*h"},
			{Item[string]{"/messages/a", "", "Message A blah"}, "...ge A *bla*h"},
		}, search[string](
			t,
			db,
			&QueryParams{path: "/messages/%"},
			&SearchParams{search: "bla*", numTokens: 7},
		),
			"prefix match with highlighting",
		)

		require.EqualValues(t, []*SearchResult[*Raw[string]]{
			{Item[*Raw[string]]{"/messages/d", "", unloadedRaw(db.getSerde(), "Message D blah blah blah")}, "...*bla*h *bla*h..."},
			{Item[*Raw[string]]{"/messages/c", "", unloadedRaw(db.getSerde(), "Message C blah blah")}, "...*bla*h *bla*h"},
			{Item[*Raw[string]]{"/messages/a", "", unloadedRaw(db.getSerde(), "Message A blah")}, "...ge A *bla*h"},
		}, rsearch[string](
			t,
			db,
			&QueryParams{path: "/messages/%"},
			&SearchParams{search: "bla*", numTokens: 7},
		),
			"raw prefix match with highlighting",
		)

		require.EqualValues(t, []*SearchResult[string]{
			{Item[string]{"/linktomessage/1", "/messages/d", "Message D blah blah blah"}, "...*bla*h *bla*h..."},
			{Item[string]{"/linktomessage/2", "/messages/c", "Message C blah blah"}, "...*bla*h *bla*h"},
			{Item[string]{"/linktomessage/4", "/messages/a", "Message A blah"}, "...ge A *bla*h"},
		}, search[string](
			t,
			db,
			&QueryParams{path: "/linktomessage/%", joinDetails: true},
			&SearchParams{search: "bla*", numTokens: 7},
		),
			"prefix match with joinDetails with highlighting",
		)

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

		require.EqualValues(t, []*SearchResult[string]{
			{Item[string]{"/messages/a", "", "Message A blah"}, "...*bla*..."},
		}, search[string](
			t,
			db,
			&QueryParams{path: "/messages/%"},
			&SearchParams{search: "blah", numTokens: 1},
		),
			"results should exclude deleted rows and deleted fulltext",
		)

		// now update
		err = Mutate(db, func(tx TX) error {
			require.NoError(t, Put(tx, "/messages/a", "Message A is different now", "Message A is different now"))
			return nil
		})
		require.NoError(t, err)

		require.Empty(t, search[string](
			t,
			db,
			&QueryParams{path: "/messages/%"},
			&SearchParams{search: "blah"},
		),
			"results exclude updated fulltext",
		)

		require.EqualValues(t, []*SearchResult[string]{
			{Item[string]{"/messages/a", "", "Message A is different now"}, "Message A is *diff*erent now"},
		}, search[string](
			t,
			db,
			&QueryParams{path: "/messages/%"},
			&SearchParams{search: "diff"},
		),
			"results include updated fulltext",
		)
	})
}

func TestSearchChinese(t *testing.T) {
	withDB(t, func(db DB) {
		err := Mutate(db, func(tx TX) error {
			require.NoError(t, Put(
				tx,
				"/item",
				"???????????????2022???????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????",
				"???????????????2022???????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????",
			))
			return nil
		})
		require.NoError(t, err)

		require.EqualValues(t, []*SearchResult[string]{
			{Item[string]{
				"/item",
				"",
				"???????????????2022???????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????"},
				"...22*????????????*?????????...",
			},
		}, search[string](
			t,
			db,
			&QueryParams{path: "%"},
			&SearchParams{search: "????????????", numTokens: 7},
		),
			"match ???????????? (winter olympics)  in larger sentence",
		)
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

func get[T any](t *testing.T, q Queryable, path string) T {
	result, err := Get[T](q, path)
	require.NoError(t, err)
	return result
}

func rget[T any](t *testing.T, q Queryable, path string) *Raw[T] {
	result, err := RGet[T](q, path)
	require.NoError(t, err)
	return result
}

func list[T any](t *testing.T, q Queryable, query *QueryParams) []*Item[T] {
	result, err := List[T](q, query)
	require.NoError(t, err)
	return result
}

func listPaths(t *testing.T, q Queryable, query *QueryParams) []string {
	result, err := ListPaths(q, query)
	require.NoError(t, err)
	return result
}

func rlist[T any](t *testing.T, q Queryable, query *QueryParams) []*Item[*Raw[T]] {
	result, err := RList[T](q, query)
	require.NoError(t, err)
	return result
}

func search[T any](t *testing.T, q Queryable, query *QueryParams, search *SearchParams) []*SearchResult[T] {
	result, err := Search[T](q, query, search)
	require.NoError(t, err)
	return result
}

func rsearch[T any](t *testing.T, q Queryable, query *QueryParams, search *SearchParams) []*SearchResult[*Raw[T]] {
	result, err := RSearch[T](q, query, search)
	require.NoError(t, err)
	return result
}

func panicOnError(err error) {
	if err != nil {
		panic(err)
	}
}
