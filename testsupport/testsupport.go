package testsupport

import (
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"

	"github.com/stretchr/testify/require"

	"github.com/getlantern/pathdb"
	"github.com/getlantern/pathdb/minisql"
)

var errTest = errors.New("test error")

type TestingT interface {
	Errorf(text string)
	FailNow()
}

func TestTransactions(t TestingT, mdb minisql.DB) {
	withDB(t, mdb, func(db pathdb.DB) {
		// put and commit something
		err := pathdb.Mutate(db, func(tx pathdb.TX) error {
			require.NoError(adapt(t), pathdb.Put(tx, "path", "hello world", ""))
			didPut, err := pathdb.PutIfAbsent(tx, "path", "hello overwritten world", "")
			require.NoError(adapt(t), err)
			require.False(adapt(t), didPut, "should not have put new value for path")
			didPut, err = pathdb.PutIfAbsent(tx, "path2", "hello other world", "")
			require.NoError(adapt(t), err)
			require.True(adapt(t), didPut, "should have put value for new path2")
			existing, err := pathdb.GetOrPut(tx, "path", "hello other overwritten world", "")
			require.NoError(adapt(t), err)
			require.EqualValues(adapt(t), "hello world", existing, "should have gotten existing value at path")
			return nil
		})
		require.NoError(adapt(t), err)

		// make sure it can be read
		require.Equal(adapt(t), "hello world", get[string](t, db, "path"))
		require.Equal(adapt(t), pathdb.UnloadedRaw(db, "hello world"), rget[string](t, db, "path"))
		require.Equal(adapt(t), pathdb.UnloadedRaw(db, "hello other world"), rget[string](t, db, "path2"))

		// delete and rollback something
		err = pathdb.Mutate(db, func(tx pathdb.TX) error {
			require.NoError(adapt(t), pathdb.Delete(tx, "path"))
			require.Empty(adapt(t), get[string](t, tx, "path"), "delete should be reflected in scope of ongoing transaction")
			return errTest
		})
		require.ErrorIs(adapt(t), err, errTest)
		require.Equal(adapt(t), "hello world", get[string](t, db, "path"), "delete should have been rolled back")

		// delete through put
		err = pathdb.Mutate(db, func(tx pathdb.TX) error {
			require.NoError(adapt(t), pathdb.Put[interface{}](tx, "path", nil, ""))
			require.Empty(adapt(t), get[string](t, tx, "path"), "delete should be reflected in scope of ongoing transaction")
			return errTest
		})
		require.ErrorIs(adapt(t), err, errTest)
		require.Equal(adapt(t), "hello world", get[string](t, db, "path"), "delete should have been rolled back")
	})
}

func TestSubscriptions(t TestingT, mdb minisql.DB) {
	withDB(t, mdb, func(db pathdb.DB) {
		var lastCS *pathdb.ChangeSet[string]

		pathdb.Subscribe(db, &pathdb.Subscription[string]{
			ID:           "s1",
			PathPrefixes: []string{"p%"},
			OnUpdate: func(cs *pathdb.ChangeSet[string]) error {
				lastCS = cs
				return nil
			},
		})

		// put and commit something
		err := pathdb.Mutate(db, func(tx pathdb.TX) error {
			require.NoError(adapt(t), pathdb.Put(tx, "p1", "0", ""), "initial value for p1")
			require.NoError(adapt(t), pathdb.Put(tx, "p1", "1", ""), "update p1")
			require.NoError(adapt(t), pathdb.Put(tx, "p2", "2", ""), "path which will be deleted")
			require.NoError(adapt(t), pathdb.Delete(tx, "p2"))
			require.NoError(adapt(t), pathdb.Put(tx, "p3", "3", ""), "path which will be deleted but later added")
			require.NoError(adapt(t), pathdb.Delete(tx, "p3"))
			require.NoError(adapt(t), pathdb.PutRaw(tx, "p3", pathdb.UnloadedRaw(db, "3"), ""), "path re-added")
			require.NoError(adapt(t), pathdb.Delete(tx, "p4"), "delete non-existent path")
			require.NoError(adapt(t), pathdb.Put(tx, "a1", "1", ""), "add path to which we're not subscribing")
			require.NoError(adapt(t), pathdb.Put(tx, "a2", "2", ""), "add path to which we're not subscribing which we'll delete")
			require.NoError(adapt(t), pathdb.Delete(tx, "a2"), "delete path to which we're not subscribing")
			return nil
		})
		require.NoError(adapt(t), err)

		// make sure subscriber was notified
		require.EqualValues(adapt(t),
			&pathdb.ChangeSet[string]{
				Updates: map[string]*pathdb.Item[*pathdb.Raw[string]]{
					"p1": {"p1", "", pathdb.LoadedRaw(db, "1")},
					"p3": {"p3", "", pathdb.UnloadedRaw(db, "3")},
				},
				Deletes: map[string]bool{"p2": true, "p4": true},
			}, lastCS)

		// unsubscribe
		lastCS = nil
		pathdb.Unsubscribe(db, "s1")

		err = pathdb.Mutate(db, func(tx pathdb.TX) error {
			require.NoError(adapt(t), pathdb.Put(tx, "p0", "0", ""), "value that should be picked up by any subscriber")
			return nil
		})
		require.NoError(adapt(t), err)
		require.Nil(adapt(t), lastCS)

		// subscribe and request initial
		pathdb.Subscribe(db, &pathdb.Subscription[string]{
			ID:             "s2",
			PathPrefixes:   []string{"p%"},
			ReceiveInitial: true,
			OnUpdate: func(cs *pathdb.ChangeSet[string]) error {
				lastCS = cs
				return nil
			},
		})
		require.EqualValues(adapt(t),
			&pathdb.ChangeSet[string]{
				Updates: map[string]*pathdb.Item[*pathdb.Raw[string]]{
					"p0": {"p0", "", pathdb.UnloadedRaw(db, "0")},
					"p1": {"p1", "", pathdb.UnloadedRaw(db, "1")},
					"p3": {"p3", "", pathdb.UnloadedRaw(db, "3")},
				},
			}, lastCS)
	})
}

func TestSubscribeToInitialDetails(t TestingT, mdb minisql.DB) {
	TestSubscription(
		t,
		mdb,
		true,
		func(db pathdb.DB) *pathdb.ChangeSet[int64] {
			return &pathdb.ChangeSet[int64]{
				Updates: map[string]*pathdb.Item[*pathdb.Raw[int64]]{
					"/index/1": {"/index/1", "/detail/1", pathdb.UnloadedRaw(db, int64(1))},
					"/index/2": {"/index/2", "/detail/2", pathdb.UnloadedRaw(db, int64(2))},
				},
			}
		},
		func(tx pathdb.TX) {
			// change nothing
		},
	)
}

func TestDetailSubscriptionModifyDetails(t TestingT, mdb minisql.DB) {
	TestSubscription(
		t,
		mdb,
		false,
		func(db pathdb.DB) *pathdb.ChangeSet[int64] {
			return &pathdb.ChangeSet[int64]{
				Updates: map[string]*pathdb.Item[*pathdb.Raw[int64]]{
					"/index/1": {"/index/1", "/detail/1", pathdb.LoadedRaw(db, int64(11))},
				},
				Deletes: map[string]bool{"/index/2": true},
			}
		},
		func(tx pathdb.TX) {
			require.NoError(adapt(t), pathdb.Put(tx, "/detail/1", int64(11), ""))
			require.NoError(adapt(t), pathdb.Delete(tx, "/detail/2"))
		},
	)
}

func TestDetailSubscriptionModifyIndex(t TestingT, mdb minisql.DB) {
	TestSubscription(
		t,
		mdb,
		false,
		func(db pathdb.DB) *pathdb.ChangeSet[int64] {
			return &pathdb.ChangeSet[int64]{
				Updates: map[string]*pathdb.Item[*pathdb.Raw[int64]]{
					"/index/1": {"/index/1", "/detail/2", pathdb.UnloadedRaw(db, int64(2))},
					"/index/3": {"/index/3", "/detail/3", pathdb.LoadedRaw(db, int64(3))},
				},
				Deletes: map[string]bool{"/index/2": true},
			}
		},
		func(tx pathdb.TX) {
			require.NoError(adapt(t), pathdb.Put(tx, "/index/1", "/detail/2", ""))
			require.NoError(adapt(t), pathdb.Put(tx, "/detail/3", int64(3), ""))
			require.NoError(adapt(t), pathdb.Delete(tx, "/index/2"))
		},
	)
}

func TestSubscription(
	t TestingT,
	mdb minisql.DB,
	receiveInitial bool,
	expected func(db pathdb.DB) *pathdb.ChangeSet[int64],
	update func(tx pathdb.TX),
) {
	withDB(t, mdb, func(db pathdb.DB) {
		// put some initial values
		err := pathdb.Mutate(db, func(tx pathdb.TX) error {
			require.NoError(adapt(t), pathdb.Put(tx, "/detail/1", int64(1), ""))
			require.NoError(adapt(t), pathdb.Put(tx, "/detail/2", int64(2), ""))
			require.NoError(adapt(t), pathdb.Put(tx, "/index/1", "/detail/1", ""))
			require.NoError(adapt(t), pathdb.Put(tx, "/index/2", "/detail/2", ""))
			require.NoError(adapt(t), pathdb.Put(tx, "/index/3", "/detail/3", "index entry to non-existent detail"))
			return nil
		})
		require.NoError(adapt(t), err)

		var lastCS *pathdb.ChangeSet[int64]
		s := &pathdb.Subscription[int64]{
			ID:             fmt.Sprintf("%d", rand.Int()),
			PathPrefixes:   []string{"/index/%"},
			ReceiveInitial: receiveInitial,
			JoinDetails:    true,
			OnUpdate: func(cs *pathdb.ChangeSet[int64]) error {
				lastCS = cs
				return nil
			},
		}
		pathdb.Subscribe(db, s)
		defer pathdb.Unsubscribe(db, s.ID)
		if update != nil {
			pathdb.Mutate(db, func(tx pathdb.TX) error {
				update(tx)
				return nil
			})
		}
		require.EqualValues(adapt(t), expected(db), lastCS, "lastCS should equal expected")
	})
}

func TestList(t TestingT, mdb minisql.DB) {
	withDB(t, mdb, func(db pathdb.DB) {
		err := pathdb.Mutate(db, func(tx pathdb.TX) error {
			return pathdb.PutAll(tx, map[string]string{
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
		require.NoError(adapt(t), err)

		require.EqualValues(adapt(t), "That Person", get[string](t, db, "/contacts/32af234asdf324"))

		require.EqualValues(adapt(t), []*pathdb.Item[string]{
			{"/messages/a", "", "Message A"},
			{"/messages/b", "", "Message B"},
			{"/messages/c", "", "Message C"},
			{"/messages/d", "", "Message D"},
		}, list[string](t, db, &pathdb.QueryParams{Path: "/messages/%"}),
			"items should be ordered ascending by path",
		)

		require.EqualValues(adapt(t), []*pathdb.Item[string]{
			{"/messages/d", "", "Message D"},
			{"/messages/c", "", "Message C"},
			{"/messages/b", "", "Message B"},
			{"/messages/a", "", "Message A"},
		}, list[string](t, db, &pathdb.QueryParams{Path: "/messages/%", ReverseSort: true}),
			"items should be ordered descending by path",
		)

		require.EqualValues(adapt(t), []*pathdb.Item[*pathdb.Raw[string]]{
			{"/messages/a", "", pathdb.UnloadedRaw(db, "Message A")},
			{"/messages/b", "", pathdb.UnloadedRaw(db, "Message B")},
			{"/messages/c", "", pathdb.UnloadedRaw(db, "Message C")},
			{"/messages/d", "", pathdb.UnloadedRaw(db, "Message D")},
		}, rlist[string](t, db, &pathdb.QueryParams{Path: "/messages/%"}),
			"should be able to retrieve raw items",
		)

		require.EqualValues(adapt(t), []*pathdb.Item[string]{
			{"/contacts/32af234asdf324/messages_by_timestamp/3", "/messages/b", "Message B"},
			{"/contacts/32af234asdf324/messages_by_timestamp/2", "/messages/a", "Message A"},
			{"/contacts/32af234asdf324/messages_by_timestamp/1", "/messages/c", "Message C"},
		}, list[string](t, db, &pathdb.QueryParams{
			Path:        "/contacts/32af234asdf324/messages_by_timestamp/%",
			Start:       0,
			Count:       10,
			JoinDetails: true,
			ReverseSort: true,
		}),
			"wildcard detail query should return the right items",
		)

		require.EqualValues(adapt(t), []*pathdb.Item[string]{
			{"/contacts/32af234asdf324/messages_by_timestamp/2", "/messages/a", "Message A"},
		}, list[string](t, db, &pathdb.QueryParams{
			Path:        "/contacts/32af234asdf324/messages_by_timestamp/2",
			Start:       0,
			Count:       10,
			JoinDetails: true,
			ReverseSort: true,
		}),
			"specific detail query should return the right items",
		)

		require.EqualValues(adapt(t), []*pathdb.Item[string]{
			{"/contacts/32af234asdf324/messages_by_timestamp/2", "/messages/a", "Message A"},
		}, list[string](t, db, &pathdb.QueryParams{
			Path:        "/contacts/32af234asdf324/messages_by_timestamp/%",
			Start:       1,
			Count:       1,
			JoinDetails: true,
		}),
			"detail query respects start and count",
		)

		require.EqualValues(adapt(t), []string{
			"/messages/b",
		}, listPaths(t, db, &pathdb.QueryParams{
			Path:  "/messages/%",
			Start: 1,
			Count: 1,
		}),
			"path query respects start and count",
		)
	})
}

func TestSearch(t TestingT, mdb minisql.DB) {
	withDB(t, mdb, func(db pathdb.DB) {
		err := pathdb.Mutate(db, func(tx pathdb.TX) error {
			require.NoError(adapt(t), pathdb.Put(tx, "/messages/c", "Message C blah blah", "Message C blah blah"))
			require.NoError(adapt(t), pathdb.Put(tx, "/messages/d", "Message D blah blah blah", "Message D blah blah blah"))
			require.NoError(adapt(t), pathdb.Put(tx, "/messages/a", "Message A blah", "Message A blah"))
			require.NoError(adapt(t), pathdb.Put(tx, "/messages/b", "Message B", "Message B"))
			return pathdb.PutAll(tx, map[string]string{
				"/linktomessage/1": "/messages/d",
				"/linktomessage/2": "/messages/c",
				"/linktomessage/3": "/messages/b",
				"/linktomessage/4": "/messages/a",
			})
		})
		require.NoError(adapt(t), err)

		require.EqualValues(adapt(t), []*pathdb.Item[string]{
			{"/messages/a", "", "Message A blah"},
			{"/messages/b", "", "Message B"},
			{"/messages/c", "", "Message C blah blah"},
			{"/messages/d", "", "Message D blah blah blah"},
		}, list[string](t, db, &pathdb.QueryParams{Path: "/messages/%"}))

		require.EqualValues(adapt(t), []*pathdb.SearchResult[string]{
			{pathdb.Item[string]{"/messages/d", "", "Message D blah blah blah"}, "...*bla*h *bla*h..."},
			{pathdb.Item[string]{"/messages/c", "", "Message C blah blah"}, "...*bla*h *bla*h"},
			{pathdb.Item[string]{"/messages/a", "", "Message A blah"}, "...ge A *bla*h"},
		}, search[string](
			t,
			db,
			&pathdb.QueryParams{Path: "/messages/%"},
			&pathdb.SearchParams{Search: "bla*", NumTokens: 7},
		),
			"prefix match with highlighting",
		)

		require.EqualValues(adapt(t), []*pathdb.SearchResult[*pathdb.Raw[string]]{
			{pathdb.Item[*pathdb.Raw[string]]{"/messages/d", "", pathdb.UnloadedRaw(db, "Message D blah blah blah")}, "...*bla*h *bla*h..."},
			{pathdb.Item[*pathdb.Raw[string]]{"/messages/c", "", pathdb.UnloadedRaw(db, "Message C blah blah")}, "...*bla*h *bla*h"},
			{pathdb.Item[*pathdb.Raw[string]]{"/messages/a", "", pathdb.UnloadedRaw(db, "Message A blah")}, "...ge A *bla*h"},
		}, rsearch[string](
			t,
			db,
			&pathdb.QueryParams{Path: "/messages/%"},
			&pathdb.SearchParams{Search: "bla*", NumTokens: 7},
		),
			"raw prefix match with highlighting",
		)

		require.EqualValues(adapt(t), []*pathdb.SearchResult[string]{
			{pathdb.Item[string]{"/linktomessage/1", "/messages/d", "Message D blah blah blah"}, "...*bla*h *bla*h..."},
			{pathdb.Item[string]{"/linktomessage/2", "/messages/c", "Message C blah blah"}, "...*bla*h *bla*h"},
			{pathdb.Item[string]{"/linktomessage/4", "/messages/a", "Message A blah"}, "...ge A *bla*h"},
		}, search[string](
			t,
			db,
			&pathdb.QueryParams{Path: "/linktomessage/%", JoinDetails: true},
			&pathdb.SearchParams{Search: "bla*", NumTokens: 7},
		),
			"prefix match with joinDetails with highlighting",
		)

		err = pathdb.Mutate(db, func(tx pathdb.TX) error {
			// delete an entry including the full text index
			require.NoError(adapt(t), pathdb.Delete(tx, "/messages/d"))
			// add the entry back without full-text indexing to make sure it doesn't show up in results
			require.NoError(adapt(t), pathdb.Put(tx, "/messages/d", "Message D blah blah blah", ""))
			// delete another entry without deleting the full text index
			require.NoError(adapt(t), pathdb.Delete(tx, "/messages/c"))
			return nil
		})
		require.NoError(adapt(t), err)

		require.EqualValues(adapt(t), []*pathdb.SearchResult[string]{
			{pathdb.Item[string]{"/messages/a", "", "Message A blah"}, "...*bla*..."},
		}, search[string](
			t,
			db,
			&pathdb.QueryParams{Path: "/messages/%"},
			&pathdb.SearchParams{Search: "blah", NumTokens: 1},
		),
			"results should exclude deleted rows and deleted fulltext",
		)

		// now update
		err = pathdb.Mutate(db, func(tx pathdb.TX) error {
			require.NoError(adapt(t), pathdb.Put(tx, "/messages/a", "Message A is different now", "Message A is different now"))
			return nil
		})
		require.NoError(adapt(t), err)

		require.Empty(adapt(t), search[string](
			t,
			db,
			&pathdb.QueryParams{Path: "/messages/%"},
			&pathdb.SearchParams{Search: "blah"},
		),
			"results exclude updated fulltext",
		)

		require.EqualValues(adapt(t), []*pathdb.SearchResult[string]{
			{pathdb.Item[string]{"/messages/a", "", "Message A is different now"}, "Message A is *diff*erent now"},
		}, search[string](
			t,
			db,
			&pathdb.QueryParams{Path: "/messages/%"},
			&pathdb.SearchParams{Search: "diff"},
		),
			"results include updated fulltext",
		)
	})
}

func TestSearchChinese(t TestingT, mdb minisql.DB) {
	withDB(t, mdb, func(db pathdb.DB) {
		err := pathdb.Mutate(db, func(tx pathdb.TX) error {
			require.NoError(adapt(t), pathdb.Put(
				tx,
				"/item",
				"当日，北京2022年冬奥会单板滑雪项目男子坡面障碍技巧决赛在张家口云顶滑雪公园举行。苏翊鸣夺得男子坡面障碍技巧银牌。",
				"当日，北京2022年冬奥会单板滑雪项目男子坡面障碍技巧决赛在张家口云顶滑雪公园举行。苏翊鸣夺得男子坡面障碍技巧银牌。",
			))
			return nil
		})
		require.NoError(adapt(t), err)

		require.EqualValues(adapt(t), []*pathdb.SearchResult[string]{
			{pathdb.Item[string]{
				"/item",
				"",
				"当日，北京2022年冬奥会单板滑雪项目男子坡面障碍技巧决赛在张家口云顶滑雪公园举行。苏翊鸣夺得男子坡面障碍技巧银牌。"},
				"...22*年冬奥会*单板滑...",
			},
		}, search[string](
			t,
			db,
			&pathdb.QueryParams{Path: "%"},
			&pathdb.SearchParams{Search: "年冬奥会", NumTokens: 7},
		),
			"match 年冬奥会 (winter olympics)  in larger sentence",
		)
	})
}

func withDB(t TestingT, mdb minisql.DB, fn func(db pathdb.DB)) {
	file, err := ioutil.TempFile("", "")
	require.NoError(adapt(t), err)
	defer panicOnError(os.Remove(file.Name()))
	db, err := pathdb.NewDB(mdb, "test")
	require.NoError(adapt(t), err)
	fn(db)
}

func get[T any](t TestingT, q pathdb.Queryable, path string) T {
	result, err := pathdb.Get[T](q, path)
	require.NoError(adapt(t), err)
	return result
}

func rget[T any](t TestingT, q pathdb.Queryable, path string) *pathdb.Raw[T] {
	result, err := pathdb.RGet[T](q, path)
	require.NoError(adapt(t), err)
	return result
}

func list[T any](t TestingT, q pathdb.Queryable, query *pathdb.QueryParams) []*pathdb.Item[T] {
	result, err := pathdb.List[T](q, query)
	require.NoError(adapt(t), err)
	return result
}

func listPaths(t TestingT, q pathdb.Queryable, query *pathdb.QueryParams) []string {
	result, err := pathdb.ListPaths(q, query)
	require.NoError(adapt(t), err)
	return result
}

func rlist[T any](t TestingT, q pathdb.Queryable, query *pathdb.QueryParams) []*pathdb.Item[*pathdb.Raw[T]] {
	result, err := pathdb.RList[T](q, query)
	require.NoError(adapt(t), err)
	return result
}

func search[T any](t TestingT, q pathdb.Queryable, query *pathdb.QueryParams, search *pathdb.SearchParams) []*pathdb.SearchResult[T] {
	result, err := pathdb.Search[T](q, query, search)
	require.NoError(adapt(t), err)
	return result
}

func rsearch[T any](t TestingT, q pathdb.Queryable, query *pathdb.QueryParams, search *pathdb.SearchParams) []*pathdb.SearchResult[*pathdb.Raw[T]] {
	result, err := pathdb.RSearch[T](q, query, search)
	require.NoError(adapt(t), err)
	return result
}

func panicOnError(err error) {
	if err != nil {
		panic(err)
	}
}

func adapt(t TestingT) require.TestingT {
	return &testingTAdapter{t}
}

type testingTAdapter struct {
	TestingT
}

func (ta *testingTAdapter) Errorf(format string, args ...interface{}) {
	ta.TestingT.Errorf(fmt.Sprintf(format, args...))
}
