package pathdb

import (
	"errors"
	"fmt"
	"math"

	"github.com/tchap/go-patricia/v2/patricia"

	"github.com/getlantern/golog"
	"github.com/getlantern/pathdb/minisql"
)

var log = golog.LoggerFor("pathdb")

var (
	ErrUnexpectedDBError = errors.New("unexpected database error")
)

type item struct {
	path       string
	detailPath string
	value      []byte
	snippet    string
}

type QueryParams struct {
	Path                string
	Start               int
	Count               int
	ReverseSort         bool
	JoinDetails         bool
	IncludeEmptyDetails bool
}

func (query *QueryParams) ApplyDefaults() {
	if query.Count == 0 {
		query.Count = math.MaxInt32
	}
}

type SearchParams struct {
	Search         string
	HighlightStart string
	HighlightEnd   string
	Ellipses       string
	NumTokens      int
}

func (search *SearchParams) ApplyDefaults() {
	if search.HighlightStart == "" {
		search.HighlightStart = "*"
	}
	if search.HighlightEnd == "" {
		search.HighlightEnd = "*"
	}
	if search.Ellipses == "" {
		search.Ellipses = "..."
	}
	if search.NumTokens <= 0 {
		search.NumTokens = 64
	}
}

type Queryable interface {
	getSerde() *serde
	Get(path string) ([]byte, error)
	List(query *QueryParams, search *SearchParams) ([]*item, error)
}

type DB interface {
	Queryable
	Begin() (TX, error)
	WithSchema(string) DB
	Subscribe(*subscription)
	Unsubscribe(string)
}

type TX interface {
	Queryable
	Put(path string, value interface{}, serializedValue []byte, fullText string, updateIfPresent bool) error
	Delete(path string) error
	Commit() error
	Rollback() error
}

type queryable struct {
	core   *minisql.QueryableAPI
	schema string
	serde  *serde
}

type db struct {
	queryable
	db                        *minisql.DBAPI
	commits                   chan *commit
	subscribes                chan *subscribeRequest
	unsubscribes              chan *unsubscribeRequest
	subscriptionsByPath       patricia.Trie
	detailSubscriptionsByPath patricia.Trie
}

type tx struct {
	queryable
	commits chan *commit
	tx      *minisql.TxAPI
	updates map[string]*Item[*Raw[any]]
	deletes map[string]bool
}

type commit struct {
	t        *tx
	finished chan error
}

func NewDB(core minisql.DB, schema string) (DB, error) {
	_core := minisql.Wrap(core)

	// All data is stored in a single table that has a TEXT path and a BLOB value. The table is
	// stored as an index organized table (WITHOUT ROWID option) as a performance
	// optimization for range scans on the path. To support full text indexing in a separate
	// fts5 table, we include a manually managed INTEGER rowid to which we can join the fts5
	// table. Rows that are not full text indexed leave rowid null to save space.
	_, err := _core.Exec(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s_data (path TEXT PRIMARY KEY, value BLOB, rowid INTEGER) WITHOUT ROWID", schema))
	if err != nil {
		return nil, fmt.Errorf("newdb: create data table: %w", err)
	}

	// Create an index on only text values to speed up detail lookups that join on path = value
	_, err = _core.Exec(fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s_data_value_index ON %s_data(value) WHERE SUBSTR(CAST(value AS TEXT), 1, 1) = 'T'", schema, schema))
	if err != nil {
		return nil, fmt.Errorf("newdb: create data value index: %w", err)
	}

	// Create a table for full text search
	_, err = _core.Exec(fmt.Sprintf("CREATE VIRTUAL TABLE IF NOT EXISTS %s_fts2 USING fts5(value, tokenize='porter trigram')", schema))
	if err != nil {
		return nil, fmt.Errorf("newdb: create search table: %w", err)
	}

	// Create a table for managing custom counters (currently used only for full text indexing)
	_, err = _core.Exec(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s_counters (id INTEGER PRIMARY KEY, value INTEGER)", schema))
	if err != nil {
		return nil, fmt.Errorf("newdb: create counters table: %w", err)
	}

	d := &db{
		queryable: queryable{
			core:   _core.QueryableAPI,
			schema: schema,
			serde:  newSerde(),
		},
		db:                        _core,
		commits:                   make(chan *commit, 100),
		subscribes:                make(chan *subscribeRequest, 100),
		unsubscribes:              make(chan *unsubscribeRequest, 100),
		subscriptionsByPath:       *patricia.NewTrie(),
		detailSubscriptionsByPath: *patricia.NewTrie(),
	}
	go d.mainLoop()
	return d, nil
}

func (d *db) WithSchema(schema string) DB {
	return &db{
		queryable: queryable{
			core:   d.core,
			schema: schema,
			serde:  d.serde,
		},
		db:      d.db,
		commits: d.commits,
	}
}

func (d *db) Begin() (TX, error) {
	_tx, err := d.db.Begin()
	if err != nil {
		return nil, fmt.Errorf("begin: %w", err)
	}

	return &tx{
		queryable: queryable{
			core:   _tx.QueryableAPI,
			schema: d.schema,
			serde:  d.serde,
		},
		tx:      _tx,
		commits: d.commits,
		updates: make(map[string]*Item[*Raw[any]]),
		deletes: make(map[string]bool),
	}, nil
}

func (d *db) mainLoop() {
	for {
		select {
		case commit := <-d.commits:
			d.onCommit(commit)
			commit.finished <- commit.t.doCommit()
		case s := <-d.subscribes:
			d.onNewSubscription(s)
		case id := <-d.unsubscribes:
			d.onDeleteSubscription(id)
		}
	}
}

func (q *queryable) getSerde() *serde {
	return q.serde
}

func (q *queryable) Get(path string) ([]byte, error) {
	serializedPath, err := q.serde.serialize(path)
	if err != nil {
		return nil, fmt.Errorf("get: serialize path: %w", err)
	}
	rows, err := q.core.Query(fmt.Sprintf("SELECT value FROM %s_data WHERE path = ?", q.schema), serializedPath)
	if err != nil {
		return nil, fmt.Errorf("get: query: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, nil
	}
	var b []byte
	err = rows.Scan(&b)
	if err != nil {
		return nil, fmt.Errorf("get: scan: %w", err)
	}
	return b, nil
}

func (q *queryable) List(query *QueryParams, search *SearchParams) ([]*item, error) {
	serializedPath, err := q.serde.serialize(query.Path)
	if err != nil {
		return nil, fmt.Errorf("list: serialize path: %w", err)
	}

	query.ApplyDefaults()
	var rows minisql.ScannableRows
	isSearch := search != nil
	if isSearch {
		search.ApplyDefaults()
		sql := fmt.Sprintf("SELECT d.path, d.value, snippet(%s_fts2, 0, ?, ?, ?, ?) FROM %s_fts2 f INNER JOIN %s_data d ON f.rowid = d.rowid WHERE d.path LIKE ? AND f.value MATCH ? ORDER BY f.rank LIMIT ? OFFSET ?", q.schema, q.schema, q.schema)
		if query.JoinDetails {
			join := "INNER JOIN"
			if query.IncludeEmptyDetails {
				join = "RIGHT OUTER JOIN"
			}
			sql = fmt.Sprintf("SELECT l.path, l.value, d.value, snippet(%s_fts2, 0, ?, ?, ?, ?) FROM %s_fts2 f INNER JOIN %s_data d ON f.rowid = d.rowid %s %s_data l ON l.value = d.path WHERE l.path LIKE ? AND SUBSTR(CAST(l.value AS TEXT), 1, 1) = 'T' AND f.value MATCH ? ORDER BY f.rank LIMIT ? OFFSET ?", q.schema, q.schema, q.schema, join, q.schema)
		}
		rows, err = q.core.Query(
			sql,
			search.HighlightStart,
			search.HighlightEnd,
			search.Ellipses,
			search.NumTokens,
			serializedPath,
			search.Search,
			query.Count,
			query.Start,
		)
	} else {
		sortOrder := "ASC"
		if query.ReverseSort {
			sortOrder = "DESC"
		}
		sql := fmt.Sprintf("SELECT path, value FROM %s_data WHERE path LIKE ? ORDER BY path %s LIMIT ? OFFSET ?", q.schema, sortOrder)
		if query.JoinDetails {
			join := "INNER JOIN"
			if query.IncludeEmptyDetails {
				join = "LEFT OUTER JOIN"
			}
			sql = fmt.Sprintf("SELECT l.path, l.value, d.value FROM %s_data l %s %s_data d ON l.value = d.path WHERE l.path LIKE ? AND SUBSTR(CAST(l.value AS TEXT), 1, 1) = 'T' ORDER BY l.path %s LIMIT ? OFFSET ?", q.schema, join, q.schema, sortOrder)
		}
		rows, err = q.core.Query(
			sql,
			serializedPath,
			query.Count,
			query.Start,
		)
	}
	if err != nil {
		return nil, fmt.Errorf("list: query: %w", err)
	}

	defer rows.Close()
	items := make([]*item, 0, 100)
	for rows.Next() {
		item := &item{}
		var _path []byte
		var _detailPath []byte
		if isSearch {
			if query.JoinDetails {
				err = rows.Scan(&_path, &_detailPath, &item.value, &item.snippet)
			} else {
				err = rows.Scan(&_path, &item.value, &item.snippet)
			}
		} else {
			if query.JoinDetails {
				err = rows.Scan(&_path, &_detailPath, &item.value)
			} else {
				err = rows.Scan(&_path, &item.value)
			}
		}
		if err != nil {
			return nil, fmt.Errorf("list: scan: %w", err)
		}
		path, err := q.serde.deserialize(_path)
		if err != nil {
			return nil, fmt.Errorf("list: deserialize path: %w", err)
		}
		item.path = path.(string)
		if _detailPath != nil {
			detailPath, err := q.serde.deserialize(_detailPath)
			if err != nil {
				return nil, fmt.Errorf("list: deserialize detail path: %w", err)
			}
			item.detailPath = detailPath.(string)
		}
		items = append(items, item)
	}

	return items, nil
}

func (t *tx) Put(path string, value interface{}, serializedValue []byte, fullText string, updateIfPresent bool) error {
	if value == nil && serializedValue == nil {
		err := t.Delete(path)
		if err != nil {
			return fmt.Errorf("put: delete: %w", err)
		}
		return nil
	}

	serializedPath, err := t.serde.serialize(path)
	if err != nil {
		return fmt.Errorf("put: serialize path: %w", err)
	}
	if serializedValue == nil && value != nil {
		serializedValue, err = t.serde.serialize(value)
		if err != nil {
			return fmt.Errorf("put: serialize value: %w", err)
		}
	}

	saveUpdate := func() {
		delete(t.deletes, path)
		t.updates[path] = &Item[*Raw[any]]{
			Path: path,
			Value: &Raw[any]{
				serde:  t.serde,
				Bytes:  serializedValue,
				loaded: value != nil,
				value:  value,
			},
		}
	}

	onConflictClause := ""
	if updateIfPresent {
		onConflictClause = " ON CONFLICT(path) DO UPDATE SET value = EXCLUDED.value"
	}
	if fullText == "" {
		// not doing full text, simple path
		_, err = t.tx.Exec(fmt.Sprintf("INSERT INTO %s_data(path, value) VALUES(?, ?)%s", t.schema, onConflictClause), serializedPath, serializedValue)
		if err != nil {
			return fmt.Errorf("put: insert: %w", err)
		}
		saveUpdate()
		return nil
	}

	// get existing row ID for full text indexing
	existingRowID := -1
	isUpdate := false
	rows, err := t.tx.Query(fmt.Sprintf("SELECT rowid FROM %s_data WHERE path = ?", t.schema), serializedPath)
	if err != nil {
		return fmt.Errorf("put: select rowid: %w", err)
	}
	defer rows.Close()
	if rows.Next() {
		// record already exists, update index
		err = rows.Scan(&existingRowID)
		if err != nil {
			return fmt.Errorf("put: scan rowid: %w", err)
		}
		isUpdate = true
	}

	// get next row ID for full text indexing
	rowID := existingRowID
	if !isUpdate {
		// we're inserting a new row, get the next rowID from the sequence
		_, err = t.tx.Exec(fmt.Sprintf("INSERT INTO %s_counters(id, value) VALUES(0, 0) ON CONFLICT(id) DO UPDATE SET value = value+1", t.schema))
		if err != nil {
			return fmt.Errorf("put: increment sequence: %w", err)
		}
		rows, err = t.tx.Query(fmt.Sprintf("SELECT value FROM %s_counters WHERE id = 0", t.schema))
		if err != nil {
			return fmt.Errorf("put: query sequence value: %w", err)
		}
		defer rows.Close()
		if !rows.Next() {
			return fmt.Errorf("put: read sequence value: %w", ErrUnexpectedDBError)
		}
		err = rows.Scan(&rowID)
		if err != nil {
			return fmt.Errorf("put: scan sequence value: %w", err)
		}
	}

	// insert value
	_, err = t.tx.Exec(fmt.Sprintf("INSERT INTO %s_data(path, value, rowid) VALUES(?, ?, ?)%s", t.schema, onConflictClause), serializedPath, serializedValue, rowID)
	if err != nil {
		return fmt.Errorf("put: insert indexed value: %w", err)
	}

	// maintain full text index
	if !isUpdate {
		_, err = t.tx.Exec(fmt.Sprintf("INSERT INTO %s_fts2(value, rowid) VALUES(?, ?)", t.schema), fullText, rowID)
		if err != nil {
			return fmt.Errorf("put: insert into fts index: %w", err)
		}
		return nil
	}
	_, err = t.tx.Exec(fmt.Sprintf("UPDATE %s_fts2 SET value = ? where rowid = ?", t.schema), fullText, rowID)
	if err != nil {
		return fmt.Errorf("put: update fts index: %w", err)
	}
	saveUpdate()
	return nil
}

func (t *tx) Delete(path string) error {
	serializedPath, err := t.serde.serialize(path)
	if err != nil {
		return fmt.Errorf("delete: serialize path: %w", err)
	}
	_, err = t.tx.Exec(fmt.Sprintf("DELETE FROM %s_data WHERE path = ?", t.schema), serializedPath)
	if err != nil {
		return fmt.Errorf("delete: delete: %w", err)
	}
	delete(t.updates, path)
	t.deletes[path] = true
	return nil
}

func (t *tx) Rollback() error {
	return t.tx.Rollback()
}

func (t *tx) Commit() error {
	// perform commit in mainLoop to avoid race conditions with registering listeners
	commit := &commit{
		t:        t,
		finished: make(chan error),
	}
	t.commits <- commit
	return <-commit.finished
}

func (t *tx) doCommit() error {
	return t.tx.Commit()
}
