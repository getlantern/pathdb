package pathdb

import (
	"errors"
	"fmt"
	"math"

	"github.com/getlantern/pathdb/minisql"
)

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
	path        string
	start       int
	count       int
	reverseSort bool
	joinDetails bool
}

func (query *QueryParams) ApplyDefaults() {
	if query.count == 0 {
		query.count = math.MaxInt32
	}
}

type SearchParams struct {
	search         string
	highlightStart string
	highlightEnd   string
	ellipses       string
	numTokens      int
}

func (search *SearchParams) ApplyDefaults() {
	if search.highlightStart == "" {
		search.highlightStart = "*"
	}
	if search.highlightEnd == "" {
		search.highlightEnd = "*"
	}
	if search.ellipses == "" {
		search.ellipses = "..."
	}
	if search.numTokens <= 0 {
		search.numTokens = 64
	}
}

type Queryable interface {
	getSerde() *serde
	get(path string) ([]byte, error)
	list(query *QueryParams, search *SearchParams) ([]*item, error)
}

type DB interface {
	Queryable
	begin() (TX, error)
	withSchema(string) DB
}

type TX interface {
	Queryable
	put(path string, value interface{}, fullText string, updateIfPresent bool) error
	delete(path string) error
	commit() error
	rollback() error
}

type queryable struct {
	core   minisql.Queryable
	schema string
	serde  *serde
}

type db struct {
	queryable
	db      minisql.DB
	commits chan *commit
}

type tx struct {
	queryable
	commits chan *commit
	tx      minisql.Tx
}

type commit struct {
	t        *tx
	finished chan error
}

func NewDB(core minisql.DB, schema string) (*db, error) {
	// All data is stored in a single table that has a TEXT path and a BLOB value. The table is
	// stored as an index organized table (WITHOUT ROWID option) as a performance
	// optimization for range scans on the path. To support full text indexing in a separate
	// fts5 table, we include a manually managed INTEGER rowid to which we can join the fts5
	// table. Rows that are not full text indexed leave rowid null to save space.
	_, err := core.Exec(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s_data (path TEXT PRIMARY KEY, value BLOB, rowid INTEGER) WITHOUT ROWID", schema))
	if err != nil {
		return nil, err
	}

	// Create an index on only text values to speed up detail lookups that join on path = value
	_, err = core.Exec(fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s_data_value_index ON %s_data(value) WHERE SUBSTR(CAST(value AS TEXT), 1, 1) = 'T'", schema, schema))
	if err != nil {
		return nil, err
	}

	// Create a table for full text search
	_, err = core.Exec(fmt.Sprintf("CREATE VIRTUAL TABLE IF NOT EXISTS %s_fts2 USING fts5(value, tokenize='porter trigram')", schema))
	if err != nil {
		return nil, err
	}

	// Create a table for managing custom counters (currently used only for full text indexing)
	_, err = core.Exec(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s_counters (id INTEGER PRIMARY KEY, value INTEGER)", schema))
	if err != nil {
		return nil, err
	}

	d := &db{
		queryable: queryable{
			core:   core,
			schema: schema,
			serde:  newSerde(),
		},
		db:      core,
		commits: make(chan *commit, 100),
	}
	go d.mainLoop()
	return d, nil
}

func (d *db) withSchema(schema string) DB {
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

func (d *db) begin() (TX, error) {
	_tx, err := d.db.Begin()
	if err != nil {
		return nil, err
	}

	return &tx{
		queryable: queryable{
			core:   _tx,
			schema: d.schema,
			serde:  d.serde,
		},
		tx:      _tx,
		commits: d.commits,
	}, nil
}

func (d *db) mainLoop() {
	for {
		select {
		case commit := <-d.commits:
			commit.finished <- commit.t.doCommit()
		}
	}
}

func (q *queryable) getSerde() *serde {
	return q.serde
}

func (q *queryable) get(path string) ([]byte, error) {
	serializedPath, err := q.serde.serialize(path)
	if err != nil {
		return nil, err
	}
	rows, err := q.core.Query(fmt.Sprintf("SELECT value FROM %s_data WHERE path = ?", q.schema), serializedPath)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, nil
	}
	var b []byte
	err = rows.Scan(&b)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func (q *queryable) list(query *QueryParams, search *SearchParams) ([]*item, error) {
	serializedPath, err := q.serde.serialize(query.path)
	if err != nil {
		return nil, err
	}

	query.ApplyDefaults()
	var rows minisql.Rows
	isSearch := search != nil
	if isSearch {
		search.ApplyDefaults()
		sql := fmt.Sprintf("SELECT d.path, d.value, snippet(%s_fts2, 0, ?, ?, ?, ?) FROM %s_fts2 f INNER JOIN %s_data d ON f.rowid = d.rowid WHERE d.path LIKE ? AND f.value MATCH ? ORDER BY f.rank LIMIT ? OFFSET ?", q.schema, q.schema, q.schema)
		if query.joinDetails {
			sql = fmt.Sprintf("SELECT l.path, d.path, d.value, snippet(%s_fts2, 0, ?, ?, ?, ?) FROM %s_fts2 f INNER JOIN %s_data d ON f.rowid = d.rowid INNER JOIN %s_data l ON l.value = d.path WHERE l.path LIKE ? AND SUBSTR(CAST(l.value AS TEXT), 1, 1) = 'T' AND f.value MATCH ? ORDER BY f.rank LIMIT ? OFFSET ?", q.schema, q.schema, q.schema, q.schema)
		}
		rows, err = q.core.Query(
			sql,
			search.highlightStart,
			search.highlightEnd,
			search.ellipses,
			search.numTokens,
			serializedPath,
			search.search,
			query.count,
			query.start,
		)
	} else {
		sortOrder := "ASC"
		if query.reverseSort {
			sortOrder = "DESC"
		}
		sql := fmt.Sprintf("SELECT path, value FROM %s_data WHERE path LIKE ? ORDER BY path %s LIMIT ? OFFSET ?", q.schema, sortOrder)
		if query.joinDetails {
			sql = fmt.Sprintf("SELECT l.path, d.path, d.value FROM %s_data l INNER JOIN %s_data d ON l.value = d.path WHERE l.path LIKE ? AND SUBSTR(CAST(l.value AS TEXT), 1, 1) = 'T' ORDER BY l.path %s LIMIT ? OFFSET ?", q.schema, q.schema, sortOrder)
		}
		rows, err = q.core.Query(
			sql,
			serializedPath,
			query.count,
			query.start,
		)
	}
	if err != nil {
		return nil, err
	}

	defer rows.Close()
	items := make([]*item, 0, 100)
	for rows.Next() {
		item := &item{}
		var _path []byte
		var _detailPath []byte
		if isSearch {
			if query.joinDetails {
				err = rows.Scan(&_path, &_detailPath, &item.value, &item.snippet)
			} else {
				err = rows.Scan(&_path, &item.value, &item.snippet)
			}
		} else {
			if query.joinDetails {
				err = rows.Scan(&_path, &_detailPath, &item.value)
			} else {
				err = rows.Scan(&_path, &item.value)
			}
		}
		if err != nil {
			return nil, err
		}
		path, err := q.serde.deserialize(_path)
		if err != nil {
			return nil, err
		}
		item.path = path.(string)
		if _detailPath != nil {
			detailPath, err := q.serde.deserialize(_detailPath)
			if err != nil {
				return nil, err
			}
			item.detailPath = detailPath.(string)
		}
		items = append(items, item)
	}

	return items, err
}

func (t *tx) put(path string, value interface{}, fullText string, updateIfPresent bool) error {
	serializedPath, err := t.serde.serialize(path)
	if err != nil {
		return err
	}
	serializedValue, err := t.serde.serialize(value)
	if err != nil {
		return err
	}

	onConflictClause := ""
	if updateIfPresent {
		onConflictClause = " ON CONFLICT(path) DO UPDATE SET value = EXCLUDED.value"
	}
	if fullText == "" {
		// not doing full text, simple path
		_, err = t.tx.Exec(fmt.Sprintf("INSERT INTO %s_data(path, value) VALUES(?, ?)%s", t.schema, onConflictClause), serializedPath, serializedValue)
		return err
	}

	// get existing row ID for full text indexing
	existingRowID := -1
	isUpdate := false
	rows, err := t.tx.Query(fmt.Sprintf("SELECT rowid FROM %s_data WHERE path = ?", t.schema), serializedPath)
	if err != nil {
		return err
	}
	defer rows.Close()
	if rows.Next() {
		// record already exists, update index
		err = rows.Scan(&existingRowID)
		if err != nil {
			return err
		}
		isUpdate = true
	}

	// get next row ID for full text indexing
	rowID := existingRowID
	if !isUpdate {
		// we're inserting a new row, get the next rowID from the sequence
		_, err = t.tx.Exec(fmt.Sprintf("INSERT INTO %s_counters(id, value) VALUES(0, 0) ON CONFLICT(id) DO UPDATE SET value = value+1", t.schema))
		if err != nil {
			return err
		}
		rows, err = t.tx.Query(fmt.Sprintf("SELECT value FROM %s_counters WHERE id = 0", t.schema))
		if err != nil {
			return err
		}
		defer rows.Close()
		if !rows.Next() {
			return ErrUnexpectedDBError
		}
		err = rows.Scan(&rowID)
		if err != nil {
			return err
		}
	}

	// insert value
	_, err = t.tx.Exec(fmt.Sprintf("INSERT INTO %s_data(path, value, rowid) VALUES(?, ?, ?)%s", t.schema, onConflictClause), serializedPath, serializedValue, rowID)
	if err != nil {
		return err
	}

	// maintain full text index
	if !isUpdate {
		_, err = t.tx.Exec(fmt.Sprintf("INSERT INTO %s_fts2(value, rowid) VALUES(?, ?)", t.schema), fullText, rowID)
		return err
	}
	_, err = t.tx.Exec(fmt.Sprintf("UPDATE %s_fts2 SET value = ? where rowid = ?", t.schema), fullText, rowID)
	return err
}

func (t *tx) delete(path string) error {
	serializedPath, err := t.serde.serialize(path)
	if err != nil {
		return err
	}
	_, err = t.tx.Exec(fmt.Sprintf("DELETE FROM %s_data WHERE path = ?", t.schema), serializedPath)
	return err
}

func (t *tx) rollback() error {
	return t.tx.Rollback()
}

func (t *tx) commit() error {
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
