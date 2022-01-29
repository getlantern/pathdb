package pathdb

import (
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/getlantern/pathdb/minisql"
)

var (
	ErrUnexpectedDBError = errors.New("unexpected database error")
)

type Queryable struct {
	core   minisql.Queryable
	schema string
	serde  *serde
}

func (q *Queryable) Get(path string) (interface{}, error) {
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
	return q.serde.deserialize(b)
}

type Query struct {
	path        string
	start       int
	count       int
	reverseSort bool
}

type Transactable struct {
	*Queryable
	core          minisql.DB
	nextSavepoint *uint64
	commits       chan *commit
}

type DB struct {
	*Transactable
}

func NewDB(core minisql.DB, schema string) (*DB, error) {
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

	nextSavepoint := uint64(0)
	db := &DB{
		Transactable: &Transactable{
			Queryable: &Queryable{
				core:   core,
				schema: schema,
				serde:  newSerde(),
			},
			core:          core,
			nextSavepoint: &nextSavepoint,
			commits:       make(chan *commit, 100),
		},
	}
	go db.mainLoop()
	return db, nil
}

func (db *DB) mainLoop() {
	for {
		select {
		case commit := <-db.commits:
			commit.finished <- commit.tx.commit()
		}
	}
}

func (db *DB) WithSchema(schema string) *Transactable {
	return &Transactable{
		Queryable: &Queryable{
			core:   db.core,
			schema: schema,
			serde:  db.serde,
		},
		core:          db.core,
		nextSavepoint: db.nextSavepoint,
		commits:       db.commits,
	}
}

type commit struct {
	tx       *Tx
	finished chan error
}

type Tx struct {
	*Queryable
	coreTx    minisql.Tx
	savepoint string
	commits   chan *commit
}

func (tr *Transactable) Begin() (*Tx, error) {
	tx, err := tr.core.Begin()
	if err != nil {
		return nil, err
	}

	return &Tx{
		Queryable: &Queryable{
			core:   tx,
			schema: tr.schema,
			serde:  tr.serde,
		},
		coreTx:    tx,
		savepoint: fmt.Sprintf("save_%d", atomic.AddUint64(tr.nextSavepoint, 1)),
		commits:   tr.commits,
	}, nil
}

func (tx *Tx) Put(path string, value interface{}, fullText string) error {
	return tx.doPut(path, value, fullText, true)
}

func (tx *Tx) PutIfAbsent(path string, value interface{}, fullText string) error {
	return tx.doPut(path, value, fullText, false)
}

func (tx *Tx) GetOrPut(path string, value interface{}, fullText string) (result interface{}, err error) {
	result, err = tx.Get(path)
	if err != nil {
		return
	}
	if result != nil {
		return
	}
	result = value
	err = tx.Put(path, value, fullText)
	return
}

func (tx *Tx) doPut(path string, value interface{}, fullText string, updateIfPresent bool) error {
	serializedPath, err := tx.serde.serialize(path)
	if err != nil {
		return err
	}
	serializedValue, err := tx.serde.serialize(value)
	if err != nil {
		return err
	}

	onConflictClause := ""
	if updateIfPresent {
		onConflictClause = " ON CONFLICT(path) DO UPDATE SET value = EXCLUDED.value"
	}
	_, err = tx.coreTx.Exec(fmt.Sprintf("INSERT INTO %s_counters(id, value) VALUES(0, 0) ON CONFLICT(id) DO UPDATE SET value = value+1", tx.schema))
	if err != nil {
		return err
	}

	if fullText == "" {
		// not doing full text, simple path
		_, err = tx.coreTx.Exec(fmt.Sprintf("INSERT INTO %s_data(path, value) VALUES(?, ?)%s", tx.schema, onConflictClause), serializedPath, serializedValue)
		return err
	}

	// get existing row ID for full text indexing
	existingRowID := -1
	isUpdate := false
	rows, err := tx.coreTx.Query(fmt.Sprintf("SELECT rowid FROM %s_data WHERE path = ?", tx.schema), serializedPath)
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
		rows, err = tx.coreTx.Query(fmt.Sprintf("SELECT value FROM %s_counters WHERE id = 0", tx.schema))
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
	_, err = tx.coreTx.Exec(fmt.Sprintf("INSERT INTO %s_data(path, value, rowid) VALUES(?, ?, ?)%s", tx.schema, onConflictClause), serializedPath, serializedValue, rowID)
	if err != nil {
		return err
	}

	// maintain full text index
	if isUpdate {
		_, err = tx.coreTx.Exec(fmt.Sprintf("INSERT INTO %s_fts2(value, rowid)", tx.schema), fullText, rowID)
		return err
	}
	_, err = tx.coreTx.Exec(fmt.Sprintf("UPDATE %s_fts2 SET value = ? where rowid = ?", tx.schema), fullText, rowID)
	return err
}

func (tx *Tx) Delete(path string) error {
	serializedPath, err := tx.serde.serialize(path)
	if err != nil {
		return err
	}
	_, err = tx.core.Exec(fmt.Sprintf("DELETE FROM %s_data WHERE path = ?", tx.schema), serializedPath)
	return err
}

func (tx *Tx) Rollback() error {
	return tx.coreTx.Rollback()
}

func (tx *Tx) Commit() error {
	// perform commit in mainLoop to avoid race conditions with registering listeners
	commit := &commit{
		tx:       tx,
		finished: make(chan error),
	}
	tx.commits <- commit
	return <-commit.finished
}

func (tx *Tx) commit() error {
	return tx.coreTx.Commit()
}
