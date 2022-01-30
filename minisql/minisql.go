// package minisql defines the subset of interfaces from database/sql that's needed to support pathdb
package minisql

import (
	"database/sql"
)

type Rows interface {
	Close() error
	Next() bool
	Scan(dest ...interface{}) error
}

type Queryable interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...interface{}) (Rows, error)
}

type DB interface {
	Close() error
	Queryable
	Begin() (Tx, error)
}

type Tx interface {
	Queryable
	Commit() error
	Rollback() error
}

type DBAdapter struct {
	*sql.DB
}

func (db *DBAdapter) Begin() (Tx, error) {
	tx, err := db.DB.Begin()
	if err != nil {
		return nil, err
	}
	return &TxAdapter{tx}, nil
}

func (db *DBAdapter) Query(query string, args ...interface{}) (Rows, error) {
	return db.DB.Query(query, args...)
}

type TxAdapter struct {
	*sql.Tx
}

func (tx *TxAdapter) Exec(query string, args ...interface{}) (sql.Result, error) {
	return tx.Tx.Exec(query, args...)
}

func (tx *TxAdapter) Query(query string, args ...interface{}) (Rows, error) {
	return tx.Tx.Query(query, args...)
}
