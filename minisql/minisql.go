// Package minisql defines the subset of interfaces from database/sql that's needed to support pathdb.
// The interfaces are optimized for use with gomobile.
package minisql

type Rows interface {
	Close() error
	Next() bool
	Scan(values Values) error
}

type Queryable interface {
	Exec(query string, args Values) error
	Query(query string, args Values) (Rows, error)
}

type DB interface {
	Exec(query string, args Values) error
	Query(query string, args Values) (Rows, error)
	Begin() (Tx, error)
	Close() error
}

type Tx interface {
	Exec(query string, args Values) error
	Query(query string, args Values) (Rows, error)
	Commit() error
	Rollback() error
}
