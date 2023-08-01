// Package minisql defines the subset of interfaces from database/sql that's needed to support pathdb.
// The interfaces are optimized for use with gomobile.
package minisql

type Rows interface {
	Close() error
	Next() bool
	Scan(values Values) error
}

// Result is a duplicate of sql.Result to allow binding with gomobile
type Result interface {
	// LastInsertId returns the integer generated by the database
	// in response to a command. Typically this will be from an
	// "auto increment" column when inserting a new row. Not all
	// databases support this feature, and the syntax of such
	// statements varies.
	LastInsertId() (int64, error)

	// RowsAffected returns the number of rows affected by an
	// update, insert, or delete. Not every database or database
	// driver may support this.
	RowsAffected() (int64, error)
}

type Queryable interface {
	Exec(query string, args Values) (Result, error)
	Query(query string, args Values) (Rows, error)
}

type DB interface {
	Exec(query string, args Values) (Result, error)
	Query(query string, args Values) (Rows, error)
	Begin() (Tx, error)
	Close() error
}

type Tx interface {
	Exec(query string, args Values) (Result, error)
	Query(query string, args Values) (Rows, error)
	Commit() error
	Rollback() error
}
