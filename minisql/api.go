package minisql

import "fmt"

type ScannableRows interface {
	Close() error
	Next() bool
	Scan(args ...interface{}) error
}

type QueryableAPI struct {
	Queryable
}

func (q *QueryableAPI) Exec(query string, args ...interface{}) (Result, error) {
	return q.Queryable.Exec(query, NewValues(args))
}

func (q *QueryableAPI) Query(query string, args ...interface{}) (ScannableRows, error) {
	rows, err := q.Queryable.Query(query, NewValues(args))
	if err != nil {
		return nil, err
	}
	return &scannableRows{rows}, nil
}

type DBAPI struct {
	db DB
	*QueryableAPI
}

func Wrap(db DB) *DBAPI {
	return &DBAPI{db: db, QueryableAPI: &QueryableAPI{Queryable: db}}
}

func (db *DBAPI) Begin() (*TxAPI, error) {
	tx, err := db.db.Begin()
	if err != nil {
		return nil, err
	}
	return &TxAPI{tx: tx, QueryableAPI: &QueryableAPI{Queryable: tx}}, nil
}

type TxAPI struct {
	tx Tx
	*QueryableAPI
}

func (tx *TxAPI) Commit() error {
	return tx.tx.Commit()
}

func (tx *TxAPI) Rollback() error {
	return tx.tx.Rollback()
}

type scannableRows struct {
	Rows
}

func (sr *scannableRows) Scan(args ...interface{}) error {
	values := convertToValueSlice(args...)
	return sr.Rows.Scan(&valueArrayWrapper{values: values})
}

func convertToValueSlice(args ...interface{}) []*Value {
	values := make([]*Value, 0, len(args))
	for _, arg := range args {
		switch v := arg.(type) {
		case []byte:
			values = append(values, NewValueBytes(v))
		case string:
			values = append(values, NewValueString(v))
		case int:
			values = append(values, NewValueInt(v))
		default:
			panic(fmt.Errorf("unsupported type provided: %T", v))
		}
	}
	return values
}
