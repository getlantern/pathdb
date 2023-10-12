package minisql

type ScannableRows interface {
	Close() error
	Next() bool
	Scan(args ...interface{}) error
}

type QueryableAPI struct {
	Queryable
}

func (q *QueryableAPI) Exec(query string, args ...interface{}) error {
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
	values := make(valueArray, 0, len(args))
	for _, arg := range args {
		values = append(values, valueFromPointer(arg))
	}
	return sr.Rows.Scan(&valueArrayWrapper{values: values})
}
