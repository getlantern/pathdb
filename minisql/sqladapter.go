package minisql

import "database/sql"

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

func (db *DBAdapter) Exec(query string, args Values) (Result, error) {
	return db.DB.Exec(query, argsToParams(args)...)
}

func (db *DBAdapter) Query(query string, args Values) (Rows, error) {
	result, err := db.DB.Query(query, argsToParams(args)...)
	return &rowsAdapter{Rows: result}, err
}

type TxAdapter struct {
	*sql.Tx
}

func (tx *TxAdapter) Exec(query string, args Values) (Result, error) {
	return tx.Tx.Exec(query, argsToParams(args)...)
}

func (tx *TxAdapter) Query(query string, args Values) (Rows, error) {
	result, err := tx.Tx.Query(query, argsToParams(args)...)
	return &rowsAdapter{Rows: result}, err
}

func argsToParams(args Values) []interface{} {
	params := make([]interface{}, 0, args.Len())
	for i := 0; i < args.Len(); i++ {
		params = append(params, args.Get(i).value())
	}
	return params
}

type rowsAdapter struct {
	*sql.Rows
}

func (ra *rowsAdapter) Scan(values Values) error {
	row := make([]interface{}, 0, values.Len())
	for i := 0; i < values.Len(); i++ {
		row = append(row, values.Get(i).pointerValue())
	}
	return ra.Rows.Scan(row...)
}
