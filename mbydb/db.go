package mbydb

import (
	"database/sql"
	"fmt"
	"os"
)

func execQuery(st *sql.Stmt, resultCh chan []interface{}) error {

	rows, err := st.Query()
	if err != nil {
		return err
	}
	defer rows.Close()

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	// Make a slice for the values
	values := make([]interface{}, len(columns))

	// rows.Scan wants '[]interface{}' as an argument, so we must copy the
	// references into such a slice
	// See http://code.google.com/p/go-wiki/wiki/InterfaceSlice for details
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			return err
		}

		newSlice := make([]interface{}, len(columns))
		copy(newSlice, values)
		resultCh <- newSlice
	}

	return rows.Err()
}

func TryQuery(db *sql.DB, query string, resultCh chan []interface{}) error {
	defer close(resultCh)

	st, err := db.Prepare(query)
	if err != nil {
		return err
	}
	defer st.Close()

	err = execQuery(st, resultCh)
	if err != nil {
		return err
	}

	return nil
}

func TrySingleQuery(dbCreds [2]string, query string, resultCh chan []interface{}) error {
	db, err := sql.Open(dbCreds[0], dbCreds[1])
	if err != nil {
		return err
	}
	defer db.Close()

	return TryQuery(db, query, resultCh)
}

func ExecQuery(db *sql.DB, query string, resultCh chan []interface{}) {
	err := TryQuery(db, query, resultCh)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
}

func ExecSingleQuery(dbCreds [2]string, query string, resultCh chan []interface{}) {
	err := TrySingleQuery(dbCreds, query, resultCh)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
}

func DisplayResult(name string, resultCh chan []interface{}) {
	for result := range resultCh {
		fmt.Printf("Result(%v): %v\n", name, result)
		for _, item := range result {
			fmt.Printf("Item(%v) %T: %s\n", name, item, item)
		}
	}
}
