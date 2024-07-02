package main

import (
	"fmt"
	"os"

	"github.com/rqlite/sql"
)

type DBIndex struct {
	Schema
	db          *Db
	forTable    *DBTable
	indexSpec   *sql.CreateIndexStatement
	colIndexMap map[string]int
}

func NewDbIndex(db *Db, schema *Schema, indexSpec *sql.CreateIndexStatement) *DBIndex {
	var colIndexMap = map[string]int{} // columnName ~> index
	fmt.Fprintf(os.Stderr, "[dbg] Index Spec: %s %d columns\n", indexSpec.Name.Name, len(indexSpec.Columns))
	for d, col := range indexSpec.Columns {
		fmt.Fprintf(os.Stderr, "[dbg]  └─COL= %s %s\n", col.String(), col.X.String())
		colIndexMap[col.String()] = d
	}

	return &DBIndex{
		db:          db,
		indexSpec:   indexSpec,
		colIndexMap: colIndexMap,
		Schema:      *schema,
	}
}

func (i *DBIndex) Name() string {
	return i.indexSpec.Name.Name
}
