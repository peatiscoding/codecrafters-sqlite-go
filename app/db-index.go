package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/rqlite/sql"
)

type DBIndex struct {
	Schema
	db            *Db
	forTable      *DBTable
	indexSpec     *sql.CreateIndexStatement
	colIndexOrder []string
}

func NewDbIndex(db *Db, schema *Schema, indexSpec *sql.CreateIndexStatement) *DBIndex {
	var colIndexOrder = []string{}
	fmt.Fprintf(os.Stderr, "[dbg] Index Spec: %s %d columns for %s\n", indexSpec.Name.Name, len(indexSpec.Columns), indexSpec.Table.Name)
	for _, col := range indexSpec.Columns {
		fmt.Fprintf(os.Stderr, "[dbg]  └─COL= %s %s %s\n", col.X.String(), col.Asc.String(), col.Desc.String())
		colIndexOrder = append(colIndexOrder, strings.ReplaceAll(col.X.String(), "\"", ""))
	}

	return &DBIndex{
		db:            db,
		indexSpec:     indexSpec,
		colIndexOrder: colIndexOrder,
		Schema:        *schema,
	}
}

func (i *DBIndex) Name() string {
	return i.indexSpec.Name.Name
}

// Determine if this index matched the table requirement or not? If yes, how much it would help? (integer)
// If 1 column (out of 3) matched it will return 1
func (i *DBIndex) Match(condition *map[string]string) string {
	// no condition being asked; this will not matched anything.
	if len(*condition) <= 0 {
		return ""
	}
	var result strings.Builder
	for j, indexColName := range i.colIndexOrder {
		if condVal, ok := (*condition)[indexColName]; ok == true {
			if j > 0 {
				result.WriteString("|")
			}
			result.WriteString(condVal)
			continue
		}
		break
	}
	return result.String()
}

// Query from index
func (i *DBIndex) SelectRange(conditionValuePrefix string) []Row {
	return make([]Row, 0)
}
