package main

import (
	"fmt"
	"log"
	"os"
	"sort"
	"strings"

	"github.com/rqlite/sql"
)

// Usage: your_sqlite3.sh sample.db .dbinfo
func main() {
	databaseFilePath := os.Args[1]
	command := os.Args[2]

	db, err := NewDb(databaseFilePath)
	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}

	switch command {
	case ".tables":
		tableNames := make([]string, 0)
		for _, tbl := range db.tables {
			tableNames = append(tableNames, tbl.Name())
		}
		sort.Strings(tableNames)
		fmt.Printf("%s", strings.Join(tableNames, " "))
	case ".dbinfo":
		fmt.Printf("database page size: %d\n", db.pageSize)

		tableCount := len(db.tables)
		fmt.Printf("number of tables: %d\n", tableCount)

	default:
		// QUERY!
		_sql := command
		// Eval sql
		stmt, err := sql.NewParser(strings.NewReader(_sql)).ParseStatement()
		if err != nil {
			log.Fatal(err)
			os.Exit(1)
		}
		switch stmt.(type) {
		case *sql.SelectStatement:
			selectStmt := stmt.(*sql.SelectStatement)
			// perform the select
			tableName := strings.Trim(selectStmt.Source.String(), "\"")
			tbl, ok := db.tables[tableName]
			if !ok {
				log.Fatal(fmt.Sprintf("unknown table %s", tableName))
				os.Exit(1)
			}

			colNames := make([]string, len(selectStmt.Columns))
			for c, column := range selectStmt.Columns {
				col := strings.ToLower(strings.Trim(column.String(), "\""))
				colNames[c] = col
			}
			where := map[string]string{}
			// apply simple condition here
			if selectStmt.WhereExpr != nil {
				whereClause := strings.SplitN(selectStmt.WhereExpr.String(), "=", 2)
				key := strings.Trim(whereClause[0], "\" ")
				where[key] = strings.Trim(whereClause[1], "' ")
			}

			if len(colNames) == 1 && colNames[0] == "count(*)" {
				// apply simple condition here
				// Read values from all cells per such column index
				count := len(tbl.rows(where))
				fmt.Printf("%d\n", count)
			} else {
				// Find where is the name of that particular table.
				pending := len(colNames)
				colIndices := make([]int, pending)
				for j, cn := range colNames {
					// fmt.Printf("Scanning for %s through %s %v\n", cn, col.Name.Name, colIndices)
					colIndices[j], ok = tbl.colIndexMap[cn]
					if !ok {
						log.Fatal(fmt.Sprintf("Unknown column %s to select", cn))
						os.Exit(1)
					}
				}

				// Read values from all cells per such column index
				for _, row := range tbl.rows(where) {
					// Print output
					for v, ci := range colIndices {
						if v != 0 {
							fmt.Print("|")
						}
						fmt.Printf("%s", row.Column(ci))
					}
					fmt.Println()
				}
			}
		default:
			log.Fatal(fmt.Sprintf("'%s' statement is not yet supported.", _sql))
			os.Exit(1)
		}
	}
}
