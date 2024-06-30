package main

import (
	"fmt"
	"log"
	"os"
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
		for _, schema := range db.tables {
			tableNames = append(tableNames, schema.tblName)
		}
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
			schema, ok := db.tables[tableName]
			if !ok {
				log.Fatal(fmt.Sprintf("unknown table %s", tableName))
				os.Exit(1)
			}

			// Read the whole page
			tableRootPage := db.readPage(int64(schema.rootPage - 1))

			firstSelection := strings.ToLower(strings.Trim(selectStmt.Columns[0].String(), "\""))
			switch firstSelection {
			case "count(*)":
				fmt.Printf("%d\n", len(tableRootPage.cellOffsets))
			default:
				// Find where is the name of that particular table.
				colIndex := 0
				for i, col := range schema.tableSpec.Columns {
					// fmt.Printf("Scanning for %s/%s\n", firstSelection, col.Name)
					if col.Name.Name == firstSelection {
						// Found the column!
						colIndex = i
						break
					}
				}
				// Read values from all cells per such column index
				for j, rowOffset := range tableRootPage.cellOffsets {
					row, err := tableRootPage.readCell(j)
					if err != nil {
						log.Fatal(fmt.Sprintf("Failed to read column data from row #%d at offset %d", j, rowOffset))
						os.Exit(1)
					}
					fmt.Printf("%s\n", string(row.fields[colIndex].data))
				}
			}
		default:
			log.Fatal(fmt.Sprintf("%s statement is not yet supported.", _sql))
			os.Exit(1)
		}
	}
}
