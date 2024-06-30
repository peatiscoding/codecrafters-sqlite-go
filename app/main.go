package main

import (
	"bytes"
	"encoding/binary"
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

			colNames := make([]string, len(selectStmt.Columns))
			for c, column := range selectStmt.Columns {
				col := strings.ToLower(strings.Trim(column.String(), "\""))
				colNames[c] = col
			}

			if len(colNames) == 1 && colNames[0] == "count(*)" {
				fmt.Printf("%d\n", len(tableRootPage.cellOffsets))
			} else {
				// Find where is the name of that particular table.
				pending := len(colNames)
				colIndices := make([]int, pending)
				for i, col := range schema.tableSpec.Columns {
					for j, cn := range colNames {
						// fmt.Printf("Scanning for %s through %s %v\n", cn, col.Name.Name, colIndices)
						if col.Name.Name == cn {
							// Found the column!
							colIndices[j] = i
						}
					}
				}
				// Read values from all cells per such column index
				for j, rowOffset := range tableRootPage.cellOffsets {
					row, err := tableRootPage.readCell(j)
					if err != nil {
						log.Fatal(fmt.Sprintf("Failed to read column data from row #%d at offset %d", j, rowOffset))
						os.Exit(1)
					}
					for v, ci := range colIndices {
						if v != 0 {
							fmt.Print("|")
						}
						if STRING == row.fields[ci].serialType {
							fmt.Printf("%s", string(row.fields[ci].data))
						} else {
							var i64 = 0
							reader := bytes.NewReader(row.fields[ci].data)
							binary.Read(reader, binary.BigEndian, &i64)
							fmt.Printf("%d", i64)
						}
					}
					fmt.Println()
				}
			}
		default:
			log.Fatal(fmt.Sprintf("%s statement is not yet supported.", _sql))
			os.Exit(1)
		}
	}
}
