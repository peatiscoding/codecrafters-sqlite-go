package main

import (
	"fmt"
	"log"
	"os"
	"strings"
	// Available if you need it!
	// "github.com/xwb1989/sqlparser"
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
		for _, schema := range db.schemas {
			if Table != schema.schemaType {
				continue
			}
			tableNames = append(tableNames, schema.tblName)
		}
		fmt.Printf("%s", strings.Join(tableNames, " "))
	case ".dbinfo":
		fmt.Printf("database page size: %d\n", db.pageSize)

		tableCount := 0
		for _, schema := range db.schemas {
			if Table == schema.schemaType {
				tableCount += 1
			}
		}
		fmt.Printf("number of tables: %d\n", tableCount)

	default:
		fmt.Println("Unknown command", command)
		os.Exit(1)
	}
}
