package main

import (
	"fmt"
	"strings"

	"github.com/rqlite/sql"
)

type SchemaType int8

const (
	Table SchemaType = iota
	Index
	View
	Trigger
	Unknown
)

type Schema struct {
	schemaType SchemaType
	name       string
	tblName    string
	sql        string
	tableSpec  *sql.CreateTableStatement
	rootPage   int
}

func typeFromRawString(str string) SchemaType {
	switch str {
	case "table":
		return Table
	case "index":
		return Index
	case "view":
		return View
	case "trigger":
		return Trigger
	}
	return Unknown
}

func NewSchema(cell *TableBTreeLeafPageCell) *Schema {
	typeStr := string(cell.fields[0].data)
	name := string(cell.fields[1].data)
	tblName := string(cell.fields[2].data)
	schemaType := typeFromRawString(typeStr)
	var rootPage int = 0
	if I8 == cell.fields[3].serialType {
		rootPage = int(cell.fields[3].data[0])
	} else {
		// Fatal! unexpected format of the Schema object.
		fmt.Printf("Cannot find root page %v", cell.fields[3])
	}
	_sql := string(cell.fields[4].data)
	// fmt.Printf("SQL: %s\n", _sql)
	stmt, err := sql.NewParser(strings.NewReader(_sql)).ParseStatement()
	if err != nil {
		// Fatal!
		// fmt.Printf("WARNING Cannot eval Schema SQL %d (%d); %s", cell.fields[3].serialType, cell.fields[3].contentSize, err.Error())
	}

	var tableSpec *sql.CreateTableStatement
	switch stmt.(type) {
	case *sql.CreateTableStatement:
		tableSpec = stmt.(*sql.CreateTableStatement)
	}

	return &Schema{
		schemaType: schemaType,
		name:       name,
		tblName:    tblName,
		sql:        _sql,
		tableSpec:  tableSpec,
		rootPage:   rootPage,
	}
}
