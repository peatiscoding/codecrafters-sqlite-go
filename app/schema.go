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
	schemaType  SchemaType
	name        string
	tblName     string
	sql         string
	tableSpec   *sql.CreateTableStatement
	colIndexMap map[string]int
	rootPage    int
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

	_sql := strings.ReplaceAll(string(cell.fields[4].data), " text", "") // it seems "text" was detected as another column :(
	_sql = strings.ReplaceAll(_sql, " integer", "")                      // it seems "integer" was detected as another column :(
	_sql = strings.ReplaceAll(_sql, "\n", "")                            // it seems was detected as another column :(
	_sql = strings.ReplaceAll(_sql, "\t", " ")                           // it seems was detected as another column :(
	// fmt.Printf("SQL: %s\n", _sql)
	stmt, err := sql.NewParser(strings.NewReader(_sql)).ParseStatement()
	if err != nil {
		// Fatal!
		// fmt.Printf("WARNING Cannot eval Schema SQL %d (%d); %s", cell.fields[3].serialType, cell.fields[3].contentSize, err.Error())
	}

	var tableSpec *sql.CreateTableStatement
	var colIndexMap = map[string]int{}
	switch stmt.(type) {
	case *sql.CreateTableStatement:
		tableSpec = stmt.(*sql.CreateTableStatement)
		fmt.Printf("Spec: %d\n", len(tableSpec.Columns))
		for d, col := range tableSpec.Columns {
			fmt.Printf(" COL= %s\n", col.Name.Name)
			colIndexMap[col.Name.Name] = d
		}
	}

	return &Schema{
		schemaType:  schemaType,
		name:        name,
		tblName:     tblName,
		sql:         _sql,
		tableSpec:   tableSpec,
		colIndexMap: colIndexMap,
		rootPage:    rootPage,
	}
}

// Simple Equal comparison bruteforce!
func (s *Schema) applyFilter(condition map[string]string, cell *TableBTreeLeafPageCell) bool {
	for key, value := range condition {
		ci := s.colIndexMap[key]
		str := cell.fields[ci].String()
		if str != value {
			return false
		}
	}
	return true
}
