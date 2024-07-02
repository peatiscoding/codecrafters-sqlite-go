package main

import (
	"fmt"
	"strings"
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
	rootPage   int
}

type DBSchema interface {
	Name() string
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

func NewSchema(cell *TableBTreeLeafTablePageCell) *Schema {
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

	// Clean SQL
	_sql := strings.ReplaceAll(string(cell.fields[4].data), " text", "") // it seems "text" was detected as another column :(
	_sql = strings.ReplaceAll(_sql, " integer", "")                      // it seems "integer" was detected as another column :(
	_sql = strings.ReplaceAll(_sql, "\n", "")                            // it seems was detected as another column :(
	_sql = strings.ReplaceAll(_sql, "\t", " ")                           // it seems was detected as another column :(

	return &Schema{
		schemaType: schemaType,
		name:       name,
		tblName:    tblName,
		sql:        _sql,
		rootPage:   rootPage,
	}
}
