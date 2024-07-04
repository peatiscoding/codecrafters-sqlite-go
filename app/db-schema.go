package main

import (
	"github.com/peatiscoding/codecrafters-sqlite-go/app/btree"
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
	rootPage   int64
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

func NewSchema(cell *btree.TableBTreeLeafTablePageCell) *Schema {
	typeStr := string(cell.Fields[0].String())
	name := string(cell.Fields[1].String())
	tblName := string(cell.Fields[2].String())
	schemaType := typeFromRawString(typeStr)
	rootPage := cell.Fields[3].Integer()

	// Clean SQL
	_sql := strings.ReplaceAll(cell.Fields[4].String(), " text", "") // it seems "text" was detected as another column :(
	_sql = strings.ReplaceAll(_sql, " integer", "")                  // it seems "integer" was detected as another column :(
	_sql = strings.ReplaceAll(_sql, "\n", "")                        // it seems was detected as another column :(
	_sql = strings.ReplaceAll(_sql, "\t", " ")                       // it seems was detected as another column :(

	return &Schema{
		schemaType: schemaType,
		name:       name,
		tblName:    tblName,
		sql:        _sql,
		rootPage:   rootPage,
	}
}
