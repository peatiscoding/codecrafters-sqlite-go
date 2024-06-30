package main

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

func fromBTreeCell(cell *TableBTreeLeafPageCell) *Schema {
	typeStr := string(cell.fields[0].data)
	name := string(cell.fields[1].data)
	tblName := string(cell.fields[2].data)
	sql := string(cell.fields[3].data)
	schemaType := typeFromRawString(typeStr)
	rootPage := 0

	return &Schema{
		schemaType: schemaType,
		name:       name,
		tblName:    tblName,
		sql:        sql,
		rootPage:   rootPage,
	}
}
