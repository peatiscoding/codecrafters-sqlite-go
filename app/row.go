package main

import "fmt"

type Row struct {
	cell  *TableBTreeLeafTablePageCell
	table *DBTable
}

func (r *Row) Column(columnIndex int) string {
	if columnIndex == r.table.rowIdAliasColIndex {
		return fmt.Sprintf("%d", r.cell.rowid)
	}
	return r.cell.fields[columnIndex].String()
}
