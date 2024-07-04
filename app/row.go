package main

import (
	"fmt"
	"github.com/peatiscoding/codecrafters-sqlite-go/app/btree"
)

type Row struct {
	cell  *btree.TableBTreeLeafTablePageCell
	table *DBTable
}

func (r *Row) Column(columnIndex int) string {
	if columnIndex == r.table.rowIdAliasColIndex {
		return fmt.Sprintf("%d", r.cell.Rowid)
	}
	return r.cell.Fields[columnIndex].String()
}
