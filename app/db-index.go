package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/peatiscoding/codecrafters-sqlite-go/app/btree"
	"github.com/rqlite/sql"
)

type DBIndex struct {
	Schema
	db            *Db
	assocTable    string // associated table that utilize this index.
	indexSpec     *sql.CreateIndexStatement
	colIndexOrder []string
}

func walkThroughIndexBTreeForRowIds(db *Db, pageNumber int64, conditionValueAsPrefix string, out *[]btree.IndexPayload) error {
	pageIndex := pageNumber - 1
	page := db.readPage(pageIndex)
	switch page.Header.PageType {
	case btree.LeafIndex:
		// Perform Full Table Scan
		for cellIndex := 0; cellIndex < len(page.CellOffsets); cellIndex++ {
			cell, err := page.ReadIndexLeafCell(cellIndex)
			if err != nil {
				return err
			}
			// Eval condition
			// fmt.Fprintf(os.Stderr, "[dbg] Eval on leaf page %d: %s vs %s (result=%d)\n", pageNumber, conditionValueAsPrefix, cell.indexStrain, len(*out))
			if strings.HasPrefix(cell.IndexStrain, conditionValueAsPrefix) {
				*out = append(*out, cell)
			} else if cell.IndexStrain > conditionValueAsPrefix {
				// nothing to search for anymore.
				return nil
			}
		}
	case btree.InteriorIndex:
		// this should recusrively call walk method with nested page. (And append the result)
		fmt.Fprintf(os.Stderr, "[dbg] IndexScan walk started.. from interior page %d .. %s\n", pageNumber, conditionValueAsPrefix)
		// scan through the ranges
		for j := 0; j < len(page.CellOffsets); j++ {
			cellOffset := page.CellOffsets[j]
			cell, err := page.ReadIndexInteriorCell(cellOffset)
			if err != nil {
				return err
			}
			if conditionValueAsPrefix > cell.MaxIndexStrain {
				// nothing to process on this page.
				continue
			}
			// Interior also holds part of index.
			if strings.HasPrefix(cell.MaxIndexStrain, conditionValueAsPrefix) {
				*out = append(*out, cell)
			}
			// fmt.Fprintf(os.Stderr, "[dbg] Reading from interior page %d %s vs %s (jump=%d)\n", pageNumber, conditionValueAsPrefix, cell.maxIndexStrain, cell.leftPageNumber)
			err = walkThroughIndexBTreeForRowIds(db, int64(cell.LeftPageNumber), conditionValueAsPrefix, out)
			if err != nil {
				return err
			}
		}
		// also go through the last wing
		err := walkThroughIndexBTreeForRowIds(db, int64(page.Header.RightMostPointer), conditionValueAsPrefix, out)
		if err != nil {
			return err
		}
	}
	return nil
}

func NewDbIndex(db *Db, schema *Schema, indexSpec *sql.CreateIndexStatement) *DBIndex {
	var colIndexOrder = []string{}
	fmt.Fprintf(os.Stderr, "[dbg] Index Spec: %s (page=%d) %d columns for %s\n", indexSpec.Name.Name, schema.rootPage, len(indexSpec.Columns), indexSpec.Table.Name)
	for _, col := range indexSpec.Columns {
		fmt.Fprintf(os.Stderr, "[dbg]  └─COL= %s %s %s\n", col.X.String(), col.Asc.String(), col.Desc.String())
		colIndexOrder = append(colIndexOrder, strings.ReplaceAll(col.X.String(), "\"", ""))
	}
	// determine the associated table?
	forTable := indexSpec.Table.Name

	// Parse the whole index?
	// pages := _walkIndexLeafPages(db, int64(schema.rootPage), "")

	// // Print Debug information
	// for p := 0; p < len(pages)-1; p++ {
	// 	page := pages[p]
	// 	fmt.Fprintf(os.Stderr, "[dbg]   %s< .. %d\n", page.maxIndexStrain, page.pageIndex)
	// }
	// lastPage := pages[len(pages)-1]
	// fmt.Fprintf(os.Stderr, "[dbg]   otherwise .. %d\n", lastPage.pageIndex)

	return &DBIndex{
		db:            db,
		indexSpec:     indexSpec,
		colIndexOrder: colIndexOrder,
		Schema:        *schema,
		assocTable:    forTable,
	}
}

func (i *DBIndex) Name() string {
	return i.indexSpec.Name.Name
}

// Determine if this index matched the table requirement or not? If yes, how much it would help? (integer)
// If 1 column (out of 3) matched it will return 1
func (i *DBIndex) Match(condition *map[string]string) string {
	// no condition being asked; this will not matched anything.
	if len(*condition) <= 0 {
		return ""
	}
	var result strings.Builder
	for j, indexColName := range i.colIndexOrder {
		if condVal, ok := (*condition)[indexColName]; ok == true {
			if j > 0 {
				result.WriteString("|")
			}
			result.WriteString(condVal)
			continue
		}
		break
	}
	return result.String()
}

// Query from index
// @param - the value returned from Match()
func (i *DBIndex) IndexScan(condition *map[string]string, conditionAsPrefix string) []int64 {
	start := time.Now()
	result := []btree.IndexPayload{}
	err := walkThroughIndexBTreeForRowIds(i.db, int64(i.rootPage), conditionAsPrefix, &result)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[dbg] IndexScan failed: %s\n", err.Error())
	}
	columnForPk := len(i.colIndexOrder)
	rowIds := make([]int64, len(result))
	for i := 0; i < len(result); i++ {
		rowIds[i] = result[i].Fields()[columnForPk].Integer()
	}
	elapsed := time.Since(start)
	fmt.Fprintf(os.Stderr, "[dbg] IndexScan prefix=%s -> matched %d rowids (%v). Done in %s\n", conditionAsPrefix, len(rowIds), rowIds, elapsed)
	return rowIds
}
