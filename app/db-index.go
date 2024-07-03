package main

import (
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/rqlite/sql"
)

type DBIndex struct {
	Schema
	db            *Db
	assocTable    string // associated table that utilize this index.
	indexSpec     *sql.CreateIndexStatement
	colIndexOrder []string
	leafPages     []_IndexLeafPage // Leaf page map for fast jump to specific pages
}

type _IndexLeafPage struct {
	maxIndexStrain string // "" means the last one.
	rowsCount      int
	pageIndex      uint32
	leafPage       *TableBTreePage // may or may not loaded. (lazy)
}

func _walkIndexLeafPages(db *Db, pageNumber int64, maxIndexStrain string) []_IndexLeafPage {
	pageIndex := pageNumber - 1
	leafPage := db.readPage(pageIndex)
	out := []_IndexLeafPage{}
	switch leafPage.header.pageType {
	case LeafIndex:
		return append(out, _IndexLeafPage{
			maxIndexStrain: maxIndexStrain,
			pageIndex:      uint32(pageIndex),
			rowsCount:      int(len(leafPage.cellOffsets)),
			leafPage:       leafPage,
		})
	case InteriorIndex:
		cells, err := leafPage.readAllIndexInteriorCells()
		if err != nil {
			log.Fatal("Failed to get associated interior cells")
		}
		for _, cell := range cells {
			out = append(out, _walkIndexLeafPages(db, int64(cell.leftPageNumber), cell.maxIndexStrain)...)
		}
		// Handle interior page's header
		out = append(out, _walkIndexLeafPages(db, int64(leafPage.header.rightMostPointer), "")...)
	default:
		log.Fatalf("Unsupported page type %#x", leafPage.header.pageType)
	}
	return out
}

func walkThroughIndexBTreeForRowIds(db *Db, pageNumber int64, conditionValueAsPrefix string, out *[]IndexPayload) error {
	pageIndex := pageNumber - 1
	page := db.readPage(pageIndex)
	switch page.header.pageType {
	case LeafIndex:
		// Perform Full Table Scan
		for cellIndex := 0; cellIndex < len(page.cellOffsets); cellIndex++ {
			cell, err := page.readIndexLeafCell(cellIndex)
			if err != nil {
				return err
			}
			// Eval condition
			// fmt.Fprintf(os.Stderr, "[dbg] Eval on leaf page %d: %s vs %s (result=%d)\n", pageNumber, conditionValueAsPrefix, cell.indexStrain, len(*out))
			if strings.HasPrefix(cell.indexStrain, conditionValueAsPrefix) {
				*out = append(*out, cell)
			} else if cell.indexStrain > conditionValueAsPrefix {
				// nothing to search for anymore.
				return nil
			}
		}
	case InteriorIndex:
		// this should recusrively call walk method with nested page. (And append the result)
		// fmt.Fprintf(os.Stderr, "[dbg] Reading from interior page %d firstCell= %d lastCell= %d\n", pageNumber, firstCell.rowid, lastCell.rowid)
		// scan through the ranges
		for j := 0; j < len(page.cellOffsets); j++ {
			cellOffset := page.cellOffsets[j]
			cell, err := page.readIndexInteriorCell(cellOffset)
			if err != nil {
				return err
			}
			if conditionValueAsPrefix > cell.maxIndexStrain {
				// nothing to process on this page.
				continue
			}
			// Interior also holds part of index.
			if strings.HasPrefix(cell.maxIndexStrain, conditionValueAsPrefix) {
				*out = append(*out, cell)
			}
			// fmt.Fprintf(os.Stderr, "[dbg] Reading from interior page %d %s vs %s (jump=%d)\n", pageNumber, conditionValueAsPrefix, cell.maxIndexStrain, cell.leftPageNumber)
			err = walkThroughIndexBTreeForRowIds(db, int64(cell.leftPageNumber), conditionValueAsPrefix, out)
			if err != nil {
				return err
			}
		}
		// also go through the last wing
		err := walkThroughIndexBTreeForRowIds(db, int64(page.header.rightMostPointer), conditionValueAsPrefix, out)
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
	pages := _walkIndexLeafPages(db, int64(schema.rootPage), "")

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
		leafPages:     pages,
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
	result := []IndexPayload{}
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
