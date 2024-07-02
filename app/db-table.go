package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/rqlite/sql"
)

type _DBLeafPage struct {
	maxRowId  int64 // 0 means the last page.
	rowsCount int
	pageIndex uint32
	leafPage  *TableBTreePage // may or may not loaded. (lazy)
}

// automatically traverse through all pages.
func walkTableLeafPages(db *Db, pageNumber int64, maxRowId int64) []_DBLeafPage {
	pageIndex := pageNumber - 1
	leafPage := db.readPage(pageIndex)
	out := []_DBLeafPage{}
	switch leafPage.header.pageType {
	case LeafTable:
		return append(out, _DBLeafPage{
			maxRowId:  maxRowId,
			pageIndex: uint32(pageIndex),
			rowsCount: int(len(leafPage.cellOffsets)),
			leafPage:  leafPage,
		})
	case InteriorTable:
		cells, err := leafPage.readAllTableInteriorCells()
		if err != nil {
			log.Fatal("Failed to get associated interior cells")
		}
		for _, cell := range cells {
			out = append(out, walkTableLeafPages(db, int64(cell.leftPageNumber), cell.rowid)...)
		}
		// Handle interior page's header
		out = append(out, walkTableLeafPages(db, int64(leafPage.header.rightMostPointer), 0)...)
	default:
		log.Fatalf("Unsupported page type %#x", leafPage.header.pageType)
	}
	return out
}

// Abstraction table
type DBTable struct {
	Schema
	rowIdAliasColIndex int // -1 means no alias, otherwise colIndex that uses rowId instead.
	colIndexMap        map[string]int
	tableSpec          *sql.CreateTableStatement
	db                 *Db
	btreePages         []_DBLeafPage
	assocIndices       []*DBIndex
}

func NewDBTable(db *Db, schema *Schema, tableSpec *sql.CreateTableStatement) *DBTable {
	var colIndexMap = map[string]int{} // columnName ~> index
	rowIdAliasColIndex := -1
	fmt.Fprintf(os.Stderr, "[dbg] Table Spec: %s %d columns\n", tableSpec.Name.Name, len(tableSpec.Columns))
	for d, col := range tableSpec.Columns {
		fmt.Fprintf(os.Stderr, "[dbg]  └─COL= %s %v\n", col.Name.Name, col.Constraints)
		if len(col.Constraints) > 0 && col.Constraints[0].String() == "PRIMARY KEY AUTOINCREMENT" {
			rowIdAliasColIndex = d
		}
		colIndexMap[col.Name.Name] = d
	}

	// assert pageNumber > 0
	leafPages := walkTableLeafPages(db, int64(schema.rootPage), 0)
	// write debug information
	// lastRowId := int64(0)
	lastP := len(leafPages) - 1
	totalRows := 0
	start := time.Now()

	for p := 0; p < lastP; p++ {
		page := leafPages[p]
		// fmt.Fprintf(os.Stderr, " page %d %d<= .. jump to %d (%d)\n", p, page.maxRowId, page.pageIndex, page.rowsCount)
		// lastRowId = page.maxRowId
		totalRows += page.rowsCount
	}
	// fmt.Fprintf(os.Stderr, "[dbg] page %d >%d .. jump to %d\n", len(leafPages), lastRowId, leafPages[lastP].pageIndex)
	totalRows += leafPages[lastP].rowsCount
	fmt.Fprintf(os.Stderr, "[dbg] total rows %d\n", totalRows)
	elapsed := time.Since(start)
	fmt.Fprintf(os.Stderr, "[dbg] Elapsed time: %s\n", elapsed)

	return &DBTable{
		tableSpec:          tableSpec,
		db:                 db,
		btreePages:         leafPages,
		rowIdAliasColIndex: rowIdAliasColIndex,
		colIndexMap:        colIndexMap,
		assocIndices:       []*DBIndex{},
		Schema:             *schema,
	}
}

func (t *DBTable) Name() string {
	return t.tableSpec.Name.Name
}

// Traverse all rows
func (t *DBTable) rows(where map[string]string) []Row {
	out := []Row{}
	for _, page := range t.btreePages {
		for c := 0; c < len(page.leafPage.cellOffsets); c++ {
			cell, err := page.leafPage.readTableLeafCell(c, t.rowIdAliasColIndex)
			if err != nil {
				fmt.Fprintf(os.Stderr, "[dbg] read row failed: %s\n", err.Error())
			}
			if t.applyFilter(&where, cell) == true {
				out = append(out, Row{
					cell:  cell,
					table: t,
				})
			}
		}
	}
	return out
}

// Determine if given condition may use the index.
func (t *DBTable) eligibleIndex(condition *map[string]string) (*DBIndex, string) {
	if len(*condition) == 0 {
		return nil, ""
	}
	for _, idx := range t.assocIndices {
		// TODO: Should look for max length!
		matched := (*idx).Match(condition)
		if len(matched) > 0 {
			// usable!
			return idx, matched
		}
	}
	//
	return nil, ""
}

// Simple Equal comparison bruteforce!
func (t *DBTable) applyFilter(condition *map[string]string, cell *TableBTreeLeafTablePageCell) bool {
	if len(*condition) == 0 {
		return true
	}
	for key, value := range *condition {
		ci := t.colIndexMap[key]
		str := cell.fields[ci].String()
		if str != value {
			return false
		}
	}
	return true
}
