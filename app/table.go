package main

import (
	"fmt"
	"log"
	"os"
	"time"
)

type DBLeafPage struct {
	maxRowId  int64 // 0 means the last page.
	rowsCount int
	pageIndex uint32
	leafPage  *TableBTreePage // may or may not loaded. (lazy)
}

// automatically traverse through all pages.
func NewDbLeafPages(db *Db, pageNumber int64, maxRowId int64) []DBLeafPage {
	pageIndex := pageNumber - 1
	leafPage := db.readPage(pageIndex)
	out := []DBLeafPage{}
	switch leafPage.header.pageType {
	case LeafTable:
		return append(out, DBLeafPage{
			maxRowId:  maxRowId,
			pageIndex: uint32(pageIndex),
			rowsCount: int(len(leafPage.cellOffsets)),
			leafPage:  leafPage,
		})
	case InteriorTable:
		cells, err := leafPage.readAllInteriorCells()
		if err != nil {
			log.Fatal("Failed to get associated interior cells")
		}
		for _, cell := range cells {
			out = append(out, NewDbLeafPages(db, int64(cell.leftPageNumber), cell.rowid)...)
		}
		// Handle interior page's header
		out = append(out, NewDbLeafPages(db, int64(leafPage.header.rightMostPointer), 0)...)
	default:
		log.Fatalf("Unsupported page type %#x", leafPage.header.pageType)
	}
	return out
}

// Abstraction table
type DBTable struct {
	schema     *Schema
	db         *Db
	btreePages []DBLeafPage
}

func NewDBTable(d *Db, s *Schema) *DBTable {
	// assert pageNumber > 0
	leafPages := NewDbLeafPages(d, int64(s.rootPage), 0)
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
		schema:     s,
		db:         d,
		btreePages: leafPages}
}

// Traverse all rows
func (t *DBTable) rows(where map[string]string) []Row {
	out := []Row{}
	for _, page := range t.btreePages {
		for c := 0; c < len(page.leafPage.cellOffsets); c++ {
			cell, err := page.leafPage.readLeafCell(c, t.schema.rowIdAliasColIndex)
			if err != nil {
				fmt.Fprintf(os.Stderr, "[dbg] read row failed: %s\n", err.Error())
			}
			if t.schema.applyFilter(&where, cell) == true {
				out = append(out, Row{
					cell:   cell,
					schema: t.schema,
				})
			}
		}
	}
	return out
}

type Row struct {
	cell   *TableBTreeLeafPageCell
	schema *Schema
}

func (r *Row) Column(columnIndex int) string {
	if columnIndex == r.schema.rowIdAliasColIndex {
		return fmt.Sprintf("%d", r.cell.rowid)
	}
	return r.cell.fields[columnIndex].String()
}
