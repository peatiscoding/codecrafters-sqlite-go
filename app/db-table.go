package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/rqlite/sql"
)

type _DBLeafPage struct {
	maxRowId  int64 // 0 means the last page.
	rowsCount int
	pageIndex uint32
	leafPage  *TableBTreePage // may or may not loaded. (lazy)
}

func walkThroughBTreeForRowId(db *Db, pageNumber int64, rowId int64) (*TableBTreeLeafTablePageCell, error) {
	pageIndex := pageNumber - 1
	// out := []TableBTreeLeafTablePageCell{}
	page := db.readPage(pageIndex)
	switch page.header.pageType {
	case LeafTable:
		// Perform Full Table Scan
		for cellIndex := 0; cellIndex < len(page.cellOffsets); cellIndex++ {
			cell, err := page.readTableLeafCell(cellIndex, 0)
			if err != nil {
				return nil, err
			}
			// Eval condition
			if cell.rowid == rowId {
				return cell, nil
			}
		}
	case InteriorTable:
		// this should recusrively call walk method with nested page. (And append the result)
		count := len(page.cellOffsets)
		firstCellOffset := page.cellOffsets[0]
		lastCellOffset := page.cellOffsets[count-1]
		firstCell, err := page.readTableInteriorCell(int(firstCellOffset))
		if err != nil {
			return nil, err
		}
		lastCell, err := page.readTableInteriorCell(int(lastCellOffset))
		if err != nil {
			return nil, err
		}
		// fmt.Fprintf(os.Stderr, "[dbg] Reading from interior page %d firstCell= %d lastCell= %d\n", pageNumber, firstCell.rowid, lastCell.rowid)
		if rowId <= firstCell.rowid {
			// go into first page
			leafCell, err := walkThroughBTreeForRowId(db, int64(firstCell.leftPageNumber), rowId)
			if err != nil {
				return nil, err
			}
			if leafCell != nil {
				return leafCell, nil
			}
			return nil, nil
		} else if rowId > lastCell.rowid {
			// go to right most cell
			leafCell, err := walkThroughBTreeForRowId(db, int64(page.header.rightMostPointer), rowId)
			if err != nil {
				return nil, err
			}
			if leafCell != nil {
				return leafCell, nil
			}
			return nil, nil
		} else {
			// scan through the ranges
			for j := 1; j < len(page.cellOffsets); j++ {
				cellOffset := page.cellOffsets[j]
				cell, err := page.readTableInteriorCell(int(cellOffset))
				if err != nil {
					return nil, err
				}
				if rowId > cell.rowid {
					continue
				}
				// fmt.Fprintf(os.Stderr, "[dbg] Reading from interior page %d %d vs %d\n", pageNumber, rowId, cell.rowid)
				leafCell, err := walkThroughBTreeForRowId(db, int64(cell.leftPageNumber), rowId)
				if err != nil {
					return nil, err
				}
				if leafCell != nil {
					return leafCell, nil
				}
			}
		}
	}
	return nil, nil
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
	if cond, ok := where["id"]; ok == true {
		// use selectByRowId
		fmt.Fprintf(os.Stderr, "[dbg] Selecting id= %s\n", cond)
		rowId, err := strconv.ParseInt(cond, 10, 64)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[dbg] read row failed: %s\n", err.Error())
		}
		found, err := t.selectByRowId([]int64{rowId})
		if err != nil {
			fmt.Fprintf(os.Stderr, "[dbg] read row failed: %s\n", err.Error())
		}
		if found != nil {
			out = append(out, found...)
			return out
		}
	}
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

// Table
// TODO: Walk to correct page; Then move to correct rowId
func (t *DBTable) selectByRowId(rowIds []int64) ([]Row, error) {
	out := make([]Row, len(rowIds))
	for i := 0; i < len(rowIds); i++ {
		rowId := rowIds[i]
		cell, err := walkThroughBTreeForRowId(t.db, int64(t.rootPage), rowId)
		if err != nil {
			return nil, err
		}
		if cell != nil {
			out[i] = Row{
				cell:  cell,
				table: t,
			}
			return out, nil
		}
	}
	return nil, errors.New(fmt.Sprintf("No record found %v", rowIds))
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
