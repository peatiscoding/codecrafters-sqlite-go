package main

import (
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/peatiscoding/codecrafters-sqlite-go/app/btree"
	"github.com/rqlite/sql"
)

type _DBLeafPage struct {
	maxRowId  int64 // 0 means the last page.
	rowsCount int
	pageIndex uint32
	leafPage  *btree.TableBTreePage // may or may not loaded. (lazy)
}

// Convert this to Generic?
type _SearchList struct {
	currentIndex int
	sortedRowIds []int64
	result       []*btree.TableBTreeLeafTablePageCell
}

func NewSearchList(rowIds []int64) *_SearchList {
	// Asecending Order
	sort.Slice(rowIds, func(i, j int) bool {
		return rowIds[i] < rowIds[j]
	})
	return &_SearchList{
		currentIndex: 0,
		sortedRowIds: rowIds,
		result:       make([]*btree.TableBTreeLeafTablePageCell, len(rowIds)),
	}
}

// the lease rowIds (lease value to search for)
func (s *_SearchList) current() int64 {
	return s.sortedRowIds[s.currentIndex]
}

// move next, and check if it is already finished.
func (s *_SearchList) matched(r *btree.TableBTreeLeafTablePageCell) bool {
	s.result[s.currentIndex] = r
	s.currentIndex++
	return s.hasMore()
}

func (s *_SearchList) hasMore() bool {
	return s.currentIndex == len(s.sortedRowIds)
}

func walkThroughBTreeForRowId(db *Db, pageNumber int64, pad *_SearchList) error {
	pageIndex := pageNumber - 1
	// out := []TableBTreeLeafTablePageCell{}
	page := db.readPage(pageIndex)
	switch page.Header.PageType {
	case btree.LeafTable:
		// Perform Full Table Scan
		for cellIndex := 0; cellIndex < len(page.CellOffsets); cellIndex++ {
			cell, err := page.ReadTableLeafCell(cellIndex, 0)
			if err != nil {
				return err
			}
			// Eval condition
			if cell.Rowid == pad.current() {
				if pad.matched(cell) {
					return nil
				}
			}
		}
	case btree.InteriorTable:
		// this should recusrively call walk method with nested page. (And append the result)
		// fmt.Fprintf(os.Stderr, "[dbg] Reading from interior page %d firstCell= %d lastCell= %d\n", pageNumber, firstCell.rowid, lastCell.rowid)
		// scan through the ranges
		for j := 0; j < len(page.CellOffsets); j++ {
			cellOffset := page.CellOffsets[j]
			cell, err := page.ReadTableInteriorCell(int(cellOffset))
			if err != nil {
				return err
			}
			if pad.current() > cell.Rowid {
				continue
			}
			// fmt.Fprintf(os.Stderr, "[dbg] Reading from interior page %d %d vs %d (jump=%d)\n", pageNumber, pad.current(), cell.rowid, cell.leftPageNumber)
			err = walkThroughBTreeForRowId(db, int64(cell.LeftPageNumber), pad)
			if err != nil {
				return err
			}
			if pad.hasMore() {
				return nil
			}
		}
		err := walkThroughBTreeForRowId(db, int64(page.Header.RightMostPointer), pad)
		if err != nil {
			return err
		}
	}
	return nil
}

// automatically traverse through all pages.
func walkTableLeafPages(db *Db, pageNumber int64, maxRowId int64) []_DBLeafPage {
	pageIndex := pageNumber - 1
	leafPage := db.readPage(pageIndex)
	out := []_DBLeafPage{}
	switch leafPage.Header.PageType {
	case btree.LeafTable:
		return append(out, _DBLeafPage{
			maxRowId:  maxRowId,
			pageIndex: uint32(pageIndex),
			rowsCount: int(len(leafPage.CellOffsets)),
			leafPage:  leafPage,
		})
	case btree.InteriorTable:
		cells, err := leafPage.ReadAllTableInteriorCells()
		if err != nil {
			log.Fatal("Failed to get associated interior cells")
		}
		for _, cell := range cells {
			out = append(out, walkTableLeafPages(db, int64(cell.LeftPageNumber), cell.Rowid)...)
		}
		// Handle interior page's header
		out = append(out, walkTableLeafPages(db, int64(leafPage.Header.RightMostPointer), 0)...)
	default:
		log.Fatalf("Unsupported page type %#x", leafPage.Header.PageType)
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
func (t *DBTable) rows(where map[string]string, eligibleIndex *DBIndex, conditionAsPrefix string) []Row {
	out := []Row{}
	// using primary key to walk.
	if cond, ok := where["id"]; ok == true {
		// use selectByRowId
		fmt.Fprintf(os.Stderr, "[dbg] Selecting id= %s\n", cond)
		condValues := strings.Split(cond, ",")
		rowIds := make([]int64, len(condValues))
		for c, clause := range condValues {
			x, err := strconv.ParseInt(strings.Trim(clause, "' "), 10, 64)
			if err != nil {
				fmt.Fprintf(os.Stderr, "[dbg] parse clause failed: %s\n", err.Error())
			}
			rowIds[c] = x
		}
		found, err := t.SelectRowsByIds(rowIds)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[dbg] read row failed: %s\n", err.Error())
		}
		if found != nil {
			// TODO: filter based on where condition.. (without id checks)
			out = append(out, found...)
			return out
		}
	}
	// Otherwise try using eligibleIndex first.
	if eligibleIndex != nil {
		rowIds := eligibleIndex.IndexScan(&where, conditionAsPrefix)
		found, err := t.SelectRowsByIds(rowIds)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[dbg] read row failed: %s\n", err.Error())
		}
		if found != nil {
			// TODO: filter based on where condition.. (without id checks)
			return found
		}
	}

	// Using Full Table Scan
	for _, page := range t.btreePages {
		for c := 0; c < len(page.leafPage.CellOffsets); c++ {
			cell, err := page.leafPage.ReadTableLeafCell(c, t.rowIdAliasColIndex)
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
func (t *DBTable) SelectRowsByIds(rowIds []int64) ([]Row, error) {
	out := []Row{}
	if len(rowIds) == 0 {
		return out, nil
	}
	start := time.Now()
	sl := NewSearchList(rowIds)
	err := walkThroughBTreeForRowId(t.db, int64(t.rootPage), sl)
	if err != nil {
		return nil, err
	}
	for _, cell := range sl.result {
		if cell != nil {
			out = append(out, Row{
				cell:  cell,
				table: t,
			})
		}
	}
	elapsed := time.Since(start)
	fmt.Fprintf(os.Stderr, "[dbg] search for %d rowid Elapsed time: %s\n", len(rowIds), elapsed)
	return out, nil
}

// Simple Equal comparison bruteforce!
func (t *DBTable) applyFilter(condition *map[string]string, cell *btree.TableBTreeLeafTablePageCell) bool {
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
