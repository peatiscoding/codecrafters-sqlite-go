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
func (i *DBIndex) SelectRange(condition *map[string]string, conditionAsPrefix string) []Row {
	out := []Row{}
	// assocTable := i.db.tables[i.assocTable]
	for _, page := range i.leafPages {
		for c := 0; c < len(page.leafPage.cellOffsets); c++ {
			cell, err := page.leafPage.readIndexLeafCell(c)
			if err != nil {
				fmt.Fprintf(os.Stderr, "[dbg] read row failed: %s\n", err.Error())
			}

			if strings.HasPrefix(cell.indexStrain, conditionAsPrefix) {
				// out = append(out, Row{
				// 	cell:  cell,
				// 	table: assocTable,
				// })
			}
		}
	}
	return out
}

func (i *DBIndex) CountRange(condition *map[string]string, conditionAsPrefix string) int {
	out := 0
	start := time.Now()
	evalCount := 0
	for _, page := range i.leafPages {
		if !strings.HasPrefix(page.maxIndexStrain, conditionAsPrefix) {
			// skip these pages
			continue
		}
		fmt.Fprintf(os.Stderr, "[dbg] comparing on page %s\n", page.maxIndexStrain)
		for c := 0; c < len(page.leafPage.cellOffsets); c++ {
			cell, err := page.leafPage.readIndexLeafCell(c)
			if err != nil {
				fmt.Fprintf(os.Stderr, "[dbg] read row failed: %s\n", err.Error())
			}
			evalCount += 1
			// TODO: This should ask assocTable to eval `condition` NOT using conditionAsPrefix.
			fmt.Fprintf(os.Stderr, "[dbg] comparing %s / %s\n", conditionAsPrefix, cell.indexStrain)
			if strings.HasPrefix(cell.indexStrain, conditionAsPrefix) {
				out += 1
			}
		}
	}
	elapsed := time.Since(start)
	fmt.Fprintf(os.Stderr, "[dbg] SelectCount prefix=%s evaled items_count=%d Elapsed time: %s\n", conditionAsPrefix, evalCount, elapsed)
	return out
}
