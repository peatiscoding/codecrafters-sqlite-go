package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/rqlite/sql"
)

const HEADER_SIZE = 100

// The object the represent the whole file.
type Db struct {
	pageSize uint16
	schemas  []*Schema // should be indices by type (e.g. indices, triggers, views).
	tables   map[string]*DBTable
	file     *os.File
}

func NewDb(databaseFilePath string) (*Db, error) {

	databaseFile, err := os.Open(databaseFilePath)
	if err != nil {
		log.Fatal(err)
	}

	header := make([]byte, 100)

	_, err = databaseFile.Read(header) // read equal to its size
	if err != nil {
		log.Fatal(err)
	}

	var pageSize uint16
	if err := binary.Read(bytes.NewReader(header[16:18]), binary.BigEndian, &pageSize); err != nil {
		log.Fatal("Failed to read integer:", err)
		return nil, err
	}
	// You can use print statements as follows for debugging, they'll be visible when running tests.

	// Parse first page for items
	pageContent := make([]byte, pageSize-HEADER_SIZE) // first page offset by 100
	_, err = databaseFile.ReadAt(pageContent, HEADER_SIZE)
	if err != nil {
		log.Fatal(err)
	}
	btreePage, err := parseBTreePage(pageContent, true)
	if err != nil {
		log.Fatal(err)
	}
	schemas := make([]*Schema, len(btreePage.cellOffsets))
	tables := map[string]*DBTable{}
	db := Db{
		pageSize: pageSize,
		schemas:  schemas,
		tables:   tables,
		file:     databaseFile,
	}

	for row := range (*btreePage).cellOffsets {
		cell, err := btreePage.readLeafCell(row, 0)
		if err != nil {
			log.Fatal(err)
		}
		sch := NewSchema(cell)
		schemas[row] = sch

		// additional initialization beyond reading simple schema record.
		stmt, err := sql.NewParser(strings.NewReader(sch.sql)).ParseStatement()
		switch stmt.(type) {
		case *sql.CreateTableStatement:
			if sch.schemaType != Table {
				return nil, errors.New(fmt.Sprintf("Invalid SQL statement: %s. Expected different SQL for %d type", sch.sql, sch.schemaType))
			}
			tableSpec := stmt.(*sql.CreateTableStatement)
			tables[sch.name] = NewDBTable(&db, sch, tableSpec)
		case *sql.CreateIndexStatement:
			if sch.schemaType != Index {
				return nil, errors.New(fmt.Sprintf("Invalid SQL statement: %s. Expected different SQL for %d type", sch.sql, sch.schemaType))
			}
			indexSpec := stmt.(*sql.CreateIndexStatement)
			// Create new Index
			NewDbIndex(&db, sch, indexSpec)
		}
	}

	return &db, nil
}

// @param pageIndex = pageNo - 1
func (d *Db) readPage(pageIndex int64) *TableBTreePage {
	// assert pageNumber > 0
	pageContent := make([]byte, d.pageSize)
	_, err := d.file.ReadAt(pageContent, int64(d.pageSize)*pageIndex)
	if err != nil {
		log.Fatal(err)
	}
	// fmt.Fprintf(os.Stderr, "[dbg] reading page (index) %d\n", pageIndex)
	btreePage, err := parseBTreePage(pageContent, false)
	if err != nil {
		log.Fatal(err)
	}
	// number of cells
	return btreePage
}
