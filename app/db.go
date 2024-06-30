package main

import (
	"bytes"
	"encoding/binary"
	"log"
	"os"
)

type Db struct {
	pageSize uint16
	schemas  []Schema
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
	pageContent := make([]byte, pageSize-100) // first page offset by 100
	_, err = databaseFile.ReadAt(pageContent, 100)
	if err != nil {
		log.Fatal(err)
	}
	btreePage, err := parseBTreePage(pageContent, true)
	if err != nil {
		log.Fatal(err)
	}

	schemas := make([]Schema, len(btreePage.cellOffsets))
	for row := range (*btreePage).cellOffsets {
		cell, err := btreePage.readCell(row)
		if err != nil {
			log.Fatal(err)
		}
		schemas[row] = *fromBTreeCell(cell)
	}

	return &Db{
		pageSize: pageSize,
		schemas:  schemas,
	}, nil
}
