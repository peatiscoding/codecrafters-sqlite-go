package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	// Available if you need it!
	// "github.com/xwb1989/sqlparser"
)

// func parseTableBTreePage(content []byte)

// Usage: your_sqlite3.sh sample.db .dbinfo
func main() {
	databaseFilePath := os.Args[1]
	command := os.Args[2]

	switch command {
	case ".dbinfo":
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
			fmt.Println("Failed to read integer:", err)
			return
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
		fmt.Printf("database page size: %v\n", btreePage.cellOffsets)

		for i := range (*btreePage).cellOffsets {
			_, err := btreePage.readCell(i)
			if err != nil {
				log.Fatal(err)
			}
		}

	default:
		fmt.Println("Unknown command", command)
		os.Exit(1)
	}
}
