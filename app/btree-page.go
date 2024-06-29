package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

type BTreePageType = int8

const (
	InteriorIndex BTreePageType = 0x02
	InteriorTable               = 0x05
	LeafIndex                   = 0x0a
	LeafTable                   = 0x0d
)

type TableBTreePageHeader struct {
	pageType                    BTreePageType
	freeBlocksOffset            int16
	numberOfCells               int16
	cellContentOffset           int16
	numberOfFragmentedFreeBytes int8
	rightMostPointer            int32 // only available in BTreePage
}

type TableBTreePage struct {
	header      TableBTreePageHeader
	cellOffsets []int16
	pageContent []byte // original pageContent
}

type TableBTreeLeafPageCell struct {
	payloadSize  uint64
	rowid        uint64
	content      []byte
	overflowPage int32
}

func parseBTreePage(pageContent []byte, isFirstPage bool) (*TableBTreePage, error) {
	// get first byte for determine the type.
	pageType := int8(pageContent[0])
	numberOfFragmentedFreeBytes := int8(pageContent[7])
	params := [3]int16{0, 0, 0}
	if err := binary.Read(bytes.NewReader(pageContent[1:7]), binary.BigEndian, &params); err != nil {
		return nil, err
	}

	header := &TableBTreePageHeader{
		pageType:                    pageType,
		freeBlocksOffset:            params[0],
		numberOfCells:               params[1],
		cellContentOffset:           params[2],
		numberOfFragmentedFreeBytes: numberOfFragmentedFreeBytes,
		rightMostPointer:            0, // optional
	}
	cellPointsArrayOffset := int16(8)

	if InteriorIndex == pageType {
		header.rightMostPointer = 0
		if err := binary.Read(bytes.NewReader(pageContent[8:12]), binary.BigEndian, &(header.rightMostPointer)); err != nil {
			return nil, err
		}
		cellPointsArrayOffset = 12
	}

	cellOffsets := make([]int16, header.numberOfCells)
	if err := binary.Read(bytes.NewReader(pageContent[cellPointsArrayOffset:cellPointsArrayOffset+2*header.numberOfCells]), binary.BigEndian, &cellOffsets); err != nil {
		return nil, err
	}

	// in case of first page, need to compensate the header offsets.
	if isFirstPage {
		for i := 0; i < len(cellOffsets); i++ {
			cellOffsets[i] -= int16(100)
		}
	}

	return &TableBTreePage{
		header:      *header,
		cellOffsets: cellOffsets,
		pageContent: pageContent,
	}, nil
}

func (p *TableBTreePage) readCell(cellIndex int) (*TableBTreeLeafPageCell, error) {
	contentOffset := p.cellOffsets[cellIndex]
	reader := bytes.NewReader(p.pageContent[contentOffset:])
	payloadSize, _, err := ReadVarint(reader)
	if err != nil {
		return nil, err
	}
	rowid, _, err := ReadVarint(reader)
	if err != nil {
		return nil, err
	}
	content := make([]byte, payloadSize)
	if err := binary.Read(reader, binary.BigEndian, &content); err != nil {
		return nil, err
	}
	fmt.Printf("Reading on %d found %d %d %s\n", contentOffset, payloadSize, rowid, string(content))

	return &TableBTreeLeafPageCell{
		payloadSize:  payloadSize,
		rowid:        rowid,
		content:      content,
		overflowPage: 0,
	}, nil
}
