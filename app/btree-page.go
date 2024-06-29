package main

import (
	"bytes"
	"encoding/binary"
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
}

func parseBTreePage(pageContent []byte) (*TableBTreePage, error) {
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

	return &TableBTreePage{
		header:      *header,
		cellOffsets: cellOffsets,
	}, nil
}
