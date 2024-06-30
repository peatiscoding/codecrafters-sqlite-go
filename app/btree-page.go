package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
)

type BTreePageType = int8
type BTreeLeafPageCellSerialType = int8

const (
	InteriorIndex BTreePageType = 0x02
	InteriorTable               = 0x05
	LeafIndex                   = 0x0a
	LeafTable                   = 0x0d
)

const (
	Null   BTreeLeafPageCellSerialType = 0
	I8                                 = 1
	I16                                = 2
	I24                                = 3
	I32                                = 4
	I48                                = 5
	I64                                = 6
	F64                                = 7
	BLOB                               = 12
	STRING                             = 13
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

type TableBTreeLeafPageCellField struct {
	serialType  BTreeLeafPageCellSerialType
	contentSize int64
	data        []byte
}

type TableBTreeLeafPageCell struct {
	payloadSize  int64
	rowid        int64
	fields       []TableBTreeLeafPageCellField
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

// this is basically SELECT * FROM TABLE/
func (p *TableBTreePage) readCell(cellIndex int) (*TableBTreeLeafPageCell, error) {
	contentOffset := p.cellOffsets[cellIndex]
	reader := bytes.NewReader(p.pageContent[contentOffset:])
	payloadSize, _, err := ReadVarint(reader) // including its' corresponding headers
	if err != nil {
		return nil, err
	}
	rowid, _, err := ReadVarint(reader)
	if err != nil {
		return nil, err
	}
	content, err := parseCellContentRecordHeadAndContent(reader, payloadSize)
	if err != nil {
		return nil, err
	}

	return &TableBTreeLeafPageCell{
		payloadSize:  payloadSize,
		rowid:        rowid,
		fields:       content,
		overflowPage: 0,
	}, nil
}

func mapSerialType(rawSerialType int64) (BTreeLeafPageCellSerialType, int64, error) {
	switch rawSerialType {
	case 0:
		return Null, 0, nil
	case 1:
		return I8, 1, nil
	case 2:
		return I16, 2, nil
	case 3:
		return I24, 3, nil
	case 4:
		return I32, 4, nil
	case 5:
		return I48, 6, nil
	case 6:
		return I64, 8, nil
	case 7:
		return F64, 8, nil
	}
	if rawSerialType >= 12 && rawSerialType&1 == 0 {
		return BLOB, (rawSerialType - 12) / 2, nil
	}
	if rawSerialType >= 13 && rawSerialType&1 == 1 {
		return STRING, (rawSerialType - 13) / 2, nil
	}
	return Null, 0, errors.New(fmt.Sprintf("unsupported serial type %d", rawSerialType))
}

func parseCellContentRecordHeadAndContent(reader *bytes.Reader, payloadSize int64) ([]TableBTreeLeafPageCellField, error) {
	readBytes := int64(0)
	headerTotalBytes, n, err := ReadVarint(reader)
	if err != nil {
		return nil, err
	}
	readBytes += int64(n)
	out := make([]TableBTreeLeafPageCellField, headerTotalBytes) // will never exceed this totalBytes anyway.
	fieldsCount := 0
	for i := 0; readBytes < int64(headerTotalBytes); i++ {
		rawSerialType, n, err := ReadVarint(reader)
		if err != nil {
			return nil, err
		}
		readBytes += int64(n)
		serialType, contentSize, err := mapSerialType(rawSerialType)
		if err != nil {
			return nil, err
		}
		out[i] = TableBTreeLeafPageCellField{
			serialType:  serialType,
			contentSize: contentSize,
			data:        []byte{},
		}
		fieldsCount += 1
	}

	for j := 0; j < fieldsCount; j++ {
		proto := out[j]
		readSize := proto.contentSize
		readBytesLookAhead := readBytes + proto.contentSize
		// Is this correct? It seems some of the `contentSize` is overshoot the expected totalBytes to be read. Hence this
		// If block contain the unexpected overflow :(
		if readBytesLookAhead > payloadSize {
			// fmt.Printf("WARNING READ OVERFLOW by %d - %d = %d!\n", readBytesLookAhead, payloadSize, readBytesLookAhead-payloadSize)
			readSize -= (readBytesLookAhead - payloadSize)
			// fmt.Printf("Correction will read %d\n", readSize)
		}
		readBytes += readSize

		valueArr := make([]byte, readSize)
		_, err := reader.Read(valueArr)
		if err != nil {
			return nil, err
		}
		out[j].data = valueArr
	}
	return out[0:fieldsCount], nil
}
