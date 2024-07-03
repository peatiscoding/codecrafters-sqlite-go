package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
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
	I0                                 = 8 // False
	I1                                 = 9 // True
	BLOB                               = 12
	STRING                             = 13
)

type TableBTreePageHeader struct {
	pageType                    BTreePageType
	freeBlocksOffset            int16
	numberOfCells               int16
	cellContentOffset           int16
	numberOfFragmentedFreeBytes int8
	rightMostPointer            uint32 // only available in BTreePage
}

type TableBTreePage struct {
	header      TableBTreePageHeader
	cellOffsets []int16
	pageContent []byte // original pageContent
}

type TableBTreeLeafPageCellField struct {
	serialType   BTreeLeafPageCellSerialType
	contentSize  int64
	isRowIdAlias bool
	data         []byte
}

type TableBTreeLeafTablePageCell struct {
	payloadSize  int64
	rowid        int64
	fields       []TableBTreeLeafPageCellField
	overflowPage int32
}

type TableBTreeLeafIndexPageCell struct {
	payloadSize  int64
	fields       []TableBTreeLeafPageCellField
	overflowPage int32
	indexStrain  string // the fields concaternated logically.
}

type TableBTreeInteriorPageCell struct {
	rowid          int64
	leftPageNumber uint32 // 4 bytes integer (8x4 = 32)
}

type TableBTreeIndexInteriorPageCell struct {
	maxIndexStrain string
	payloadSize    int64
	fields         []TableBTreeLeafPageCellField
	leftPageNumber uint32
	overflowPage   int32
}

func (f *TableBTreeLeafPageCellField) String() string {
	switch f.serialType {
	case STRING:
		return string(f.data)
	case Null:
		return "<null>"
	case I0:
		return "0"
	case I1:
		return "1"
	default:
		i64 := f.Integer()
		return fmt.Sprintf("%d", i64)
	}
}

func (f *TableBTreeLeafPageCellField) Integer() int64 {
	// This code would be easier for compiler to optimize?
	switch f.serialType {
	case I8:
		return int64(f.data[0]) // size = 1 byte
	case I16:
		return int64(f.data[0])<<8 | int64(f.data[1]) // size = 2 bytes
	case I24:
		return int64(f.data[0])<<16 | int64(f.data[1])<<8 | int64(f.data[2]) // size = 3 bytes
	case I32:
		return int64(f.data[0])<<24 | int64(f.data[1])<<16 | int64(f.data[2])<<8 | int64(f.data[3]) // size = 4 bytes
	case I48:
		return int64(f.data[0])<<40 | int64(f.data[1])<<32 | int64(f.data[2])<<24 | int64(f.data[3])<<16 | int64(f.data[4])<<8 | int64(f.data[5]) // size = 6 bytes
	case I64:
		return int64(f.data[0])<<56 | int64(f.data[1])<<48 | int64(f.data[2])<<40 | int64(f.data[3])<<32 | int64(f.data[4])<<24 | int64(f.data[5])<<16 | int64(f.data[6])<<8 | int64(f.data[7]) // size = 8 bytes
	}
	var i64 = int64(0)
	reader := bytes.NewReader(f.data)
	binary.Read(reader, binary.BigEndian, &i64)
	return i64
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

	if InteriorTable == pageType || InteriorIndex == pageType {
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

// Reading "Cell Content"
// ===

// this is basically SELECT * FROM TABLE
//
// this consider as a Single Row. (in TableLeafPage)
//
// * A varint which is the total number of bytes of payload, including any overflow
// * A varint which is the integer key, a.k.a. "rowid"
// * The initial portion of the payload that does not spill to overflow pages.
// * A 4-byte big-endian integer page number for the first page of the overflow page list - omitted if all payload fits on the b-tree page. (FIXME)
func (p *TableBTreePage) readTableLeafCell(cellIndex int, rowidAliasIndex int) (*TableBTreeLeafTablePageCell, error) {
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
	content, err := parseCellRecordFormat(reader, payloadSize)
	if err != nil {
		return nil, err
	}

	if rowidAliasIndex >= 0 {
		content[rowidAliasIndex].isRowIdAlias = true
	}

	return &TableBTreeLeafTablePageCell{
		payloadSize:  payloadSize,
		rowid:        rowid,
		fields:       content,
		overflowPage: 0,
	}, nil
}

func (p *TableBTreePage) readIndexLeafCell(cellIndex int) (*TableBTreeLeafIndexPageCell, error) {
	contentOffset := p.cellOffsets[cellIndex]
	reader := bytes.NewReader(p.pageContent[contentOffset:])
	payloadSize, _, err := ReadVarint(reader) // including its' corresponding headers
	if err != nil {
		return nil, err
	}
	content, err := parseCellRecordFormat(reader, payloadSize)
	if err != nil {
		return nil, err
	}
	var b strings.Builder
	for c, cnt := range content {
		if c > 0 {
			b.WriteString("|")
		}
		b.WriteString(cnt.String())
		// b.WriteString(fmt.Sprintf("(%d|%d=%s", d, cnt.serialType, cnt.String()))
	}

	return &TableBTreeLeafIndexPageCell{
		payloadSize:  payloadSize,
		fields:       content,
		indexStrain:  b.String(),
		overflowPage: 0,
	}, nil
}

// this consider as a Single Row. (in TableInteriorPage))
// * A 4-byte big-endian page number which is the left child pointer.
// * A varint which is the integer key
func (p *TableBTreePage) readAllTableInteriorCells() ([]TableBTreeInteriorPageCell, error) {
	res := make([]TableBTreeInteriorPageCell, len(p.cellOffsets))
	for j, cellOffset := range p.cellOffsets {
		cell, err := p.readTableInteriorCell(int(cellOffset))
		if err != nil {
			return nil, err
		}
		res[j] = *cell
	}
	return res, nil
}

func (p *TableBTreePage) readTableInteriorCell(cellOffset int) (*TableBTreeInteriorPageCell, error) {
	var i32 = uint32(0) // 4 bytes
	i32Reader := bytes.NewReader(p.pageContent[cellOffset : cellOffset+4])
	binary.Read(i32Reader, binary.BigEndian, &i32)

	reader := bytes.NewReader(p.pageContent[cellOffset+4:])
	rowid, _, err := ReadVarint(reader)
	if err != nil {
		return nil, err
	}
	return &TableBTreeInteriorPageCell{
		rowid:          rowid,
		leftPageNumber: i32,
	}, nil
}

// Each Cell (RecordFormat)
// - A 4-byte big-endian page number which is the left child pointer.
// - A varint which is the total number of bytes of key payload, including any overflow
// - The initial portion of the payload that does not spill to overflow pages.
// - A 4-byte big-endian integer page number for the first page of the overflow page list - omitted if all payload fits on the b-tree page. (FIXME)
func (p *TableBTreePage) readAllIndexInteriorCells() ([]TableBTreeIndexInteriorPageCell, error) {
	res := make([]TableBTreeIndexInteriorPageCell, len(p.cellOffsets))
	for j, cellOffset := range p.cellOffsets {

		// pageNumber of Left Child
		var leftPageNumber = uint32(0) // 4 bytes
		i32Reader := bytes.NewReader(p.pageContent[cellOffset : cellOffset+4])
		binary.Read(i32Reader, binary.BigEndian, &leftPageNumber)

		// total bytes
		reader := bytes.NewReader(p.pageContent[cellOffset+4:])
		payloadSize, _, err := ReadVarint(reader)
		if err != nil {
			return nil, err
		}
		// read payloads based on Payload Size
		content, err := parseCellRecordFormat(reader, payloadSize)
		var b strings.Builder
		for _, cnt := range content {
			b.WriteString(cnt.String())
			// b.WriteString(fmt.Sprintf("(%d|%d=%s", d, cnt.serialType, cnt.String()))
		}
		res[j] = TableBTreeIndexInteriorPageCell{
			leftPageNumber: leftPageNumber,
			fields:         content,
			payloadSize:    payloadSize,
			maxIndexStrain: b.String(),
			overflowPage:   0,
		}
	}
	return res, nil
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
	case 8:
		return I0, 0, nil // = 0
	case 9:
		return I1, 0, nil // = 1
	}
	if rawSerialType >= 12 && rawSerialType&1 == 0 {
		return BLOB, (rawSerialType - 12) / 2, nil
	}
	if rawSerialType >= 13 && rawSerialType&1 == 1 {
		return STRING, (rawSerialType - 13) / 2, nil
	}
	return Null, 0, errors.New(fmt.Sprintf("unsupported serial type %d", rawSerialType))
}

func parseCellRecordFormat(reader *bytes.Reader, payloadSize int64) ([]TableBTreeLeafPageCellField, error) {
	readBytes := int64(0)
	headerTotalBytes, n, err := ReadVarint(reader)
	if err != nil {
		return nil, err
	}
	readBytes += int64(n)
	out := make([]TableBTreeLeafPageCellField, headerTotalBytes) // will never exceed this totalBytes anyway.
	fieldsCount := 0
	// Parse Cell Header
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
			serialType:   serialType,
			contentSize:  contentSize,
			isRowIdAlias: false,
			data:         []byte{},
		}
		fieldsCount += 1
	}

	// Parse Cell's value
	for j := 0; j < fieldsCount; j++ {
		proto := out[j]
		if proto.contentSize <= 0 {
			continue
		}
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
