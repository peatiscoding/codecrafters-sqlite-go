package btree

import (
	"bytes"
	"encoding/binary"
	"strings"
)

type TableBTreePageHeader struct {
	PageType                    BTreePageType
	freeBlocksOffset            int16
	NumberOfCells               int16
	cellContentOffset           int16
	numberOfFragmentedFreeBytes int8
	RightMostPointer            uint32 // only available in BTreePage
}

type TableBTreePage struct {
	Header      TableBTreePageHeader
	CellOffsets []int16
	pageContent []byte // original pageContent
}

func ParseBTreePage(pageContent []byte, isFirstPage bool) (*TableBTreePage, error) {
	// get first byte for determine the type.
	pageType := int8(pageContent[0])
	numberOfFragmentedFreeBytes := int8(pageContent[7])
	params := [3]int16{0, 0, 0}
	if err := binary.Read(bytes.NewReader(pageContent[1:7]), binary.BigEndian, &params); err != nil {
		return nil, err
	}

	header := &TableBTreePageHeader{
		PageType:                    pageType,
		freeBlocksOffset:            params[0],
		NumberOfCells:               params[1],
		cellContentOffset:           params[2],
		numberOfFragmentedFreeBytes: numberOfFragmentedFreeBytes,
		RightMostPointer:            0, // optional
	}
	cellPointsArrayOffset := int16(8)

	if InteriorTable == pageType || InteriorIndex == pageType {
		if err := binary.Read(bytes.NewReader(pageContent[8:12]), binary.BigEndian, &(header.RightMostPointer)); err != nil {
			return nil, err
		}
		cellPointsArrayOffset = 12
	}

	cellOffsets := make([]int16, header.NumberOfCells)
	if err := binary.Read(bytes.NewReader(pageContent[cellPointsArrayOffset:cellPointsArrayOffset+2*header.NumberOfCells]), binary.BigEndian, &cellOffsets); err != nil {
		return nil, err
	}

	// in case of first page, need to compensate the header offsets.
	if isFirstPage {
		for i := 0; i < len(cellOffsets); i++ {
			cellOffsets[i] -= int16(100)
		}
	}

	return &TableBTreePage{
		Header:      *header,
		CellOffsets: cellOffsets,
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
func (p *TableBTreePage) ReadTableLeafCell(cellIndex int, rowidAliasIndex int) (*TableBTreeLeafTablePageCell, error) {
	contentOffset := p.CellOffsets[cellIndex]
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
		Rowid:        rowid,
		Fields:       content,
		overflowPage: 0,
	}, nil
}

func (p *TableBTreePage) ReadIndexLeafCell(cellIndex int) (*TableBTreeLeafIndexPageCell, error) {
	contentOffset := p.CellOffsets[cellIndex]
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
		IndexStrain:  b.String(),
		overflowPage: 0,
	}, nil
}

// this consider as a Single Row. (in TableInteriorPage))
// * A 4-byte big-endian page number which is the left child pointer.
// * A varint which is the integer key
func (p *TableBTreePage) ReadAllTableInteriorCells() ([]TableBTreeInteriorPageCell, error) {
	res := make([]TableBTreeInteriorPageCell, len(p.CellOffsets))
	for j, cellOffset := range p.CellOffsets {
		cell, err := p.ReadTableInteriorCell(int(cellOffset))
		if err != nil {
			return nil, err
		}
		res[j] = *cell
	}
	return res, nil
}

// this consider as a Single Row. (in TableInteriorPage))
// * A 4-byte big-endian page number which is the left child pointer.
// * A varint which is the integer key
func (p *TableBTreePage) ReadTableInteriorCell(cellOffset int) (*TableBTreeInteriorPageCell, error) {
	var i32 = uint32(0) // 4 bytes
	i32Reader := bytes.NewReader(p.pageContent[cellOffset : cellOffset+4])
	binary.Read(i32Reader, binary.BigEndian, &i32)

	reader := bytes.NewReader(p.pageContent[cellOffset+4:])
	rowid, _, err := ReadVarint(reader)
	if err != nil {
		return nil, err
	}
	return &TableBTreeInteriorPageCell{
		Rowid:          rowid,
		LeftPageNumber: i32,
	}, nil
}

// Read single Cell (RecordFormat)
// - A 4-byte big-endian page number which is the left child pointer.
// - A varint which is the total number of bytes of key payload, including any overflow
// - The initial portion of the payload that does not spill to overflow pages.
// - A 4-byte big-endian integer page number for the first page of the overflow page list - omitted if all payload fits on the b-tree page. (FIXME)
func (p *TableBTreePage) ReadIndexInteriorCell(cellOffset int16) (*TableBTreeIndexInteriorPageCell, error) {
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
	for c, cnt := range content {
		if c > 0 {
			b.WriteString("|")
		}
		b.WriteString(cnt.String())
		// b.WriteString(fmt.Sprintf("(%d|%d=%s", d, cnt.serialType, cnt.String()))
	}
	return &TableBTreeIndexInteriorPageCell{
		LeftPageNumber: leftPageNumber,
		fields:         content,
		payloadSize:    payloadSize,
		MaxIndexStrain: b.String(),
		overflowPage:   0,
	}, nil
}
