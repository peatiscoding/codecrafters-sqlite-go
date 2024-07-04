package btree

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
	I0                                 = 8 // False
	I1                                 = 9 // True
	BLOB                               = 12
	STRING                             = 13
)

type TableBTreeLeafPageCellField struct {
	serialType   BTreeLeafPageCellSerialType
	contentSize  int64
	isRowIdAlias bool
	data         []byte
}

type TableBTreeLeafTablePageCell struct {
	payloadSize  int64
	Rowid        int64
	Fields       []TableBTreeLeafPageCellField
	overflowPage int32
}

type IndexPayload interface {
	Fields() []TableBTreeLeafPageCellField
	// FIXME Use leftPageNumber?
}

type TableBTreeLeafIndexPageCell struct {
	payloadSize  int64
	fields       []TableBTreeLeafPageCellField
	overflowPage int32
	IndexStrain  string // the fields concaternated logically.
}

type TableBTreeInteriorPageCell struct {
	Rowid          int64
	LeftPageNumber uint32 // 4 bytes integer (8x4 = 32)
}

type TableBTreeIndexInteriorPageCell struct {
	MaxIndexStrain string
	payloadSize    int64
	fields         []TableBTreeLeafPageCellField
	LeftPageNumber uint32
	overflowPage   int32
}

func (iipc *TableBTreeIndexInteriorPageCell) Fields() []TableBTreeLeafPageCellField {
	return iipc.fields
}

func (iipc *TableBTreeLeafIndexPageCell) Fields() []TableBTreeLeafPageCellField {
	return iipc.fields
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
