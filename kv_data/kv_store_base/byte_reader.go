package kv_store_base

import (
	"bytes"
	"encoding/binary"
	"pixiu-dkv-go/dkv_tool/str_tool"
)

type ByteReader struct {
	CurrentIdx int32
	ByteData   []byte
}

func (r *ByteReader) NextByte() byte {
	var rst = r.ByteData[r.CurrentIdx]
	r.CurrentIdx = r.CurrentIdx + 1
	return rst
}

func (r *ByteReader) NextBytes(size int32) []byte {
	var rst = r.ByteData[r.CurrentIdx : r.CurrentIdx+size]
	r.CurrentIdx = r.CurrentIdx + size
	return rst
}

func (r *ByteReader) NextInt16() int16 {
	var rst int16 = 0
	bBuf := bytes.NewBuffer(r.ByteData[r.CurrentIdx : r.CurrentIdx+2])
	binary.Read(bBuf, binary.BigEndian, &rst)
	r.CurrentIdx = r.CurrentIdx + 2
	return rst
}

func (r *ByteReader) NextInt32() int32 {
	var rst int32 = 0
	bBuf := bytes.NewBuffer(r.ByteData[r.CurrentIdx : r.CurrentIdx+4])
	binary.Read(bBuf, binary.BigEndian, &rst)
	r.CurrentIdx = r.CurrentIdx + 4
	return rst
}

func (r *ByteReader) NextInt64() int64 {
	var rst int64 = 0
	bBuf := bytes.NewBuffer(r.ByteData[r.CurrentIdx : r.CurrentIdx+8])
	binary.Read(bBuf, binary.BigEndian, &rst)
	r.CurrentIdx = r.CurrentIdx + 8
	return rst
}

func (r *ByteReader) NextUInt64() uint64 {
	var rst uint64 = 0
	bBuf := bytes.NewBuffer(r.ByteData[r.CurrentIdx : r.CurrentIdx+8])
	binary.Read(bBuf, binary.BigEndian, &rst)
	r.CurrentIdx = r.CurrentIdx + 8
	return rst
}

func (r *ByteReader) NextStr() string {
	var strSize = r.NextInt32()
	var strBytes = r.NextBytes(strSize)
	return str_tool.Bytes2Str(strBytes)
}

func CreateByteReader(bytes []byte) *ByteReader {
	var reader = ByteReader{0, bytes}
	return &reader
}
