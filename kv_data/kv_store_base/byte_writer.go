package kv_store_base

import (
	"bytes"
	"encoding/binary"
	"pixiu-dkv-go/dkv_tool/str_tool"
)

type ByteWriter struct {
	byteData []byte
}

func (r *ByteWriter) GetByteData() []byte {
	return r.byteData
}

func (r *ByteWriter) AppendByte(value byte) {
	r.byteData = append(r.byteData, value)
}

func (r *ByteWriter) AppendBytes(value []byte) {
	r.byteData = append(r.byteData, value...)
}

func (r *ByteWriter) AppendInt16(value int16) {
	bBuf := bytes.NewBuffer([]byte{})
	_ = binary.Write(bBuf, binary.BigEndian, value)
	r.byteData = append(r.byteData, bBuf.Bytes()...)
}

func (r *ByteWriter) AppendInt32(value int32) {
	bBuf := bytes.NewBuffer([]byte{})
	_ = binary.Write(bBuf, binary.BigEndian, value)
	r.byteData = append(r.byteData, bBuf.Bytes()...)
}

func (r *ByteWriter) AppendInt64(value int64) {
	bBuf := bytes.NewBuffer([]byte{})
	_ = binary.Write(bBuf, binary.BigEndian, value)
	r.byteData = append(r.byteData, bBuf.Bytes()...)
}

func (r *ByteWriter) AppendUInt64(value uint64) {
	bBuf := bytes.NewBuffer([]byte{})
	_ = binary.Write(bBuf, binary.BigEndian, value)
	r.byteData = append(r.byteData, bBuf.Bytes()...)
}

func (r *ByteWriter) AppendStr(value string) {
	if str_tool.StrIsEmpty(value) {
		r.AppendInt32(0)
	} else {
		r.AppendInt32(int32(len(value)))
		r.AppendBytes(str_tool.Str2Bytes(value))
	}
}

func CreateByteWriter(capacity int) *ByteWriter {
	var writer = ByteWriter{make([]byte, 0, capacity)}
	return &writer
}
