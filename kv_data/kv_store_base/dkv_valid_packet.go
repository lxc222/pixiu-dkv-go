package kv_store_base

import (
	"pixiu-dkv-go/dkv_conf"
)

type DkvValidPacket struct {
	IsValid bool
}

func (m *DkvValidPacket) DeserializeFromReader(reader *ByteReader) {
	var flag = reader.NextByte()
	if dkv_conf.ByteOne == flag {
		m.IsValid = true
	} else {
		m.IsValid = false
	}
}

func (m *DkvValidPacket) SerializeToWriter(writer *ByteWriter) {
	if m.IsValid {
		writer.AppendByte(dkv_conf.ByteOne)
	} else {
		writer.AppendByte(dkv_conf.ByteZero)
	}
}

func (m *DkvValidPacket) ToBytes() []byte {
	var writer = CreateByteWriter(256)
	m.SerializeToWriter(writer)
	return writer.GetByteData()
}
