package kv_byte_str

import (
	"pixiu-dkv-go/kv_data/kv_store_base"
)

type DkvByteStr struct {
	kv_store_base.DkvValidPacket
	Value string
}

func (m *DkvByteStr) DeserializeFromReader(reader *kv_store_base.ByteReader) {
	m.DkvValidPacket.DeserializeFromReader(reader)
	m.Value = reader.NextStr()
}

func (m *DkvByteStr) SerializeToWriter(writer *kv_store_base.ByteWriter) {
	m.DkvValidPacket.SerializeToWriter(writer)
	writer.AppendStr(m.Value)
}

func (m *DkvByteStr) ToBytes() []byte {
	var writer = kv_store_base.CreateByteWriter(len(m.Value) + 64)
	m.SerializeToWriter(writer)
	return writer.GetByteData()
}

func CreateFromStr(isValid bool, v string) *DkvByteStr {
	var obj = DkvByteStr{}
	obj.IsValid = isValid
	if len(v) <= 0 {
		obj.Value = ""
	} else {
		obj.Value = v
	}

	return &obj
}

func CreateFromByte(v []byte) *DkvByteStr {
	if v == nil || len(v) <= 0 {
		return CreateFromStr(false, "")
	}

	var obj = DkvByteStr{}
	obj.DeserializeFromReader(kv_store_base.CreateByteReader(v))

	return &obj
}
