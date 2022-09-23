package kv_store_base

type DkvBasePacket interface {
	ToBytes() []byte
	DeserializeFromReader(reader *ByteReader)
	SerializeToWriter(writer *ByteWriter)
}
