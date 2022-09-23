package kv_data_meta

import (
	"errors"
	"math"
	"pixiu-dkv-go/dkv_conf"
	"pixiu-dkv-go/dkv_tool/common_tool"
	"pixiu-dkv-go/dkv_tool/str_tool"
	"pixiu-dkv-go/kv_data/kv_store_base"
	"pixiu-dkv-go/store_engine/badger_store"
)

const (
	KmStr                    byte = 1
	KmSet                    byte = 2
	KmHash                   byte = 3
	KmList                   byte = 5
	KM_FIRST_BYTE_SIMPLE     byte = 5
	KM_FIRST_BYTE_SET_MEMBER byte = 11
	KmFirstByteListIndex     byte = 20
	KmFirstByteHashField     byte = 31
	KmFirstByteKeyMeta       byte = 111
)

type DkvKeyMeta struct {
	kv_store_base.DkvValidPacket
	Type    byte  // data type
	Version int64 // date version
	Size    int32 // hash=>field size
	Expire  int64 // expire
	LIndex  int64
	RIndex  int64
	Value   string
}

func (m *DkvKeyMeta) DeserializeFromReader(reader *kv_store_base.ByteReader) {
	m.DkvValidPacket.DeserializeFromReader(reader)
	m.Type = reader.NextByte()
	m.Version = reader.NextInt64()
	m.Size = reader.NextInt32()
	m.Expire = reader.NextInt64()
	m.LIndex = reader.NextInt64()
	m.RIndex = reader.NextInt64()
	m.Value = reader.NextStr()
}

func (m *DkvKeyMeta) SerializeToWriter(writer *kv_store_base.ByteWriter) {
	m.DkvValidPacket.SerializeToWriter(writer)
	writer.AppendByte(m.Type)
	writer.AppendInt64(m.Version)
	writer.AppendInt32(m.Size)
	writer.AppendInt64(m.Expire)
	writer.AppendInt64(m.LIndex)
	writer.AppendInt64(m.RIndex)
	writer.AppendStr(m.Value)
}

func (m *DkvKeyMeta) ToBytes() []byte {
	var writer = kv_store_base.CreateByteWriter(len(m.Value) + 64)
	m.SerializeToWriter(writer)
	return writer.GetByteData()
}

func (m *DkvKeyMeta) IsExpired() bool {
	if m == nil {
		return false
	} else if m.Expire <= 0 {
		return false
	} else if m.Expire <= common_tool.CurrentMS() {
		return true
	} else {
		return false
	}
}

func (m *DkvKeyMeta) IsTypeStr() bool {
	return KmStr == m.Type
}

func (m *DkvKeyMeta) IsTypeSet() bool {
	return KmSet == m.Type
}

func (m *DkvKeyMeta) IsTypeList() bool {
	return KmList == m.Type
}

func (m *DkvKeyMeta) IsTypeHash() bool {
	return KmHash == m.Type
}

func CreateKeyMeta(dataType byte, version int64) *DkvKeyMeta {
	var data = DkvKeyMeta{}
	data.IsValid = true
	data.Type = dataType
	data.Version = version
	data.Size = 0
	data.Expire = -1
	data.LIndex = math.MaxInt64 / 2   //待写入的下一个位置
	data.RIndex = math.MaxInt64/2 + 1 //待写入的下一个位置
	data.Value = ""

	return &data
}

func CreateKeyMetaWithExpire(dataType byte, version int64, absExpTimeMillis int64) *DkvKeyMeta {
	var data = DkvKeyMeta{}
	data.IsValid = true
	data.Type = dataType
	data.Version = version
	data.Size = 0
	data.Expire = absExpTimeMillis
	data.LIndex = math.MaxInt64 / 2
	data.RIndex = math.MaxInt64/2 + 1
	data.Value = ""

	return &data
}

func CreateTypeStr(expireSecond int64) *DkvKeyMeta {
	if expireSecond <= 0 {
		return CreateKeyMeta(KmStr, dkv_conf.GetSysConf().GetNextDataVersion())
	} else {
		return CreateKeyMetaWithExpire(KmStr, dkv_conf.GetSysConf().GetNextDataVersion(),
			common_tool.CurrentMS()+1000*expireSecond)
	}
}

func CreateTypeSet() *DkvKeyMeta {
	return CreateKeyMeta(KmSet, dkv_conf.GetSysConf().GetNextDataVersion())
}

func CreateTypeHash() *DkvKeyMeta {
	return CreateKeyMeta(KmHash, dkv_conf.GetSysConf().GetNextDataVersion())
}

func CreateTypeList() *DkvKeyMeta {
	return CreateKeyMeta(KmList, dkv_conf.GetSysConf().GetNextDataVersion())
}

func IsExpiredThenRemove(keyMeta *DkvKeyMeta, key string) bool {
	if keyMeta == nil {
		return true
	}

	if !keyMeta.IsValid || keyMeta.IsExpired() {
		RemoveKeyMeta(key)
		return true
	} else {
		return false
	}
}

func LoadKeyMeta(key string) *DkvKeyMeta {
	if str_tool.StrIsEmpty(key) {
		return nil
	}

	var keyBytes = StrKey2KeyMetaBytes(key)
	var value, err = badger_store.GetKvStoreEngine().RawGet(keyBytes)
	if err != nil || str_tool.ByteIsEmpty(value) {
		var keyMeta = CreateKeyMeta(0, -1)
		keyMeta.IsValid = false
		return keyMeta
	} else {
		var keyMeta = CreateKeyMeta(0, -1)
		keyMeta.DeserializeFromReader(kv_store_base.CreateByteReader(value))
		if !keyMeta.IsValid || keyMeta.IsExpired() {
			RemoveKeyMeta(key)
			keyMeta.IsValid = false
			keyMeta.Type = 0
		}

		return keyMeta
	}
}

func SaveKeyMeta(key string, keyMeta *DkvKeyMeta) {
	if str_tool.StrIsEmpty(key) {
		return
	}

	badger_store.GetKvStoreEngine().RawPut(StrKey2KeyMetaBytes(key), keyMeta.ToBytes())
}

func SaveKeyMetas(keyValueMap map[string]*DkvKeyMeta) error {
	if len(keyValueMap) <= 0 {
		return nil
	}

	var bytePair = make([][]byte, 0, len(keyValueMap)*2)
	for key, keyMeta := range keyValueMap {
		bytePair = append(bytePair, StrKey2KeyMetaBytes(key))
		bytePair = append(bytePair, keyMeta.ToBytes())
	}

	return badger_store.GetKvStoreEngine().RawPutBatch(bytePair)
}

func RemoveKeyMeta(key string) error {
	if str_tool.StrIsEmpty(key) {
		return nil
	}

	return badger_store.GetKvStoreEngine().RawDel(StrKey2KeyMetaBytes(key))
}

func ExpireKeyMetaAbsMs(key string, expireMs int64) (int, error) {
	if str_tool.StrIsEmpty(key) {
		return 0, errors.New(dkv_conf.ErrMsgKeyEmpty)
	}

	if expireMs <= 0 {
		return 0, nil
	}

	var keyMeta = LoadKeyMeta(key)
	if IsExpiredThenRemove(keyMeta, key) {
		return 0, nil
	} else {
		keyMeta.Expire = expireMs
		SaveKeyMeta(key, keyMeta)
		return 1, nil
	}
}

func StrKey2KeyMetaBytes(key string) []byte {
	var writer = kv_store_base.CreateByteWriter(len(key) + 64)
	writer.AppendByte(KmFirstByteKeyMeta)
	writer.AppendStr(key)
	return writer.GetByteData()
}

func keyMetaBytes2StrKey(keyBytes []byte) string {
	if str_tool.ByteIsEmpty(keyBytes) {
		return ""
	} else {
		var reader = kv_store_base.CreateByteReader(keyBytes)
		reader.NextByte()
		return reader.NextStr()
	}
}
