package kv_data_hash

import (
	"bytes"
	"errors"
	badger "github.com/dgraph-io/badger/v3"
	"pixiu-dkv-go/dkv_conf"
	"pixiu-dkv-go/dkv_tool/common_tool"
	"pixiu-dkv-go/dkv_tool/str_tool"
	"pixiu-dkv-go/kv_data/kv_byte_str"
	"pixiu-dkv-go/kv_data/kv_data_meta"
	"pixiu-dkv-go/kv_data/kv_store_base"
	"pixiu-dkv-go/store_engine/badger_store"
)

func HSet(key string, field string, value string) (int32, error) {
	if str_tool.StrIsEmpty(key) || str_tool.StrIsEmpty(field) {
		return 0, errors.New(dkv_conf.ErrMsgKeyEmpty)
	}

	var regionMutex = dkv_conf.GetSysConf().GetRegionMutex(key)
	regionMutex.Lock()
	defer regionMutex.Unlock()

	var keyMeta = kv_data_meta.LoadKeyMeta(key)
	if kv_data_meta.IsExpiredThenRemove(keyMeta, key) {
		keyMeta = kv_data_meta.CreateTypeHash()
	}

	if !keyMeta.IsTypeHash() {
		return 0, errors.New(dkv_conf.ErrMsgDataTypeErr)
	}

	var fieldKeyByte = buildFieldKey(key, keyMeta.Version, field)
	var fieldValByte, _ = badger_store.GetKvStoreEngine().RawGet(fieldKeyByte)
	var fieldValStr = kv_byte_str.CreateFromByte(fieldValByte)
	if !fieldValStr.IsValid {
		keyMeta.Size = keyMeta.Size + 1

		var keyValues = make([][]byte, 0, 4)
		keyValues = append(keyValues, kv_data_meta.StrKey2KeyMetaBytes(key))
		keyValues = append(keyValues, keyMeta.ToBytes())
		keyValues = append(keyValues, fieldKeyByte)
		keyValues = append(keyValues, kv_byte_str.CreateFromStr(true, value).ToBytes())

		return 1, badger_store.GetKvStoreEngine().RawPutBatch(keyValues)
	} else if value != fieldValStr.Value {
		return 0, badger_store.GetKvStoreEngine().RawPut(fieldKeyByte, kv_byte_str.CreateFromStr(true, value).ToBytes())
	} else {
		return 0, nil
	}
}

func HGet(key string, field string) (bool, string, error) {
	if str_tool.StrIsEmpty(key) || str_tool.StrIsEmpty(field) {
		return false, "", errors.New(dkv_conf.ErrMsgKeyEmpty)
	}

	var keyMeta = kv_data_meta.LoadKeyMeta(key)
	if kv_data_meta.IsExpiredThenRemove(keyMeta, key) {
		return false, "", nil
	}

	if !keyMeta.IsTypeHash() {
		return false, "", errors.New(dkv_conf.ErrMsgDataTypeErr)
	}

	var fieldKeyByte = buildFieldKey(key, keyMeta.Version, field)
	var fieldValByte, _ = badger_store.GetKvStoreEngine().RawGet(fieldKeyByte)
	var fieldValStr = kv_byte_str.CreateFromByte(fieldValByte)
	if !fieldValStr.IsValid {
		return false, "", nil
	} else {
		return true, fieldValStr.Value, nil
	}
}

func HGetAll(key string) (map[string]string, error) {
	if str_tool.StrIsEmpty(key) {
		return nil, errors.New(dkv_conf.ErrMsgKeyEmpty)
	}

	var keyMeta = kv_data_meta.LoadKeyMeta(key)
	if kv_data_meta.IsExpiredThenRemove(keyMeta, key) {
		return nil, nil
	}

	if !keyMeta.IsTypeHash() {
		return nil, errors.New(dkv_conf.ErrMsgDataTypeErr)
	}

	return HGetMul(key, keyMeta.Size)
}

func HGetMul(key string, size int32) (map[string]string, error) {
	if str_tool.StrIsEmpty(key) {
		return nil, errors.New(dkv_conf.ErrMsgKeyEmpty)
	}

	var keyMeta = kv_data_meta.LoadKeyMeta(key)
	if kv_data_meta.IsExpiredThenRemove(keyMeta, key) {
		return nil, nil
	}

	if !keyMeta.IsTypeHash() {
		return nil, errors.New(dkv_conf.ErrMsgDataTypeErr)
	}

	var mapRst = make(map[string]string)
	var fieldKeyPrefix = buildFieldKeyPrefix(key, keyMeta.Version)

	errOutter := badger_store.GetKvStoreLocalDb().View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(fieldKeyPrefix); it.ValidForPrefix(fieldKeyPrefix); it.Next() {
			item := it.Item()
			tmpFieldKeyByte := item.Key()
			if !bytes.HasPrefix(tmpFieldKeyByte, fieldKeyPrefix) {
				break
			}

			var tmpFieldValByte, errInner = item.ValueCopy(nil)
			if errInner != nil {
				return errInner
			}

			var tmpFieldValStr = kv_byte_str.CreateFromByte(tmpFieldValByte)
			if !tmpFieldValStr.IsValid {
				continue
			}

			var trip = parseFieldKey(tmpFieldKeyByte)
			if key != trip.Left {
				break
			} else if keyMeta.Version != trip.Middle {
				break
			} else if str_tool.StrIsEmpty(trip.Right) {
				break
			} else {
				mapRst[trip.Right] = tmpFieldValStr.Value
				if int32(len(mapRst)) >= size {
					break
				}
			}
		}

		return nil
	})

	if errOutter != nil {
		return nil, errOutter
	} else {
		return mapRst, nil
	}
}

func HGetMulAndDel(key string, size int32) (map[string]string, error) {
	if str_tool.StrIsEmpty(key) {
		return nil, errors.New(dkv_conf.ErrMsgKeyEmpty)
	}

	var regionMutex = dkv_conf.GetSysConf().GetRegionMutex(key)
	regionMutex.Lock()
	defer regionMutex.Unlock()

	var keyMeta = kv_data_meta.LoadKeyMeta(key)
	if kv_data_meta.IsExpiredThenRemove(keyMeta, key) {
		return nil, nil
	}

	if !keyMeta.IsTypeHash() {
		return nil, errors.New(dkv_conf.ErrMsgDataTypeErr)
	}

	var sizeMin = str_tool.Int32Min(size, keyMeta.Size)
	var updateKvs = make([][]byte, 0, 2+sizeMin*2)
	var mapRst = make(map[string]string)
	var fieldKeyPrefix = buildFieldKeyPrefix(key, keyMeta.Version)

	errOutter := badger_store.GetKvStoreLocalDb().View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(fieldKeyPrefix); it.ValidForPrefix(fieldKeyPrefix); it.Next() {
			item := it.Item()
			tmpFieldKeyByte := item.Key()
			//if !bytes.HasPrefix(tmpFieldKeyByte, fieldKeyPrefix) {
			//	break
			//}

			var tmpFieldValByte, errInner = item.ValueCopy(nil)
			if errInner != nil {
				return errInner
			}

			var tmpFieldValStr = kv_byte_str.CreateFromByte(tmpFieldValByte)
			if !tmpFieldValStr.IsValid {
				continue
			}

			var trip = parseFieldKey(tmpFieldKeyByte)
			if key != trip.Left {
				break
			} else if keyMeta.Version != trip.Middle {
				break
			} else if str_tool.StrIsEmpty(trip.Right) {
				break
			} else {
				mapRst[trip.Right] = tmpFieldValStr.Value
				keyMeta.Size = keyMeta.Size - 1
				updateKvs = append(updateKvs, tmpFieldKeyByte)
				updateKvs = append(updateKvs, kv_byte_str.CreateFromStr(false, "invalid").ToBytes())

				if int32(len(mapRst)) >= size {
					break
				}
			}
		}

		return nil
	})

	if errOutter != nil {
		return nil, errOutter
	}

	updateKvs = append(updateKvs, kv_data_meta.StrKey2KeyMetaBytes(key))
	updateKvs = append(updateKvs, keyMeta.ToBytes())

	err := badger_store.GetKvStoreEngine().RawPutBatch(updateKvs)
	if err != nil {
		return nil, err
	} else {
		return mapRst, nil
	}
}

func HDel(key string, fields []string) (int32, error) {
	if str_tool.StrIsEmpty(key) || len(fields) <= 0 {
		return 0, errors.New(dkv_conf.ErrMsgKeyEmpty)
	}

	for _, field := range fields {
		if str_tool.StrIsEmpty(field) {
			return 0, errors.New(dkv_conf.ErrMsgKeyEmpty)
		}
	}

	var regionMutex = dkv_conf.GetSysConf().GetRegionMutex(key)
	regionMutex.Lock()
	defer regionMutex.Unlock()

	var keyMeta = kv_data_meta.LoadKeyMeta(key)
	if kv_data_meta.IsExpiredThenRemove(keyMeta, key) {
		return 0, nil
	}

	if !keyMeta.IsTypeHash() {
		return 0, errors.New(dkv_conf.ErrMsgDataTypeErr)
	}

	var updateKvs = make([][]byte, 0, 2+len(fields)*2)
	var totalDelSize int32 = 0
	for _, field := range fields {
		num, err := HDelOneNoLock(key, field, keyMeta, updateKvs)
		if err != nil {
			return 0, err
		} else {
			totalDelSize = totalDelSize + num
		}
	}

	updateKvs = append(updateKvs, kv_data_meta.StrKey2KeyMetaBytes(key))
	updateKvs = append(updateKvs, keyMeta.ToBytes())

	err := badger_store.GetKvStoreEngine().RawPutBatch(updateKvs)
	if err != nil {
		return 0, err
	} else {
		return totalDelSize, nil
	}
}

func HDelOne(key string, field string) (int32, error) {
	return HDel(key, []string{field})
}

func HDelOneNoLock(key string, field string, keyMeta *kv_data_meta.DkvKeyMeta, updateKv [][]byte) (int32, error) {
	var fieldKey = buildFieldKey(key, keyMeta.Version, field)
	var fieldVal, err = badger_store.GetKvStoreEngine().RawGet(fieldKey)
	if err != nil {
		return 0, errors.New(dkv_conf.ErrMsgOperateErr)
	}

	var fieldStr = kv_byte_str.CreateFromByte(fieldVal)
	if fieldStr.IsValid {
		keyMeta.Size = keyMeta.Size - 1
		updateKv = append(updateKv, fieldKey)
		updateKv = append(updateKv, kv_byte_str.CreateFromStr(false, "invalid").ToBytes())

		return 1, nil
	} else {
		return 0, nil
	}
}

func HLen(key string) (int32, error) {
	if str_tool.StrIsEmpty(key) {
		return 0, errors.New(dkv_conf.ErrMsgKeyEmpty)
	}

	var keyMeta = kv_data_meta.LoadKeyMeta(key)
	if kv_data_meta.IsExpiredThenRemove(keyMeta, key) {
		return 0, nil
	}

	if !keyMeta.IsTypeHash() {
		return 0, errors.New(dkv_conf.ErrMsgDataTypeErr)
	}

	return keyMeta.Size, nil
}

func buildFieldKey(key string, version int64, field string) []byte {
	var writer = kv_store_base.CreateByteWriter(len(key) + len(field) + 64)
	writer.AppendByte(kv_data_meta.KmFirstByteHashField)
	writer.AppendStr(key)
	writer.AppendInt64(version)
	writer.AppendStr(field)

	return writer.GetByteData()
}

func buildFieldKeyPrefix(key string, version int64) []byte {
	var writer = kv_store_base.CreateByteWriter(len(key) + 64)
	writer.AppendByte(kv_data_meta.KmFirstByteHashField)
	writer.AppendStr(key)
	writer.AppendInt64(version)

	return writer.GetByteData()
}

func parseFieldKey(bytes []byte) *common_tool.Triple[string, int64, string] {
	var reader = kv_store_base.CreateByteReader(bytes)
	reader.NextByte()
	var key string = reader.NextStr()
	var version int64 = reader.NextInt64()
	var field string = reader.NextStr()

	return common_tool.CreateTriple(key, version, field)
}

//func getScanItemValue(item *badger.Item) ([]byte, error) {
//	var tmpValueByte []byte = nil
//	errInner := item.Value(func(v []byte) error {
//		tmpValueByte = append([]byte{}, v...)
//		return nil
//	})
//
//	if errInner != nil {
//		return nil, errInner
//	} else {
//		return tmpValueByte, nil
//	}
//}
