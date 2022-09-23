package kv_data_list

import (
	"bytes"
	"errors"
	"github.com/dgraph-io/badger/v3"
	"pixiu-dkv-go/dkv_conf"
	"pixiu-dkv-go/dkv_tool/common_tool"
	"pixiu-dkv-go/dkv_tool/str_tool"
	"pixiu-dkv-go/kv_data/kv_byte_str"
	"pixiu-dkv-go/kv_data/kv_data_meta"
	"pixiu-dkv-go/kv_data/kv_store_base"
	"pixiu-dkv-go/store_engine/badger_store"
)

func LRange(key string, startIdx int32, stopIdx int32) ([]string, error) {
	if str_tool.StrIsEmpty(key) {
		return nil, errors.New(dkv_conf.ErrMsgParamErr)
	}

	var keyMeta = kv_data_meta.LoadKeyMeta(key)
	if kv_data_meta.IsExpiredThenRemove(keyMeta, key) {
		return nil, nil
	}

	if !keyMeta.IsTypeList() {
		return nil, errors.New(dkv_conf.ErrMsgDataTypeErr)
	}

	if keyMeta.Size <= 0 {
		return nil, nil
	}

	if startIdx <= 0 {
		startIdx = startIdx + keyMeta.Size
	}

	if stopIdx <= 0 {
		stopIdx = stopIdx + keyMeta.Size
	}

	if startIdx > stopIdx {
		return nil, errors.New(dkv_conf.ErrMsgParamErr)
	}

	var totalSize = (stopIdx - startIdx) + 1
	var resultValue = make([]string, 0, totalSize)
	var currentListIdx int32 = 0
	var fieldKeyPrefix = buildFieldKeyPrefix(key, keyMeta.Version)
	errOutter := badger_store.GetKvStoreLocalDb().View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(fieldKeyPrefix); it.ValidForPrefix(fieldKeyPrefix); it.Next() {
			item := it.Item()
			tmpFieldByte := item.Key()
			if !bytes.HasPrefix(tmpFieldByte, fieldKeyPrefix) {
				break
			}

			tmpValueByte, errInner := item.ValueCopy(nil)
			if errInner != nil {
				return errInner
			}

			tmpByteStr := kv_byte_str.CreateFromByte(tmpValueByte)
			if !tmpByteStr.IsValid {
				continue
			}

			if currentListIdx < startIdx {
				currentListIdx = currentListIdx + 1
			} else if currentListIdx > stopIdx {
				break
			} else {
				currentListIdx = currentListIdx + 1
				resultValue = append(resultValue, tmpByteStr.Value)
			}
		}

		return nil
	})

	if errOutter != nil {
		return nil, errOutter
	} else {
		return resultValue, nil
	}
}

func LLen(key string) (int32, error) {
	if str_tool.StrIsEmpty(key) {
		return 0, errors.New(dkv_conf.ErrMsgParamErr)
	}

	var keyMeta = kv_data_meta.LoadKeyMeta(key)
	if kv_data_meta.IsExpiredThenRemove(keyMeta, key) {
		return 0, nil
	}

	if !keyMeta.IsTypeList() {
		return 0, errors.New(dkv_conf.ErrMsgDataTypeErr)
	}

	return keyMeta.Size, nil
}

func LPush(key string, fields []string) (int32, error) {
	if str_tool.StrIsEmpty(key) || len(fields) <= 0 {
		return 0, errors.New(dkv_conf.ErrMsgParamErr)
	}

	for _, field := range fields {
		if str_tool.StrIsEmpty(field) {
			return 0, errors.New(dkv_conf.ErrMsgParamErr)
		}
	}

	for _, field := range fields {
		err := PushLeftOrRightOne(key, field, true)
		if err != nil {
			return 0, errors.New(dkv_conf.ErrMsgOperateErr)
		}
	}

	return LLen(key)
}

func RPush(key string, fields []string) (int32, error) {
	if str_tool.StrIsEmpty(key) || len(fields) <= 0 {
		return 0, errors.New(dkv_conf.ErrMsgParamErr)
	}

	for _, field := range fields {
		if str_tool.StrIsEmpty(field) {
			return 0, errors.New(dkv_conf.ErrMsgParamErr)
		}
	}

	for _, field := range fields {
		err := PushLeftOrRightOne(key, field, false)
		if err != nil {
			return 0, errors.New(dkv_conf.ErrMsgOperateErr)
		}
	}

	return LLen(key)
}

func LPop(key string, count int32) ([]string, error) {
	if str_tool.StrIsEmpty(key) || count <= 0 {
		return nil, errors.New(dkv_conf.ErrMsgParamErr)
	}

	var keyMeta = kv_data_meta.LoadKeyMeta(key)
	if kv_data_meta.IsExpiredThenRemove(keyMeta, key) {
		return nil, nil
	}

	if !keyMeta.IsTypeList() {
		return nil, errors.New(dkv_conf.ErrMsgDataTypeErr)
	}

	var values = make([]string, 0, str_tool.Int32Min(count, keyMeta.Size))
	for idx := int32(0); idx < count; idx++ {
		str, err := PopLeftOrRightOne(key, true)
		if err != nil {
			return nil, errors.New(dkv_conf.ErrMsgOperateErr)
		} else {
			values = append(values, str)
		}
	}

	return values, nil
}

func RPop(key string, count int32) ([]string, error) {
	if str_tool.StrIsEmpty(key) || count <= 0 {
		return nil, errors.New(dkv_conf.ErrMsgParamErr)
	}

	var keyMeta = kv_data_meta.LoadKeyMeta(key)
	if kv_data_meta.IsExpiredThenRemove(keyMeta, key) {
		return nil, nil
	}

	if !keyMeta.IsTypeList() {
		return nil, errors.New(dkv_conf.ErrMsgDataTypeErr)
	}

	var values = make([]string, 0, str_tool.Int32Min(count, keyMeta.Size))
	for idx := int32(0); idx < count; idx++ {
		str, err := PopLeftOrRightOne(key, false)
		if err != nil {
			return nil, errors.New(dkv_conf.ErrMsgOperateErr)
		} else {
			values = append(values, str)
		}
	}

	return values, nil
}

func PushLeftOrRightOne(key string, field string, isleft bool) error {
	if str_tool.StrIsEmpty(key) || str_tool.StrIsEmpty(field) {
		return errors.New(dkv_conf.ErrMsgParamErr)
	}

	var regionMutex = dkv_conf.GetSysConf().GetRegionMutex(key)
	regionMutex.Lock()
	defer regionMutex.Unlock()

	var keyMeta = kv_data_meta.LoadKeyMeta(key)
	if kv_data_meta.IsExpiredThenRemove(keyMeta, key) {
		keyMeta = kv_data_meta.CreateTypeList()
	}

	if !keyMeta.IsTypeList() {
		return errors.New(dkv_conf.ErrMsgDataTypeErr)
	}

	var thisPushIndex int64 = 0
	if isleft {
		thisPushIndex = keyMeta.LIndex
		keyMeta.LIndex = keyMeta.LIndex - 1
		keyMeta.Size = keyMeta.Size + 1
	} else {
		thisPushIndex = keyMeta.RIndex
		keyMeta.RIndex = keyMeta.RIndex + 1
		keyMeta.Size = keyMeta.Size + 1
	}

	var keyValues = make([][]byte, 0, 4)
	keyValues = append(keyValues, kv_data_meta.StrKey2KeyMetaBytes(key))
	keyValues = append(keyValues, keyMeta.ToBytes())
	keyValues = append(keyValues, buildFieldKey(key, keyMeta.Version, thisPushIndex))
	keyValues = append(keyValues, kv_byte_str.CreateFromStr(true, field).ToBytes())

	return badger_store.GetKvStoreEngine().RawPutBatch(keyValues)
}

func PopLeftOrRightOne(key string, isleft bool) (string, error) {
	if str_tool.StrIsEmpty(key) {
		return "", errors.New(dkv_conf.ErrMsgParamErr)
	}

	var regionMutex = dkv_conf.GetSysConf().GetRegionMutex(key)
	regionMutex.Lock()
	defer regionMutex.Unlock()

	var keyMeta = kv_data_meta.LoadKeyMeta(key)
	if kv_data_meta.IsExpiredThenRemove(keyMeta, key) {
		keyMeta = kv_data_meta.CreateTypeList()
	}

	if !keyMeta.IsTypeList() {
		return "", errors.New(dkv_conf.ErrMsgDataTypeErr)
	}

	if keyMeta.Size <= 0 {
		return "", nil
	}

	var popFieldKey []byte = nil
	var popFieldVal string = ""
	if isleft {
		for {
			keyMeta.LIndex = keyMeta.LIndex + 1
			tmpFieldKey := buildFieldKey(key, keyMeta.Version, keyMeta.LIndex)
			tmpFieldByte, err := badger_store.GetKvStoreEngine().RawGet(tmpFieldKey)
			if err != nil {
				return "", errors.New(dkv_conf.ErrMsgOperateErr)
			}

			tmpFieldVal := kv_byte_str.CreateFromByte(tmpFieldByte)
			if tmpFieldVal.IsValid {
				popFieldKey = tmpFieldKey
				popFieldVal = tmpFieldVal.Value
				keyMeta.Size = keyMeta.Size - 1
				break
			}

			if keyMeta.LIndex+1 >= keyMeta.RIndex {
				break
			}
		}
	} else {
		for {
			keyMeta.RIndex = keyMeta.RIndex - 1
			tmpFieldKey := buildFieldKey(key, keyMeta.Version, keyMeta.RIndex)
			tmpFieldByte, err := badger_store.GetKvStoreEngine().RawGet(tmpFieldKey)
			if err != nil {
				return "", errors.New(dkv_conf.ErrMsgOperateErr)
			}

			tmpFieldVal := kv_byte_str.CreateFromByte(tmpFieldByte)
			if tmpFieldVal.IsValid {
				popFieldKey = tmpFieldKey
				popFieldVal = tmpFieldVal.Value
				keyMeta.Size = keyMeta.Size - 1
				break
			}

			if keyMeta.LIndex+1 >= keyMeta.RIndex {
				break
			}
		}
	}

	var keyValues = make([][]byte, 0, 4)
	keyValues = append(keyValues, kv_data_meta.StrKey2KeyMetaBytes(key))
	keyValues = append(keyValues, keyMeta.ToBytes())
	if popFieldKey != nil {
		keyValues = append(keyValues, popFieldKey)
		keyValues = append(keyValues, kv_byte_str.CreateFromStr(false, "invalid").ToBytes())
	}

	batchErr := badger_store.GetKvStoreEngine().RawPutBatch(keyValues)
	if batchErr != nil {
		return "", errors.New(dkv_conf.ErrMsgOperateErr)
	} else {
		if popFieldKey == nil {
			return "", nil
		} else {
			return popFieldVal, nil
		}
	}
}

func buildFieldKey(key string, version int64, index int64) []byte {
	var writer = kv_store_base.CreateByteWriter(len(key) + 32)
	writer.AppendByte(kv_data_meta.KmFirstByteListIndex)
	writer.AppendStr(key)
	writer.AppendInt64(version)
	writer.AppendInt64(index)

	return writer.GetByteData()
}

func buildFieldKeyPrefix(key string, version int64) []byte {
	var writer = kv_store_base.CreateByteWriter(len(key) + 64)
	writer.AppendByte(kv_data_meta.KmFirstByteHashField)
	writer.AppendStr(key)
	writer.AppendInt64(version)

	return writer.GetByteData()
}

func parseFieldKey(bytes []byte) *common_tool.Triple[string, int64, int64] {
	var reader = kv_store_base.CreateByteReader(bytes)
	reader.NextByte()
	var key string = reader.NextStr()
	var version int64 = reader.NextInt64()
	var index int64 = reader.NextInt64()

	return common_tool.CreateTriple(key, version, index)
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
