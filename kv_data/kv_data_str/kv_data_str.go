package kv_data_str

import (
	"errors"
	"pixiu-dkv-go/dkv_conf"
	"pixiu-dkv-go/dkv_tool/common_tool"
	"pixiu-dkv-go/dkv_tool/str_tool"
	"pixiu-dkv-go/kv_data/kv_data_meta"
)

func IncrBy(key string, increment int64) (int64, error) {
	if str_tool.StrIsEmpty(key) {
		return 0, errors.New(dkv_conf.ErrMsgKeyEmpty)
	}

	var regionMutex = dkv_conf.GetSysConf().GetRegionMutex(key)
	regionMutex.Lock()
	defer regionMutex.Unlock()

	var keyMeta = kv_data_meta.LoadKeyMeta(key)
	if kv_data_meta.IsExpiredThenRemove(keyMeta, key) {
		keyMeta = kv_data_meta.CreateTypeStr(-1)
		keyMeta.Value = str_tool.Int642Str(increment)
		kv_data_meta.SaveKeyMeta(key, keyMeta)
		return 0, nil
	}

	if !keyMeta.IsTypeStr() {
		return 0, errors.New(dkv_conf.ErrMsgDataTypeErr)
	}

	var valueNum int64 = 0
	if !str_tool.StrIsEmpty(keyMeta.Value) {
		valueNum = str_tool.Str2Int64(keyMeta.Value, 0) + increment
	} else {
		valueNum = increment
	}

	keyMeta.Value = str_tool.Int642Str(valueNum)
	kv_data_meta.SaveKeyMeta(key, keyMeta)
	return valueNum, nil
}

func SetEx(key string, value string, expireSecond int64) error {
	if str_tool.StrIsEmpty(key) {
		return errors.New(dkv_conf.ErrMsgKeyEmpty)
	}

	if str_tool.StrIsEmpty(value) {
		return errors.New(dkv_conf.ErrMsgValueEmpty)
	}

	var regionMutex = dkv_conf.GetSysConf().GetRegionMutex(key)
	regionMutex.Lock()
	defer regionMutex.Unlock()

	var keyMeta = kv_data_meta.LoadKeyMeta(key)
	if kv_data_meta.IsExpiredThenRemove(keyMeta, key) {
		keyMeta = kv_data_meta.CreateTypeStr(expireSecond)
		keyMeta.Value = value
		kv_data_meta.SaveKeyMeta(key, keyMeta)
		return nil
	}

	if !keyMeta.IsTypeStr() {
		return errors.New(dkv_conf.ErrMsgDataTypeErr)
	}

	if expireSecond > 0 {
		keyMeta.Expire = common_tool.CurrentMS() + 1000*expireSecond
	}

	keyMeta.Value = value
	kv_data_meta.SaveKeyMeta(key, keyMeta)
	return nil
}

func Get(key string) (string, error) {
	if str_tool.StrIsEmpty(key) {
		return "", errors.New(dkv_conf.ErrMsgKeyEmpty)
	}

	var keyMeta = kv_data_meta.LoadKeyMeta(key)
	if kv_data_meta.IsExpiredThenRemove(keyMeta, key) {
		return "", nil
	}

	if !keyMeta.IsTypeStr() {
		return "", errors.New(dkv_conf.ErrMsgDataTypeErr)
	}

	if str_tool.StrIsEmpty(keyMeta.Value) {
		return "", nil
	} else {
		return keyMeta.Value, nil
	}
}

func MGet(keys []string) ([]string, error) {
	if len(keys) <= 0 {
		return nil, errors.New(dkv_conf.ErrMsgKeyEmpty)
	}

	var result = make([]string, 0, len(keys))
	for _, tmpKey := range keys {
		value, _ := Get(tmpKey)
		result = append(result, value)
	}

	return result, nil
}

func MSet(keys map[string]string) error {
	if len(keys) <= 0 {
		return errors.New(dkv_conf.ErrMsgKeyEmpty)
	}

	for key, value := range keys {
		if str_tool.StrIsEmpty(key) {
			return errors.New(dkv_conf.ErrMsgKeyEmpty)
		}

		if str_tool.StrIsEmpty(value) {
			return errors.New(dkv_conf.ErrMsgValueEmpty)
		}

		err := SetEx(key, value, -1)
		if err != nil {
			return err
		}
	}

	return nil
}
