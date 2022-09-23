package kv_data_set

import (
	"errors"
	"pixiu-dkv-go/dkv_conf"
	"pixiu-dkv-go/dkv_tool/str_tool"
	"pixiu-dkv-go/kv_data/kv_data_hash"
)

func SAdd(key string, members []string) (int32, error) {
	if str_tool.StrIsEmpty(key) || len(members) <= 0 {
		return 0, errors.New(dkv_conf.ErrMsgParamErr)
	}

	for _, member := range members {
		if str_tool.StrIsEmpty(member) {
			return 0, errors.New(dkv_conf.ErrMsgParamErr)
		}
	}

	var totalNum int32 = 0
	for _, member := range members {
		num, err := kv_data_hash.HSet(key, member, member)
		if err != nil {
			return totalNum, err
		} else {
			totalNum = totalNum + num
		}
	}

	return totalNum, nil
}

func SPop(key string, count int32) ([]string, error) {
	if str_tool.StrIsEmpty(key) || count <= 0 {
		return nil, errors.New(dkv_conf.ErrMsgParamErr)
	}

	maps, err := kv_data_hash.HGetMulAndDel(key, count)
	if err != nil {
		return nil, err
	}

	if len(maps) <= 0 {
		return nil, nil
	}

	var result []string = make([]string, 0, len(maps))
	for k, _ := range maps {
		result = append(result, k)
	}

	return result, nil
}
