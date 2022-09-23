package redis_cmd

import (
	"pixiu-dkv-go/kv_data/kv_data_str"
	"pixiu-dkv-go/redis_response"
)

type RedisMSet struct {
}

func (s *RedisMSet) GetCmdLabel() string {
	return "mset"
}

func (s *RedisMSet) IsNeedRaftSync() bool {
	return true
}

func (s *RedisMSet) ExecuteCmd(params []string) (redis_response.RaftRedisRsp, error) {
	if 3 <= len(params) {
		var kvMap = make(map[string]string)
		for i := 1; i < len(params); i = i + 2 {
			var key = params[i]
			var val = params[i+1]
			kvMap[key] = val
		}

		err := kv_data_str.MSet(kvMap)
		//var byteSlice = make([][]byte, 0, len(kvMap)*2)
		//for key, value := range kvMap {
		//	byteSlice = append(byteSlice, dkv_tool.Str2Bytes(key))
		//	byteSlice = append(byteSlice, dkv_tool.Str2Bytes(value))
		//}
		//
		//err := badger_store.GetKvStoreEngine().RawPutBatch(byteSlice)
		if err != nil {
			return redis_response.CreateCmdErrRsp(err.Error()), nil
		} else {
			return redis_response.CreateStrSimpleRsp(redis_response.StrOk), nil
		}
	} else {
		return redis_response.CreateCmdErrRsp(redis_response.StrParamErr), nil
	}
}

func init() {
	var cmd = &RedisMSet{}
	RegistCmdHandler(cmd)
}
