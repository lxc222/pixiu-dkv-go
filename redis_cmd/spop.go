package redis_cmd

import (
	"pixiu-dkv-go/dkv_tool/str_tool"
	"pixiu-dkv-go/kv_data/kv_data_set"
	"pixiu-dkv-go/redis_response"
)

type RedisSPop struct {
}

func (s *RedisSPop) GetCmdLabel() string {
	return "spop"
}

func (s *RedisSPop) IsNeedRaftSync() bool {
	return true
}

func (s *RedisSPop) ExecuteCmd(params []string) (redis_response.RaftRedisRsp, error) {
	if 2 == len(params) {
		key := params[1]
		count := int32(1)

		values, err := kv_data_set.SPop(key, count)
		if err != nil {
			return redis_response.CreateCmdErrRsp(err.Error()), nil
		} else if values != nil && len(values) > 0 {
			return redis_response.CreateStrBulkRsp(values[0]), nil
		} else {
			return redis_response.CreateStrBulkNilRsp(), nil
		}
	} else if 3 == len(params) {
		key := params[1]
		count := str_tool.Str2Int32(params[2], 0)

		values, err := kv_data_set.SPop(key, count)
		if err != nil {
			return redis_response.CreateCmdErrRsp(err.Error()), nil
		} else {
			return redis_response.CreateStrArrayRsp(values), nil
		}
	} else {
		return redis_response.CreateCmdErrRsp(redis_response.StrParamErr), nil
	}
}

func init() {
	var cmd = &RedisSPop{}
	RegistCmdHandler(cmd)
}
