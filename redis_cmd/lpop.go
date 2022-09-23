package redis_cmd

import (
	"pixiu-dkv-go/dkv_tool/str_tool"
	"pixiu-dkv-go/kv_data/kv_data_list"
	"pixiu-dkv-go/redis_response"
)

type RedisLPop struct {
}

func (s *RedisLPop) GetCmdLabel() string {
	return "lpop"
}

func (s *RedisLPop) IsNeedRaftSync() bool {
	return true
}

func (s *RedisLPop) ExecuteCmd(params []string) (redis_response.RaftRedisRsp, error) {
	if 2 == len(params) {
		key := params[1]
		count := int32(1)

		values, err := kv_data_list.LPop(key, count)
		if err != nil {
			return redis_response.CreateCmdErrRsp(err.Error()), nil
		} else if len(values) > 0 {
			return redis_response.CreateStrBulkRsp(values[0]), nil
		} else {
			return redis_response.CreateStrBulkRsp(""), nil
		}
	} else if 3 == len(params) {
		key := params[1]
		count := str_tool.Str2Int32(params[2], 0)

		values, err := kv_data_list.LPop(key, count)
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
	var cmd = &RedisLPop{}
	RegistCmdHandler(cmd)
}
