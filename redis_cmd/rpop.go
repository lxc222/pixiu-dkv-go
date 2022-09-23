package redis_cmd

import (
	"pixiu-dkv-go/dkv_tool/str_tool"
	"pixiu-dkv-go/kv_data/kv_data_list"
	"pixiu-dkv-go/redis_response"
)

type RedisRPop struct {
}

func (s *RedisRPop) GetCmdLabel() string {
	return "rpop"
}

func (s *RedisRPop) IsNeedRaftSync() bool {
	return true
}

func (s *RedisRPop) ExecuteCmd(params []string) (redis_response.RaftRedisRsp, error) {
	if 2 == len(params) {
		key := params[1]
		count := int32(1)

		values, err := kv_data_list.RPop(key, count)
		if err != nil {
			return redis_response.CreateCmdErrRsp(err.Error()), nil
		} else {
			return redis_response.CreateStrBulkRsp(values[0]), nil
		}
	} else if 3 == len(params) {
		key := params[1]
		count := str_tool.Str2Int32(params[2], 0)

		values, err := kv_data_list.RPop(key, count)
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
	var cmd = &RedisRPop{}
	RegistCmdHandler(cmd)
}
