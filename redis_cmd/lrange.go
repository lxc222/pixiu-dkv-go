package redis_cmd

import (
	"pixiu-dkv-go/dkv_tool/str_tool"
	"pixiu-dkv-go/kv_data/kv_data_list"
	"pixiu-dkv-go/redis_response"
)

type RedisLRange struct {
}

func (s *RedisLRange) GetCmdLabel() string {
	return "lrange"
}

func (s *RedisLRange) IsNeedRaftSync() bool {
	return false
}

func (s *RedisLRange) ExecuteCmd(params []string) (redis_response.RaftRedisRsp, error) {
	if 4 == len(params) {
		key := params[1]
		startIdx := str_tool.Str2Int32(params[2], 0)
		stopIdx := str_tool.Str2Int32(params[3], 0)

		values, err := kv_data_list.LRange(key, startIdx, stopIdx)
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
	var cmd = &RedisLRange{}
	RegistCmdHandler(cmd)
}
