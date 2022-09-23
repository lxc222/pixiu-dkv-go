package redis_cmd

import (
	"pixiu-dkv-go/dkv_tool/str_tool"
	"pixiu-dkv-go/kv_data/kv_data_str"
	"pixiu-dkv-go/redis_response"
)

type RedisIncrby struct {
}

func (s *RedisIncrby) GetCmdLabel() string {
	return "incrby"
}

func (s *RedisIncrby) IsNeedRaftSync() bool {
	return true
}

func (s *RedisIncrby) ExecuteCmd(params []string) (redis_response.RaftRedisRsp, error) {
	if 3 == len(params) {
		key := params[1]
		incrNum := params[2]

		num, err := kv_data_str.IncrBy(key, str_tool.Str2Int64(incrNum, 0))
		if err != nil {
			return redis_response.CreateCmdErrRsp(err.Error()), nil
		} else {
			return redis_response.CreateNumInt64Rsp(num), nil
		}
	} else {
		return redis_response.CreateCmdErrRsp(redis_response.StrParamErr), nil
	}
}

func init() {
	var cmd = &RedisIncrby{}
	RegistCmdHandler(cmd)
}
