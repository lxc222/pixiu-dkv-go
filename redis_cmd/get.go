package redis_cmd

import (
	"pixiu-dkv-go/kv_data/kv_data_str"
	"pixiu-dkv-go/redis_response"
)

type RedisGet struct {
}

func (s *RedisGet) GetCmdLabel() string {
	return "get"
}

func (s *RedisGet) IsNeedRaftSync() bool {
	return false
}

func (s *RedisGet) ExecuteCmd(params []string) (redis_response.RaftRedisRsp, error) {
	if 2 == len(params) {
		key := params[1]

		value, err := kv_data_str.Get(key)
		if err != nil {
			return redis_response.CreateCmdErrRsp(err.Error()), nil
		} else {
			return redis_response.CreateStrBulkRsp(value), nil
		}
	} else {
		return redis_response.CreateCmdErrRsp(redis_response.StrParamErr), nil
	}
}

func init() {
	var cmd = &RedisGet{}
	RegistCmdHandler(cmd)
}
