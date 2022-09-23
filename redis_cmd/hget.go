package redis_cmd

import (
	"pixiu-dkv-go/kv_data/kv_data_hash"
	"pixiu-dkv-go/redis_response"
)

type RedisHGet struct {
}

func (s *RedisHGet) GetCmdLabel() string {
	return "hget"
}

func (s *RedisHGet) IsNeedRaftSync() bool {
	return false
}

func (s *RedisHGet) ExecuteCmd(params []string) (redis_response.RaftRedisRsp, error) {
	if 3 == len(params) {
		key := params[1]
		field := params[2]

		_, value, err := kv_data_hash.HGet(key, field)
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
	var cmd = &RedisHGet{}
	RegistCmdHandler(cmd)
}
