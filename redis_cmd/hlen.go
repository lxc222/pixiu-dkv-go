package redis_cmd

import (
	"pixiu-dkv-go/kv_data/kv_data_hash"
	"pixiu-dkv-go/redis_response"
)

type RedisHLen struct {
}

func (s *RedisHLen) GetCmdLabel() string {
	return "hlen"
}

func (s *RedisHLen) IsNeedRaftSync() bool {
	return false
}

func (s *RedisHLen) ExecuteCmd(params []string) (redis_response.RaftRedisRsp, error) {
	if 2 == len(params) {
		key := params[1]

		value, err := kv_data_hash.HLen(key)
		if err != nil {
			return redis_response.CreateCmdErrRsp(err.Error()), nil
		} else {
			return redis_response.CreateNumInt32Rsp(value), nil
		}
	} else {
		return redis_response.CreateCmdErrRsp(redis_response.StrParamErr), nil
	}
}

func init() {
	var cmd = &RedisHLen{}
	RegistCmdHandler(cmd)
}
