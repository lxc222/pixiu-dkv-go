package redis_cmd

import (
	"pixiu-dkv-go/kv_data/kv_data_hash"
	"pixiu-dkv-go/redis_response"
)

type RedisHSet struct {
}

func (s *RedisHSet) GetCmdLabel() string {
	return "hset"
}

func (s *RedisHSet) IsNeedRaftSync() bool {
	return true
}

func (s *RedisHSet) ExecuteCmd(params []string) (redis_response.RaftRedisRsp, error) {
	if 4 == len(params) {
		key := params[1]
		field := params[2]
		value := params[3]

		num, err := kv_data_hash.HSet(key, field, value)
		if err != nil {
			return redis_response.CreateCmdErrRsp(err.Error()), nil
		} else {
			return redis_response.CreateNumInt32Rsp(num), nil
		}
	} else {
		return redis_response.CreateCmdErrRsp(redis_response.StrParamErr), nil
	}
}

func init() {
	var cmd = &RedisHSet{}
	RegistCmdHandler(cmd)
}
