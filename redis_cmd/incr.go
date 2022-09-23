package redis_cmd

import (
	"pixiu-dkv-go/kv_data/kv_data_str"
	"pixiu-dkv-go/redis_response"
)

type RedisIncr struct {
}

func (s *RedisIncr) GetCmdLabel() string {
	return "incr"
}

func (s *RedisIncr) IsNeedRaftSync() bool {
	return true
}

func (s *RedisIncr) ExecuteCmd(params []string) (redis_response.RaftRedisRsp, error) {
	if 2 == len(params) {
		key := params[1]

		num, err := kv_data_str.IncrBy(key, 1)
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
	var cmd = &RedisIncr{}
	RegistCmdHandler(cmd)
}
