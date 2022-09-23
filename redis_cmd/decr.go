package redis_cmd

import (
	"pixiu-dkv-go/kv_data/kv_data_str"
	"pixiu-dkv-go/redis_response"
)

type RedisDecr struct {
}

func (s *RedisDecr) GetCmdLabel() string {
	return "decr"
}

func (s *RedisDecr) IsNeedRaftSync() bool {
	return true
}

func (s *RedisDecr) ExecuteCmd(params []string) (redis_response.RaftRedisRsp, error) {
	if 2 == len(params) {
		key := params[1]

		num, err := kv_data_str.IncrBy(key, -1)
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
	var cmd = &RedisDecr{}
	RegistCmdHandler(cmd)
}
