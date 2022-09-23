package redis_cmd

import (
	"pixiu-dkv-go/kv_data/kv_data_list"
	"pixiu-dkv-go/redis_response"
)

type RedisLLen struct {
}

func (s *RedisLLen) GetCmdLabel() string {
	return "llen"
}

func (s *RedisLLen) IsNeedRaftSync() bool {
	return false
}

func (s *RedisLLen) ExecuteCmd(params []string) (redis_response.RaftRedisRsp, error) {
	if 2 == len(params) {
		key := params[1]

		num, err := kv_data_list.LLen(key)
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
	var cmd = &RedisLLen{}
	RegistCmdHandler(cmd)
}
