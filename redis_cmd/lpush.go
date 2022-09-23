package redis_cmd

import (
	"pixiu-dkv-go/kv_data/kv_data_list"
	"pixiu-dkv-go/redis_response"
)

type RedisLPush struct {
}

func (s *RedisLPush) GetCmdLabel() string {
	return "lpush"
}

func (s *RedisLPush) IsNeedRaftSync() bool {
	return true
}

func (s *RedisLPush) ExecuteCmd(params []string) (redis_response.RaftRedisRsp, error) {
	if 3 <= len(params) {
		key := params[1]
		values := params[2:]

		num, err := kv_data_list.LPush(key, values)
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
	var cmd = &RedisLPush{}
	RegistCmdHandler(cmd)
}
