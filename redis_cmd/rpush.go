package redis_cmd

import (
	"pixiu-dkv-go/kv_data/kv_data_list"
	"pixiu-dkv-go/redis_response"
)

type RedisRPush struct {
}

func (s *RedisRPush) GetCmdLabel() string {
	return "rpush"
}

func (s *RedisRPush) IsNeedRaftSync() bool {
	return true
}

func (s *RedisRPush) ExecuteCmd(params []string) (redis_response.RaftRedisRsp, error) {
	if 3 <= len(params) {
		key := params[1]
		values := params[2:]

		num, err := kv_data_list.RPush(key, values)
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
	var cmd = &RedisRPush{}
	RegistCmdHandler(cmd)
}
