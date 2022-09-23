package redis_cmd

import (
	"pixiu-dkv-go/kv_data/kv_data_set"
	"pixiu-dkv-go/redis_response"
)

type RedisSAdd struct {
}

func (s *RedisSAdd) GetCmdLabel() string {
	return "sadd"
}

func (s *RedisSAdd) IsNeedRaftSync() bool {
	return true
}

func (s *RedisSAdd) ExecuteCmd(params []string) (redis_response.RaftRedisRsp, error) {
	if 3 <= len(params) {
		key := params[1]
		members := params[2:]

		num, err := kv_data_set.SAdd(key, members)
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
	var cmd = &RedisSAdd{}
	RegistCmdHandler(cmd)
}
