package redis_cmd

import (
	"pixiu-dkv-go/kv_data/kv_data_str"
	"pixiu-dkv-go/redis_response"
)

type RedisMGet struct {
}

func (s *RedisMGet) GetCmdLabel() string {
	return "mget"
}

func (s *RedisMGet) IsNeedRaftSync() bool {
	return false
}

func (s *RedisMGet) ExecuteCmd(params []string) (redis_response.RaftRedisRsp, error) {
	if 2 <= len(params) {
		keys := params[1:]

		value, err := kv_data_str.MGet(keys)
		if err != nil {
			return redis_response.CreateCmdErrRsp(err.Error()), nil
		} else {
			return redis_response.CreateStrArrayRsp(value), nil
		}
	} else {
		return redis_response.CreateCmdErrRsp(redis_response.StrParamErr), nil
	}
}

func init() {
	var cmd = &RedisMGet{}
	RegistCmdHandler(cmd)
}
