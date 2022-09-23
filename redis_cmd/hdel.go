package redis_cmd

import (
	"pixiu-dkv-go/kv_data/kv_data_hash"
	"pixiu-dkv-go/redis_response"
)

type RedisHDel struct {
}

func (s *RedisHDel) GetCmdLabel() string {
	return "hdel"
}

func (s *RedisHDel) IsNeedRaftSync() bool {
	return true
}

func (s *RedisHDel) ExecuteCmd(params []string) (redis_response.RaftRedisRsp, error) {
	if 3 <= len(params) {
		key := params[1]
		fields := params[2:]

		num, err := kv_data_hash.HDel(key, fields)
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
	var cmd = &RedisHDel{}
	RegistCmdHandler(cmd)
}
