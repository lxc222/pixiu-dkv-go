package redis_cmd

import (
	"pixiu-dkv-go/dkv_tool/str_tool"
	"pixiu-dkv-go/kv_data/kv_data_meta"
	"pixiu-dkv-go/redis_response"
)

type RedisExists struct {
}

func (s *RedisExists) GetCmdLabel() string {
	return "exists"
}

func (s *RedisExists) IsNeedRaftSync() bool {
	return false
}

func (s *RedisExists) ExecuteCmd(params []string) (redis_response.RaftRedisRsp, error) {
	if 2 == len(params) {
		key := params[1]

		if str_tool.StrIsEmpty(key) {
			return redis_response.CreateCmdErrRsp(redis_response.StrParamErr), nil
		}

		var keyMeta = kv_data_meta.LoadKeyMeta(key)
		if kv_data_meta.IsExpiredThenRemove(keyMeta, key) {
			return redis_response.CreateNumIntRsp(0), nil
		} else {
			return redis_response.CreateNumIntRsp(1), nil
		}
	} else {
		return redis_response.CreateCmdErrRsp(redis_response.StrParamErr), nil
	}
}

func init() {
	var cmd = &RedisExists{}
	RegistCmdHandler(cmd)
}
