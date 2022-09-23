package redis_cmd

import (
	"pixiu-dkv-go/dkv_tool/str_tool"
	"pixiu-dkv-go/kv_data/kv_data_str"
	"pixiu-dkv-go/redis_response"
)

type RedisSetEx struct {
}

func (s *RedisSetEx) GetCmdLabel() string {
	return "setex"
}

func (s *RedisSetEx) IsNeedRaftSync() bool {
	return true
}

func (s *RedisSetEx) ExecuteCmd(params []string) (redis_response.RaftRedisRsp, error) {
	if 4 == len(params) {
		key := params[1]
		expireSecond := params[2]
		value := params[3]

		err := kv_data_str.SetEx(key, value, str_tool.Str2Int64(expireSecond, -1))
		if err != nil {
			return redis_response.CreateCmdErrRsp(err.Error()), nil
		} else {
			return redis_response.CreateStrSimpleRsp(redis_response.StrOk), nil
		}
	} else {
		return redis_response.CreateCmdErrRsp(redis_response.StrParamErr), nil
	}
}

func init() {
	var cmd = &RedisSetEx{}
	RegistCmdHandler(cmd)
}
