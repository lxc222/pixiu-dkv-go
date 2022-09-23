package redis_cmd

import (
	"pixiu-dkv-go/dkv_tool/str_tool"
	"pixiu-dkv-go/kv_data/kv_data_meta"
	"pixiu-dkv-go/redis_response"
)

type RedisExpireAt struct {
}

func (s *RedisExpireAt) GetCmdLabel() string {
	return "expireat"
}

func (s *RedisExpireAt) IsNeedRaftSync() bool {
	return true
}

func (s *RedisExpireAt) ExecuteCmd(params []string) (redis_response.RaftRedisRsp, error) {
	if 3 == len(params) {
		key := params[1]
		expire := params[2]

		var expireMs = str_tool.Str2Int64(expire, -1) * 1000
		if expireMs <= 0 {
			return redis_response.CreateCmdErrRsp(redis_response.StrParamErr), nil
		}

		num, err := kv_data_meta.ExpireKeyMetaAbsMs(key, expireMs)
		if err != nil {
			return redis_response.CreateCmdErrRsp(err.Error()), nil
		} else {
			return redis_response.CreateNumIntRsp(num), nil
		}
	} else {
		return redis_response.CreateCmdErrRsp(redis_response.StrParamErr), nil
	}
}

func init() {
	var cmd = &RedisExpireAt{}
	RegistCmdHandler(cmd)
}
