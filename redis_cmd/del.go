package redis_cmd

import (
	"pixiu-dkv-go/kv_data/kv_data_meta"
	"pixiu-dkv-go/redis_response"
)

type RedisDel struct {
}

func (s *RedisDel) GetCmdLabel() string {
	return "del"
}

func (s *RedisDel) IsNeedRaftSync() bool {
	return true
}

func (s *RedisDel) ExecuteCmd(params []string) (redis_response.RaftRedisRsp, error) {
	if 2 == len(params) {
		key := params[1]

		err := kv_data_meta.RemoveKeyMeta(key)
		if err != nil {
			return redis_response.CreateCmdErrRsp(err.Error()), nil
		} else {
			return redis_response.CreateNumIntRsp(1), nil
		}
	} else {
		return redis_response.CreateCmdErrRsp(redis_response.StrParamErr), nil
	}
}

func init() {
	var cmd = &RedisDel{}
	RegistCmdHandler(cmd)
}
