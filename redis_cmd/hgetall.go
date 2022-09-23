package redis_cmd

import (
	"pixiu-dkv-go/kv_data/kv_data_hash"
	"pixiu-dkv-go/redis_response"
)

type RedisHGetall struct {
}

func (s *RedisHGetall) GetCmdLabel() string {
	return "hgetall"
}

func (s *RedisHGetall) IsNeedRaftSync() bool {
	return false
}

func (s *RedisHGetall) ExecuteCmd(params []string) (redis_response.RaftRedisRsp, error) {
	if 2 == len(params) {
		key := params[1]

		maps, err := kv_data_hash.HGetAll(key)
		if err != nil {
			return redis_response.CreateCmdErrRsp(err.Error()), nil
		} else {
			var rstList = make([]string, 0, len(maps)*2)
			for key, value := range maps {
				rstList = append(rstList, key)
				rstList = append(rstList, value)
			}

			return redis_response.CreateStrArrayRsp(rstList), nil
		}
	} else {
		return redis_response.CreateCmdErrRsp(redis_response.StrParamErr), nil
	}
}

func init() {
	var cmd = &RedisHGetall{}
	RegistCmdHandler(cmd)
}
