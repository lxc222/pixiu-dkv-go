package redis_cmd

import (
	"pixiu-dkv-go/redis_response"
)

type RedisQuit struct {
}

func (s *RedisQuit) GetCmdLabel() string {
	return "quit"
}

func (s *RedisQuit) IsNeedRaftSync() bool {
	return false
}

func (s *RedisQuit) ExecuteCmd(params []string) (redis_response.RaftRedisRsp, error) {
	return redis_response.CreateStrSimpleRsp(redis_response.StrOk), nil
}

func init() {
	var cmd = &RedisQuit{}
	RegistCmdHandler(cmd)
}
