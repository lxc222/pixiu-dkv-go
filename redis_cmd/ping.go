package redis_cmd

import (
	"pixiu-dkv-go/redis_response"
)

type RedisPing struct {
}

func (s *RedisPing) GetCmdLabel() string {
	return "ping"
}

func (s *RedisPing) IsNeedRaftSync() bool {
	return false
}

func (s *RedisPing) ExecuteCmd(params []string) (redis_response.RaftRedisRsp, error) {
	if 2 <= len(params) {
		value := params[1]
		return redis_response.CreateStrBulkRsp(value), nil
	} else {
		return redis_response.CreateStrBulkRsp("PONG"), nil
	}
}

func init() {
	var cmd = &RedisPing{}
	RegistCmdHandler(cmd)
}
