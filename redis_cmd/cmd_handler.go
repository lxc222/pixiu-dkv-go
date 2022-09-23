package redis_cmd

import (
	"pixiu-dkv-go/dkv_tool/str_tool"
	"pixiu-dkv-go/redis_response"
	"strings"
)

type RedisCmdHandler interface {
	GetCmdLabel() string
	IsNeedRaftSync() bool
	ExecuteCmd(params []string) (redis_response.RaftRedisRsp, error)
}

var redisCmdContainer = make(map[string]RedisCmdHandler, 200)

func GetCmdHandler(cmd string) RedisCmdHandler {
	if str_tool.StrIsEmpty(cmd) {
		return nil
	}

	var redisCmdStr = strings.ToLower(cmd)
	return redisCmdContainer[redisCmdStr]
}

func RegistCmdHandler(handler RedisCmdHandler) {
	if handler == nil || str_tool.StrIsEmpty(handler.GetCmdLabel()) {
		return
	}

	var redisCmdStr = strings.ToLower(handler.GetCmdLabel())
	redisCmdContainer[redisCmdStr] = handler
}
