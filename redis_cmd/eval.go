package redis_cmd

import (
	lua "github.com/yuin/gopher-lua"
	"log"
	"pixiu-dkv-go/redis_response"
)

type RedisEval struct {
}

func (s *RedisEval) GetCmdLabel() string {
	return "eval"
}

func (s *RedisEval) IsNeedRaftSync() bool {
	return true
}

func (s *RedisEval) ExecuteCmd(params []string) (redis_response.RaftRedisRsp, error) {
	if 2 <= len(params) {
		script := params[1]

		L := lua.NewState()
		defer L.Close()
		if err := L.DoString(script); err != nil {
			log.Println(err)
		}

		return redis_response.CreateNumInt32Rsp(1), nil
	} else {
		//return redis_request.CreateCmdErrRsp(redis_request.StrParamErr), nil
		return redis_response.CreateNumInt32Rsp(1), nil
	}
}

func init() {
	var cmd = &RedisEval{}
	RegistCmdHandler(cmd)
}
