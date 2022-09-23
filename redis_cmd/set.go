package redis_cmd

import (
	"log"
	"pixiu-dkv-go/dkv_tool/str_tool"
	"pixiu-dkv-go/kv_data/kv_data_str"
	"pixiu-dkv-go/redis_response"
	"strings"
)

type RedisSet struct {
}

func (s *RedisSet) GetCmdLabel() string {
	return "set"
}

func (s *RedisSet) IsNeedRaftSync() bool {
	return true
}

func (s *RedisSet) ExecuteCmd(params []string) (redis_response.RaftRedisRsp, error) {
	if 3 > len(params) {
		return redis_response.CreateCmdErrRsp(redis_response.StrParamErr), nil
	} else if 3 == len(params) {
		var key = params[1]
		var value = params[2]

		err := kv_data_str.SetEx(key, value, -1)
		if err != nil {
			return redis_response.CreateCmdErrRsp(err.Error()), nil
		} else {
			return redis_response.CreateStrSimpleRsp(redis_response.StrOk), nil
		}
	} else {
		var key = params[1]
		var value = params[2]

		var isNxOrXx = 0
		var expireSecond int64 = -1
		for i := 3; i < len(params); {
			var strV = strings.ToLower(params[i])
			if "nx" == strV {
				isNxOrXx = 1
				i = i + 1
			} else if "xx" == strV {
				isNxOrXx = 2
				i = i + 1
			} else if "ex" == strV {
				if (i + 1) >= len(params) {
					return redis_response.CreateCmdErrRsp(redis_response.StrParamErr), nil
				}

				expireSecond = str_tool.Str2Int64(params[i+1], -1)
				i = i + 2
			} else if "px" == strV {
				if i >= (len(params) - 1) {
					return redis_response.CreateCmdErrRsp(redis_response.StrParamErr), nil
				}

				expireSecond = str_tool.Str2Int64(params[i+1], -1) / 1000
				i = i + 2
			} else {
				return redis_response.CreateCmdErrRsp(redis_response.StrParamErr), nil
			}
		}

		isNxOrXx = 0
		log.Printf("isNxOrXx: %d", isNxOrXx)
		log.Printf("expireSecond: %d", expireSecond)

		err := kv_data_str.SetEx(key, value, expireSecond)
		if err != nil {
			return redis_response.CreateCmdErrRsp(err.Error()), nil
		} else {
			return redis_response.CreateStrSimpleRsp(redis_response.StrOk), nil
		}
	}
}

func init() {
	var cmd = &RedisSet{}
	RegistCmdHandler(cmd)
}
