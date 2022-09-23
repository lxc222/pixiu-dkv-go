package redis_response

const (
	CODE_RSP_EMPTY     byte   = 0
	CodeRspCmdErr      byte   = 1
	CodeRspNumber      byte   = 2
	CodeRspStrBulk     byte   = 3
	CodeRspStrBulkNil  byte   = 7
	CodeRspStrSimple   byte   = 4
	CodeRspStrArray    byte   = 5
	CodeRspCloseClient byte   = 6
	StrOk              string = "OK"
	StrParamErr        string = "param err"
)

type BaseRaftRsp struct {
}

type RaftRedisRsp struct {
	BaseRaftRsp
	RspType     byte
	RspNum      int64
	RspStr      string
	RspStrArray []string
}

func CreateCmdCloseClient() RaftRedisRsp {
	return RaftRedisRsp{BaseRaftRsp{}, CodeRspCloseClient, 0, "", nil}
}

func CreateStrSimpleRsp(str string) RaftRedisRsp {
	return RaftRedisRsp{BaseRaftRsp{}, CodeRspStrSimple, 0, str, nil}
}

func CreateStrBulkRsp(str string) RaftRedisRsp {
	return RaftRedisRsp{BaseRaftRsp{}, CodeRspStrBulk, 0, str, nil}
}

func CreateStrBulkNilRsp() RaftRedisRsp {
	return RaftRedisRsp{BaseRaftRsp{}, CodeRspStrBulkNil, 0, "", nil}
}

func CreateStrArrayRsp(strs []string) RaftRedisRsp {
	return RaftRedisRsp{BaseRaftRsp{}, CodeRspStrArray, 0, "", strs}
}

func CreateNumIntRsp(num int) RaftRedisRsp {
	return CreateNumInt64Rsp(int64(num))
}

func CreateNumInt32Rsp(num int32) RaftRedisRsp {
	return CreateNumInt64Rsp(int64(num))
}

func CreateNumInt64Rsp(num int64) RaftRedisRsp {
	return RaftRedisRsp{BaseRaftRsp{}, CodeRspNumber, num, "", nil}
}

func CreateCmdErrRsp(errStr string) RaftRedisRsp {
	return RaftRedisRsp{BaseRaftRsp{}, CodeRspCmdErr, 0, errStr, nil}
}
