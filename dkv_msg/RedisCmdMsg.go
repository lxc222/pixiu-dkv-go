package dkv_msg

type RedisCmdMsg struct {
	NodeId  int
	MsgId   string
	Payload []string
}

func CreateRedisCmdMsg(nodeId int, msgId string, payload []string) RedisCmdMsg {
	var msg = RedisCmdMsg{}
	msg.NodeId = nodeId
	msg.MsgId = msgId
	msg.Payload = payload
	return msg
}
