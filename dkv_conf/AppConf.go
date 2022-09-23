package dkv_conf

import (
	"fmt"
	"io/ioutil"
	"log"
	"pixiu-dkv-go/dkv_tool/json_tool"
	"pixiu-dkv-go/dkv_tool/str_tool"
)

var gAppConf *AppConf = nil

func GetAppConf() *AppConf {
	return gAppConf
}

type AppConf struct {
	RedisAddr        string `json:"listen_redis"`
	RpcAddr          string `json:"listen_rpc"`
	CleanTrashKey    bool   `json:"clean_trash_key"`
	AutoCleanAllKeys bool   `json:"auto_clean_all_keys"`
	KeyRangeNum      int    `json:"key_range_num"`
	KvDataPath       string `json:"kv_data_path"`
	RaftDataPath     string `json:"raft_data_path"`
	RaftNodeId       int    `json:"raft_node_id"`
	RaftNodeAll      string `json:"raft_node_all"`
	SingleMode       bool   `json:"single_mode"`
}

func (th *AppConf) GetNodeIdStr() string {
	return str_tool.Int2Str(th.RaftNodeId)
}

func loadAppConf() {
	content, _ := ioutil.ReadFile("app_dkv.conf")
	appConf := AppConf{}
	err := json_tool.FromJsonStr(content, &appConf)
	if err == nil {
		gAppConf = &appConf
		fmt.Println(json_tool.ToJsonStr2(appConf))
	} else {
		log.Printf("loadAppConf err, %s\n", err)
	}
}

func init() {
	loadAppConf()
}
