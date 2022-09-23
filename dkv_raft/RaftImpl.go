package dkv_raft

import (
	"github.com/coreos/etcd/raft/raftpb"
	"log"
	"pixiu-dkv-go/dkv_conf"
	"strings"
)

var (
	gRaftKvAgent *DkvRaftAgent = nil
)

func GetDkvRaftAgent() *DkvRaftAgent {
	return gRaftKvAgent
}

func StartRaftServer() {
	if dkv_conf.GetAppConf().SingleMode {
		return
	}

	go func() {
		log.Printf("Begin start raft server......")
		var clusterIds = dkv_conf.GetAppConf().RaftNodeAll
		var raftNodeId = dkv_conf.GetAppConf().RaftNodeId
		var isJoinExistCluster = false

		var confChangeChan = make(chan raftpb.ConfChange)
		defer close(confChangeChan)

		// raft provides a DkvCommit stream for the proposals from the http api
		var getSnapshotFunc = func() ([]byte, error) { return gRaftKvAgent.getForSnap() }
		commitChan, errorChan, snapReadyChan := CreateDkvRaftNode(raftNodeId, strings.Split(clusterIds, ","), isJoinExistCluster, getSnapshotFunc, confChangeChan)

		gRaftKvAgent = NewRaftAgent(<-snapReadyChan, commitChan, errorChan)
		log.Printf("完成raft_server启动(raftnode/kvstore)......%d", gRaftKvAgent)
		// exit when raft goes down
		if err, ok := <-errorChan; ok {
			log.Fatal(err)
		}

		log.Printf("Exit raft server......")
	}()
}

func init() {
	//
}
