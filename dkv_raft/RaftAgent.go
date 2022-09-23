package dkv_raft

import (
	"bytes"
	"context"
	"encoding/gob"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/snap"
	"log"
	"pixiu-dkv-go/dkv_conf"
	"pixiu-dkv-go/dkv_msg"
	"pixiu-dkv-go/dkv_tool/common_tool"
	"pixiu-dkv-go/redis_cmd"
	"pixiu-dkv-go/redis_response"
	"sync"
)

type DkvRaftAgent struct {
	mutex       sync.Mutex
	snapshotter *snap.Snapshotter
}

func NewRaftAgent(snap *snap.Snapshotter, commitChan <-chan *DkvCommit, errorChan <-chan error) *DkvRaftAgent {
	log.Println("Begin start kv store......")

	s := &DkvRaftAgent{snapshotter: snap}
	snapshot, err := s.loadSnapshot()
	if err != nil {
		log.Panic(err)
	}

	if snapshot != nil {
		log.Printf("【NewRaftAgent】loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
		if err := s.recoverFromSnap(snapshot.Data); err != nil {
			log.Panic(err)
		}
	}

	log.Println("------------启动goroutine-readCommits-------------")
	// read commits from raft into kvStore map until error
	go s.readCommits(commitChan, errorChan)

	log.Println("完成raft_kv_store启动......")
	return s
}

func (th *DkvRaftAgent) Propose(cmdParams []string) redis_response.RaftRedisRsp {
	var cmdMsg = dkv_msg.CreateRedisCmdMsg(dkv_conf.GetAppConf().RaftNodeId, common_tool.Uuid(), cmdParams)
	var proposeBuf bytes.Buffer
	if err := gob.NewEncoder(&proposeBuf).Encode(cmdMsg); err != nil {
		log.Printf("提交Propose失败【编码】: %s\n", cmdMsg)
		return redis_response.CreateCmdErrRsp("inner_error")
	}

	th.mutex.Lock()
	defer th.mutex.Unlock()

	// blocks until accepted by raft state machine
	err := GetDkvRaftNode().raftNode.Propose(context.TODO(), []byte(proposeBuf.String()))
	if err != nil {
		log.Printf("提交Propose失败【proposeChan】: %s, %s\n", err, cmdMsg)
		return redis_response.CreateCmdErrRsp("inner_error")
	} else {
		log.Printf("提交Propose成功【proposeChan】: %s\n", cmdMsg)
		var redisHandler = redis_cmd.GetCmdHandler(cmdMsg.Payload[0])
		var redisRsp, err = redisHandler.ExecuteCmd(cmdMsg.Payload)
		if err != nil {
			log.Printf("Redis面临异常: %s\n", cmdMsg)
			return redis_response.CreateCmdErrRsp("inner_error")
		} else {
			return redisRsp
		}
	}
}

func (th *DkvRaftAgent) readCommits(commitChan <-chan *DkvCommit, errorChan <-chan error) {
	for commitChan != nil {
		commit, ok := <-commitChan
		if !ok {
			log.Printf("开始处理readCommits事件失败\n")
			continue
		} else {
			log.Printf("开始处理readCommits事件成功\n")
		}

		log.Println("开始处理readCommits事件")
		if commit == nil {
			log.Println("开始处理readCommits事件， commitData == nil")
			return
		}

		//log.Println("开始处理readCommits事件， commitData != nil")
		var cmdMsg dkv_msg.RedisCmdMsg
		dec := gob.NewDecoder(bytes.NewBufferString(commit.data))
		if err := dec.Decode(&cmdMsg); err != nil {
			log.Fatalf("raftexample: could not decode message (%v)", err)
		}

		log.Println("**************************************")
		for idx, v := range cmdMsg.Payload {
			log.Printf("ReadCommits: %d --> %s", idx, v)
		}

		var redisHandler = redis_cmd.GetCmdHandler(cmdMsg.Payload[0])
		redisHandler.ExecuteCmd(cmdMsg.Payload)

		close(commit.applyDoneC)

		if cmdMsg.NodeId == dkv_conf.GetAppConf().RaftNodeId {
			log.Println(cmdMsg)
		}
	}

	log.Println("开始处理readCommits事件， errorChan")
	if err, ok := <-errorChan; ok {
		log.Fatal(err)
	}
}

func (th *DkvRaftAgent) getForSnap() ([]byte, error) {
	//th.mu.Lock()
	//defer th.mu.Unlock()
	//return json.Marshal(th.kvStoreMap)
	return []byte(""), nil
}

func (th *DkvRaftAgent) recoverFromSnap(snapshot []byte) error {
	//var store map[string]string
	//if err := json.Unmarshal(snapshot, &store); err != nil {
	//	return err
	//}

	//th.mu.Lock()
	//th.kvStoreMap = store
	//th.mu.Unlock()
	return nil
}

func (th *DkvRaftAgent) loadSnapshot() (*raftpb.Snapshot, error) {
	snapshot, err := th.snapshotter.Load()
	if err == snap.ErrNoSnapshot {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	return snapshot, nil
}
