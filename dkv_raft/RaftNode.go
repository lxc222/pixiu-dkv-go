package dkv_raft

import (
	"context"
	"fmt"
	"github.com/coreos/etcd/etcdserver/stats"
	"github.com/coreos/etcd/pkg/fileutil"
	"github.com/coreos/etcd/pkg/types"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/rafthttp"
	"github.com/coreos/etcd/snap"
	"github.com/coreos/etcd/wal"
	"github.com/coreos/etcd/wal/walpb"
	"log"
	"net/http"
	"net/url"
	"os"
	"pixiu-dkv-go/dkv_conf"
	"pixiu-dkv-go/dkv_tool/json_tool"
	"pixiu-dkv-go/dkv_tool/str_tool"
	"strconv"
	"time"
)

const (
	SnapCount               = 100
	SnapshotCatchUpEntriesN = 10000
)

type DkvCommit struct {
	data       string
	applyDoneC chan<- struct{}
}

type DkvRaftNode struct {
	confChangeChan <-chan raftpb.ConfChange // proposed cluster config changes
	commitChan     chan<- *DkvCommit        // entries committed to log (k,v)
	errorChan      chan<- error             // errors from raft session

	nodeId       int      // client ID for raft session
	nodePeers    []string // raft peer URLs
	isJoin       bool     // raftNode is joining an existing cluster
	walDir       string   // path to WAL directory
	snapDir      string   // path to snapshot directory
	getSnapDataF func() ([]byte, error)

	confState     raftpb.ConfState
	snapshotIndex uint64
	appliedIndex  uint64

	raftNode    raft.Node // raft backing for the DkvCommit/error channel
	raftStorage *raft.MemoryStorage
	wal         *wal.WAL

	snapshotter      *snap.Snapshotter
	snapshotterReady chan *snap.Snapshotter // signals when snapshotter is ready

	snapCount uint64
	transport *rafthttp.Transport
	stopChan  chan struct{} // signals proposal channel closed
	httpStopc chan struct{} // signals http server to shutdown
	httpDonec chan struct{} // signals http server shutdown complete
}

var dkvRaftNode *DkvRaftNode = nil

func GetDkvRaftNode() *DkvRaftNode {
	return dkvRaftNode
}

func CreateDkvRaftNode(id int, peers []string, isJoin bool, getSnapshot func() ([]byte, error),
	confChangeChan <-chan raftpb.ConfChange) (<-chan *DkvCommit, <-chan error, <-chan *snap.Snapshotter) {
	log.Println("Begin start raft raftNode......")

	commitChan := make(chan *DkvCommit)
	errorChan := make(chan error)

	dkvRaftNode = &DkvRaftNode{
		confChangeChan:   confChangeChan,
		commitChan:       commitChan,
		errorChan:        errorChan,
		nodeId:           id,
		nodePeers:        peers,
		isJoin:           isJoin,
		walDir:           fmt.Sprintf("%sdkv-raft-%d-wal", dkv_conf.GetAppConf().RaftDataPath, id),
		snapDir:          fmt.Sprintf("%sdkv-raft-%d-snap", dkv_conf.GetAppConf().RaftDataPath, id),
		getSnapDataF:     getSnapshot,
		snapCount:        SnapCount,
		stopChan:         make(chan struct{}),
		httpStopc:        make(chan struct{}),
		httpDonec:        make(chan struct{}),
		snapshotterReady: make(chan *snap.Snapshotter, 1),
		// rest of structure populated after WAL replay
	}

	go dkvRaftNode.startRaft()

	log.Println("Complete start raft raftNode......")
	return commitChan, errorChan, dkvRaftNode.snapshotterReady
}

func (d *DkvRaftNode) saveSnap(raftSnap raftpb.Snapshot) error {
	log.Printf("【saveSnap】--> %d, %s\n", d.nodeId, json_tool.ToJsonStr2(raftSnap))
	// must save the snapshot index to the WAL before saving the
	// snapshot to maintain the invariant that we only Open the
	// wal at previously-saved snapshot indexes.
	walSnap := walpb.Snapshot{
		Index: raftSnap.Metadata.Index,
		Term:  raftSnap.Metadata.Term,
	}

	//log.Println(pbutil.MustMarshal(&walSnap))
	if err := d.snapshotter.SaveSnap(raftSnap); err != nil {
		return err
	}

	if err := d.wal.SaveSnapshot(walSnap); err != nil {
		return err
	}

	return d.wal.ReleaseLockTo(raftSnap.Metadata.Index)
}

func (d *DkvRaftNode) entriesToApply(ents []raftpb.Entry) (nents []raftpb.Entry) {
	log.Printf("【entriesToApply】--> %d", d.nodeId)
	if len(ents) == 0 {
		return
	}

	firstIdx := ents[0].Index
	log.Printf("【entriesToApply】--> %d, %d, %d\n", d.nodeId, firstIdx, d.appliedIndex)
	if firstIdx > d.appliedIndex+1 {
		log.Fatalf("first index of committed entry[%d] should <= progress.appliedIndex[%d] 1", firstIdx, d.appliedIndex)
	}

	if d.appliedIndex-firstIdx+1 < uint64(len(ents)) {
		nents = ents[d.appliedIndex-firstIdx+1:]
	}

	return nents
}

// publishEntries writes committed log entries to DkvCommit channel and returns
// whether all entries could be published.
func (d *DkvRaftNode) publishEntries(pushEntry raftpb.Entry) (<-chan struct{}, bool) {
	log.Printf("【publishEntries】--> %d\n", d.nodeId)

	var data string = ""
	switch pushEntry.Type {
	case raftpb.EntryNormal:
		if len(pushEntry.Data) == 0 {
			break
		}

		data = string(pushEntry.Data)
	case raftpb.EntryConfChange:
		var cc raftpb.ConfChange
		cc.Unmarshal(pushEntry.Data)
		d.confState = *d.raftNode.ApplyConfChange(cc)
		switch cc.Type {
		case raftpb.ConfChangeAddNode:
			if len(cc.Context) > 0 {
				d.transport.AddPeer(types.ID(cc.NodeID), []string{string(cc.Context)})
			}
		case raftpb.ConfChangeRemoveNode:
			if cc.NodeID == uint64(d.nodeId) {
				log.Println("I've been removed from the cluster! Shutting down.")
				return nil, false
			}
			d.transport.RemovePeer(types.ID(cc.NodeID))
		}
	}

	var applyDoneC chan struct{}

	if !str_tool.StrIsEmpty(data) {
		applyDoneC = make(chan struct{}, 1)
		select {
		case d.commitChan <- &DkvCommit{data, applyDoneC}:
		case <-d.stopChan:
			return nil, false
		}
	}

	// after DkvCommit, update appliedIndex
	d.appliedIndex = pushEntry.Index
	log.Printf("【entriesToApply】--> %d, %d", d.nodeId, d.appliedIndex)

	return applyDoneC, true
}

func (d *DkvRaftNode) loadSnapshot() *raftpb.Snapshot {
	log.Printf("【loadSnapshot】--> %d", d.nodeId)
	snapshot, err := d.snapshotter.Load()
	if err != nil && err != snap.ErrNoSnapshot {
		log.Fatalf("raftexample: error loading snapshot (%v)", err)
	}

	log.Printf("------loadSnapshot: %s\n", json_tool.ToJsonStr2(snapshot))
	return snapshot
}

// openWal returns a WAL ready for reading.
func (d *DkvRaftNode) openWal(snapshot *raftpb.Snapshot) *wal.WAL {
	log.Printf("【openWal】--> %d", d.nodeId)
	if !wal.Exist(d.walDir) {
		if err := os.Mkdir(d.walDir, 0750); err != nil {
			log.Fatalf("raftexample: cannot create dir for wal (%v)", err)
		}

		w, err := wal.Create(d.walDir, nil)
		if err != nil {
			log.Fatalf("raftexample: create wal error (%v)", err)
		}

		err = w.Close()
		if err != nil {
			log.Fatalf("【openWal】: new wal close (%v)", err)
		}
	}

	walSnap := walpb.Snapshot{}
	if snapshot != nil {
		walSnap.Index, walSnap.Term = snapshot.Metadata.Index, snapshot.Metadata.Term
	}

	log.Printf("loading WAL at term %d and index %d", walSnap.Term, walSnap.Index)
	w, err := wal.Open(d.walDir, walSnap)
	if err != nil {
		log.Fatalf("raftexample: error loading wal (%v)", err)
	}

	return w
}

// replayWal replays WAL entries into the raft instance.
func (d *DkvRaftNode) replayWal() *wal.WAL {
	log.Printf("【replayWal】--> %d", d.nodeId)
	snapshot := d.loadSnapshot()
	w := d.openWal(snapshot)
	_, st, entrys, err := w.ReadAll()
	if err != nil {
		log.Fatalf("raftexample: failed to read WAL (%v)", err)
	} else {
		log.Printf("【replayWal】ReadAll--> %d, %d", d.nodeId, len(entrys))
	}

	d.raftStorage = raft.NewMemoryStorage()
	if snapshot != nil {
		err := d.raftStorage.ApplySnapshot(*snapshot)
		if err != nil {
			log.Fatalf("【replayWal】ApplySnapshot_fatal--> %d, %s\n", d.nodeId, json_tool.ToJsonStr2(*snapshot))
		} else {
			log.Printf("【replayWal】ApplySnapshot_succ--> %d, %s\n", d.nodeId, json_tool.ToJsonStr2(*snapshot))
		}
	}

	err = d.raftStorage.SetHardState(st)
	if err != nil {
		log.Fatalf("【replayWal】SetHardState_fatal--> %d, %s\n", d.nodeId, json_tool.ToJsonStr2(st))
	} else {
		log.Printf("【replayWal】SetHardState_succ--> %d, %s\n", d.nodeId, json_tool.ToJsonStr2(st))
	}

	// append to storage so raft starts at the right place in log
	err = d.raftStorage.Append(entrys)
	if err != nil {
		log.Fatalf("【replayWal】AppendRaft_fatal--> %d, %s\n", d.nodeId, json_tool.ToJsonStr2(st))
	} else {
		log.Printf("【replayWal】AppendRaft_succ--> %d, %s\n", d.nodeId, json_tool.ToJsonStr2(st))
	}

	return w
}

func (d *DkvRaftNode) writeError(err error) {
	d.stopHttp()
	close(d.commitChan)
	d.errorChan <- err
	close(d.errorChan)
	d.raftNode.Stop()
}

func (rc *DkvRaftNode) startRaft() {
	if !fileutil.Exist(rc.snapDir) {
		if err := os.Mkdir(rc.snapDir, 0750); err != nil {
			log.Fatalf("raftexample: cannot create dir for snapshot (%v)", err)
		}
	}

	rc.snapshotter = snap.New(rc.snapDir)
	rc.snapshotterReady <- rc.snapshotter

	isExistWal := wal.Exist(rc.walDir)
	rc.wal = rc.replayWal()

	rpeers := make([]raft.Peer, len(rc.nodePeers))
	for i := range rpeers {
		rpeers[i] = raft.Peer{ID: uint64(i + 1)}
	}

	nodeConfig := &raft.Config{
		ID:              uint64(rc.nodeId),
		ElectionTick:    200,
		HeartbeatTick:   10,
		Storage:         rc.raftStorage,
		MaxSizePerMsg:   1024 * 1024,
		MaxInflightMsgs: 256,
		CheckQuorum:     true,
	}

	if isExistWal || rc.isJoin {
		rc.raftNode = raft.RestartNode(nodeConfig)
		log.Printf("RestartNode, %s\n", json_tool.ToJsonStr2(nodeConfig))
	} else {
		rc.raftNode = raft.StartNode(nodeConfig, rpeers)
		log.Printf("StartNode, %s, %s\n", json_tool.ToJsonStr2(rpeers), json_tool.ToJsonStr2(nodeConfig))
	}

	rc.transport = &rafthttp.Transport{
		ID:          types.ID(rc.nodeId),
		ClusterID:   0x1000,
		Raft:        rc,
		ServerStats: stats.NewServerStats("", ""),
		LeaderStats: stats.NewLeaderStats(strconv.Itoa(rc.nodeId)),
		ErrorC:      make(chan error),
	}

	err := rc.transport.Start()
	if err != nil {
		log.Fatalf("【StartRaft】transport_start, %s\n", json_tool.ToJsonStr2(nodeConfig))
	}

	for i := range rc.nodePeers {
		if i+1 != rc.nodeId {
			rc.transport.AddPeer(types.ID(i+1), []string{rc.nodePeers[i]})
		}
	}

	go rc.serveRaft()
	go rc.serveChannels()
}

func (rc *DkvRaftNode) serveRaft() {
	nodeUrl, err := url.Parse(rc.nodePeers[rc.nodeId-1])
	if err != nil {
		log.Fatalf("raftexample: Failed parsing URL (%v)", err)
	}

	listener, err := createRaftStopListener(nodeUrl.Host, rc.httpStopc)
	if err != nil {
		log.Fatalf("raftexample: Failed to listen rafthttp (%v)", err)
	}

	err = (&http.Server{Handler: rc.transport.Handler()}).Serve(listener)
	select {
	case <-rc.httpStopc:
	default:
		log.Fatalf("raftexample: Failed to serve rafthttp (%v)", err)
	}
	close(rc.httpDonec)
}

// stop closes http, closes all channels, and stops raft.
func (d *DkvRaftNode) stop() {
	log.Printf("【stop】--> %d", d.nodeId)
	d.stopHttp()
	close(d.commitChan)
	close(d.errorChan)
	d.raftNode.Stop()
}

func (rc *DkvRaftNode) stopHttp() {
	log.Printf("【stopHttp】--> %d", rc.nodeId)
	rc.transport.Stop()
	close(rc.httpStopc)
	<-rc.httpDonec
}

func (d *DkvRaftNode) publishSnapshot(snapshotToSave raftpb.Snapshot) {
	log.Printf("【publishSnapshot】--> %d", d.nodeId)
	if raft.IsEmptySnap(snapshotToSave) {
		return
	}

	log.Printf("publishing snapshot at index %d", d.snapshotIndex)
	defer log.Printf("finished publishing snapshot at index %d", d.snapshotIndex)

	if snapshotToSave.Metadata.Index <= d.appliedIndex {
		log.Fatalf("snapshot index [%d] should > progress.appliedIndex [%d] + 1", snapshotToSave.Metadata.Index, d.appliedIndex)
	}

	d.commitChan <- nil // trigger kvstore to load snapshot

	d.confState = snapshotToSave.Metadata.ConfState
	d.snapshotIndex = snapshotToSave.Metadata.Index
	d.appliedIndex = snapshotToSave.Metadata.Index
}

func (d *DkvRaftNode) maybeTriggerSnapshot() {
	log.Printf("【maybeTriggerSnapshot】--> nodeId(%d), appliedIndex(%d), snapshotIndex(%d)\n", d.nodeId, d.appliedIndex, d.snapshotIndex)
	if (d.appliedIndex - d.snapshotIndex) <= d.snapCount { //超过指定数量才开始打快照
		return
	}

	log.Printf("start snapshot [applied index: %d | last snapshot index: %d]", d.appliedIndex, d.snapshotIndex)
	data, err := d.getSnapDataF()
	if err != nil {
		log.Printf("【maybeTriggerSnapshot】getSnapDataF faile, %s\n", err)
		return
	}

	raftSnap, err := d.raftStorage.CreateSnapshot(d.appliedIndex, &d.confState, data)
	if err != nil {
		log.Printf("【maybeTriggerSnapshot】CreateSnapshot faile, %s\n", err)
		return
	}

	if err := d.saveSnap(raftSnap); err != nil {
		log.Printf("【maybeTriggerSnapshot】saveSnap faile, %s\n", err)
		return
	}

	compactIndex := uint64(1)
	if d.appliedIndex > SnapshotCatchUpEntriesN {
		compactIndex = d.appliedIndex - SnapshotCatchUpEntriesN
	}

	if err := d.raftStorage.Compact(compactIndex); err != nil {
		log.Printf("【maybeTriggerSnapshot】Compact faile, %s\n", err)
	}

	log.Printf("【maybeTriggerSnapshot】compacted log at index %d", compactIndex)
	d.snapshotIndex = d.appliedIndex
}

func (d *DkvRaftNode) serveChannels() {
	raftSnap, err := d.raftStorage.Snapshot()
	if err != nil {
		panic(err)
	}

	d.confState = raftSnap.Metadata.ConfState
	d.snapshotIndex = raftSnap.Metadata.Index
	d.appliedIndex = raftSnap.Metadata.Index

	defer d.wal.Close()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	// send proposals over raft
	go func() {
		var confChangeCount uint64 = 0

		for d.confChangeChan != nil {
			select {
			case cc, ok := <-d.confChangeChan:
				if !ok {
					d.confChangeChan = nil
					log.Printf("收到confChangeChan变更失败--> %d", d.nodeId)
				} else {
					confChangeCount += 1
					cc.ID = confChangeCount
					log.Printf("收到confChangeChan变更，发送raft内部--> %d", d.nodeId)
					d.raftNode.ProposeConfChange(context.TODO(), cc)
				}
			}
		}
		// client closed channel; shutdown raft if not already
		close(d.stopChan)
	}()

	// event loop on raft state machine updates
	for {
		select {
		case <-ticker.C:
			//log.Printf("sm-ev-loop-ticker--> %d\n", d.nodeId)
			d.raftNode.Tick()
		// store raft entries to wal, then publish over DkvCommit channel
		case rd := <-d.raftNode.Ready():
			log.Println("")
			log.Println("")
			log.Println("**************************************************************")
			log.Printf("sm-ev-loop-ready(begin)--> %d, %s\n", d.nodeId, json_tool.ToJsonStr2(rd))
			// Must save the snapshot file and WAL snapshot entry before saving any other entries
			// or hardstate to ensure that recovery after a snapshot restore is possible.
			if !raft.IsEmptySnap(rd.Snapshot) {
				d.saveSnap(rd.Snapshot)
				log.Printf("sm-ev-loop-ready(save_snap)--> %d, %s\n", d.nodeId, json_tool.ToJsonStr2(rd.Snapshot))
			}

			d.wal.Save(rd.HardState, rd.Entries)
			log.Printf("sm-ev-loop-ready(save_wal)--> %d, %s\n", d.nodeId, json_tool.ToJsonStr2(rd.HardState))
			if !raft.IsEmptySnap(rd.Snapshot) {
				log.Printf("sm-ev-loop-ready(apply_snap)--> %d, %s\n", d.nodeId, json_tool.ToJsonStr2(rd.Snapshot))
				d.raftStorage.ApplySnapshot(rd.Snapshot)
				d.publishSnapshot(rd.Snapshot)
			}

			d.raftStorage.Append(rd.Entries)
			log.Printf("sm-ev-loop-ready(append_entry)--> %d, %s\n", d.nodeId, json_tool.ToJsonStr2(rd.Entries))
			d.transport.Send(rd.Messages)
			log.Printf("sm-ev-loop-ready(transport_msg)--> %d, %s\n", d.nodeId, json_tool.ToJsonStr2(rd.Messages))

			if rd.CommittedEntries != nil && len(rd.CommittedEntries) > 0 {
				pubEntries := d.entriesToApply(rd.CommittedEntries)
				for _, tmpEntry := range pubEntries {
					pubRstC, ok := d.publishEntries(tmpEntry)
					if !ok {
						log.Printf("publishEntries err, %s\n", ok)
						d.stop()
						return
					}

					// wait until all committed entries are applied (or server is closed)
					if pubRstC != nil {
						select {
						case <-pubRstC:
						case <-d.stopChan:
							return
						}
					}

					log.Printf("sm-ev-loop-ready(commit_entry)--> %d, %s\n", d.nodeId, json_tool.ToJsonStr2(tmpEntry))
				}

				d.maybeTriggerSnapshot()
			}

			d.raftNode.Advance()
			log.Printf("sm-ev-loop-ready(end)--> %d, %s\n", d.nodeId, json_tool.ToJsonStr2(d.raftNode.Status()))
		case err := <-d.transport.ErrorC:
			log.Printf("sm-ev-loop-err--> %d, %s\n", d.nodeId, err)
			d.writeError(err)
			return
		case <-d.stopChan:
			log.Printf("sm-ev-loop-stopChan--> %d\n", d.nodeId)
			d.stop()
			return
		}
	}
}

func (d *DkvRaftNode) Process(ctx context.Context, m raftpb.Message) error {
	return d.raftNode.Step(ctx, m)
}

func (rc *DkvRaftNode) IsIDRemoved(id uint64) bool {
	return false
}

func (rc *DkvRaftNode) ReportUnreachable(id uint64) {
	rc.raftNode.ReportUnreachable(id)
}

func (rc *DkvRaftNode) ReportSnapshot(id uint64, status raft.SnapshotStatus) {
	rc.raftNode.ReportSnapshot(id, status)
}
