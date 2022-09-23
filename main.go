package main

import (
	"flag"
	"pixiu-dkv-go/dkv_raft"
	"pixiu-dkv-go/redis_cli"
	"pixiu-dkv-go/redis_server"
	"pixiu-dkv-go/store_engine/badger_store"
	"runtime"
	"strings"
)

func main() {
	var redisBench = flag.String("redisbench", "false", "redisbench")
	flag.Parse()

	if strings.Compare(*redisBench, "true") == 0 {
		redis_cli.TestRedisBench()
	}

	//flag.Set("log_dir", "logs")
	//flag.Set("alsologtostderr", "true")
	//flag.Set("stderrthreshold", "ERROR")

	runtime.GOMAXPROCS(runtime.NumCPU())

	//goleveldb.GetKvStoreEngine().InitStoreEngine()
	badger_store.GetKvStoreEngine().InitStoreEngine()
	dkv_raft.StartRaftServer()
	redis_server.ListenAndServe()
}

func init() {

}
