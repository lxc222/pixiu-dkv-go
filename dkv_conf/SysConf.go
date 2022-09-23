package dkv_conf

import (
	"github.com/twmb/murmur3"
	"pixiu-dkv-go/dkv_tool/common_tool"
	"runtime"
	"sync"
	"sync/atomic"
)

var gSysConfig *SysConfig = nil

func GetSysConf() *SysConfig {
	return gSysConfig
}

type SysConfig struct {
	processorNum int
	sysStartupMs int64
	dataVersion  int64
	regionMutex  []*sync.RWMutex
}

func (c *SysConfig) GetNextDataVersion() int64 {
	return atomic.AddInt64(&c.dataVersion, 1)
}

func (c *SysConfig) GetKeyRegion(key string) (uint64, uint16) {
	var _, sum = murmur3.StringSum128(key)
	var mod = uint16(sum % 1000)
	return sum, mod
}

func (c *SysConfig) GetRegionMutex(key string) *sync.RWMutex {
	var _, region = c.GetKeyRegion(key)
	return c.regionMutex[region]
}

func init() {
	gSysConfig = new(SysConfig)
	gSysConfig.processorNum = runtime.NumCPU()
	gSysConfig.sysStartupMs = common_tool.CurrentMS()
	gSysConfig.dataVersion = common_tool.CurrentMS()
	gSysConfig.regionMutex = make([]*sync.RWMutex, RegionNum)
	for i := 0; i < RegionNum; i++ {
		gSysConfig.regionMutex[i] = &sync.RWMutex{}
	}
}
