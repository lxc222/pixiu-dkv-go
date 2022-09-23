package common_tool

import (
	uuid "github.com/satori/go.uuid"
	"pixiu-dkv-go/dkv_tool/str_tool"
	"time"
	"unsafe"
)

func CurrentMS() int64 {
	return time.Now().UnixNano() / 1e6
}

func IsLittleEndian() bool {
	i := uint16(1)
	return (*(*[2]byte)(unsafe.Pointer(&i)))[0] == 1
}

func Uuid() string {
	id := uuid.NewV4()
	return id.String() + "-" + str_tool.Int642Str(CurrentMS())
}
