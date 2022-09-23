package redis_response

import (
	"net"
	"pixiu-dkv-go/dkv_tool/str_tool"
)

func EchoStrSimple(msg string, conn net.Conn) {
	conn.Write([]byte("+" + msg + "\r\n"))
}

func EchoStrBulkNil(conn net.Conn) {
	conn.Write([]byte("$-1\r\n"))
}

func EchoStrBulk(msg string, conn net.Conn) {
	if len(msg) <= 0 {
		conn.Write([]byte("$0\r\n\r\n"))
	} else {
		conn.Write([]byte("$" + str_tool.Int2Str(len(msg)) + "\r\n" + msg + "\r\n"))
	}
}

func EchoStrArray(msgs []string, conn net.Conn) {
	if msgs == nil || len(msgs) <= 0 {
		conn.Write([]byte("*0\r\n"))
	} else {
		conn.Write([]byte("*" + str_tool.Int2Str(len(msgs)) + "\r\n"))
		for _, v := range msgs {
			EchoStrBulk(v, conn)
		}
	}
}

func EchoStrErr(msg string, conn net.Conn) {
	conn.Write([]byte("-ERR " + msg + "\r\n"))
}

func EchoNumInt(num int, conn net.Conn) {
	conn.Write([]byte(":" + str_tool.Int2Str(num) + "\r\n"))
}

func EchoNumInt64(num int64, conn net.Conn) {
	conn.Write([]byte(":" + str_tool.Int642Str(num) + "\r\n"))
}
