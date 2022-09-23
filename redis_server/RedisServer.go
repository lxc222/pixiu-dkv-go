package redis_server

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"pixiu-dkv-go/dkv_conf"
	"pixiu-dkv-go/dkv_raft"
	"pixiu-dkv-go/redis_cmd"
	"pixiu-dkv-go/redis_request"
	"pixiu-dkv-go/redis_response"
)

const (
	FLAG_CR         = '\r'
	FLAG_STOP_LF    = '\n'
	FLAG_BULK_STR   = '$'
	FLAG_STOP_ARRAY = '*'
	FLAG_SIMPLE_STR = '+'
)

func processClient2(conn net.Conn) {
	defer conn.Close()
	for {
		var readBuf [1024]byte
		readSize, err := conn.Read(readBuf[:])
		if err != nil {
			log.Println("Read from tcp server failed,err:", err)
			break
		}

		var packetData = string(readBuf[:readSize])
		log.Printf("Recived from client,packetData:%s\n", packetData)
		break
	}
}

func runAcceptClientLoop(listener net.Listener, connChannel chan net.Conn) {
	for {
		connection, err := listener.Accept()
		if err != nil {
			log.Println("【redis_server】Accept 失败: " + err.Error())
		} else {
			connChannel <- connection
		}
	}
}

func runHandleClientLoop(connChannel chan net.Conn) {
	log.Println("【redis_server】Wating connection ....")
	for {
		select {
		case conn := <-connChannel:
			remoteAddr := conn.RemoteAddr()
			log.Println("【redis_server】Client " + remoteAddr.String() + " connected")
			go handleConnLoop(conn)
		}
	}

}

func readOneLine(reader *bufio.Reader) (string, error) {
	msg, err := reader.ReadString(FLAG_STOP_LF)
	if err != nil {
		// 通常遇到的错误是连接中断或被关闭，用io.EOF表示
		if err == io.EOF {
			log.Println("【redis_server】readOneLine close")
		} else {
			log.Printf("【redis_server】readOneLine err: %s\n", err)
		}

		return "", err
	} else {
		return msg, nil
	}
}

func handleCmd(params []string) redis_response.RaftRedisRsp {
	if !dkv_conf.GetAppConf().SingleMode && dkv_raft.GetDkvRaftAgent() == nil {
		return redis_response.CreateCmdErrRsp("dkv_raft.GetDkvRaftAgent() nil")
	}

	log.Println("++++++++++++++++++++++++++++++++++++++++++++++++++++++")
	for idx, v := range params {
		log.Printf("Param_C: %d --> %s", idx, v)
	}

	var handler = redis_cmd.GetCmdHandler(params[0])
	if handler == nil {
		log.Println("++++++++++++++++++++++++++++++++++++++++++++++++++++++")
		for idx, v := range params {
			log.Printf("UnknownCmd：%d --> %s", idx, v)
		}

		return redis_response.CreateCmdErrRsp("unknown command")
	}

	if handler.IsNeedRaftSync() {
		if dkv_raft.GetDkvRaftAgent() != nil {
			return dkv_raft.GetDkvRaftAgent().Propose(params)
		} else {
			var redisRsp, err = handler.ExecuteCmd(params)
			if err == nil {
				return redisRsp
			} else {
				return redis_response.CreateCmdErrRsp(err.Error())
			}
		}
	} else {
		var redisRsp, err = handler.ExecuteCmd(params)
		if err == nil {
			return redisRsp
		} else {
			return redis_response.CreateCmdErrRsp(err.Error())
		}
	}
}

func writeResponse(conn net.Conn, cmdRsp redis_response.RaftRedisRsp) {
	if redis_response.CodeRspStrSimple == cmdRsp.RspType {
		redis_response.EchoStrSimple(cmdRsp.RspStr, conn)
	} else if redis_response.CodeRspCmdErr == cmdRsp.RspType {
		redis_response.EchoStrErr(cmdRsp.RspStr, conn)
	} else if redis_response.CodeRspNumber == cmdRsp.RspType {
		redis_response.EchoNumInt64(cmdRsp.RspNum, conn)
	} else if redis_response.CodeRspStrBulk == cmdRsp.RspType {
		redis_response.EchoStrBulk(cmdRsp.RspStr, conn)
	} else if redis_response.CodeRspStrBulkNil == cmdRsp.RspType {
		redis_response.EchoStrBulkNil(conn)
	} else if redis_response.CodeRspStrArray == cmdRsp.RspType {
		redis_response.EchoStrArray(cmdRsp.RspStrArray, conn)
	} else if redis_response.CodeRspCmdErr == cmdRsp.RspType {
		log.Println(cmdRsp)
		redis_response.EchoStrErr("cmd err", conn)
	} else if redis_response.CodeRspCloseClient == cmdRsp.RspType {
		conn.Close()
	} else {
		log.Println(cmdRsp)
		redis_response.EchoStrErr("unknown cmd response", conn)
	}
}

func handleConnLoop(conn net.Conn) {
	log.Println("【redis_server】handle connection " + conn.RemoteAddr().String())
	defer conn.Close()
	reader := bufio.NewReader(conn)
	var paramList []string = nil
	for {
		paramList = nil
		pkgMsgLine, err := readOneLine(reader)
		if err != nil {
			break
		}

		if pkgMsgLine[0] == FLAG_STOP_ARRAY {
			var paramSize = -99
			{
				paramSize = redis_request.ParseParamSize(pkgMsgLine)
				//fmt.Println(paramSize)
				if paramSize >= 1 {
					paramList = make([]string, 0, paramSize)
				} else {
					break
				}
			}

			for i := 0; i < paramSize; i++ {
				{
					msg, err := readOneLine(reader)
					if err != nil {
						return
					}

					redis_request.ParseParamStrSize(msg)
				}
				{
					msg, err := readOneLine(reader)
					if err != nil {
						return
					}

					var param = redis_request.ParseParamStr(msg)
					paramList = append(paramList, param)
				}
			}
		} else {
			paramList = append(paramList, redis_request.ParseParamStr(pkgMsgLine))
		}

		//log.Println(paramList)

		var raftRedisRsp = handleCmd(paramList)
		writeResponse(conn, raftRedisRsp)
	}
}

var connChannel = make(chan net.Conn)
var signChannel = make(chan os.Signal, 1)
var exitChanel = make(chan int)

func ListenAndServe() {
	var address = dkv_conf.GetAppConf().RedisAddr
	go installSignNotify()
	listener, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatal(fmt.Sprintf("【redis_server】listen err: %v", err))
	}

	defer func() {
		log.Println("【redis_server】Close listenning ....")
		listener.Close()
		log.Println("【redis_server】Shutdown")
	}()

	log.Printf(fmt.Sprintf("【redis_server】bind: %s, start listening...", address))

	go runAcceptClientLoop(listener, connChannel)
	go runHandleClientLoop(connChannel)
	go dispatchInnerTask()

	for {
		select {
		case <-signChannel:
			log.Println("【redis_server】Get shutdown sign")
			go notifyExitSignal()
			goto EXIT
		}
	}

EXIT:
	log.Println("【redis_server】Waiting gorouting exit ....")
}

func installSignNotify() {
	signal.Notify(signChannel, os.Interrupt, os.Kill)
}

func notifyExitSignal() {
	exitChanel <- 1
}

func dispatchInnerTask() {
	log.Println("【redis_server】Init task moniter ....")
	go runInnerTaskLoop()
	log.Println("【redis_server】Init task moniter DONE!")
}

func runInnerTaskLoop() {
	for {
		log.Println("【redis_server】Wating task ....")
		select {
		case <-exitChanel:
			log.Println("【redis_server】Woker get exit sign")
			goto LOOP_STOP
			//default:
		}
	}
LOOP_STOP:
	//TODO: Clear undo task
}
