package dkv_raft

import (
	"errors"
	"net"
	"time"
)

// RaftStopListener sets TCP keep-alive timeouts on accepted
// connections and waits on stopChan message
type RaftStopListener struct {
	*net.TCPListener
	stopChan <-chan struct{}
}

func createRaftStopListener(addr string, stopc <-chan struct{}) (*RaftStopListener, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &RaftStopListener{ln.(*net.TCPListener), stopc}, nil
}

func (ln RaftStopListener) Accept() (c net.Conn, err error) {
	connc := make(chan *net.TCPConn, 1)
	errc := make(chan error, 1)
	go func() {
		tc, err := ln.AcceptTCP()
		if err != nil {
			errc <- err
			return
		}
		connc <- tc
	}()
	select {
	case <-ln.stopChan:
		return nil, errors.New("【RaftStopListener】server stopped")
	case err := <-errc:
		return nil, err
	case tc := <-connc:
		tc.SetKeepAlive(true)
		tc.SetKeepAlivePeriod(3 * time.Minute)
		return tc, nil
	}
}
