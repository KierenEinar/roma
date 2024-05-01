package main

import (
	"bytes"
	"container/list"
	"errors"
	"golang.org/x/sys/unix"
	"io"
	"net"
	"sync/atomic"
	"syscall"
)

var server Server

const (
	objectTypeString = iota + 1
	objectTypeHash
	objectTypeList
	objectTypeSet
	objectTypeZSet
)

const (
	objectEncodingRaw = iota
)

type RObj struct {
	ObjectType uint8
	Encoding   uint8
	Data       []byte
}

func CreateStringObject(data []byte) RObj {
	return RObj{
		ObjectType: objectTypeString,
		Encoding:   objectEncodingRaw,
		Data:       data,
	}
}

type Client struct {
	Id       int64
	Fd       int
	Conn     *net.TCPConn
	QueryBuf *bytes.Buffer

	ReqType int

	Argv         []RObj
	Argc         int
	MultiBulkLen int32
	BulkLen      int64

	ClientElement *list.Element
}

type Server struct {
	NextClientId int64
	Clients      *list.List
}

func CreateClient(el *EventLoop, fd int, tcpConn *net.TCPConn) error {

	tcpFd, err := tcpConn.File()

	if err != nil {
		Log("acceptConnection AddFileEvent error=%v", err)
		return err
	}

	client := &Client{
		Id:           atomic.LoadInt64(&server.NextClientId),
		Conn:         tcpConn,
		Fd:           fd,
		QueryBuf:     bytes.NewBuffer(nil),
		Argv:         make([]RObj, 0),
		MultiBulkLen: 0,
		BulkLen:      -1,
	}

	if fd != -1 {
		if err = tcpConn.SetNoDelay(true); err != nil {
			Log("SetNoDelay error=%v, fd=%d", err, tcpFd.Fd())
		}

		if err = tcpConn.SetKeepAlive(true); err != nil {
			Log("SetKeepAlive error=%v, fd=%d", err, tcpFd.Fd())
		}

		if err = unix.SetNonblock(int(tcpFd.Fd()), true); err != nil {
			Log("SetNonblock error=%v,  fd=%d", err, tcpFd.Fd())
		}

		err = el.AddFileEvent(tcpFd, ELMaskReadable, readQueryFromClient, client)
		if err != nil {
			Log("readData AddFileEvent error=%v", err)
			_ = tcpConn.Close()
			return err
		}

	}

	atomic.AddInt64(&server.NextClientId, 1)
	server.Clients.PushBack(client)
	client.ClientElement = server.Clients.Back()
	return nil
}

func FreeClient(client *Client) {

}

func readQueryFromClient(el *EventLoop, fd int, mask uint8, clientData interface{}) {
	client := clientData.(*Client)
	Log("readQueryFromClient fd=%d", fd)

	nRead := genericIOBufferLength

	if client.ReqType == ReqTypeMultiBulk && client.BulkLen != -1 && client.BulkLen >= bulkBigArgs {
		remaining := int(client.BulkLen) + 2 - client.QueryBuf.Len()
		if remaining < nRead {
			nRead = remaining
		}
	}

	// todo make buffer poolable and reuse
	buf := make([]byte, nRead)

	read, err := client.Conn.Read(buf)

	if err != nil {
		if err == io.EOF || errors.Is(err, syscall.EINVAL) {
			FreeClient(client)
			return
		}
		Log("try to Read From Connection error=%v", err)
		return
	}

	_, err = client.QueryBuf.Write(buf[:read])

	if err != nil {
		Log("write data from connection error=%v", err)
		FreeClient(client)
		return
	}

	if client.QueryBuf.Len() > clientMaxQueryBufLen {
		// todo reply error.
		FreeClient(client)
		return
	}

	ProcessInputBuffer(client)

}

func initServer() {
	server.Clients = list.New()
	server.NextClientId = 0
}
