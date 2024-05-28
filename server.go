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

var rServer server

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

const (
	clientFlagCloseAfterReply = 1 << 0
)

const (
	genericReplyBlockLen = 1024
)

type rObj struct {
	objectType uint8
	encoding   uint8
	data       any
}

func createStringObject(data any) rObj {
	return rObj{
		objectType: objectTypeString,
		encoding:   objectEncodingRaw,
		data:       data,
	}
}

type client struct {
	id       int64
	fd       int
	conn     *net.TCPConn
	queryBuf *bytes.Buffer

	reqType int

	argv         []rObj
	argc         int
	multiBulkLen int32
	bulkLen      int64

	flag int64

	reply                     [genericIOBufferLength]byte
	replyPos                  int64
	replyList                 *list.List
	sentLen                   int64
	clientElement             *list.Element
	clientPendingWriteElement *list.Element
}

type server struct {
	NextClientId int64
	Clients      *list.List

	ClientsPendingWrite *list.List

	EL *EventLoop
}

func createClient(el *EventLoop, tcpConn *net.TCPConn) error {

	tcpFd, err := tcpConn.File()

	if err != nil {
		Log("acceptConnection AddFileEvent error=%v", err)
		return err
	}

	fd := int(tcpFd.Fd())

	client := &client{
		id:           atomic.LoadInt64(&rServer.NextClientId),
		conn:         tcpConn,
		fd:           fd,
		queryBuf:     bytes.NewBuffer(nil),
		argv:         make([]rObj, 0),
		multiBulkLen: 0,
		bulkLen:      -1,
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

	atomic.AddInt64(&rServer.NextClientId, 1)
	rServer.Clients.PushBack(client)
	client.clientElement = rServer.Clients.Back()
	return nil
}

func freeClient(client *client) {

	Log("client closed, fd=%d", client.fd)

	if err := rServer.EL.DelFileEvent(client.fd, ELMaskReadable|ELMaskWritable); err != nil {
		Log("del client event, fd=%d, err=%v", client.fd, err)
	}

	rServer.Clients.Remove(client.clientElement)
	if client.clientPendingWriteElement != nil {
		rServer.ClientsPendingWrite.Remove(client.clientPendingWriteElement)
	}

	_ = client.conn.Close()
	client.queryBuf = nil
	client.argv = nil
	if client.replyList != nil {
		client.replyList = nil
	}
}

func readQueryFromClient(el *EventLoop, fd int, mask uint8, clientData interface{}) {
	client := clientData.(*client)
	Log("readQueryFromClient fd=%d", fd)

	nRead := genericIOBufferLength

	if client.reqType == reqTypeMultiBulk && client.bulkLen != -1 && client.bulkLen >= bulkBigArgs {
		remaining := int(client.bulkLen) + 2 - client.queryBuf.Len()
		if remaining < nRead {
			nRead = remaining
		}
	}

	// todo make buffer poolable and reuse
	buf := make([]byte, nRead)

	read, err := client.conn.Read(buf)

	if err != nil {
		if err == io.EOF || errors.Is(err, syscall.EINVAL) {
			freeClient(client)
			return
		}
		Log("try to Read From Connection error=%v", err)
		return
	}

	_, err = client.queryBuf.Write(buf[:read])

	if err != nil {
		Log("write data from connection error=%v", err)
		freeClient(client)
		return
	}

	if client.queryBuf.Len() > clientMaxQueryBufLen {
		// todo reply error.
		freeClient(client)
		return
	}

	processInputBuffer(client)

}

func initServer(el *EventLoop) {
	rServer.Clients = list.New()
	rServer.ClientsPendingWrite = list.New()
	rServer.NextClientId = 0
	rServer.EL = el
}

func handleClientsWithPendingWrite() {

	for rServer.ClientsPendingWrite.Len() > 0 {

		c := rServer.ClientsPendingWrite.Front().Value.(*client)
		c.flag ^= clientPendingWrite
		rServer.ClientsPendingWrite.Remove(c.clientPendingWriteElement)
		if err := c.writeToClient(false); err != nil {
			continue
		}

		if c.hasPendingOutputs() {
			f, err := c.conn.File()
			if err != nil {
				freeClient(c)
				continue
			}
			err = rServer.EL.AddFileEvent(f, ELMaskWritable, c.sendReplyToClient, c)
			if err != nil {
				freeClient(c)
				continue
			}
		}

	}

}

func (c *client) sendReplyToClient(el *EventLoop, fd int, mask uint8, clientData any) {
	_ = c.writeToClient(true)
}

func (c *client) writeToClient(handleInstalled bool) error {

	var nWritten int
	var err error

	defer func() {
		if err != nil {
			freeClient(c)
		}
	}()

	for c.hasPendingOutputs() {

		if c.replyPos > 0 {
			nWritten, err = c.conn.Write(c.reply[c.sentLen:c.replyPos])
			if err != nil {
				return err
			}
			c.sentLen += int64(nWritten)
			if c.sentLen == c.replyPos {
				c.sentLen = 0
				c.replyPos = 0
			}
		} else if c.replyList != nil {

			block := c.replyList.Front().Value.(*bufferBlock)

			if block.len == 0 {
				c.replyList.Remove(c.replyList.Front())
				continue
			}

			nWritten, err = c.conn.Write(block.data[int(c.sentLen):block.pos])
			if err != nil {
				return err
			}

			c.sentLen += int64(nWritten)
			if c.sentLen == int64(block.pos) {
				c.sentLen = 0
				c.replyList.Remove(c.replyList.Front())
			}

		}
	}

	if !c.hasPendingOutputs() {

		c.sentLen = 0

		if handleInstalled {
			err = rServer.EL.DelFileEvent(c.fd, ELMaskWritable)
			if err != nil {
				return err
			}
		}

		if c.flag&clientCloseAfterReply > 0 {
			return errors.New("client close after reply")
		}

	}

	return nil

}
