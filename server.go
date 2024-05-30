package main

import (
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
	objectEncodingEmbedding
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
	queryBuf []byte

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
	nextClientId int64
	clients      *list.List

	clientsPendingWrite *list.List

	el *EventLoop
}

func createClient(el *EventLoop, tcpConn *net.TCPConn) error {

	tcpFd, err := tcpConn.File()

	if err != nil {
		Log("acceptConnection AddFileEvent error=%v", err)
		return err
	}

	fd := int(tcpFd.Fd())

	client := &client{
		id:           atomic.LoadInt64(&rServer.nextClientId),
		conn:         tcpConn,
		fd:           fd,
		queryBuf:     make([]byte, 0, genericIOBufferLength),
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

	atomic.AddInt64(&rServer.nextClientId, 1)
	rServer.clients.PushBack(client)
	client.clientElement = rServer.clients.Back()
	return nil
}

func freeClient(c *client) {

	Log("client closed, fd=%d", c.fd)

	if err := rServer.el.DelFileEvent(c.fd, ELMaskReadable|ELMaskWritable); err != nil {
		Log("del client event, fd=%d, err=%v", c.fd, err)
	}

	rServer.clients.Remove(c.clientElement)
	if c.clientPendingWriteElement != nil {
		rServer.clientsPendingWrite.Remove(c.clientPendingWriteElement)
	}

	_ = c.conn.Close()
	c.argv = nil
	if c.replyList != nil {
		c.replyList = nil
	}

	c.queryBuf = c.queryBuf[:0]
}

func readQueryFromClient(el *EventLoop, fd int, mask uint8, clientData any) {
	c := clientData.(*client)
	Log("readQueryFromClient fd=%d", fd)

	nRead := genericIOBufferLength

	if c.reqType == reqTypeMultiBulk && c.bulkLen != -1 && c.bulkLen >= bulkBigArgs {
		remaining := int(c.bulkLen) + 2 - len(c.queryBuf)
		if remaining < nRead {
			nRead = remaining
		}
	}

	buf := make([]byte, nRead)

	read, err := c.conn.Read(buf)

	if err != nil {
		if err == io.EOF || errors.Is(err, syscall.EINVAL) {
			freeClient(c)
			return
		}
		Log("try to Read From Connection error=%v", err)
		return
	}

	c.queryBuf = append(c.queryBuf, buf[:read]...)

	if len(c.queryBuf) > clientMaxQueryBufLen {
		addReplyError(c, "invalid query buf length")
		setProtocolError(c, "query buf too long")
		return
	}

	processInputBuffer(c)

}

func initServer(el *EventLoop) {
	rServer.clients = list.New()
	rServer.clientsPendingWrite = list.New()
	rServer.nextClientId = 0
	rServer.el = el
}

func handleClientsWithPendingWrite() {

	for rServer.clientsPendingWrite.Len() > 0 {

		c := rServer.clientsPendingWrite.Front().Value.(*client)
		c.flag ^= clientPendingWrite
		rServer.clientsPendingWrite.Remove(c.clientPendingWriteElement)
		if err := c.writeToClient(false); err != nil {
			continue
		}

		if c.hasPendingOutputs() {
			f, err := c.conn.File()
			if err != nil {
				freeClient(c)
				continue
			}
			err = rServer.el.AddFileEvent(f, ELMaskWritable, c.sendReplyToClient, c)
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
			err = rServer.el.DelFileEvent(c.fd, ELMaskWritable)
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

func processCommand(c *client) {

}
