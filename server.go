package main

import (
	"container/list"
	"context"
	"errors"
	"golang.org/x/sys/unix"
	"io"
	"net"
	"runtime"
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

const (
	enableAsyncRWMinCPUS = 4
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

	clientsPendingWrite     *list.List
	clientsPendingRead      *list.List
	activeAsyncReadWrite    bool // is server in async read mode
	numConcurrenceReadWrite int  // num of goroutines in async read

	readWriteThreadActive   bool
	readWriteIOList         []*list.List
	readWriteIORecvChannels []chan chan struct{}
	readWriteIOSendChannels []chan struct{}
	ioRead                  bool

	stop func()

	el *EventLoop
}

func createClient(el *EventLoop, tcpConn *net.TCPConn) error {

	tcpFd, err := tcpConn.File()

	if err != nil {
		Log("acceptConnection AddFileEvent error=%v", err)
		return err
	}

	fd := int(tcpFd.Fd())

	c := &client{
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

		err = el.AddFileEvent(tcpFd, ELMaskReadable, acceptHandle, c)
		if err != nil {
			Log("readData AddFileEvent error=%v", err)
			_ = tcpConn.Close()
			return err
		}

	}

	atomic.AddInt64(&rServer.nextClientId, 1)
	rServer.clients.PushBack(c)
	c.clientElement = rServer.clients.Back()
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

func acceptHandle(el *EventLoop, fd int, mask uint8, clientData any) {
	c := clientData.(*client)
	readQueryFromClient(c)
}

func readQueryFromClient(c *client) {

	if postponeClientRead(c) {
		return
	}

	Log("readQueryFromClient fd=%d", c.fd)

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
	ctx, cancel := context.WithCancel(context.Background())
	rServer.stop = cancel
	rServer.clients = list.New()
	rServer.clientsPendingWrite = list.New()
	rServer.clientsPendingRead = list.New()
	rServer.nextClientId = 0

	cpus := runtime.NumCPU()
	if cpus >= enableAsyncRWMinCPUS {
		rServer.activeAsyncReadWrite = true
		rServer.numConcurrenceReadWrite = cpus / 2
	}

	rServer.el = el
	initThreadIO(ctx)

}

func postponeClientRead(c *client) bool {

	if !rServer.readWriteThreadActive {
		return false
	}

	if c.flag&clientPendingRead == 0 {
		c.flag |= clientPendingRead
		rServer.clientsPendingRead.PushBack(c)
		return true
	}

	return false
}

func initThreadIO(ctx context.Context) {

	if !rServer.activeAsyncReadWrite {
		return
	}

	rServer.readWriteThreadActive = false
	rServer.readWriteIOList = make([]*list.List, rServer.numConcurrenceReadWrite)
	rServer.readWriteIORecvChannels = make([]chan chan struct{}, rServer.numConcurrenceReadWrite)
	rServer.readWriteIOSendChannels = make([]chan struct{}, rServer.numConcurrenceReadWrite)

	for ix := 0; ix < rServer.numConcurrenceReadWrite; ix++ {
		rServer.readWriteIOList[ix] = list.New()
	}

	for i := 0; i < rServer.numConcurrenceReadWrite; i++ {
		go readWriteIO(ctx, i)
	}

	rServer.readWriteThreadActive = true

}

func readWriteIO(ctx context.Context, ioId int) {

	for {

		select {
		case <-ctx.Done():
			return
		case ch := <-rServer.readWriteIORecvChannels[ioId]:

			ioList := rServer.readWriteIOList[ioId]
			for ele := ioList.Front(); ele != nil; ele = ele.Next() {
				c := ioList.Remove(ele).(*client)
				if rServer.ioRead {
					readQueryFromClient(c)
				}
			}
			ch <- struct{}{} // notify main thread finished.
		}

	}

}

func handleClientsWithPendingRead() {

	if !rServer.activeAsyncReadWrite {
		return
	}

	if rServer.clientsPendingRead.Len() == 0 {
		return
	}

	for ix := 0; ix < rServer.clientsPendingRead.Len(); ix++ {
		ele := rServer.clientsPendingRead.Front()
		c := ele.Value.(*client)
		rServer.readWriteIOList[ix%rServer.numConcurrenceReadWrite].PushBack(c)
	}

	rServer.ioRead = true

	for ix := 1; ix < rServer.numConcurrenceReadWrite; ix++ {
		rServer.readWriteIORecvChannels[ix] <- rServer.readWriteIOSendChannels[ix]
	}

	for rServer.readWriteIOList[0].Len() > 0 {
		ele := rServer.readWriteIOList[0].Front()
		c := ele.Value.(*client)
		rServer.readWriteIOList[0].Remove(ele)

		readQueryFromClient(c)
	}

	// wait all goroutine to finished.
	for ix := 1; ix < rServer.numConcurrenceReadWrite; ix++ {
		<-rServer.readWriteIOSendChannels[ix]
	}

	for ele := rServer.clientsPendingRead.Front(); ele != nil; ele = ele.Next() {

		c := ele.Value.(*client)
		c.flag ^= clientPendingRead
		rServer.clientsPendingRead.Remove(ele)
		if c.flag&clientPendingCommand > 0 {
			c.flag ^= clientPendingCommand
			if !processCommandAndResetClient(c) {
				continue
			}
		}
		processInputBuffer(c)
	}

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

func processCommandAndResetClient(c *client) bool {
	return false
}

func resetClient(c *client) {

	c.argc = 0
	c.argv = nil
	c.multiBulkLen = 0
	c.bulkLen = -1
	c.flag = 0

}
