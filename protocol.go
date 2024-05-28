package main

import (
	"bytes"
	"container/list"
	"strconv"
)

const (
	maxInlineLength       = 1024 * 64 // 64k for inline
	genericIOBufferLength = 1024 * 16 // 16k
	bulkBigArgs           = 1024 * 32
	clientMaxQueryBufLen  = 1024 * 1024 * 1024 // 1Gi
	maxBulkLen            = 1024 * 1024 * 512
)

const (
	genericBufferInitialLen = 1024
)

const (
	reqTypeInline = iota + 1
	reqTypeMultiBulk
)

const (
	replyBufferLen = 1024 * 16
)

// client flag
const (
	clientSlave           = 1 << 0
	clientCloseAfterReply = 1 << 6
	clientPendingWrite    = 1 << 21
)

func processInlineBuffer(c *client) bool {
	return false
}

type bufferBlock struct {
	len  int
	pos  int
	data []byte
}

func newBufferBlock(len int) *bufferBlock {
	return &bufferBlock{
		len:  len,
		data: make([]byte, len),
	}
}

func addReply(c *client, reply []byte) {

	if !prepareClientTotWrite(c) {
		return
	}

	replyLen := len(reply)

	// copy to reply buffer
	if int(c.replyPos) < len(c.reply) {
		copyLen := len(c.reply) - int(c.replyPos)
		if copyLen > replyLen {
			copyLen = replyLen
		}
		copy(c.reply[c.replyPos:], reply[:copyLen])
		c.replyPos += int64(copyLen)
		reply = reply[copyLen:]
		replyLen -= copyLen
	}

	// copy to reply block list
	for replyLen > 0 {

		var replyBlock *bufferBlock
		if c.replyList == nil {
			replyBlock = newBufferBlock(genericReplyBlockLen)
			c.replyList = list.New()
			c.replyList.PushBack(replyBlock)
		}

		replyBlock = c.replyList.Back().Value.(*bufferBlock)
		if replyBlock.pos == replyBlock.len {
			replyBlock = newBufferBlock(genericReplyBlockLen)
			c.replyList.PushBack(replyBlock)
		}

		copyLen := replyBlock.len - replyBlock.pos
		if replyLen < copyLen {
			copyLen = replyLen
		}

		copy(replyBlock.data[replyBlock.pos:], reply[:copyLen])
		replyLen -= copyLen
		reply = reply[copyLen:]
	}

}

func prepareClientTotWrite(c *client) bool {

	if !c.hasPendingOutputs() && c.flag&clientPendingWrite == 0 {
		c.flag |= clientPendingWrite
		rServer.ClientsPendingWrite.PushBack(c)
		c.clientPendingWriteElement = rServer.ClientsPendingWrite.Back()
	}

	return true
}

func (c *client) hasPendingOutputs() bool {
	return c.replyPos > 0 || c.replyList.Len() > 0
}

func AddReplyError(c *client, err string) {

}

func SetProtocolError(c *client, err string) {
}

func ProcessMultiBulkBuffer(c *client) bool {

	pos := 0
	buf := c.queryBuf.Bytes()

	if c.multiBulkLen == 0 {

		indexAt := bytes.IndexByte(buf, '\r')
		if indexAt == -1 {
			if len(buf) > maxInlineLength {
				AddReplyError(c, "query buf inline length too long.")
				SetProtocolError(c, "invalid inline length")
			}
			return false
		}

		// also need contains \n
		if indexAt+1 > len(buf)-1 {
			return false
		}

		multiBulkLen, err := strconv.ParseInt(string(buf[1:indexAt]), 10, 32)
		if err != nil || multiBulkLen > 1024*1024 {
			AddReplyError(c, "invalid multi bulk length")
			SetProtocolError(c, "invalid multi bulk length")
			return false
		}

		c.multiBulkLen = int32(multiBulkLen)
		pos += indexAt + 2
		buf = buf[pos:]
		c.queryBuf.Next(pos)
		pos = 0

		if multiBulkLen <= 0 {
			return true
		}

		c.argv = make([]rObj, multiBulkLen)

	}

	for c.multiBulkLen > 0 {

		if c.bulkLen == -1 {
			indexAt := bytes.IndexByte(buf, '\r')
			if indexAt == -1 {
				if len(buf) > maxInlineLength {
					AddReplyError(c, "query buf inline length too long.")
					SetProtocolError(c, "invalid inline length")
				}
				return false
			}

			if indexAt+1 > len(buf)-1 {
				return false
			}

			if buf[0] != '$' {
				AddReplyError(c, "bulk must start with $")
				SetProtocolError(c, "bulk must start with $")
				return false
			}

			bulkLen, err := strconv.ParseInt(string(buf[1:indexAt]), 10, 64)
			if err != nil {
				AddReplyError(c, "invalid bulk length")
				SetProtocolError(c, "invalid bulk length")
				return false
			}

			if bulkLen > maxBulkLen || bulkLen < 0 {
				AddReplyError(c, "invalid bulk length")
				SetProtocolError(c, "invalid bulk length")
				return false
			}

			c.bulkLen = bulkLen
			pos += indexAt + 2
			buf = buf[pos:]
			c.queryBuf.Next(pos)
			pos = 0

			if bulkLen >= bulkBigArgs {
				if bulkLen+2 > int64(c.queryBuf.Cap()) {
					c.queryBuf.Grow(int(bulkLen+2) - c.queryBuf.Cap())
				}
			}
		}

		if c.bulkLen+2 > int64(len(buf)) {
			return false
		} else {
			c.argv[c.argc] = createStringObject(buf[:c.bulkLen])
			pos += int((c.bulkLen) + 2)
			buf = buf[pos:]
			c.queryBuf.Next(pos)
			pos = 0
			c.argc++
			c.bulkLen = -1
			c.multiBulkLen--
		}
	}

	if c.multiBulkLen == 0 {
		return true
	}

	return false
}

func processInputBuffer(c *client) {

	for c.queryBuf.Len() > 0 {

		if c.reqType == 0 {
			if c.queryBuf.Bytes()[0] == '*' {
				c.reqType = reqTypeMultiBulk
			} else {
				c.reqType = reqTypeInline
			}
		}

		if c.reqType == reqTypeInline {
			if !processInlineBuffer(c) {
				break
			}
		} else {
			if !ProcessMultiBulkBuffer(c) {
				break
			}
		}

		Log("client get commands, %#v", c.argv)
	}

}
