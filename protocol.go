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
		rServer.clientsPendingWrite.PushBack(c)
		c.clientPendingWriteElement = rServer.clientsPendingWrite.Back()
	}

	return true
}

func (c *client) hasPendingOutputs() bool {
	return c.replyPos > 0 || c.replyList.Len() > 0
}

func addReplyError(c *client, err string) {

}

func setProtocolError(c *client, err string) {
	// todo log
	c.flag |= clientCloseAfterReply
	c.queryBuf = c.queryBuf[:0]
}

func processMultiBulkBuffer(c *client) bool {

	pos := 0
	if c.multiBulkLen == 0 {
		idx := bytes.IndexByte(c.queryBuf, '\r')
		if idx == -1 {
			if len(c.queryBuf) >= maxInlineLength {
				addReplyError(c, "Protocol error: too big mbulk count string")
				setProtocolError(c, "too big mbulk count string")
			}
			return false
		}

		if c.queryBuf[0] != '*' {
			addReplyError(c, "Protocol error: mbulk string must start with *")
			setProtocolError(c, "mbulk string must start with *")
			return false
		}

		// should contains \n
		if idx+2 > len(c.queryBuf) {
			return false
		}

		mbulk, err := strconv.ParseInt(string(c.queryBuf[1:idx]), 10, 64)
		if err != nil {
			addReplyError(c, "Protocol error: mbulk count invalid")
			setProtocolError(c, "mbulk count invalid")
			return false
		}

		pos += idx + 2
		if mbulk <= 0 {
			c.queryBuf = c.queryBuf[pos:]
			return false
		}
		c.multiBulkLen = int32(mbulk)

		c.argv = make([]rObj, mbulk)
		c.argc = 0
	}

	for c.multiBulkLen > 0 {

		if c.bulkLen == -1 {
			idx := bytes.IndexByte(c.queryBuf[pos:], '\r')
			if idx == -1 {
				if len(c.queryBuf) >= maxInlineLength {
					addReplyError(c, "Protocol error: too big bulk count string")
					setProtocolError(c, "too big mbulk count string")
				}
				return false
			}

			// should contain \n
			if pos+idx+2 > len(c.queryBuf) {
				break
			}

			if c.queryBuf[pos] != '$' {
				addReplyError(c, "Protocol error: bulk string must start with $")
				setProtocolError(c, "bulk string must start with $")
				return false
			}

			bulk, err := strconv.ParseInt(string(c.queryBuf[pos+1:pos+idx]), 10, 64)
			if err != nil {
				addReplyError(c, "Protocol error: bulk len invalid")
				setProtocolError(c, "bulk len invalid")
				return false
			}

			if bulk < 0 || bulk > maxBulkLen {
				addReplyError(c, "Protocol error: bulk len exceed max len 512MB")
				setProtocolError(c, "bulk len exceed max len 512MB")
				return false
			}

			c.bulkLen = bulk
			pos += idx + 2
			if bulk >= bulkBigArgs {
				c.queryBuf = c.queryBuf[pos:]
				pos = 0
				if cap(c.queryBuf)-len(c.queryBuf) < bulkBigArgs+2 {
					newBuffer := make([]byte, 0, bulkBigArgs+2)
					newBuffer = append(newBuffer, c.queryBuf...)
					c.queryBuf = newBuffer
				}
			}

		}

		if c.bulkLen+2 > int64(len(c.queryBuf[pos:])) {
			break
		}

		c.argv[c.argc] = rObj{
			objectType: objectTypeString,
			encoding:   objectEncodingRaw,
			data:       c.queryBuf[:c.bulkLen],
		}

		if c.bulkLen >= bulkBigArgs && pos == 0 &&
			int64(len(c.queryBuf)) == c.bulkLen+2 {
			c.queryBuf = make([]byte, 0, genericIOBufferLength)
		} else {
			pos += int(c.bulkLen) + 2
		}
		c.bulkLen--
		c.multiBulkLen--
	}

	if pos > 0 {
		c.queryBuf = c.queryBuf[pos:]
	}

	if c.multiBulkLen == 0 {
		return true
	}

	return false
}

func processInputBuffer(c *client) {

	for len(c.queryBuf) > 0 {

		if c.reqType == 0 {
			if c.queryBuf[0] == '*' {
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
			if !processMultiBulkBuffer(c) {
				break
			}
		}

		Log("client get commands, %#v", c.argv)

		processCommand(c)

	}

}
