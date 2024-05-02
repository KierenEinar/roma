package main

import (
	"bytes"
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
	ReqTypeInline = iota + 1
	ReqTypeMultiBulk
)

const (
	replyBufferLen = 1024 * 16
)

func ProcessInlineBuffer(c *Client) bool {
	return false
}

func AddReply(c *Client, reply []byte) {
	c.Reply.Write(reply)
	if c.ReplyElement != nil {
		return
	}
	server.Replies.PushBack(c)
	c.ReplyElement = server.Replies.Back()
}

func AddReplyError(c *Client, err string) {

}

func SetProtocolError(c *Client, err string) {
}

func ProcessMultiBulkBuffer(c *Client) bool {

	pos := 0
	buf := c.QueryBuf.Bytes()

	if c.MultiBulkLen == 0 {

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

		c.MultiBulkLen = int32(multiBulkLen)
		pos += indexAt + 2
		buf = buf[pos:]
		c.QueryBuf.Next(pos)
		pos = 0

		if multiBulkLen <= 0 {
			return true
		}

		c.Argv = make([]RObj, multiBulkLen)

	}

	for c.MultiBulkLen > 0 {

		if c.BulkLen == -1 {
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

			c.BulkLen = bulkLen
			pos += indexAt + 2
			buf = buf[pos:]
			c.QueryBuf.Next(pos)
			pos = 0

			if bulkLen >= bulkBigArgs {
				if bulkLen+2 > int64(c.QueryBuf.Cap()) {
					c.QueryBuf.Grow(int(bulkLen+2) - c.QueryBuf.Cap())
				}
			}
		}

		if c.BulkLen+2 > int64(len(buf)) {
			return false
		} else {
			c.Argv[c.Argc] = CreateStringObject(buf[:c.BulkLen])
			pos += int((c.BulkLen) + 2)
			buf = buf[pos:]
			c.QueryBuf.Next(pos)
			pos = 0
			c.Argc++
			c.BulkLen = -1
			c.MultiBulkLen--
		}
	}

	if c.MultiBulkLen == 0 {
		return true
	}

	return false
}

func ProcessInputBuffer(c *Client) {

	for c.QueryBuf.Len() > 0 {

		if c.ReqType == 0 {
			if c.QueryBuf.Bytes()[0] == '*' {
				c.ReqType = ReqTypeMultiBulk
			} else {
				c.ReqType = ReqTypeInline
			}
		}

		if c.ReqType == ReqTypeInline {
			if !ProcessInlineBuffer(c) {
				break
			}
		} else {
			if !ProcessMultiBulkBuffer(c) {
				break
			}
		}

		Log("client get commands, %#v", c.Argv)
	}

}
