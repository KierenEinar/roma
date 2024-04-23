package main

import (
	"net"
)

func main() {

	el := NewEventLoop(1024, nil, nil)

	laddr, err := net.ResolveTCPAddr("tcp", ":6379")
	if err != nil {
		Log("ResolveTCPAddr error=%v", err)
		panic(err)
	}

	listener, err := net.ListenTCP(laddr.Network(), laddr)
	if err != nil {
		Log("ListenTCP error=%v", err)
		panic(err)
	}

	lf, err := listener.File()
	if err != nil {
		Log("listener get file fd error=%v", err)
		panic(err)
	}
	err = el.AddFileEvent(lf, ELMaskReadable, acceptConnection, listener)
	if err != nil {
		Log("AddFileEvent error=%v, fd=%d", err, lf.Fd())
		panic(err)
	}

	el.Main()
}

func acceptConnection(el *EventLoop, fd int, mask uint8, clientData interface{}) {

	Log("accept new connection")

	listener := clientData.(*net.TCPListener)
	conn, err := listener.Accept()
	if err != nil {
		Log("acceptConnection failed, err=%v", err)
		return
	}

	tcpConn := conn.(*net.TCPConn)

	tcpFd, err := tcpConn.File()

	if err != nil {
		Log("acceptConnection AddFileEvent error=%v", err)
		return
	}

	err = el.AddFileEvent(tcpFd, ELMaskReadable, readData, conn)

	if err != nil {
		Log("readData AddFileEvent error=%v", err)
	}
}

func readData(el *EventLoop, fd int, mask uint8, clientData interface{}) {
	conn := clientData.(*net.TCPConn)
	buf := make([]byte, 1024)
	n, _ := conn.Read(buf)
	//buf, err := io.ReadAll(conn)
	//if err != nil {
	//	Log("readData ReadAll failed, err=%v", err)
	//	return
	//}
	Log("readData %s", string(buf[:n]))

	_, _ = conn.Write([]byte("success\r\n"))
}
