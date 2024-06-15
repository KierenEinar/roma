package main

import (
	"net"
)

func main() {

	el := NewEventLoop(1024, beforeSleep, afterSleep)
	initServer(el)

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
	Log("accept new connection: fd=%d", fd)
	listener := clientData.(*net.TCPListener)
	conn, err := listener.Accept()
	if err != nil {
		Log("acceptConnection failed, err=%v", err)
		return
	}

	err = createClient(el, conn.(*net.TCPConn))
	if err != nil {
		Log("acceptConnection createClient error=%v", err)
		return
	}
}

func beforeSleep() {

	handleClientsWithPendingRead()

	handleClientsWithPendingWrite()

}

func afterSleep() {

}
