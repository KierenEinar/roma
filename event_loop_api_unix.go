package main

import (
	"golang.org/x/sys/unix"
	"time"
)

type EventLoopSelector struct {
	el                 *EventLoop
	setSize            int
	wfds, rfds         unix.FdSet // writefds, readfds
	wfdsPoll, rfdsPoll unix.FdSet // writefds, readfds
}

func NewEventLoopSelector(el *EventLoop, setSize int) EventLoopApi {
	return &EventLoopSelector{
		el:      el,
		setSize: setSize,
	}
}

func (e *EventLoopSelector) AddFileEvent(fd int, mask uint8) error {
	if mask&ELMaskWritable != 0 {
		e.wfds.Set(fd)
	}
	if mask&ELMaskReadable != 0 {
		e.rfds.Set(fd)
	}

	return nil
}

func (e *EventLoopSelector) DelFileEvent(fd int, mask uint8) error {

	if mask&ELMaskWritable != 0 {
		e.wfds.Clear(fd)
	}
	if mask&ELMaskReadable != 0 {
		e.rfds.Clear(fd)
	}

	return nil
}

func (e *EventLoopSelector) Poll(t *time.Duration) (int, error) {
	var tv *unix.Timeval

	if t != nil {
		tv = &unix.Timeval{
			Sec:  t.Milliseconds() / 1000,
			Usec: int32(t.Milliseconds() % 1000),
		}
	}

	copy(e.wfdsPoll.Bits[:], e.wfds.Bits[:])
	copy(e.rfdsPoll.Bits[:], e.rfds.Bits[:])

	n, err := unix.Select(e.el.maxFd+1, &e.rfdsPoll, &e.wfdsPoll, nil, tv)
	if err != nil {
		return n, err
	}

	numEvents := 0

	if n > 0 {
		for j := 0; j < e.el.maxFd+1; j++ {
			event := e.el.Events[j]
			if event.Mask&(ELMaskReadable|ELMaskWritable) == ELMaskNone {
				continue
			}

			if !e.rfdsPoll.IsSet(j) && !e.wfdsPoll.IsSet(j) {
				continue
			}

			if event.Mask&ELMaskReadable != 0 && e.rfdsPoll.IsSet(j) {
				e.el.Fired[numEvents].Mask |= ELMaskReadable
			}
			if event.Mask&ELMaskWritable != 0 && e.wfdsPoll.IsSet(j) {
				e.el.Fired[numEvents].Mask |= ELMaskWritable
			}
			e.el.Fired[numEvents].Fd = j
			e.el.Fired[numEvents].clientData = event.clientData
			e.el.Fired[numEvents].rProc = event.rProc
			e.el.Fired[numEvents].wProc = event.wProc
			numEvents++
		}
	}

	return numEvents, nil

}
