package main

import (
	"errors"
	"os"
	"reflect"
	"time"
)

const (
	ELMaskNone     = 0
	ELMaskReadable = 1 << 0
	ELMaskWritable = 1 << 1
)

const (
	ELFlagsTimeEvents = 1 << 0
	ELFlagsFileEvents = 1 << 1
	ELFlagsAllEvents  = 1 << 2
	ELFlagsNoWait     = 1 << 3
	ELFlagsBarrier    = 1 << 4
)

type ProcFileEvent func(el *EventLoop, fd int, mask uint8, clientData interface{})
type ProcTimerEvent func(el *EventLoop, timerId int64, clientData interface{}) time.Duration

type ProcBeforeSleep func()
type ProcAfterSleep func()

type FireEvent struct {
	Fd         int
	File       *os.File
	Mask       uint8
	rProc      ProcFileEvent
	wProc      ProcFileEvent
	clientData interface{}
}

type EventLoop struct {
	ElApi   EventLoopApi
	SetSize int
	Events  []FireEvent
	Fired   []FireEvent

	TimerHead   *TimerEvent
	TimerTail   *TimerEvent
	NextTimerId int64

	BeforeSleep ProcBeforeSleep
	AfterSleep  ProcAfterSleep

	Stop  chan chan struct{}
	maxFd int
}

type TimerEvent struct {
	Id               int64
	Next             *TimerEvent
	Prev             *TimerEvent
	ProcTimer        ProcTimerEvent
	clientData       interface{}
	when             int64
	whenMilliseconds int64
}

type EventLoopApi interface {
	AddFileEvent(fd int, mask uint8) error
	DelFileEvent(fd int, mask uint8) error
	Poll(duration *time.Duration) (int, error)
}

func NewEventLoop(setSize int, beforeSleep ProcBeforeSleep, afterSleep ProcAfterSleep) *EventLoop {
	el := &EventLoop{
		SetSize:     setSize,
		Events:      make([]FireEvent, setSize),
		Fired:       make([]FireEvent, setSize),
		NextTimerId: 0,
		BeforeSleep: beforeSleep,
		AfterSleep:  afterSleep,
		Stop:        make(chan chan struct{}),
		maxFd:       -1,
	}
	el.ElApi = NewEventLoopSelector(el, setSize)
	return el
}

func (el *EventLoop) Serve() {

	Log("EventLoop start...")

	for {
		select {
		case c := <-el.Stop:
			Log("EventLoop stop...")
			c <- struct{}{}
			return
		default:
			if el.BeforeSleep != nil {
				el.BeforeSleep()
			}
			_, err := el.poll(ELFlagsAllEvents | ELFlagsNoWait | ELFlagsBarrier)
			if err != nil {
				Log("poll error", err)
			}
			if el.AfterSleep != nil {
				el.AfterSleep()
			}
		}
	}

}

func (el *EventLoop) AddFileEvent(f *os.File, mask uint8, procFileEvent ProcFileEvent, clientData interface{}) error {

	fd := int(f.Fd())

	if fd >= el.SetSize {
		return errors.New("AddFileEvent fd out of range")
	}

	if mask&(ELMaskReadable|ELMaskWritable) == ELMaskNone {
		return errors.New("AddFileEvent mask only support read and write")
	}

	el.Events[fd].Mask = mask
	if mask&ELMaskReadable != 0 {
		el.Events[fd].rProc = procFileEvent
	}
	if mask&ELMaskWritable != 0 {
		el.Events[fd].wProc = procFileEvent
	}

	el.Events[fd].clientData = clientData
	el.Events[fd].File = f
	if el.maxFd < fd {
		el.maxFd = fd
	}

	err := el.ElApi.AddFileEvent(fd, mask)
	return err

}

func (el *EventLoop) DelFileEvent(fd int, mask uint8) error {

	if fd >= el.SetSize {
		return errors.New("DelFileEvent fd out of range")
	}
	el.Events[fd].Mask = ELMaskNone
	el.Events[fd].File = nil
	err := el.ElApi.DelFileEvent(fd, mask)

	if fd == el.maxFd {
		for j := el.maxFd; j >= 0; j-- {
			if el.Events[j].Mask&(ELMaskReadable|ELMaskWritable) > 0 {
				el.maxFd = j
				break
			}
		}
		// current fd is the last one
		if el.maxFd == fd {
			el.maxFd = -1
		}
	}

	return err

}

func (el *EventLoop) AddTimer(t time.Duration, procTimerEvent ProcTimerEvent, clientData interface{}) {

	procWhen := time.Now().Add(t).UnixMilli()
	timerEvent := &TimerEvent{
		Id:               el.NextTimerId,
		when:             procWhen / 1000,
		whenMilliseconds: procWhen % 1000,
		ProcTimer:        procTimerEvent,
		clientData:       clientData,
	}

	if el.TimerHead == nil {
		el.TimerHead = timerEvent
		el.TimerTail = timerEvent
	} else {
		el.TimerTail.Next = timerEvent
		timerEvent.Prev = el.TimerTail
		el.TimerTail = timerEvent
	}

}

func (el *EventLoop) searchNearestTimer() *TimerEvent {
	p := el.TimerHead
	nearest := p
	for p != nil {
		if p.when < nearest.when || (p.when == nearest.when && p.whenMilliseconds < nearest.whenMilliseconds) {
			nearest = p
		}
		p = p.Next
	}
	return nearest

}

func (el *EventLoop) poll(flags int) (int, error) {

	var nearest *TimerEvent
	numEvents := 0
	if el.maxFd != -1 || (flags&ELFlagsTimeEvents != 0 && flags&ELFlagsNoWait == 0) {

		if flags&ELFlagsTimeEvents != 0 && flags&ELFlagsNoWait == 0 {
			nearest = el.searchNearestTimer()
		}

		var waitDuration *time.Duration

		if nearest != nil {
			now := time.Now().UnixMilli()
			wait := time.Second*time.Duration(nearest.when-now/1000) +
				time.Millisecond*time.Duration(nearest.whenMilliseconds-now%1000)
			if wait < 0 {
				wait = time.Duration(0)
			}
			waitDuration = &wait
		} else {
			if flags&ELFlagsNoWait != 0 {
				wait := time.Duration(0)
				waitDuration = &wait
			}
		}

		n, err := el.ElApi.Poll(waitDuration)

		if err != nil {
			Log("poll error", err)
			return 0, err
		}

		for i := 0; i < n; i++ {
			fired := el.Fired[i]
			proc := 0
			if fired.Mask&ELMaskReadable != 0 && flags&ELFlagsBarrier == 0 {
				fired.rProc(el, fired.Fd, ELMaskReadable, fired.clientData)
				proc++
			}

			if fired.Mask&ELMaskWritable != 0 {
				if proc == 0 || reflect.ValueOf(fired.rProc).Pointer() != reflect.ValueOf(fired.wProc).Pointer() {
					fired.wProc(el, fired.Fd, ELMaskWritable, fired.clientData)
				}
				proc++
			}

			if fired.Mask&ELMaskReadable != 0 && flags&ELFlagsBarrier != 0 {
				fired.rProc(el, fired.Fd, ELMaskReadable, fired.clientData)
				proc++
			}
			numEvents += proc
		}

	}

	numEvents += el.processTimerEvents()
	return numEvents, nil

}

func (el *EventLoop) processTimerEvents() int {
	numEvents := 0
	timerEvent := el.TimerHead
	maxId := el.NextTimerId

	now := time.Now().UnixMilli()
	var (
		prev *TimerEvent
	)

	for {
		if timerEvent == nil {
			break
		}

		if maxId >= timerEvent.Id {
			continue
		}

		if timerEvent.when >= now/1000 && timerEvent.whenMilliseconds >= now%1000 {
			nextTrigger := timerEvent.ProcTimer(el, timerEvent.Id, timerEvent.clientData)
			if nextTrigger <= 0 {
				if prev != nil {
					prev.Next = timerEvent.Next
				}
				if timerEvent == el.TimerTail {
					el.TimerTail = prev
				}
			} else {
				getNow := time.Now().UnixMilli()
				timerEvent.when = getNow/1000 + int64(nextTrigger/1000)
				timerEvent.whenMilliseconds = int64(nextTrigger % 1000)
			}

			numEvents++

		}
		prev = timerEvent

	}
	return numEvents
}

func (el *EventLoop) StopAndWait() {
	c := make(chan struct{})
	el.Stop <- c
	<-c
	Log("EventLoop wait stop done...")
}
