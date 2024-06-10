package main

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

const (
	opened = 1 + iota
	closed
)

const (
	defaultCleanupInterval = time.Second
)

var (
	errNilFunc    = errors.New("nil function")
	errOverloaded = errors.New("nonblocking mode and too many tasks submitted")
	errPoolClosed = errors.New("pool closed")
)

type workPool struct {
	capacity    int32
	running     int32
	waiting     int32
	state       int32
	stop        func()
	workers     workQueue
	lock        sync.RWMutex
	cond        sync.Cond
	workerCache sync.Pool
	purgeExpiry time.Duration
	now         atomic.Value
}

func newPool(capacity int32) *workPool {

	pool := &workPool{
		capacity: capacity,
		workers: &stackWorkQueue{
			items: make([]*goWorker, 0, capacity),
		},
		state:       opened,
		purgeExpiry: defaultCleanupInterval,
	}
	pool.cond = *sync.NewCond(&pool.lock)
	ctx, cancel := context.WithCancel(context.Background())
	pool.workerCache = sync.Pool{
		New: func() any {
			w := &goWorker{pool: pool}
			w.submitTask = make(chan func())
			w.run()
			return w
		},
	}
	pool.stop = cancel
	go pool.goPurgeStaleWorkers(ctx)
	go pool.goTickTok(ctx)
	return pool
}

func (pool *workPool) nowTime() time.Time {
	return pool.now.Load().(time.Time)
}

func (pool *workPool) submitTask(task func()) error {
	w, err := pool.retrieveWorker()
	if err != nil {
		return err
	}
	return w.submit(task)
}

func (pool *workPool) goPurgeStaleWorkers(ctx context.Context) {

	ticker := time.NewTicker(pool.purgeExpiry)
	defer func() {
		ticker.Stop()
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		pool.lock.Lock()
		purgeWs := pool.workers.refresh(pool.nowTime())
		n := pool.nRunning()
		isDormant := n == 0 || n == int32(len(purgeWs))
		pool.lock.Unlock()

		if len(purgeWs) == 0 {
			continue
		}

		for ix, w := range purgeWs {
			w.finish()
			purgeWs[ix] = nil
		}

		if isDormant && pool.nWaiting() > 0 {
			pool.cond.Broadcast()
		}
	}

}

func (pool *workPool) goTickTok(ctx context.Context) {

	ticker := time.NewTicker(time.Millisecond * 500)
	defer func() {
		ticker.Stop()
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pool.now.Store(time.Now())
		}
	}
}

func (pool *workPool) addRunning(x int32) {
	atomic.AddInt32(&pool.running, x)
}

func (pool *workPool) nRunning() int32 {
	return atomic.LoadInt32(&pool.running)
}

func (pool *workPool) addWaiting(x int32) {
	atomic.AddInt32(&pool.waiting, x)
}

func (pool *workPool) nWaiting() int32 {
	return atomic.LoadInt32(&pool.waiting)
}

func (pool *workPool) retrieveWorker() (*goWorker, error) {

	if pool.isClosed() {
		return nil, errPoolClosed
	}

	pool.lock.Lock()

	for {

		w := pool.workers.detach()
		if w != nil {
			pool.lock.Unlock()
			return w, nil
		}

		if pool.capacity < pool.nRunning() {
			pool.lock.Unlock()
			w = pool.workerCache.Get().(*goWorker)
			return w, nil
		}

		pool.addRunning(1)
		pool.cond.Wait()
		pool.addRunning(-1)

		if pool.isClosed() {
			pool.lock.Unlock()
			return nil, errPoolClosed
		}

		continue
	}

}

func (pool *workPool) revertWorker(worker *goWorker) bool {

	if pool.isClosed() {
		return false
	}

	worker.lastUsed = pool.nowTime()

	pool.lock.Lock()
	if pool.capacity <= pool.nRunning() {
		pool.lock.Unlock()
		return false
	}

	_ = pool.workers.insert(worker)
	pool.cond.Signal()
	pool.lock.Unlock()
	return true
}

func (pool *workPool) release() error {

	if pool.isClosed() {
		return errPoolClosed
	}

	if !atomic.CompareAndSwapInt32(&pool.state, opened, closed) {
		return errPoolClosed
	}

	pool.lock.Lock()

	pool.stop()
	pool.stop = nil
	pool.workers.reset()
	pool.lock.Unlock()
	pool.cond.Broadcast() // wake up all waiting goroutines if there are any
	return nil
}

func (pool *workPool) isClosed() bool {
	return atomic.LoadInt32(&pool.state) == closed
}

type goWorker struct {
	pool       *workPool
	submitTask chan func()
	lastUsed   time.Time
}

func (worker *goWorker) run() {

	worker.pool.addRunning(1)

	go func() {
		defer func() {
			worker.pool.addRunning(-1)
			if !worker.pool.isClosed() {
				worker.pool.workerCache.Put(worker)
			}
		}()

		for {
			task := <-worker.submitTask
			if task == nil {
				return
			}
			task()
			if ok := worker.pool.revertWorker(worker); !ok {
				return
			}
		}
	}()

}

func (worker *goWorker) finish() {
	worker.submitTask <- nil
}

func (worker *goWorker) submit(task func()) error {
	if task == nil {
		return errNilFunc
	}
	worker.submitTask <- task
	return nil
}

type workQueue interface {
	len() int
	cap() int
	refresh(expire time.Time) []*goWorker
	reset()
	insert(worker *goWorker) error
	detach() *goWorker
}

type stackWorkQueue struct {
	items  []*goWorker
	expiry []*goWorker
}

func (q *stackWorkQueue) len() int {
	return len(q.items)
}

func (q *stackWorkQueue) cap() int {
	return cap(q.items)
}

func (q *stackWorkQueue) refresh(expire time.Time) []*goWorker {

	r := q.binarySearch(expire)
	if r == -1 {
		return nil
	}

	q.expiry = q.expiry[:0]
	q.expiry = append(q.expiry, q.items[:r+1]...)
	m := copy(q.items, q.items[r+1:])
	q.items = q.items[:m]
	return q.expiry
}

func (q *stackWorkQueue) reset() {
	for ix := 0; ix < len(q.items); ix++ {
		q.items[ix].finish()
		q.items[ix] = nil
	}
}

func (q *stackWorkQueue) insert(worker *goWorker) error {
	q.items = append(q.items, worker)
	return nil
}

func (q *stackWorkQueue) detach() *goWorker {

	if q.len() == 0 {
		return nil
	}
	w := q.items[q.len()-1]
	q.items[q.len()-1] = nil
	q.items = q.items[:q.len()-1]
	return w
}

func (q *stackWorkQueue) binarySearch(expiry time.Time) int {
	l, r := 0, q.len()-1
	for l <= r {
		mid := l + (r-l)<<1
		if q.items[mid].lastUsed.After(expiry) {
			r = mid - 1
		} else {
			l = mid + 1
		}
	}
	return r
}
