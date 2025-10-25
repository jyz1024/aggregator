package aggregator

import (
	"log"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

type Aggregator[T any] struct {
	conf     Config
	callback func([]T)

	lock       sync.RWMutex
	ticker     *time.Ticker
	eventQueue []T // 事件队列
	copyInUse  atomic.Bool
	copyQueue  []T // 拷贝队列
	wg         sync.WaitGroup
}

type Config struct {
	DelayDuration  time.Duration // 延迟推送时间
	DelayBatchSize int           // 批次最大处理数量
	SyncSend       bool          // 同步提交
}

func NewAggregator[T any](callback func(eventList []T), conf Config) *Aggregator[T] {
	nm := &Aggregator[T]{
		conf:       conf,
		eventQueue: make([]T, 0, conf.DelayBatchSize*4),
		copyQueue:  make([]T, conf.DelayBatchSize),
		ticker:     time.NewTicker(conf.DelayDuration),
		callback:   callback,
	}
	go nm.delaySendChecker()
	return nm
}

func (n *Aggregator[T]) delaySendChecker() {
	for {
		select {
		case <-n.ticker.C:
			func() {
				n.lock.Lock()
				defer n.lock.Unlock()
				n.flush(false)
			}()
		}
	}
}

func (n *Aggregator[T]) Send(event T) {
	n.lock.Lock()
	defer n.lock.Unlock()
	n.eventQueue = append(n.eventQueue, event)
	if len(n.eventQueue) >= n.conf.DelayBatchSize {
		n.flush(true)
	}
}

func (n *Aggregator[T]) Flush() {
	n.ticker.Stop()
	n.lock.Lock()
	defer n.lock.Unlock()
	n.flush(false)
	n.wg.Wait()
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (n *Aggregator[T]) flush(resetTicker bool) {
	eventLen := len(n.eventQueue)
	if eventLen == 0 {
		return
	}
	if resetTicker {
		n.ticker.Reset(n.conf.DelayDuration)
	}
	useCopyQueue := n.copyInUse.CompareAndSwap(false, true)
	copyLen := minInt(eventLen, n.conf.DelayBatchSize)
	if useCopyQueue {
		copy(n.copyQueue[:copyLen], n.eventQueue[:copyLen])
		n.sendReq(n.copyQueue[:copyLen], true)
	} else {
		if n.conf.SyncSend {
			return
		}
		copyQueue := make([]T, copyLen)
		copy(copyQueue[:copyLen], n.eventQueue[:copyLen])
		n.sendReq(copyQueue[:copyLen], false)
	}
	if copyLen == eventLen {
		n.eventQueue = n.eventQueue[:0]
	} else {
		n.eventQueue = n.eventQueue[copyLen:]
	}
}

func (n *Aggregator[T]) sendReq(eventList []T, reuseQueue bool) {
	n.wg.Add(1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Println("Aggregator exec panic, stack:\n", string(debug.Stack()))
			}
		}()
		if reuseQueue {
			defer func() {
				n.copyInUse.Store(false)
			}()
		}
		defer n.wg.Done()
		n.callback(eventList)
	}()
}
