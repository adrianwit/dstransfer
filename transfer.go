package dstransfer

import (
	"sync/atomic"
	"time"
)

type transfer struct {
	closed            int32
	batchSize         uint64
	records           chan map[string]interface{}
	batchCompleted    chan bool
	transferCompleted chan bool

	count uint64
}

func (t *transfer) push(record map[string]interface{}) {
	if t.isClose() {
		return
	}
	result := atomic.AddUint64(&t.count, 1)
	if result%t.batchSize == 0 {
		t.notify()
	}

	select {
	case t.records <- record:
	case <-t.transferCompleted:
	}
}

func (t *transfer) notify() {
	select {
	case t.batchCompleted <- true:
	case <-time.After(time.Millisecond):
	}
}

func (t *transfer) isClose() bool {
	return atomic.LoadInt32(&t.closed) == 1
}

func (t *transfer) close() {
	select {
	case t.transferCompleted <- true:
	case <-time.After(time.Millisecond):
	}
	atomic.StoreInt32(&t.closed, 1)
}

func (t *transfer) waitForBatch() {
	select {
	case <-t.batchCompleted:
	case <-t.transferCompleted:
	}
}

func (t *transfer) getBatch() []map[string]interface{} {
	var result = []map[string]interface{}{}
outer:
	for i := 0; i < int(t.batchSize); i++ {
		select {
		case item := <-t.records:
			if item == nil {
				break outer
			}
			result = append(result, item)
		case <-time.After(300 * time.Millisecond):
			break outer
		}
	}
	return result
}

func newTransfer(batchSize int) *transfer {
	if batchSize == 0 {
		batchSize = 1
	}
	return &transfer{
		batchSize:         uint64(batchSize),
		records:           make(chan map[string]interface{}, batchSize + 1 + int(0.5 *batchSize)),
		batchCompleted:    make(chan bool, 1),
		transferCompleted: make(chan bool, 1),
	}
}

type transfers struct {
	transfers []*transfer
	index     uint64
}

func (t *transfers) push(record map[string]interface{}) {
	index := int(atomic.AddUint64(&t.index, 1)) % len(t.transfers)
	t.transfers[index].push(record)
}

func (t *transfers) close() {
	for _, transfer := range t.transfers {
		transfer.notify()
	}
}

func newTransfers(writerCount, batchSize int) *transfers {
	var result = &transfers{
		transfers: make([]*transfer, writerCount),
	}
	for i := 0; i < writerCount; i++ {
		result.transfers[i] = newTransfer(batchSize)
	}
	return result
}
