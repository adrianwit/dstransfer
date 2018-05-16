package dstransfer

import (
	"sync/atomic"
	"time"
)

type transfer struct {
	batchSize      uint64
	records        chan map[string]interface{}
	batchCompleted chan bool
	count          uint64
}

func (t *transfer) push(record map[string]interface{}) {
	result := atomic.AddUint64(&t.count, 1)
	if result%t.batchSize == 0 {

		t.batchCompleted <- true;
	}
	t.records <- record
}


func (t *transfer) notify() {
	select {
	case t.batchCompleted <- true:
	case <-time.After(time.Millisecond):
	}
}

func (t *transfer) waitForBatch() bool {
	return <-t.batchCompleted
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
		batchSize:      uint64(batchSize),
		records:        make(chan map[string]interface{}, 2*batchSize),
		batchCompleted: make(chan bool, 1),
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
		result.transfers[i] = newTransfer(writerCount)
	}
	return result
}
