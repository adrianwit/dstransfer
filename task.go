package dstransfer

import (
	"github.com/viant/dsc"
	"sync"
	"sync/atomic"
)


//TransferTask represents a transfer tasks
type TransferTask struct {
	source    dsc.Manager
	dest      dsc.Manager
	transfers *transfers
	ReadDone  int32
	Error     string
	hasError  int32
	WriteDone *sync.WaitGroup
}


//IsReading returns true if transfer read data from the source
func (t *TransferTask) IsReading() bool {
	return atomic.LoadInt32(&t.ReadDone) == 0
}

//IsReading returns true if error occured
func (t *TransferTask) HasError() bool {
	return atomic.LoadInt32(&t.hasError) == 1
}



func (t *TransferTask) SetError(err error)  {
	if err == nil {
		return
	}
	atomic.StoreInt32(&t.hasError, 1)
	t.Error = err.Error()
	t.transfers.close()
}


func NewTransferTask(request *TransferRequest) (*TransferTask, error) {
	var task = &TransferTask{
		transfers: newTransfers(request.WriterCount, request.BatchSize),
		WriteDone: &sync.WaitGroup{},
	}
	var err error
	if task.source, err = dsc.NewManagerFactory().Create(request.Source.Config); err != nil {
		return nil, err
	}
	if task.dest, err = dsc.NewManagerFactory().Create(request.Dest.Config); err != nil {
		return nil, err
	}
	return task, nil
}
