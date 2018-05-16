package dstransfer

import (
	"github.com/viant/dsc"
	"fmt"
	"sync/atomic"
	"github.com/viant/toolbox"
)

type Service struct {
	transfer *TransferResponse
}

func (s Service) TransferStatus() *TransferResponse {
	return s.transfer
}

func (s Service) Transfer(request *TransferRequest) *TransferResponse {
	toolbox.DumpIndent(request, true)
	var response = &TransferResponse{
		Status: "running",
	}
	s.transfer = response
	var err error
	defer func() {
		fmt.Errorf("err: %v\n", err)
	}()

	var task *TransferTask
	if err = request.Init(); err == nil {
		if err = request.Validate(); err == nil {
			task, err = NewTransferTask(request)
		}
	}

	if err != nil {
		response.SetError(err)
		return response
	}
	for i := 0; i < request.WriterCount; i++ {
		go s.writeData(request, response, task, task.transfers.transfers[i])

	}
	err = s.readData(request, response, task)
	response.SetError(err)
	task.WriteDone.Wait()
	if response.Error == "" {
		response.Status = "done"
	}

	return response
}

func (s Service) writeData(request *TransferRequest, response *TransferResponse, task *TransferTask, transfer *transfer) (err error) {
	task.WriteDone.Add(1)
	defer func() {
		task.WriteDone.Done()
		task.SetError(err)
	}()
	var persist func(batch []map[string]interface{}) error;
	table := task.dest.TableDescriptorRegistry().Get(request.Dest.Table)
	dmlProvider := dsc.NewMapDmlProvider(table)
	sqlProvider := func(item interface{}) *dsc.ParametrizedSQL {
		return dmlProvider.Get(dsc.SQLTypeInsert, item)
	}

	if request.Mode == TransferModeInsert {
		persist = func(batch []map[string]interface{}) error {
			if len(batch) == 0 {
				return nil
			}
			connection, err := task.dest.ConnectionProvider().Get()
			if err != nil {
				return err
			}
			defer connection.Close()
			var batchItems = []interface{}{}
			for _, item := range batch {
				batchItems = append(batchItems, item)
			}
			_, err = task.dest.PersistData(connection, batchItems, request.Dest.Table, dmlProvider, sqlProvider);
			return err
		}
	} else {
		persist = func(batch []map[string]interface{}) error {
			if len(batch) == 0 {
				return nil
			}
			connection, err := task.dest.ConnectionProvider().Get()
			if err != nil {
				return err
			}
			defer connection.Close()
			_, _, err = task.dest.PersistAllOnConnection(connection, batch, request.Dest.Table, nil);
			return err
		}
	}
	for ; ; {
		if task.HasError() {
			break
		}
		if task.IsReading() { //blocking call
			transfer.waitForBatch()
		}
		batch := transfer.getBatch()
		if len(batch) == 0 && ! task.IsReading() {
			break
		}
		if err = persist(batch); err != nil {
			return err
		}
	}
	err = persist(transfer.getBatch());
	return err
}


func (s Service) readData(request *TransferRequest, response *TransferResponse, task *TransferTask) error {
	atomic.StoreInt32(&task.ReadDone, 0)
	defer func() {
		atomic.StoreInt32(&task.ReadDone, 1)
		for _, transfer := range task.transfers.transfers {
			transfer.notify()
		}
	}()
	return task.source.ReadAllWithHandler(request.Source.Query, nil, func(scanner dsc.Scanner) (bool, error) {
		var record = make(map[string]interface{})
		response.ReadCount++
		err := scanner.Scan(&record)
		if err != nil {
			return false, fmt.Errorf("failed to scan:%v", err)
		}
		task.transfers.push(record)
		return true, nil
	})
}

func New() *Service {
	return &Service{}
}
