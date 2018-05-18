package dstransfer

import (
	"github.com/viant/dsc"
	"fmt"
	"sync/atomic"
)

type Service struct {
	transfer *TransferResponse
}

func (s *Service) TransferStatus() *TransferResponse {
	return s.transfer
}

func (s *Service) Transfer(request *TransferRequest) *TransferResponse {
	var response = &TransferResponse{
		Status: "running",
	}
	s.transfer = response
	var err error
	defer func() {
		if response.Error == "" {
			response.Status = "done"
		}
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
	return response
}

func (s *Service) getTargetTable(request *TransferRequest, task *TransferTask, batch []map[string]interface{}) (*dsc.TableDescriptor, error) {
	table := task.dest.TableDescriptorRegistry().Get(request.Dest.Table)
	if table == nil {
		return  nil, fmt.Errorf("target table %v not found", request.Dest.Table)
	}

	if len(table.PkColumns) == 0 {
		request.Mode = TransferModeInsert
	}

	if len(table.Columns) == 0 && len(batch) > 0 {
		table.Columns = []string{}
		for k := range batch[0] {
			table.Columns = append(table.Columns, k)
		}
	}
	return table, nil
}

func (s *Service) writeData(request *TransferRequest, response *TransferResponse, task *TransferTask, transfer *transfer) (err error) {
	task.WriteDone.Add(1)
	defer func() {
		task.WriteDone.Done()
		task.SetError(err)
		if err != nil {
			transfer.close()
		}
	}()
	var persist func(batch []map[string]interface{}) error

	if task.IsReading() { //blocking call
		transfer.waitForBatch()
	}
	batch := transfer.getBatch()
	var table *dsc.TableDescriptor
	table, err = s.getTargetTable(request, task, batch)
	if err != nil {
		return err
	}
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
			if err == nil {
				atomic.AddUint64(&response.WriteCount, uint64(len(batch)))
			}
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
			if err == nil {
				atomic.AddUint64(&response.WriteCount, uint64(len(batch)))
			}
			return err
		}
	}

	if err = persist(batch); err != nil {
		return err
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
	err = persist(transfer.getBatch())
	return err
}


func (s *Service) readData(request *TransferRequest, response *TransferResponse, task *TransferTask) error {
	atomic.StoreInt32(&task.ReadDone, 0)
	defer func() {
		atomic.StoreInt32(&task.ReadDone, 1)
		for _, transfer := range task.transfers.transfers {
			transfer.notify()
		}
	}()
	err := task.source.ReadAllWithHandler(request.Source.Query, nil, func(scanner dsc.Scanner) (bool, error) {

		var record = make(map[string]interface{})
		response.ReadCount++
		err := scanner.Scan(&record)
		if err != nil {
			return false, fmt.Errorf("failed to scan:%v", err)
		}
		task.transfers.push(record)
		return true, nil
	})
	return err
}

func New() *Service {
	return &Service{}
}
