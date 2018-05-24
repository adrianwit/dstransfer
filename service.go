package dstransfer

import (
	"fmt"
	"github.com/viant/dsc"
	"math/rand"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type Service struct {
	mux   *sync.RWMutex
	tasks map[int]*TransferTask
}

func (s *Service) Tasks() *TasksResponse {
	var response = &TasksResponse{
		Tasks: make([]*TransferTask, 0),
	}
	s.mux.Lock()
	defer s.mux.Unlock()
	var taskCount = len(s.tasks)
	for k, task := range s.tasks {
		if taskCount > 10 && task.CanEvict() {
			delete(s.tasks, k)
		}
		response.Tasks = append(response.Tasks, task)
	}
	sort.Sort(response.Tasks)
	return response
}

func (s *Service) Task(id int, writer http.ResponseWriter) *TransferTask {
	s.mux.RLock()
	defer s.mux.RUnlock()
	response, ok := s.tasks[id]
	if !ok {
		writer.WriteHeader(http.StatusNotFound)
		return nil
	}
	return response
}


func (s *Service) Transfer(request *TransferRequest) *TransferResponse {
	var response = &TransferResponse{Status: "ok"}
	rand.Seed((time.Now().UTC().UnixNano()))
	response.TaskId = int(rand.Int31())
	var task *TransferTask
	var err error
	if err = request.Init(); err == nil {
		if err = request.Validate(); err == nil {
			task, err = NewTransferTask(request)
		}
	}
	if err != nil {
		response.SetError(err)
		return response
	}

	s.mux.Lock()
	task.ID = response.TaskId
	s.tasks[task.ID] = task
	s.mux.Unlock()
	task.Request = request
	go s.transferInBackground(request, response, task)
	return response
}

func (s *Service) transferInBackground(request *TransferRequest, response *TransferResponse, task *TransferTask) {
	var err error
	defer func() {
		var endTime = time.Now()
		task.EndTime = &endTime
		task.TimeTakenMs = int(task.EndTime.Sub(task.StartTime) / time.Millisecond)
		if response.Error == "" {
			task.Status = "done"
		}
	}()
	if err != nil {
		response.SetError(err)
	}
	for i := 0; i < request.WriterCount; i++ {
		go s.writeData(request, response, task, task.transfers.transfers[i])
	}
	err = s.readData(request, response, task)
	response.SetError(err)
	task.isWriteCompleted.Wait()
}

func (s *Service) getTargetTable(request *TransferRequest, task *TransferTask, batch []map[string]interface{}) (*dsc.TableDescriptor, error) {
	table := task.dest.TableDescriptorRegistry().Get(request.Dest.Table)
	if table == nil {
		return nil, fmt.Errorf("target table %v not found", request.Dest.Table)
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
	task.isWriteCompleted.Add(1)
	var count = 0
	defer func() {
		task.isWriteCompleted.Done()
		task.SetError(err)
		if err != nil {
			response.SetError(err)
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

	connection, err := task.dest.ConnectionProvider().Get()
	if err != nil {
		return err
	}
	defer connection.Close()

	if request.Mode == TransferModeInsert {
		persist = func(batch []map[string]interface{}) error {
			if len(batch) == 0 {
				return nil
			}
			if err != nil {
				return err
			}
			var batchItems = []interface{}{}
			for _, item := range batch {
				batchItems = append(batchItems, item)
			}
			_, err = task.dest.PersistData(connection, batchItems, request.Dest.Table, dmlProvider, sqlProvider)
			if err == nil {
				atomic.AddUint64(&task.WriteCount, uint64(len(batch)))
			}
			count += len(batch)
			return err
		}
	} else {
		persist = func(batch []map[string]interface{}) error {
			if len(batch) == 0 {
				return nil
			}
			_, _, err = task.dest.PersistAllOnConnection(connection, batch, request.Dest.Table, nil)
			if err == nil {
				atomic.AddUint64(&task.WriteCount, uint64(len(batch)))
				count += len(batch)
			}
			return err
		}
	}
	if err = persist(batch); err != nil {
		return err
	}
	for {
		if task.HasError() {
			break
		}
		if task.IsReading() { //blocking call
			transfer.waitForBatch()
		}
		batch := transfer.getBatch()
		if len(batch) == 0 && !task.IsReading() {
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
	atomic.StoreInt32(&task.isReadCompleted, 0)
	var err error
	defer func() {
		atomic.StoreInt32(&task.isReadCompleted, 1)
		if err != nil {
			task.SetError(err)
			response.SetError(err)
		}
		for _, transfer := range task.transfers.transfers {
			transfer.notify()
		}
	}()
	err = task.source.ReadAllWithHandler(request.Source.Query, nil, func(scanner dsc.Scanner) (bool, error) {
		if task.HasError() {
			return false, nil
		}
		var record = make(map[string]interface{})
		task.ReadCount++

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
	return &Service{
		mux:   &sync.RWMutex{},
		tasks: make(map[int]*TransferTask),
	}
}
