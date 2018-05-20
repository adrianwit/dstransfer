package dstransfer

import (
	"fmt"
	"github.com/viant/dsc"
)

const (
	TransferModeInsert = "insert"
)

type Source struct {
	*dsc.Config
	Query string
}

func (s *Source) Validate() error {
	if s.Config == nil {
		return fmt.Errorf("source config was empty")
	}
	if s.Query == "" {
		return fmt.Errorf("source query was empty")
	}
	return nil
}

type Dest struct {
	*dsc.Config
	Table string
}

func (s *Dest) Validate() error {
	if s.Config == nil {
		return fmt.Errorf("dest config was empty")
	}
	if s.Table == "" {
		return fmt.Errorf("dest table was empty")
	}
	return nil
}

type TransferRequest struct {
	Source      *Source
	Dest        *Dest
	WriterCount int
	BatchSize   int
	Mode        string //
}

func (r *TransferRequest) Init() error {
	if r.BatchSize == 0 {
		r.BatchSize = 1
	}
	if r.WriterCount == 0 {
		r.WriterCount = 1
	}
	return nil
}

func (r *TransferRequest) Validate() error {
	if r.Source == nil {
		return fmt.Errorf("source was empty")
	}
	if err := r.Source.Validate(); err != nil {
		return err
	}
	if r.Dest == nil {
		return fmt.Errorf("source was empty")
	}
	return r.Dest.Validate()
}

type TransferResponse struct {
	TaskId int
	Status string
	Error  string
}

func (r *TransferResponse) SetError(err error) {
	if err == nil {
		return
	}
	r.Status = "error"
	r.Error = err.Error()
}

type TasksResponse struct {
	Tasks Tasks
}
