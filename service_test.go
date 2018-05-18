package dstransfer

import (
	"testing"
	"github.com/viant/dsc"
	"github.com/stretchr/testify/assert"
	"github.com/viant/t"
)





func TestService_Transfer(t *testing.T) {
	toolbox.RemoveFileIfExist("test/transfer/test_users.json")
	config, err := dsc.NewConfigFromURL("test/config.yaml")
	if ! assert.Nil(t, err) {
		return
	}

	request := &TransferRequest{
		Source:&Source{
			Config: config,
			Query:"SELECT id, name, email, address.state AS state FROM users",
		},
		Dest:&Dest{
			Config: config,
			Table:"test_users",
		},
		Mode:TransferModeInsert,
	}

	service := New()
	assert.NotNil(t, service)
	response := service.Transfer(request)
	assert.NotNil(t, response)
	assert.Equal(t, "done", response.Status)
	assert.Equal(t, "", response.Error)
	assert.Equal(t, uint64(3), response.WriteCount)
	assert.Equal(t, 3, response.ReadCount)
	var status = service.TransferStatus()
	assert.Equal(t, response, status)
}
