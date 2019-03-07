package gcp

import (
	"encoding/json"
	"github.com/adrianwit/dstransfer"
	_ "github.com/adrianwit/fbc"
	_ "github.com/adrianwit/fsc"
	_ "github.com/adrianwit/mgc"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/viant/asc"
	_ "github.com/viant/bgc"
	"net/http"
)

//TransferFn cloud function entry point to transfer data from source to dest datastore
func TransferFn(writer http.ResponseWriter, request *http.Request) {
	service := dstransfer.New(true, nil)
	decoder := json.NewDecoder(request.Body)
	transferRequest := &dstransfer.TransferRequest{}
	if err := decoder.Decode(&request);err != nil {
		http.Error(writer, err.Error(),  http.StatusInternalServerError)
		return
	}
	response := service.Transfer(transferRequest)
	if err := json.NewEncoder(writer).Encode(response);err != nil {
		http.Error(writer, err.Error(),  http.StatusInternalServerError)
	}
}