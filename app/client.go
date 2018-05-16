package main

import (
	"flag"
	"github.com/adrianwit/dstransfer"
	"github.com/viant/toolbox/url"
	"log"
	"github.com/viant/toolbox"
	"fmt"
)

var req = flag.String("req", "", "transfer request")
var serviceHost = flag.String("endpoint", "localhost:8080", "service host")

func main() {
	flag.Parse()
	request := &dstransfer.TransferRequest{}
	resource := url.NewResource(*req)
	err := resource.Decode(request)
	if err != nil {
		log.Fatal(err)
	}
	toolbox.DumpIndent(request, true)

	var response =  &dstransfer.TransferResponse{}
	err = toolbox.RouteToService("POST", fmt.Sprintf("http://%v//v1/api/transfer", *serviceHost), request, response)
	if err != nil {
		log.Fatal(err)
	}
	toolbox.DumpIndent(response, true)
}
