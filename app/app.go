package main

import (
	_ "github.com/adrianwit/mgc"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/viant/asc"
	_ "github.com/viant/bgc"
	_ "github.com/alexbrainman/odbc"
	"flag"
	"os"
	"fmt"
	"github.com/adrianwit/dstransfer"
)

var port = flag.Int("port", 8080, "service port")

func main() {
	flag.Parse()
	service := dstransfer.New()
	server := dstransfer.NewServer(service, *port)
	go server.StopOnSiginals(os.Interrupt)
	fmt.Printf("start listening on :%d\n", *port)
	server.ListenAndServe()
}
