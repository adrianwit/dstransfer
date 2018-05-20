package main

import (
	_ "github.com/adrianwit/mgc"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/viant/asc"
	_ "github.com/viant/bgc"
	"github.com/adrianwit/dstransfer"
	"flag"
	"os"
	"fmt"
	"github.com/google/gops/agent"
	"log"
)

var port = flag.Int("port", 8080, "service port")


func main() {


	go func() {
		if err := agent.Listen(agent.Options{}); err != nil {
			log.Fatal(err)
		}
	} ()
	flag.Parse()
	service := dstransfer.New()
	server := dstransfer.NewServer(service, *port)
	go server.StopOnSiginals(os.Interrupt)
	fmt.Printf("dstransfer listening on :%d\n", *port)
	server.ListenAndServe()
}
