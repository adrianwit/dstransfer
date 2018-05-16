package dstransfer

import (
	"net/http"
	"github.com/viant/toolbox"
	"fmt"
)

const baseURI = "/v1/api"

type Router struct {
	*http.ServeMux
	service *Service
}

func (r Router) route() {
	r.ServeMux.Handle(baseURI + "/", r.api())
	r.ServeMux.Handle("/", r.static())
}


func (r Router) api() http.Handler {
	router := toolbox.NewServiceRouter(
		toolbox.ServiceRouting{
			HTTPMethod: "POST",
			URI:        fmt.Sprintf("%v/transfer", baseURI),
			Handler:    r.service.Transfer,
			Parameters: []string{"request"},
		},
		toolbox.ServiceRouting{
			HTTPMethod: "GET",
			URI:        fmt.Sprintf("%v/transfer", baseURI),
			Handler:    r.service.TransferStatus,
			Parameters: []string{},
		},
	)
	return http.HandlerFunc(func(writer http.ResponseWriter, reader *http.Request) {
		defer func() {
			fmt.Printf("Done:\n")
			if r := recover(); r != nil {
				var err = fmt.Errorf("%v", r)
				http.Error(writer, err.Error(), 500)
			}
		}()

		if err := router.Route(writer, reader);err != nil {
			http.Error(writer, err.Error(), 500)
		}
	})
}

func (r Router) static() http.Handler {
	return http.FileServer(http.Dir("static"))
}

func NewRouter(dummyService *Service) http.Handler {
	var result = &Router{
		ServeMux: http.NewServeMux(),
		service:  dummyService,
	}
	result.route()
	return result
}
