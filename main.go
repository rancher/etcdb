package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"

	"github.com/gorilla/mux"

	"github.com/rancherio/etcdb/backend"
	"github.com/rancherio/etcdb/models"
	"github.com/rancherio/etcdb/restapi"
	"github.com/rancherio/etcdb/restapi/operations"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "0"
	}

	host := os.Getenv("HOST")
	if host == "" {
		host = "localhost"
	}

	listener, err := net.Listen("tcp", host+":"+port)
	if err != nil {
		log.Fatalln(err)
	}

	fmt.Printf("serving etcd at http://%s\n", listener.Addr())

	store, _ := backend.New("postgres", "sslmode=disable")

	r := mux.NewRouter()

	r.HandleFunc("/version", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "2")
	})

	r.HandleFunc("/v2/machines", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "http://%s", listener.Addr())
	})

	r.HandleFunc("/v2/keys{key:/.*}", func(rw http.ResponseWriter, r *http.Request) {
		var op operations.Operation
		switch r.Method {
		case "GET":
			op = &operations.GetNode{Store: store}
		case "PUT":
			op = &operations.SetNode{Store: store}
		case "POST":
			op = &operations.CreateInOrderNode{Store: store}
		case "DELETE":
			op = &operations.DeleteNode{Store: store}
		default:
			rw.Header().Set("Allow", "GET, PUT, POST, DELETE")
			rw.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		res := func() interface{} {
			if err := restapi.Unmarshal(r, op.Params()); err != nil {
				return models.InvalidField(err.Error())
			}

			res, err := op.Call()
			if _, ok := err.(models.Error); ok {
				return err
			} else if err != nil {
				return models.RaftInternalError(err.Error())
			}

			return res
		}()

		js, _ := json.Marshal(res)
		// FIXME handle serialization errors?

		rw.Header().Set("Content-Type", "application/json")

		if err, ok := res.(models.Error); ok {
			rw.Header().Add("X-Etcd-Index", fmt.Sprint(err.Index))

			switch err.ErrorCode {
			default:
				rw.WriteHeader(http.StatusBadRequest)
			case 100:
				rw.WriteHeader(http.StatusNotFound)
			case 101:
				rw.WriteHeader(http.StatusPreconditionFailed)
			case 102:
				rw.WriteHeader(http.StatusForbidden)
			case 105:
				rw.WriteHeader(http.StatusPreconditionFailed)
			case 108:
				rw.WriteHeader(http.StatusForbidden)
			case 300:
				rw.WriteHeader(http.StatusInternalServerError)
			}
		}

		fmt.Fprintln(rw, string(js))
	})

	if err := http.Serve(listener, r); err != nil {
		log.Fatalln(err)
	}
}
