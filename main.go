package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/gorilla/mux"

	"github.com/rancherio/etcdb/backend"
	"github.com/rancherio/etcdb/models"
	"github.com/rancherio/etcdb/restapi"
	"github.com/rancherio/etcdb/restapi/operations"
)

var initDb = flag.Bool("init-db", false, "Initialize the DB schema and exit.")

func main() {
	flag.Usage = func() {
		executable := os.Args[0]
		cmd := filepath.Base(executable)

		fmt.Fprintf(os.Stderr, "Usage of %s:\n\n", executable)
		fmt.Fprintf(os.Stderr, "  %s [-init-db] <postgres|mysql> <datasource>\n\n", cmd)
		flag.PrintDefaults()

		fmt.Fprintln(os.Stderr, "\n  Examples:")
		fmt.Fprintf(os.Stderr, "    %s postgres \"user=username password=password host=hostname dbname=dbname sslmode=disable\"\n", cmd)
		fmt.Fprintf(os.Stderr, "    %s mysql username:password@tcp(hostname:3306)/dbname\n", cmd)

		fmt.Fprintln(os.Stderr, "\n  Datasource formats:")
		fmt.Fprintln(os.Stderr, "    postgres: https://godoc.org/github.com/lib/pq#hdr-Connection_String_Parameters")
		fmt.Fprintln(os.Stderr, "    mysql: https://github.com/go-sql-driver/mysql#dsn-data-source-name")
	}

	flag.Parse()
	if flag.NArg() != 2 {
		flag.Usage()
		os.Exit(2)
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "2379"
	}

	host := os.Getenv("HOST")
	if host == "" {
		host = "localhost"
	}

	dbDriver := flag.Arg(0)
	dbDataSource := flag.Arg(1)

	fmt.Println("connecting to database:", dbDriver, dbDataSource)
	store, err := backend.New(dbDriver, dbDataSource)
	if err != nil {
		log.Fatalln(err)
	}

	if *initDb {
		fmt.Println("initializing db schema...")
		err = store.CreateSchema()
		if err != nil {
			log.Fatalln(err)
		}
		return
	}

	listener, err := net.Listen("tcp", host+":"+port)
	if err != nil {
		log.Fatalln(err)
	}

	fmt.Printf("serving etcd at http://%s\n", listener.Addr())

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
			op = &operations.GetNode{Store: store, WaitPollPeriod: time.Second}
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
				log.Println(err)
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
