package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gorilla/mux"

	"github.com/rancher/etcdb/backend"
	"github.com/rancher/etcdb/models"
	"github.com/rancher/etcdb/restapi"
	"github.com/rancher/etcdb/restapi/operations"
)

type UrlsValue []url.URL

func (uv *UrlsValue) Set(s string) error {
	vals := strings.Split(s, ",")
	urls := make([]url.URL, len(vals))

	for i, val := range vals {
		val = strings.TrimSpace(val)
		u, err := url.Parse(val)
		if err != nil {
			return err
		}
		if u.Scheme != "http" {
			return fmt.Errorf("URLs must use the http scheme: %s", val)
		}
		if u.Path != "" {
			return fmt.Errorf("URLs cannot include a path: %s", val)
		}
		if _, _, err := net.SplitHostPort(u.Host); err != nil {
			return fmt.Errorf("URLs must include a port: %s", val)
		}

		urls[i] = *u
	}

	*uv = urls
	return nil
}

func (uv *UrlsValue) String() string {
	// for flags, join with just comma since spaces are less shell-friendly
	return uv.Join(",")
}

func (uv *UrlsValue) Join(sep string) string {
	vals := make([]string, len(*uv))
	for i, u := range *uv {
		vals[i] = u.String()
	}
	return strings.Join(vals, sep)
}

func UrlsFlag(name, value, usage string) *UrlsValue {
	urls := &UrlsValue{}
	urls.Set(value)
	flag.Var(urls, name, usage)
	return urls
}

var defaultClientUrls = "http://localhost:2379,http://localhost:4001"

var initDb = flag.Bool("init-db", false, "Initialize the DB schema and exit.")
var watchPoll = flag.Duration("watch-poll", 1*time.Second, "Poll rate for watches.")
var listenClientUrls = UrlsFlag("listen-client-urls", defaultClientUrls, "List of URLs to listen on for client traffic.")
var advertiseClientUrls = UrlsFlag("advertise-client-urls", defaultClientUrls, "List of public URLs available to access the client.")

func main() {
	flag.Usage = func() {
		executable := os.Args[0]
		cmd := filepath.Base(executable)

		fmt.Fprintf(os.Stderr, "Usage of %s:\n\n", executable)
		fmt.Fprintf(os.Stderr, "  %s [options] <postgres|mysql> <datasource>\n\n", cmd)
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

	cw := backend.Watch(store, *watchPoll)

	r := mux.NewRouter()

	r.HandleFunc("/version", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "2")
	})

	r.HandleFunc("/v2/machines", func(w http.ResponseWriter, r *http.Request) {
		// for etcdctl it expects a comma and space separator instead of comma-only
		fmt.Fprint(w, advertiseClientUrls.Join(", "))
	})

	r.HandleFunc("/v2/keys{key:/.*}", func(rw http.ResponseWriter, r *http.Request) {
		var op operations.Operation
		switch r.Method {
		case "GET":
			op = &operations.GetNode{Store: store, Watcher: cw}
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

	log.Println("etcdb: advertise client URLs", advertiseClientUrls.String())

	listenErr := make(chan error)

	for _, u := range *listenClientUrls {
		go func(u url.URL) {
			log.Println("etcdb: listening for client requests on", u.String())
			listenErr <- http.ListenAndServe(u.Host, r)
		}(u)
	}

	if err := <-listenErr; err != nil {
		log.Fatalln(err)
	}
}
