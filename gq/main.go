package main

import (
	"flag"
	"github.com/GeertJohan/go.rice"
	"github.com/golang/glog"
	"net/http"
)

var (
	addr = flag.String("addr", ":8080", "service address")
	dir  = flag.String("db", "./data", "work dir")
)

func main() {
	flag.Parse()
	defer glog.Flush()
	defer glog.Info("server exit")

	hub := newHub()
	go hub.run()

	http.Handle("/", http.FileServer(rice.MustFindBox("ui").HTTPBox()))
	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		serveWs(hub, w, r)
	})

	glog.Info("server listen on", *addr)
	glog.Error(http.ListenAndServe(*addr, nil))
}
