package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"time"
)

func main() {
	port := flag.String("port", "9001", "port to listen on")
	flag.Parse()

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "Hello from backend on port %s\n", *port)
	})

	http.HandleFunc("/slow", func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(15 * time.Second)
		fmt.Fprintf(w, "finally done (port %s)\n", *port)
	})

	log.Println("backend listening on :" + *port)
	log.Fatal(http.ListenAndServe(":"+*port, nil))
}
