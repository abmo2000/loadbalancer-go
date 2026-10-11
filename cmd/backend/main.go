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
	flakyPeriod := flag.Duration("flaky-period", 0, "testing aid: alternate healthy and 503 root periods")
	logRequests := flag.Bool("log-requests", false, "testing aid: log request timestamp, method, and path")
	flag.Parse()
	startedAt := time.Now()
	if *logRequests {
		log.SetFlags(0)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" && *flakyPeriod > 0 && int64(time.Since(startedAt) / *flakyPeriod)%2 == 1 {
			http.Error(w, "temporarily unhealthy", http.StatusServiceUnavailable)
			return
		}
		fmt.Fprintf(w, "Hello from backend on port %s\n", *port)
	})

	mux.HandleFunc("/slow", func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(15 * time.Second)
		fmt.Fprintf(w, "finally done (port %s)\n", *port)
	})

	log.Println("backend listening on :" + *port)
	handler := http.Handler(mux)
	if *logRequests {
		handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			log.Printf("%s %s %s", time.Now().Format(time.RFC3339Nano), r.Method, r.URL.Path)
			mux.ServeHTTP(w, r)
		})
	}
	log.Fatal(http.ListenAndServe(":"+*port, handler))
}
