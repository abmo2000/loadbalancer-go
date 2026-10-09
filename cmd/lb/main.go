package main

import (
	"context"
	"errors"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync"
	"sync/atomic"
	"time"
)

type Backend struct {
	URL   *url.URL
	Proxy *httputil.ReverseProxy
	mu    sync.RWMutex
	alive bool
}

func NewBackend(u *url.URL, responseTimeout time.Duration) *Backend {
	b := &Backend{URL: u, alive: true}
	b.Proxy = httputil.NewSingleHostReverseProxy(u)
	b.Proxy.Transport = &http.Transport{
		DialContext:           (&net.Dialer{Timeout: 3 * time.Second}).DialContext,
		ResponseHeaderTimeout: responseTimeout,
		IdleConnTimeout:       90 * time.Second,
		MaxIdleConnsPerHost:   100,
	}
	b.Proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		if errors.Is(err, context.Canceled) {
			return
		}
		log.Printf("proxy error for %s: %v", b.URL, err)
		b.SetAlive(false)
		http.Error(w, "backend unavailable", http.StatusBadGateway)
	}
	return b
}

func (b *Backend) SetAlive(a bool) {
	b.mu.Lock()
	b.alive = a
	b.mu.Unlock()
}

func (b *Backend) IsAlive() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.alive
}

type Pool struct {
	backends []*Backend
	counter  uint64
}

func NewPool(targets []string, responseTimeout time.Duration) (*Pool, error) {
	pool := &Pool{}
	for _, target := range targets {
		u, err := url.Parse(target)
		if err != nil {
			return nil, err
		}
		pool.backends = append(pool.backends, NewBackend(u, responseTimeout))
	}
	return pool, nil
}

func (p *Pool) Next() *Backend {
	n := uint64(len(p.backends))
	if n == 0 {
		return nil
	}
	start := atomic.AddUint64(&p.counter, 1)
	for i := uint64(0); i < n; i++ {
		b := p.backends[(start+i)%n]
		if b.IsAlive() {
			return b
		}
	}
	return nil
}

func (p *Pool) CheckOnce() {
	client := http.Client{Timeout: 2 * time.Second}
	for _, b := range p.backends {
		resp, err := client.Get(b.URL.String())
		alive := err == nil && resp.StatusCode < 500
		if err == nil {
			_ = resp.Body.Close()
		}
		if alive != b.IsAlive() {
			log.Printf("backend %s alive=%v", b.URL, alive)
		}
		b.SetAlive(alive)
	}
}

func (p *Pool) HealthCheck(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		p.CheckOnce()
	}
}

func (p *Pool) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b := p.Next()
		if b == nil {
			http.Error(w, "no healthy backends", http.StatusServiceUnavailable)
			return
		}
		b.Proxy.ServeHTTP(w, r)
	})
}

func main() {
	targets := []string{
		"http://localhost:9001",
		"http://localhost:9002",
		"http://localhost:9003",
	}

	pool, err := NewPool(targets, 10*time.Second)
	if err != nil {
		log.Fatal(err)
	}

	go pool.HealthCheck(5 * time.Second)

	srv := &http.Server{
		Addr:              ":8080",
		Handler:           pool.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	log.Println("load balancer listening on :8080")
	log.Fatal(srv.ListenAndServe())
}
