package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type Backend struct {
	URL   *url.URL
	Proxy *httputil.ReverseProxy
	mu    sync.RWMutex
	alive bool
}

func NewBackend(u *url.URL, args ...time.Duration) *Backend {
	dialTimeout := 3 * time.Second
	responseTimeout := 10 * time.Second
	switch len(args) {
	case 1:
		responseTimeout = args[0]
	case 2:
		dialTimeout = args[0]
		responseTimeout = args[1]
	}

	b := &Backend{URL: u, alive: true}
	b.Proxy = httputil.NewSingleHostReverseProxy(u)
	b.Proxy.Transport = &http.Transport{
		DialContext:           (&net.Dialer{Timeout: dialTimeout}).DialContext,
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
	backends      []*Backend
	counter       uint64
	healthTimeout time.Duration
}

func NewPool(targets []string, args ...time.Duration) (*Pool, error) {
	dialTimeout := 3 * time.Second
	responseTimeout := 10 * time.Second
	switch len(args) {
	case 1:
		responseTimeout = args[0]
	case 2:
		dialTimeout = args[0]
		responseTimeout = args[1]
	}
	pool := &Pool{healthTimeout: 2 * time.Second}
	for _, target := range targets {
		u, err := url.Parse(target)
		if err != nil {
			return nil, err
		}
		pool.backends = append(pool.backends, NewBackend(u, dialTimeout, responseTimeout))
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
	timeout := p.healthTimeout
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	client := http.Client{Timeout: timeout}
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

func (p *Pool) HealthCheckContext(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.CheckOnce()
		}
	}
}

func (p *Pool) HealthCheck(interval time.Duration) {
	p.HealthCheckContext(context.Background(), interval)
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

func runServerWithServeFunc(ctx context.Context, srv *http.Server, shutdownTimeout time.Duration, serve func() error) error {
	serverErr := make(chan error, 1)
	go func() {
		err := serve()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
			return
		}
		serverErr <- nil
	}()

	select {
	case err := <-serverErr:
		if err != nil {
			return err
		}
		return nil
	case <-ctx.Done():
		log.Println("shutdown signal received")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				log.Printf("shutdown timeout hit after %s", shutdownTimeout)
				_ = srv.Close()
				return err
			}
			return err
		}
		if err := <-serverErr; err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	}
}

func runServer(ctx context.Context, srv *http.Server, shutdownTimeout time.Duration) error {
	return runServerWithServeFunc(ctx, srv, shutdownTimeout, srv.ListenAndServe)
}

func runServerWithListener(ctx context.Context, srv *http.Server, shutdownTimeout time.Duration, listener net.Listener) error {
	return runServerWithServeFunc(ctx, srv, shutdownTimeout, func() error {
		return srv.Serve(listener)
	})
}

func main() {
	env := func(name string) string {
		if value, ok := os.LookupEnv(name); ok {
			return value
		}
		return missingEnvValue
	}
	cfg, err := LoadConfig(env, os.Args[1:])
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(2)
	}
	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(2)
	}
	log.Println(cfg.String())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := NewPool(cfg.Backends, cfg.DialTimeout, cfg.ResponseTimeout)
	if err != nil {
		log.Fatal(err)
	}
	pool.healthTimeout = cfg.HealthTimeout
	go pool.HealthCheckContext(ctx, cfg.HealthInterval)

	srv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           pool.Handler(),
		ReadHeaderTimeout: cfg.ReadHeaderTimeout,
		ReadTimeout:       cfg.ReadTimeout,
		WriteTimeout:      cfg.WriteTimeout,
		IdleTimeout:       cfg.IdleTimeout,
	}

	log.Printf("load balancer listening on %s", cfg.ListenAddr)
	if err := runServer(ctx, srv, cfg.ShutdownTimeout); err != nil {
		log.Printf("server shutdown error: %v", err)
		os.Exit(1)
	}
	log.Println("shutdown complete")
}
