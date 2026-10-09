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
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type Backend struct {
	URL                 *url.URL
	Proxy               *httputil.ReverseProxy
	mu                  sync.RWMutex
	alive               bool
	activeConns         atomic.Int64
	totalRequests       atomic.Uint64
	totalFailures       atomic.Uint64
	consecutiveFailures atomic.Uint64
	lastCheck           time.Time
	lastError           string
	addedAt             time.Time
	transport           *http.Transport
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

	b := &Backend{URL: u, alive: true, addedAt: time.Now()}
	b.Proxy = httputil.NewSingleHostReverseProxy(u)
	b.transport = &http.Transport{
		DialContext:           (&net.Dialer{Timeout: dialTimeout}).DialContext,
		ResponseHeaderTimeout: responseTimeout,
		IdleConnTimeout:       90 * time.Second,
		MaxIdleConnsPerHost:   100,
	}
	b.Proxy.Transport = b.transport
	b.Proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		if errors.Is(err, context.Canceled) {
			return
		}
		log.Printf("proxy error for %s: %v", b.URL, err)
		b.recordProxyFailure(err)
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

func (b *Backend) recordProxyFailure(err error) {
	b.totalFailures.Add(1)
	b.consecutiveFailures.Add(1)
	b.mu.Lock()
	b.alive = false
	b.lastError = err.Error()
	b.mu.Unlock()
}

func (b *Backend) recordHealthCheck(alive bool, checkErr error) bool {
	b.mu.Lock()
	changed := b.alive != alive
	b.alive = alive
	b.lastCheck = time.Now()
	if checkErr != nil {
		b.lastError = checkErr.Error()
		b.consecutiveFailures.Add(1)
	} else {
		b.lastError = ""
		b.consecutiveFailures.Store(0)
	}
	b.mu.Unlock()
	return changed
}

func (b *Backend) status() BackendStatus {
	b.mu.RLock()
	status := BackendStatus{
		URL:                 b.URL.String(),
		Alive:               b.alive,
		ConsecutiveFailures: b.consecutiveFailures.Load(),
		LastCheck:           b.lastCheck,
		LastError:           b.lastError,
		AddedAt:             b.addedAt,
	}
	b.mu.RUnlock()
	status.TotalRequests = b.totalRequests.Load()
	status.TotalFailures = b.totalFailures.Load()
	status.ActiveConns = b.activeConns.Load()
	return status
}

type Pool struct {
	registry      *Registry
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
	registry := NewRegistry(responseTimeout, dialTimeout)
	for _, target := range targets {
		if _, err := registry.Add(target); err != nil {
			return nil, err
		}
	}
	return &Pool{registry: registry, healthTimeout: 2 * time.Second}, nil
}

func (p *Pool) Next() *Backend {
	backends := p.registry.snapshot()
	n := uint64(len(backends))
	if n == 0 {
		return nil
	}
	start := atomic.AddUint64(&p.counter, 1)
	for i := uint64(0); i < n; i++ {
		b := backends[(start+i)%n]
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
	for _, b := range p.registry.snapshot() {
		resp, err := client.Get(b.URL.String())
		alive := err == nil && resp.StatusCode < 500
		checkErr := err
		if err == nil {
			if resp.StatusCode >= 500 {
				checkErr = fmt.Errorf("health check returned HTTP status %d", resp.StatusCode)
			}
			_ = resp.Body.Close()
		}
		if b.recordHealthCheck(alive, checkErr) {
			log.Printf("backend %s alive=%v", b.URL, alive)
		}
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
		b.totalRequests.Add(1)
		b.activeConns.Add(1)
		defer b.activeConns.Add(-1)
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
		return err
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

func printProblems(prefix string, err error) {
	for _, problem := range strings.Split(err.Error(), "\n") {
		if problem != "" {
			fmt.Fprintf(os.Stderr, "%s%s\n", prefix, problem)
		}
	}
}

func warnUnresolvableBackends(backends []string) {
	for _, backend := range backends {
		u, err := url.Parse(backend)
		if err != nil || u.Hostname() == "" {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err = net.DefaultResolver.LookupHost(ctx, u.Hostname())
		cancel()
		if err != nil {
			fmt.Fprintf(os.Stderr, "warning: backend %q hostname %q could not be resolved: %v\n", backend, u.Hostname(), err)
		}
	}
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
	}
	err = errors.Join(err, cfg.Validate())
	if err != nil {
		printProblems("config error: ", err)
		os.Exit(2)
	}
	for _, warning := range cfg.Warnings() {
		fmt.Fprintf(os.Stderr, "warning: %s\n", warning)
	}
	if err := Preflight(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "preflight error: %v\n", err)
		os.Exit(1)
	}
	if cfg.CheckOnly {
		fmt.Println("config OK")
		return
	}
	warnUnresolvableBackends(cfg.Backends)
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
