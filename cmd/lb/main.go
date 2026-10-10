package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
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
	URL                  *url.URL
	Proxy                *httputil.ReverseProxy
	mu                   sync.RWMutex
	alive                bool
	activeConns          atomic.Int64
	totalRequests        atomic.Uint64
	totalFailures        atomic.Uint64
	consecutiveFailures  atomic.Uint64
	consecutiveSuccesses atomic.Uint64
	lastCheck            time.Time
	lastError            string
	lastFailureKind      string
	lastStateChange      time.Time
	addedAt              time.Time
	transport            *http.Transport
	healthTransport      *http.Transport
	healthClient         *http.Client
	failThreshold        int
	riseThreshold        int
	checking             atomic.Bool
	skipLogged           atomic.Bool
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

	now := time.Now()
	b := &Backend{
		URL:             u,
		alive:           true,
		addedAt:         now,
		lastStateChange: now,
		failThreshold:   3,
		riseThreshold:   2,
	}
	b.Proxy = httputil.NewSingleHostReverseProxy(u)
	b.transport = &http.Transport{
		DialContext:           (&net.Dialer{Timeout: dialTimeout}).DialContext,
		ResponseHeaderTimeout: responseTimeout,
		IdleConnTimeout:       90 * time.Second,
		MaxIdleConnsPerHost:   100,
	}
	b.Proxy.Transport = b.transport
	b.healthTransport = &http.Transport{
		DialContext:         (&net.Dialer{Timeout: dialTimeout}).DialContext,
		IdleConnTimeout:     10 * time.Second,
		DisableKeepAlives:   false,
		MaxIdleConnsPerHost: 1,
	}
	b.healthClient = &http.Client{
		Transport: b.healthTransport,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	b.Proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		if errors.Is(err, context.Canceled) {
			return
		}
		if failures, kind, changed := b.recordProxyFailure(err); changed {
			log.Printf("backend %s marked dead after %d failures: %s", b.URL, failures, kind)
		}
		http.Error(w, "backend unavailable", http.StatusBadGateway)
	}
	return b
}

func (b *Backend) SetAlive(a bool) {
	b.mu.Lock()
	if b.alive != a {
		b.lastStateChange = time.Now()
	}
	b.alive = a
	b.mu.Unlock()
}

func (b *Backend) IsAlive() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.alive
}

func (b *Backend) recordProxyFailure(err error) (uint64, string, bool) {
	kind := classifyFailure(err)
	hardFailure := isHardProxyFailure(err)
	b.totalFailures.Add(1)
	b.mu.Lock()
	failures := b.consecutiveFailures.Add(1)
	b.consecutiveSuccesses.Store(0)
	b.lastError = err.Error()
	b.lastFailureKind = kind
	changed := b.alive && (hardFailure || failures >= uint64(b.failThreshold))
	if changed {
		b.alive = false
		b.lastStateChange = time.Now()
	}
	b.mu.Unlock()
	return failures, kind, changed
}

func (b *Backend) recordActiveCheck(success bool, kind string, checkErr error) (string, uint64) {
	b.mu.Lock()
	now := time.Now()
	b.lastCheck = now
	if success {
		b.lastError = ""
		b.lastFailureKind = ""
		b.consecutiveFailures.Store(0)
		if b.alive {
			b.consecutiveSuccesses.Store(0)
			b.mu.Unlock()
			return "", 0
		}
		successes := b.consecutiveSuccesses.Add(1)
		if successes >= uint64(b.riseThreshold) {
			b.alive = true
			b.lastStateChange = now
			b.mu.Unlock()
			return "recovered", successes
		}
		b.mu.Unlock()
		return "", successes
	}
	b.lastError = checkErr.Error()
	b.lastFailureKind = kind
	b.consecutiveSuccesses.Store(0)
	if !b.alive {
		b.mu.Unlock()
		return "", 0
	}
	failures := b.consecutiveFailures.Add(1)
	if failures >= uint64(b.failThreshold) {
		b.alive = false
		b.lastStateChange = now
		b.mu.Unlock()
		return kind, failures
	}
	b.mu.Unlock()
	return "", failures
}

func (b *Backend) configureHealth(failThreshold, riseThreshold int) {
	b.mu.Lock()
	b.failThreshold = failThreshold
	b.riseThreshold = riseThreshold
	b.mu.Unlock()
}

func classifyFailure(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	var networkError net.Error
	if errors.As(err, &networkError) && networkError.Timeout() {
		return "timeout"
	}
	var dnsError *net.DNSError
	if errors.As(err, &dnsError) {
		return "dns"
	}
	if errors.Is(err, syscall.ECONNREFUSED) {
		return "connection_refused"
	}
	return "other"
}

func isHardProxyFailure(err error) bool {
	if classifyFailure(err) == "timeout" {
		return false
	}
	if errors.Is(err, syscall.ECONNREFUSED) {
		return true
	}
	var operationError *net.OpError
	return errors.As(err, &operationError) && operationError.Op == "dial"
}

func (b *Backend) status() BackendStatus {
	b.mu.RLock()
	status := BackendStatus{
		URL:                  b.URL.String(),
		Alive:                b.alive,
		ConsecutiveFailures:  b.consecutiveFailures.Load(),
		ConsecutiveSuccesses: b.consecutiveSuccesses.Load(),
		LastCheck:            b.lastCheck,
		LastError:            b.lastError,
		LastFailureKind:      b.lastFailureKind,
		LastStateChange:      b.lastStateChange,
		AddedAt:              b.addedAt,
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
	failThreshold int
	riseThreshold int
	healthPath    string
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
	return &Pool{registry: registry, healthTimeout: 2 * time.Second, failThreshold: 3, riseThreshold: 2, healthPath: "/"}, nil
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
	p.CheckOnceContext(context.Background())
}

func (p *Pool) CheckOnceContext(ctx context.Context) {
	var checks sync.WaitGroup
	for _, backend := range p.registry.snapshot() {
		if !backend.checking.CompareAndSwap(false, true) {
			if backend.skipLogged.CompareAndSwap(false, true) {
				log.Printf("health check for %s still running, skipping tick", backend.URL)
			}
			continue
		}
		backend.skipLogged.Store(false)
		checks.Add(1)
		go func(backend *Backend) {
			defer checks.Done()
			defer backend.checking.Store(false)
			defer backend.skipLogged.Store(false)
			p.checkBackend(ctx, backend)
		}(backend)
	}
	checks.Wait()
}

func (p *Pool) checkBackend(parent context.Context, backend *Backend) {
	timeout := p.healthTimeout
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	probeURL := *backend.URL
	path := p.healthPath
	if path == "" {
		path = "/"
	}
	probeURL.Path = path
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, probeURL.String(), nil)
	if err != nil {
		p.recordCheckResult(backend, false, classifyFailure(err), err)
		return
	}
	response, err := backend.healthClient.Do(request)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return
		}
		p.recordCheckResult(backend, false, classifyFailure(err), err)
		return
	}
	defer response.Body.Close()
	if _, err := io.Copy(io.Discard, io.LimitReader(response.Body, 4096)); err != nil {
		if errors.Is(err, context.Canceled) {
			return
		}
		p.recordCheckResult(backend, false, classifyFailure(err), err)
		return
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusBadRequest {
		checkErr := fmt.Errorf("health check returned HTTP status %d", response.StatusCode)
		p.recordCheckResult(backend, false, "bad_status", checkErr)
		return
	}
	p.recordCheckResult(backend, true, "", nil)
}

func (p *Pool) recordCheckResult(backend *Backend, success bool, kind string, checkErr error) {
	transition, count := backend.recordActiveCheck(success, kind, checkErr)
	switch transition {
	case "recovered":
		log.Printf("backend %s recovered after %d successes", backend.URL, count)
	case "timeout", "connection_refused", "dns", "bad_status", "other":
		log.Printf("backend %s marked dead after %d failures: %s", backend.URL, count, transition)
	}
}

func (p *Pool) ConfigureHealthChecks(timeout time.Duration, failThreshold, riseThreshold int, path string) {
	p.healthTimeout = timeout
	p.failThreshold = failThreshold
	p.riseThreshold = riseThreshold
	p.healthPath = path
	p.registry.configureHealth(failThreshold, riseThreshold)
}

func (p *Pool) HealthCheckContext(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.CheckOnceContext(ctx)
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
	pool.ConfigureHealthChecks(cfg.HealthTimeout, cfg.FailThreshold, cfg.RiseThreshold, cfg.HealthPath)
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
