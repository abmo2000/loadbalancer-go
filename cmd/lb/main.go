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
	URL                        *url.URL
	Proxy                      *httputil.ReverseProxy
	mu                         sync.RWMutex
	alive                      bool
	activeConns                atomic.Int64
	totalRequests              atomic.Uint64
	totalFailures              atomic.Uint64
	consecutiveFailures        atomic.Uint64
	consecutiveSuccesses       atomic.Uint64
	lastCheck                  time.Time
	lastError                  string
	lastFailureKind            string
	lastStateChange            time.Time
	addedAt                    time.Time
	lastAliveChange            time.Time
	lastTransitionTimes        [16]time.Time
	transitionCount            int
	transitionNext             int
	transitionsSinceQuarantine int
	quarantined                bool
	quarantinedUntil           time.Time
	quarantineLevel            int
	quarantineLimitLogged      bool
	transport                  *http.Transport
	healthTransport            *http.Transport
	healthClient               *http.Client
	failThreshold              int
	riseThreshold              int
	checking                   atomic.Bool
	skipLogged                 atomic.Bool
	registry                   *Registry
	now                        func() time.Time
	flapWindow                 time.Duration
	flapMaxTransitions         int
	quarantineBase             time.Duration
	quarantineMax              time.Duration
	quarantineResetAfter       time.Duration
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

	nowFunc := time.Now
	now := nowFunc()
	b := &Backend{
		URL:             u,
		alive:           true,
		addedAt:         now,
		lastStateChange: now,
		lastAliveChange: now,
		now:             nowFunc,
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
		logHealthEvents(b, b.recordProxyFailure(err))
		http.Error(w, "backend unavailable", http.StatusBadGateway)
	}
	return b
}

func (b *Backend) SetAlive(a bool) {
	b.mu.Lock()
	if b.alive != a {
		now := b.currentTime()
		b.lastStateChange = now
		b.lastAliveChange = now
		b.appendTransitionLocked(now)
	}
	b.alive = a
	b.quarantined = false
	b.quarantinedUntil = time.Time{}
	b.mu.Unlock()
}

func (b *Backend) IsAlive() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.alive && !b.quarantined
}

type healthLogEvent struct {
	kind        string
	failureKind string
	count       uint64
	level       int
	duration    time.Duration
	transitions int
	window      time.Duration
	blocked     bool
}

func (b *Backend) recordProxyFailure(err error) []healthLogEvent {
	if b.registry != nil {
		return b.registry.recordProxyFailure(b, err)
	}
	kind := classifyFailure(err)
	b.totalFailures.Add(1)
	b.mu.Lock()
	failures := b.consecutiveFailures.Add(1)
	changed := b.alive && (isHardProxyFailure(err) || failures >= uint64(b.failThreshold))
	if changed {
		b.alive = false
		b.lastStateChange = b.currentTime()
		b.lastAliveChange = b.lastStateChange
		b.appendTransitionLocked(b.lastStateChange)
	}
	b.lastError = err.Error()
	b.lastFailureKind = kind
	b.consecutiveSuccesses.Store(0)
	b.mu.Unlock()
	if changed {
		return []healthLogEvent{{kind: "dead", failureKind: kind, count: failures, window: b.flapWindow}}
	}
	return nil
}

func (b *Backend) recordActiveCheck(success bool, kind string, checkErr error) []healthLogEvent {
	if b.registry != nil {
		return b.registry.recordActiveCheck(b, success, kind, checkErr)
	}
	return nil
}

func (b *Backend) configureHealth(failThreshold, riseThreshold int) {
	b.mu.Lock()
	b.failThreshold = failThreshold
	b.riseThreshold = riseThreshold
	b.mu.Unlock()
}

func (b *Backend) configureFlap(window time.Duration, maxTransitions int, base, maximum, resetAfter time.Duration) {
	b.mu.Lock()
	b.flapWindow = window
	b.flapMaxTransitions = maxTransitions
	b.quarantineBase = base
	b.quarantineMax = maximum
	b.quarantineResetAfter = resetAfter
	b.mu.Unlock()
}

func (b *Backend) currentTime() time.Time {
	if b.now != nil {
		return b.now()
	}
	return time.Now()
}

func (b *Backend) pruneTransitionsLocked(now time.Time) int {
	if b.flapWindow <= 0 {
		b.lastTransitionTimes = [16]time.Time{}
		b.transitionCount = 0
		b.transitionNext = 0
		return 0
	}
	cutoff := now.Add(-b.flapWindow)
	var retained [16]time.Time
	count := 0
	start := (b.transitionNext - b.transitionCount + len(b.lastTransitionTimes)) % len(b.lastTransitionTimes)
	for i := 0; i < b.transitionCount; i++ {
		transition := b.lastTransitionTimes[(start+i)%len(b.lastTransitionTimes)]
		if !transition.Before(cutoff) {
			retained[count] = transition
			count++
		}
	}
	b.lastTransitionTimes = retained
	b.transitionCount = count
	b.transitionNext = count % len(b.lastTransitionTimes)
	return count
}

func (b *Backend) appendTransitionLocked(now time.Time) int {
	b.pruneTransitionsLocked(now)
	b.transitionsSinceQuarantine++
	b.lastTransitionTimes[b.transitionNext] = now
	b.transitionNext = (b.transitionNext + 1) % len(b.lastTransitionTimes)
	if b.transitionCount < len(b.lastTransitionTimes) {
		b.transitionCount++
	}
	count := b.pruneTransitionsLocked(now)
	if count < b.flapMaxTransitions {
		b.quarantineLimitLogged = false
	}
	return count
}

func (b *Backend) recentTransitionsLocked(now time.Time) int {
	return b.pruneTransitionsLocked(now)
}

func (b *Backend) resetQuarantineLevelLocked(now time.Time) {
	if b.alive && !b.quarantined && b.quarantineLevel > 0 && b.quarantineResetAfter > 0 && now.Sub(b.lastAliveChange) >= b.quarantineResetAfter {
		b.quarantineLevel = 0
	}
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
	b.mu.Lock()
	now := b.currentTime()
	b.resetQuarantineLevelLocked(now)
	state := "dead"
	if b.quarantined {
		state = "quarantined"
	} else if b.alive {
		state = "alive"
	}
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
		State:                state,
		QuarantinedUntil:     b.quarantinedUntil,
		QuarantineLevel:      b.quarantineLevel,
		RecentTransitions:    b.recentTransitionsLocked(now),
	}
	b.mu.Unlock()
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
	logHealthEvents(backend, backend.recordActiveCheck(success, kind, checkErr))
}

func logHealthEvents(backend *Backend, events []healthLogEvent) {
	for _, event := range events {
		switch event.kind {
		case "dead":
			log.Printf("backend %s marked dead after %d failures: %s", backend.URL, event.count, event.failureKind)
		case "recovered":
			if event.blocked {
				log.Printf("backend %s recovered after %d successes; quarantine blocked by capacity safety limit", backend.URL, event.count)
			} else {
				log.Printf("backend %s recovered after %d successes", backend.URL, event.count)
			}
		case "quarantined":
			log.Printf("backend %s quarantined for %s (level %d, %d transitions in %s)", backend.URL, event.duration, event.level, event.transitions, event.window)
		case "released":
			log.Printf("backend %s released from quarantine", backend.URL)
		}
	}
}

func (p *Pool) ConfigureHealthChecks(timeout time.Duration, failThreshold, riseThreshold int, path string) {
	p.healthTimeout = timeout
	p.failThreshold = failThreshold
	p.riseThreshold = riseThreshold
	p.healthPath = path
	p.registry.configureHealth(failThreshold, riseThreshold)
}

func (p *Pool) ConfigureFlapProtection(window time.Duration, maxTransitions int, base, maximum, resetAfter time.Duration, maxPercent int) {
	p.registry.ConfigureFlapProtection(window, maxTransitions, base, maximum, resetAfter, maxPercent)
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
	pool.ConfigureFlapProtection(cfg.FlapWindow, cfg.FlapMaxTransitions, cfg.QuarantineBase, cfg.QuarantineMax, cfg.QuarantineResetAfter, cfg.MaxQuarantinePercent)
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
