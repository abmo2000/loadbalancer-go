package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func TestRegistryAddAndNormalize(t *testing.T) {
	registry := NewRegistry(time.Second, time.Second)
	backend, err := registry.Add("http://Localhost:9001/")
	if err != nil {
		t.Fatalf("Add returned error: %v", err)
	}
	if got, want := backend.URL.String(), "http://localhost:9001"; got != want {
		t.Fatalf("stored URL = %q; want %q", got, want)
	}
	if registry.Len() != 1 {
		t.Fatalf("Len = %d; want 1", registry.Len())
	}

	invalid := []string{"localhost:9001", "ftp://localhost:9001", "http://", "http://user:pass@localhost:9001"}
	for _, rawURL := range invalid {
		t.Run("invalid "+rawURL, func(t *testing.T) {
			if _, err := registry.Add(rawURL); !errors.Is(err, ErrInvalidBackend) {
				t.Fatalf("Add(%q) error = %v; want ErrInvalidBackend", rawURL, err)
			}
		})
	}

	duplicates := [][]string{
		{"http://localhost:9002/", "http://LOCALHOST:9002"},
		{"http://example.com", "http://example.com:80"},
	}
	for index, pair := range duplicates {
		t.Run(fmt.Sprintf("duplicate %d", index), func(t *testing.T) {
			separate := NewRegistry(time.Second, time.Second)
			if _, err := separate.Add(pair[0]); err != nil {
				t.Fatalf("initial Add returned error: %v", err)
			}
			if _, err := separate.Add(pair[1]); !errors.Is(err, ErrDuplicateBackend) {
				t.Fatalf("duplicate Add error = %v; want ErrDuplicateBackend", err)
			}
		})
	}
}

func TestRegistryRemoveInFlight(t *testing.T) {
	arrived := make(chan struct{})
	release := make(chan struct{})
	var arrivalOnce sync.Once
	var releaseOnce sync.Once
	backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		arrivalOnce.Do(func() { close(arrived) })
		select {
		case <-release:
			_, _ = io.WriteString(w, "finished")
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(backendServer.Close)
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })

	registry := NewRegistry(time.Second, time.Second)
	backend, err := registry.Add(backendServer.URL)
	if err != nil {
		t.Fatalf("Add returned error: %v", err)
	}
	pool := &Pool{registry: registry, healthTimeout: time.Second}
	requestDone := make(chan struct {
		status int
		body   string
	}, 1)
	go func() {
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodGet, "http://balancer.test/", nil)
		pool.Handler().ServeHTTP(recorder, request)
		requestDone <- struct {
			status int
			body   string
		}{recorder.Code, recorder.Body.String()}
	}()
	waitRegistrySignal(t, arrived, "backend request arrival")

	if err := registry.Remove(backend.URL.String()); err != nil {
		t.Fatalf("Remove returned error: %v", err)
	}
	if registry.Len() != 0 || pool.Next() != nil {
		t.Fatal("removed backend remained registered or selectable")
	}
	if err := registry.Remove(backend.URL.String()); !errors.Is(err, ErrBackendNotFound) {
		t.Fatalf("second Remove error = %v; want ErrBackendNotFound", err)
	}
	releaseOnce.Do(func() { close(release) })
	result := waitRegistryResult(t, requestDone, "in-flight response")
	if result.status != http.StatusOK || result.body != "finished" {
		t.Fatalf("in-flight response = %d %q; want 200 %q", result.status, result.body, "finished")
	}
}

func TestRegistryListSnapshotAndOrder(t *testing.T) {
	first := newRegistryTestServer(t, "first", nil)
	second := newRegistryTestServer(t, "second", nil)
	registry := NewRegistry(time.Second, time.Second)
	if _, err := registry.Add(first.URL); err != nil {
		t.Fatalf("Add first: %v", err)
	}
	if _, err := registry.Add(second.URL); err != nil {
		t.Fatalf("Add second: %v", err)
	}

	snapshot := registry.List()
	if len(snapshot) != 2 || snapshot[0].URL != first.URL || snapshot[1].URL != second.URL {
		t.Fatalf("List order = %#v; want insertion order", snapshot)
	}
	snapshot[0].URL = "corrupted"
	snapshot[0].Alive = false
	snapshot[1] = BackendStatus{URL: "replaced"}
	current := registry.List()
	if current[0].URL != first.URL || !current[0].Alive || current[1].URL != second.URL {
		t.Fatalf("mutating List result changed registry: %#v", current)
	}
}

func TestRegistrySetAliveAndHealthy(t *testing.T) {
	first := newRegistryTestServer(t, "first", nil)
	second := newRegistryTestServer(t, "second", nil)
	registry := NewRegistry(time.Second, time.Second)
	if _, err := registry.Add(first.URL); err != nil {
		t.Fatalf("Add first: %v", err)
	}
	if _, err := registry.Add(second.URL); err != nil {
		t.Fatalf("Add second: %v", err)
	}
	if err := registry.SetAlive(first.URL, false); err != nil {
		t.Fatalf("SetAlive: %v", err)
	}
	healthy := registry.Healthy()
	if len(healthy) != 1 || healthy[0].URL.String() != second.URL {
		t.Fatalf("Healthy returned %#v; want only %s", healthy, second.URL)
	}
	if err := registry.SetAlive(first.URL, true); err != nil {
		t.Fatalf("SetAlive recovery: %v", err)
	}
	if len(registry.Healthy()) != 2 {
		t.Fatal("Healthy did not reflect SetAlive(true)")
	}
}

func TestPoolDynamicRoundRobin(t *testing.T) {
	var hits [3]atomic.Int32
	servers := make([]*httptest.Server, 3)
	for index := range servers {
		index := index
		servers[index] = newRegistryTestServer(t, fmt.Sprintf("backend-%d", index+1), func() { hits[index].Add(1) })
	}
	registry := NewRegistry(time.Second, time.Second)
	for _, server := range servers[:2] {
		if _, err := registry.Add(server.URL); err != nil {
			t.Fatalf("Add initial backend: %v", err)
		}
	}
	pool := &Pool{registry: registry, healthTimeout: time.Second}
	serveRequests(t, pool, 2)
	if hits[0].Load() == 0 || hits[1].Load() == 0 {
		t.Fatal("both initial backends should receive requests")
	}
	if _, err := registry.Add(servers[2].URL); err != nil {
		t.Fatalf("Add third backend: %v", err)
	}
	serveRequests(t, pool, 6)
	if hits[2].Load() == 0 {
		t.Fatal("new backend did not receive requests")
	}
	if err := registry.Remove(servers[1].URL); err != nil {
		t.Fatalf("Remove second backend: %v", err)
	}
	removedHits := hits[1].Load()
	serveRequests(t, pool, 6)
	if got := hits[1].Load(); got != removedHits {
		t.Fatalf("removed backend received %d new requests", got-removedHits)
	}
}

func TestPoolHealthCheckIncludesAddedBackend(t *testing.T) {
	dead := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "unhealthy", http.StatusInternalServerError)
	}))
	t.Cleanup(dead.Close)
	registry := NewRegistry(time.Second, time.Second)
	pool := &Pool{registry: registry, healthTimeout: time.Second}
	pool.CheckOnce()
	if _, err := registry.Add(dead.URL); err != nil {
		t.Fatalf("Add backend: %v", err)
	}
	for range 3 {
		pool.CheckOnce()
	}
	statuses := registry.List()
	if len(statuses) != 1 || statuses[0].Alive || statuses[0].ConsecutiveFailures != 3 || statuses[0].LastCheck.IsZero() || statuses[0].LastError == "" {
		t.Fatalf("new backend health status = %#v; want newly checked dead backend", statuses)
	}
}

func TestBackendCountersAndHealthRecovery(t *testing.T) {
	healthyServer := newRegistryTestServer(t, "healthy", nil)
	registry := NewRegistry(time.Second, time.Second)
	if _, err := registry.Add(healthyServer.URL); err != nil {
		t.Fatalf("Add healthy backend: %v", err)
	}
	pool := &Pool{registry: registry, healthTimeout: time.Second}
	const requests = 7
	serveRequests(t, pool, requests)
	backend, ok := registry.Get(healthyServer.URL)
	if !ok {
		t.Fatal("healthy backend missing")
	}
	status := backend.status()
	if status.TotalRequests != requests || status.ActiveConns != 0 || status.TotalFailures != 0 {
		t.Fatalf("healthy counters = %#v; want requests=%d, active=0, failures=0", status, requests)
	}
	for range 3 {
		backend.recordProxyFailure(errors.New("synthetic backend failure"))
	}
	for range 2 {
		pool.CheckOnce()
	}
	status = backend.status()
	if !status.Alive || status.ConsecutiveFailures != 0 || status.LastError != "" {
		t.Fatalf("successful health check did not reset failure state: %#v", status)
	}

	refused := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	refusedURL := refused.URL
	refused.Close()
	failedRegistry := NewRegistry(time.Second, time.Second)
	if _, err := failedRegistry.Add(refusedURL); err != nil {
		t.Fatalf("Add refused backend: %v", err)
	}
	failedPool := &Pool{registry: failedRegistry, healthTimeout: time.Second}
	recorder := httptest.NewRecorder()
	failedPool.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "http://balancer.test/", nil))
	failedStatus := failedRegistry.List()[0]
	if recorder.Code != http.StatusBadGateway || failedStatus.TotalRequests != 1 || failedStatus.TotalFailures != 1 || failedStatus.ActiveConns != 0 || failedStatus.Alive {
		t.Fatalf("refused-backend result status=%d counters=%#v", recorder.Code, failedStatus)
	}
}

func TestRegistryRaceConcurrentChanges(t *testing.T) {
	servers := []*httptest.Server{
		newRegistryTestServer(t, "one", nil),
		newRegistryTestServer(t, "two", nil),
		newRegistryTestServer(t, "three", nil),
	}
	registry := NewRegistry(time.Second, time.Second)
	for _, server := range servers[:2] {
		if _, err := registry.Add(server.URL); err != nil {
			t.Fatalf("Add initial backend: %v", err)
		}
	}
	pool := &Pool{registry: registry, healthTimeout: time.Second}
	var badStatus atomic.Int32
	var workers sync.WaitGroup
	for i := 0; i < 50; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for requestIndex := 0; requestIndex < 40; requestIndex++ {
				recorder := httptest.NewRecorder()
				request := httptest.NewRequest(http.MethodGet, "http://balancer.test/", nil)
				pool.Handler().ServeHTTP(recorder, request)
				if recorder.Code != http.StatusOK && recorder.Code != http.StatusBadGateway && recorder.Code != http.StatusServiceUnavailable {
					badStatus.Add(1)
				}
			}
		}()
	}
	workers.Add(1)
	go func() {
		defer workers.Done()
		for i := 0; i < 100; i++ {
			_ = registry.Remove(servers[2].URL)
			_, _ = registry.Add(servers[2].URL)
		}
	}()
	workers.Add(1)
	go func() {
		defer workers.Done()
		for i := 0; i < 25; i++ {
			_ = registry.List()
			pool.CheckOnce()
		}
	}()
	workers.Wait()
	if got := badStatus.Load(); got != 0 {
		t.Fatalf("got %d unexpected response statuses", got)
	}
}

func TestPoolEmptyRegistry(t *testing.T) {
	pool, err := NewPool(nil, 200*time.Millisecond)
	if err != nil {
		t.Fatalf("NewPool returned error: %v", err)
	}
	if pool.Next() != nil || pool.registry.Len() != 0 {
		t.Fatal("empty registry should have no selection")
	}
	recorder := httptest.NewRecorder()
	pool.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "http://balancer.test/", nil))
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("empty handler status = %d; want 503", recorder.Code)
	}
}

func newRegistryTestServer(t *testing.T, body string, hit func()) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if hit != nil {
			hit()
		}
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(server.Close)
	return server
}

func serveRequests(t *testing.T, pool *Pool, count int) {
	t.Helper()
	for range count {
		recorder := httptest.NewRecorder()
		pool.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "http://balancer.test/", nil))
		if recorder.Code != http.StatusOK {
			t.Fatalf("request returned %d: %s", recorder.Code, strings.TrimSpace(recorder.Body.String()))
		}
	}
}

func waitRegistrySignal(t *testing.T, signal <-chan struct{}, what string) {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case <-signal:
	case <-timer.C:
		t.Fatalf("timed out waiting for %s", what)
	}
}

func waitRegistryResult[T any](t *testing.T, result <-chan T, what string) T {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case value := <-result:
		return value
	case <-timer.C:
		t.Fatalf("timed out waiting for %s", what)
		var zero T
		return zero
	}
}

func TestHealthFailureThresholdAndReset(t *testing.T) {
	backendServer := newControllableBackend(t, "healthy")
	defer backendServer.Close()
	pool, err := NewPool([]string{backendServer.Target()}, time.Second)
	if err != nil {
		t.Fatalf("NewPool returned error: %v", err)
	}
	pool.ConfigureHealthChecks(time.Second, 3, 2, "/")
	backend, _ := pool.registry.Get(backendServer.Target())

	backendServer.SetHealthy(false)
	for expected := uint64(1); expected <= 2; expected++ {
		pool.CheckOnce()
		status := backend.status()
		if !status.Alive || status.ConsecutiveFailures != expected {
			t.Fatalf("after failure %d status = %#v; want alive with %d failures", expected, status, expected)
		}
	}

	backendServer.SetHealthy(true)
	pool.CheckOnce()
	if got := backend.status().ConsecutiveFailures; got != 0 {
		t.Fatalf("success did not reset consecutive failures: got %d", got)
	}
	backendServer.SetHealthy(false)
	for range 2 {
		pool.CheckOnce()
	}
	if !backend.IsAlive() {
		t.Fatal("two post-reset failures marked backend dead")
	}
	pool.CheckOnce()
	if backend.IsAlive() {
		t.Fatal("third consecutive failure did not mark backend dead")
	}
}

func TestHealthRecoveryThresholdAndFlapping(t *testing.T) {
	backendServer := newControllableBackend(t, "healthy")
	defer backendServer.Close()
	pool, err := NewPool([]string{backendServer.Target()}, time.Second)
	if err != nil {
		t.Fatalf("NewPool returned error: %v", err)
	}
	pool.ConfigureHealthChecks(time.Second, 3, 2, "/")
	backend, _ := pool.registry.Get(backendServer.Target())
	if err := pool.registry.SetAlive(backendServer.Target(), false); err != nil {
		t.Fatalf("SetAlive: %v", err)
	}

	pool.CheckOnce()
	if backend.IsAlive() || backend.status().ConsecutiveSuccesses != 1 {
		t.Fatal("one successful check recovered a dead backend")
	}
	backendServer.SetHealthy(false)
	pool.CheckOnce()
	if backend.IsAlive() || backend.status().ConsecutiveSuccesses != 0 {
		t.Fatal("a failure between successes did not reset recovery progress")
	}
	backendServer.SetHealthy(true)
	pool.CheckOnce()
	if backend.IsAlive() || backend.status().ConsecutiveSuccesses != 1 {
		t.Fatal("backend recovered before two consecutive active successes")
	}
	pool.CheckOnce()
	if !backend.IsAlive() {
		t.Fatal("backend did not recover after two consecutive active successes")
	}
}

func TestHealthFailureKindsAndRedirect(t *testing.T) {
	tests := []struct {
		name string
		code int
	}{
		{"service unavailable", http.StatusServiceUnavailable},
		{"not found", http.StatusNotFound},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(test.code)
			}))
			t.Cleanup(server.Close)
			pool, err := NewPool([]string{server.URL}, time.Second)
			if err != nil {
				t.Fatalf("NewPool returned error: %v", err)
			}
			pool.CheckOnce()
			if got := pool.registry.List()[0].LastFailureKind; got != "bad_status" {
				t.Fatalf("LastFailureKind = %q; want bad_status", got)
			}
		})
	}

	t.Run("connection refused", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
		target := server.URL
		server.Close()
		pool, err := NewPool([]string{target}, time.Second)
		if err != nil {
			t.Fatalf("NewPool returned error: %v", err)
		}
		pool.CheckOnce()
		if got := pool.registry.List()[0].LastFailureKind; got != "connection_refused" {
			t.Fatalf("LastFailureKind = %q; want connection_refused", got)
		}
	})

	t.Run("timeout", func(t *testing.T) {
		release := make(chan struct{})
		server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
			select {
			case <-release:
			case <-request.Context().Done():
			}
		}))
		t.Cleanup(server.Close)
		t.Cleanup(func() { close(release) })
		pool, err := NewPool([]string{server.URL}, time.Second)
		if err != nil {
			t.Fatalf("NewPool returned error: %v", err)
		}
		pool.ConfigureHealthChecks(40*time.Millisecond, 3, 2, "/")
		pool.CheckOnce()
		if got := pool.registry.List()[0].LastFailureKind; got != "timeout" {
			t.Fatalf("LastFailureKind = %q; want timeout", got)
		}
	})

	t.Run("redirect is not followed", func(t *testing.T) {
		var targetHits atomic.Int32
		target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			targetHits.Add(1)
		}))
		t.Cleanup(target.Close)
		redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Redirect(w, httptest.NewRequest(http.MethodGet, "/", nil), target.URL, http.StatusFound)
		}))
		t.Cleanup(redirect.Close)
		pool, err := NewPool([]string{redirect.URL}, time.Second)
		if err != nil {
			t.Fatalf("NewPool returned error: %v", err)
		}
		pool.CheckOnce()
		status := pool.registry.List()[0]
		if !status.Alive || status.LastFailureKind != "" || targetHits.Load() != 0 {
			t.Fatalf("redirect status = %#v; target hits = %d", status, targetHits.Load())
		}
	})
}

func TestHealthTimeoutBound(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
		select {
		case <-release:
		case <-request.Context().Done():
		}
	}))
	t.Cleanup(server.Close)
	t.Cleanup(func() { close(release) })
	pool, err := NewPool([]string{server.URL}, time.Second)
	if err != nil {
		t.Fatalf("NewPool returned error: %v", err)
	}
	pool.ConfigureHealthChecks(200*time.Millisecond, 3, 2, "/")
	started := time.Now()
	pool.CheckOnce()
	if elapsed := time.Since(started); elapsed >= 2*time.Second {
		t.Fatalf("CheckOnce took %v; want less than 2s", elapsed)
	}
	if got := pool.registry.List()[0].LastFailureKind; got != "timeout" {
		t.Fatalf("LastFailureKind = %q; want timeout", got)
	}
}

func TestHealthChecksRunInParallel(t *testing.T) {
	entered := make(chan struct{}, 2)
	servers := make([]*httptest.Server, 3)
	for index := range servers {
		index := index
		servers[index] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
			if index < 2 {
				entered <- struct{}{}
				<-request.Context().Done()
				return
			}
			w.WriteHeader(http.StatusOK)
		}))
		t.Cleanup(servers[index].Close)
	}
	targets := []string{servers[0].URL, servers[1].URL, servers[2].URL}
	pool, err := NewPool(targets, time.Second)
	if err != nil {
		t.Fatalf("NewPool returned error: %v", err)
	}
	pool.ConfigureHealthChecks(300*time.Millisecond, 3, 2, "/")
	done := make(chan struct{})
	started := time.Now()
	go func() {
		pool.CheckOnce()
		close(done)
	}()
	waitRegistrySignal(t, entered, "first blocking backend")
	waitRegistrySignal(t, entered, "second blocking backend")
	waitRegistrySignal(t, done, "parallel health-check round")
	if elapsed := time.Since(started); elapsed >= 500*time.Millisecond {
		t.Fatalf("parallel health-check round took %v; want less than 500ms (one timeout budget)", elapsed)
	}
}

func TestHealthCheckDoesNotOverlap(t *testing.T) {
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseBackend := func() { releaseOnce.Do(func() { close(release) }) }
	var hits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		hits.Add(1)
		entered <- struct{}{}
		select {
		case <-release:
			w.WriteHeader(http.StatusOK)
		case <-request.Context().Done():
		}
	}))
	t.Cleanup(server.Close)
	t.Cleanup(releaseBackend)
	pool, err := NewPool([]string{server.URL}, time.Second)
	if err != nil {
		t.Fatalf("NewPool returned error: %v", err)
	}
	pool.ConfigureHealthChecks(3*time.Second, 3, 2, "/")
	firstDone := make(chan struct{})
	go func() {
		pool.CheckOnce()
		close(firstDone)
	}()
	waitRegistrySignal(t, entered, "first health request")
	pool.CheckOnce()
	if got := hits.Load(); got != 1 {
		t.Fatalf("overlapping check entered handler %d times; want 1", got)
	}
	releaseBackend()
	waitRegistrySignal(t, firstDone, "first health round")
	pool.CheckOnce()
	if got := hits.Load(); got != 2 {
		t.Fatalf("next round entered handler %d times; want 2", got)
	}
}

func TestHealthCheckLoopCancellationAbortsProbe(t *testing.T) {
	entered := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
		close(entered)
		<-request.Context().Done()
	}))
	t.Cleanup(server.Close)
	pool, err := NewPool([]string{server.URL}, time.Second)
	if err != nil {
		t.Fatalf("NewPool returned error: %v", err)
	}
	pool.ConfigureHealthChecks(10*time.Second, 3, 2, "/")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		pool.HealthCheckContext(ctx, time.Millisecond)
		close(done)
	}()
	waitRegistrySignal(t, entered, "health probe to start")
	started := time.Now()
	cancel()
	waitRegistrySignal(t, done, "health-check loop to stop")
	if elapsed := time.Since(started); elapsed >= time.Second {
		t.Fatalf("health-check cancellation took %v; want less than 1s", elapsed)
	}
	if status := pool.registry.List()[0]; status.ConsecutiveFailures != 0 || !status.Alive {
		t.Fatalf("canceled probe changed health state: %#v", status)
	}
}

func TestHealthPathIsUsed(t *testing.T) {
	paths := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		paths <- request.URL.Path
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)
	pool, err := NewPool([]string{server.URL}, time.Second)
	if err != nil {
		t.Fatalf("NewPool returned error: %v", err)
	}
	pool.ConfigureHealthChecks(time.Second, 3, 2, "/healthz")
	pool.CheckOnce()
	if got := waitRegistryResult(t, paths, "health path"); got != "/healthz" {
		t.Fatalf("health request path = %q; want /healthz", got)
	}
}

func TestPassiveHardFailureRequiresActiveRecovery(t *testing.T) {
	var healthy atomic.Bool
	healthy.Store(true)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if !healthy.Load() {
			http.Error(w, "unhealthy", http.StatusServiceUnavailable)
			return
		}
		_, _ = io.WriteString(w, "ok")
	}))
	t.Cleanup(server.Close)
	registry := NewRegistry(time.Second, time.Second)
	backend, err := registry.Add(server.URL)
	if err != nil {
		t.Fatalf("Add backend: %v", err)
	}
	pool := &Pool{registry: registry, healthTimeout: time.Second, failThreshold: 3, riseThreshold: 2, healthPath: "/"}
	originalDial := backend.transport.DialContext
	backend.transport.DialContext = func(context.Context, string, string) (net.Conn, error) {
		return nil, &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
	}
	first := httptest.NewRecorder()
	pool.Handler().ServeHTTP(first, httptest.NewRequest(http.MethodGet, "http://balancer.test/", nil))
	if first.Code != http.StatusBadGateway || backend.IsAlive() {
		t.Fatalf("hard failure response=%d alive=%v; want one 502 and dead backend", first.Code, backend.IsAlive())
	}
	backend.transport.DialContext = originalDial
	for range 4 {
		recorder := httptest.NewRecorder()
		pool.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "http://balancer.test/", nil))
		if recorder.Code != http.StatusServiceUnavailable {
			t.Fatalf("dead backend received production traffic: status %d; want 503", recorder.Code)
		}
	}
	healthy.Store(true)
	pool.CheckOnce()
	if backend.IsAlive() {
		t.Fatal("one active success recovered backend; want RiseThreshold successes")
	}
	pool.CheckOnce()
	if !backend.IsAlive() {
		t.Fatal("two active successes did not recover backend")
	}
}

func TestPassiveSoftFailureUsesThreshold(t *testing.T) {
	arrived := make(chan struct{}, 3)
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
		arrived <- struct{}{}
		<-request.Context().Done()
	}))
	t.Cleanup(server.Close)
	pool, err := NewPool([]string{server.URL}, 40*time.Millisecond)
	if err != nil {
		t.Fatalf("NewPool returned error: %v", err)
	}
	pool.ConfigureHealthChecks(time.Second, 3, 2, "/")
	backend, _ := pool.registry.Get(server.URL)
	for failure := 1; failure <= 3; failure++ {
		recorder := httptest.NewRecorder()
		pool.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "http://balancer.test/", nil))
		if recorder.Code != http.StatusBadGateway {
			t.Fatalf("soft failure %d returned %d; want 502", failure, recorder.Code)
		}
		waitRegistrySignal(t, arrived, "proxied request arrival")
		status := backend.status()
		if status.ConsecutiveFailures != uint64(failure) {
			t.Fatalf("soft failure count = %d; want %d", status.ConsecutiveFailures, failure)
		}
		if backend.IsAlive() != (failure < 3) {
			t.Fatalf("backend alive after soft failure %d = %v", failure, backend.IsAlive())
		}
	}
}

func TestFailureClassification(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
		hard bool
	}{
		{"timeout", context.DeadlineExceeded, "timeout", false},
		{"dial timeout", &net.OpError{Op: "dial", Err: context.DeadlineExceeded}, "timeout", false},
		{"connection refused", fmt.Errorf("wrapped: %w", syscall.ECONNREFUSED), "connection_refused", true},
		{"dns", &net.DNSError{Err: "not found", Name: "backend.invalid"}, "dns", false},
		{"other", errors.New("reset"), "other", false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := classifyFailure(test.err); got != test.want {
				t.Fatalf("classifyFailure = %q; want %q", got, test.want)
			}
			if got := isHardProxyFailure(test.err); got != test.hard {
				t.Fatalf("isHardProxyFailure = %v; want %v", got, test.hard)
			}
		})
	}
}
