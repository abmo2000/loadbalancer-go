package main

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
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
	pool.CheckOnce()
	statuses := registry.List()
	if len(statuses) != 1 || statuses[0].Alive || statuses[0].ConsecutiveFailures != 1 || statuses[0].LastCheck.IsZero() || statuses[0].LastError == "" {
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
	backend.recordProxyFailure(errors.New("synthetic backend failure"))
	pool.CheckOnce()
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
