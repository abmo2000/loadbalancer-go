package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type controllableBackend struct {
	server  *httptest.Server
	healthy atomic.Bool
	hits    atomic.Int32
}

func newControllableBackend(t *testing.T, response string) *controllableBackend {
	t.Helper()
	cb := &controllableBackend{}
	cb.healthy.Store(true)
	cb.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cb.hits.Add(1)
		if !cb.healthy.Load() {
			http.Error(w, "backend unavailable", http.StatusServiceUnavailable)
			return
		}
		_, _ = fmt.Fprint(w, response)
	}))
	return cb
}

func (cb *controllableBackend) SetHealthy(v bool) {
	cb.healthy.Store(v)
}

func (cb *controllableBackend) IsAlive() bool {
	return cb.healthy.Load()
}

func (cb *controllableBackend) Target() string {
	return cb.server.URL
}

func (cb *controllableBackend) Close() {
	cb.server.Close()
}

func newPoolFromBackends(t *testing.T, backends ...*controllableBackend) *Pool {
	t.Helper()
	targets := make([]string, 0, len(backends))
	for _, b := range backends {
		targets = append(targets, b.Target())
	}
	pool, err := NewPool(targets, 200*time.Millisecond)
	if err != nil {
		t.Fatalf("NewPool returned error: %v", err)
	}
	return pool
}

func doRequest(t *testing.T, pool *Pool) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "http://example.com", nil)
	pool.Handler().ServeHTTP(rec, req)
	return rec
}

func TestRoundRobin(t *testing.T) {
	// Round robin should cycle through the healthy backends in a stable repeating order.
	b1 := newControllableBackend(t, "backend-1")
	b2 := newControllableBackend(t, "backend-2")
	b3 := newControllableBackend(t, "backend-3")
	defer b1.Close()
	defer b2.Close()
	defer b3.Close()

	pool := newPoolFromBackends(t, b1, b2, b3)
	want := []string{"backend-2", "backend-3", "backend-1", "backend-2", "backend-3", "backend-1", "backend-2", "backend-3", "backend-1"}
	actual := make([]string, 0, len(want))
	for i := 0; i < len(want); i++ {
		rec := doRequest(t, pool)
		if rec.Code != http.StatusOK {
			t.Fatalf("request %d returned %d %s", i, rec.Code, strings.TrimSpace(rec.Body.String()))
		}
		actual = append(actual, strings.TrimSpace(rec.Body.String()))
	}
	if len(actual) != len(want) {
		t.Fatalf("unexpected length: got %d want %d", len(actual), len(want))
	}
	for i := range want {
		if actual[i] != want[i] {
			t.Fatalf("round robin mismatch at %d: got %q want %q", i, actual[i], want[i])
		}
	}

	counts := map[string]int{"backend-1": 0, "backend-2": 0, "backend-3": 0}
	for _, body := range actual {
		counts[body]++
	}
	for _, backendName := range []string{"backend-1", "backend-2", "backend-3"} {
		if counts[backendName] != 3 {
			t.Fatalf("backend %s saw %d requests, want 3", backendName, counts[backendName])
		}
	}
}

func TestFailover(t *testing.T) {
	// A backend that reports 503 should be marked dead; once it recovers, it should receive traffic again.
	b1 := newControllableBackend(t, "backend-1")
	b2 := newControllableBackend(t, "backend-2")
	b3 := newControllableBackend(t, "backend-3")
	defer b1.Close()
	defer b2.Close()
	defer b3.Close()

	pool := newPoolFromBackends(t, b1, b2, b3)
	b1.SetHealthy(false)
	pool.CheckOnce()
	if pool.backends[0].IsAlive() {
		t.Fatal("backend-1 should be marked dead after the health check")
	}

	baseHits := b1.hits.Load()
	for i := 0; i < 12; i++ {
		rec := doRequest(t, pool)
		if rec.Code == http.StatusBadGateway && strings.Contains(rec.Body.String(), "backend unavailable") {
			t.Fatalf("request %d hit the dead backend after failover", i)
		}
	}
	if got := b1.hits.Load() - baseHits; got != 0 {
		t.Fatalf("backend-1 received %d requests after failing health check; want 0", got)
	}

	b1.SetHealthy(true)
	pool.CheckOnce()
	seenRecovery := false
	for i := 0; i < 6; i++ {
		rec := doRequest(t, pool)
		if rec.Code == http.StatusOK && strings.TrimSpace(rec.Body.String()) == "backend-1" {
			seenRecovery = true
			break
		}
	}
	if !seenRecovery {
		t.Fatal("backend-1 never received traffic again after recovering")
	}
}

func TestFailover_PassiveDetection(t *testing.T) {
	// Closing one backend without a health check should cause exactly one failed request before it is skipped.
	b1 := newControllableBackend(t, "backend-1")
	b2 := newControllableBackend(t, "backend-2")
	b3 := newControllableBackend(t, "backend-3")
	defer b2.Close()
	defer b3.Close()

	pool := newPoolFromBackends(t, b1, b2, b3)
	b1.Close()

	failures := 0
	for i := 0; i < 6; i++ {
		rec := doRequest(t, pool)
		if rec.Code == http.StatusBadGateway {
			failures++
		}
	}
	if failures != 1 {
		t.Fatalf("got %d failed requests from the closed backend; want 1", failures)
	}
	if pool.backends[0].IsAlive() {
		t.Fatal("closed backend should be marked dead after the passive proxy error")
	}
}

func TestTimeout(t *testing.T) {
	// A backend that takes longer than the configured response timeout should trigger a 502.
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond)
		_, _ = fmt.Fprint(w, "slow-backend")
	}))
	defer backend.Close()

	pool, err := NewPool([]string{backend.URL}, 200*time.Millisecond)
	if err != nil {
		t.Fatalf("NewPool returned error: %v", err)
	}

	started := time.Now()
	rec := doRequest(t, pool)
	elapsed := time.Since(started)
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("unexpected status %d: %s", rec.Code, strings.TrimSpace(rec.Body.String()))
	}
	if elapsed > 2*time.Second {
		t.Fatalf("timeout took too long: %v", elapsed)
	}
}

func TestNoBackends(t *testing.T) {
	// An empty pool should return 503 with a clear no-healthy-backends message.
	pool, err := NewPool(nil, 200*time.Millisecond)
	if err != nil {
		t.Fatalf("NewPool returned error: %v", err)
	}

	rec := doRequest(t, pool)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("got status %d; want %d", rec.Code, http.StatusServiceUnavailable)
	}
	if !strings.Contains(rec.Body.String(), "no healthy backends") {
		t.Fatalf("unexpected body: %s", rec.Body.String())
	}
}

func TestAllBackendsDown(t *testing.T) {
	// When every backend is marked unhealthy, the pool should refuse requests with 503.
	b1 := newControllableBackend(t, "backend-1")
	b2 := newControllableBackend(t, "backend-2")
	b3 := newControllableBackend(t, "backend-3")
	defer b1.Close()
	defer b2.Close()
	defer b3.Close()

	pool := newPoolFromBackends(t, b1, b2, b3)
	for _, b := range []*controllableBackend{b1, b2, b3} {
		b.SetHealthy(false)
	}
	pool.CheckOnce()

	rec := doRequest(t, pool)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("got status %d; want %d", rec.Code, http.StatusServiceUnavailable)
	}
	if !strings.Contains(rec.Body.String(), "no healthy backends") {
		t.Fatalf("unexpected body: %s", rec.Body.String())
	}
}

func TestConcurrentRequests(t *testing.T) {
	// Many concurrent requests should all succeed when the pool still has healthy backends.
	b1 := newControllableBackend(t, "backend-1")
	b2 := newControllableBackend(t, "backend-2")
	b3 := newControllableBackend(t, "backend-3")
	defer b1.Close()
	defer b2.Close()
	defer b3.Close()

	pool := newPoolFromBackends(t, b1, b2, b3)
	var wg sync.WaitGroup
	var failures atomic.Int32
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rec := doRequest(t, pool)
			if rec.Code != http.StatusOK {
				failures.Add(1)
			}
		}()
	}
	wg.Wait()
	if got := failures.Load(); got != 0 {
		t.Fatalf("%d concurrent requests failed; want 0", got)
	}
}
