package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
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

func newLeastConnsPool(t *testing.T, targets ...string) *Pool {
	t.Helper()
	pool, err := NewPool(targets)
	if err != nil {
		t.Fatalf("NewPool returned error: %v", err)
	}
	if err := pool.SetStrategy("least_conns"); err != nil {
		t.Fatalf("SetStrategy returned error: %v", err)
	}
	return pool
}

func waitForServerReady(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("server at %s did not become ready for TCP connections", addr)
}

type blockingTestBackend struct {
	server      *httptest.Server
	arrived     chan struct{}
	release     chan struct{}
	arrivalOnce sync.Once
	releaseOnce sync.Once
}

func newBlockingTestBackend(t *testing.T, body string) *blockingTestBackend {
	t.Helper()
	backend := &blockingTestBackend{
		arrived: make(chan struct{}),
		release: make(chan struct{}),
	}
	backend.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backend.arrivalOnce.Do(func() { close(backend.arrived) })
		select {
		case <-backend.release:
			_, _ = fmt.Fprint(w, body)
		case <-r.Context().Done():
		}
	}))
	// Cleanups run in reverse order: release blocked handlers before Server.Close waits on them.
	t.Cleanup(backend.server.Close)
	t.Cleanup(backend.Release)
	return backend
}

func (backend *blockingTestBackend) Release() {
	backend.releaseOnce.Do(func() { close(backend.release) })
}

func waitForSignal(t *testing.T, signal <-chan struct{}, description string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	select {
	case <-signal:
	case <-ctx.Done():
		t.Fatalf("timed out waiting for %s", description)
	}
}

func waitForResult[T any](t *testing.T, result <-chan T, description string, timeout time.Duration) T {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	select {
	case value := <-result:
		return value
	case <-ctx.Done():
		t.Fatalf("timed out waiting for %s: %v", description, ctx.Err())
		var zero T
		return zero
	}
}

func waitForStoppedListener(t *testing.T, addr string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err != nil {
			return
		}
		_ = conn.Close()
		select {
		case <-ctx.Done():
			t.Fatalf("listener at %s still accepts connections after shutdown: %v", addr, ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
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

func TestRoundRobinMaintainsRotationWhenBackendUnavailable(t *testing.T) {
	first := newControllableBackend(t, "first")
	second := newControllableBackend(t, "second")
	third := newControllableBackend(t, "third")
	defer first.Close()
	defer second.Close()
	defer third.Close()

	pool := newPoolFromBackends(t, first, second, third)
	if err := pool.registry.SetAlive(second.Target(), false); err != nil {
		t.Fatalf("SetAlive second backend: %v", err)
	}
	want := []string{third.Target(), third.Target(), first.Target(), third.Target(), third.Target(), first.Target()}
	for index, expected := range want {
		if got := pool.Next().URL.String(); got != expected {
			t.Fatalf("selection %d = %s; want legacy round-robin backend %s", index, got, expected)
		}
	}
}

func TestLeastConnsPicksLowestActiveCount(t *testing.T) {
	first := newControllableBackend(t, "first")
	second := newControllableBackend(t, "second")
	third := newControllableBackend(t, "third")
	defer first.Close()
	defer second.Close()
	defer third.Close()

	pool := newLeastConnsPool(t, first.Target(), second.Target(), third.Target())
	for target, active := range map[string]int64{first.Target(): 4, second.Target(): 1, third.Target(): 3} {
		backend, ok := pool.registry.Get(target)
		if !ok {
			t.Fatalf("backend %s missing from registry", target)
		}
		backend.activeConns.Store(active)
	}
	if got := pool.Next().URL.String(); got != second.Target() {
		t.Fatalf("Next selected %s; want least-busy backend %s", got, second.Target())
	}
}

func TestLeastConnsTieBreakRotates(t *testing.T) {
	backends := []*controllableBackend{
		newControllableBackend(t, "one"),
		newControllableBackend(t, "two"),
		newControllableBackend(t, "three"),
	}
	for _, backend := range backends {
		defer backend.Close()
	}
	pool := newLeastConnsPool(t, backends[0].Target(), backends[1].Target(), backends[2].Target())
	counts := make(map[string]int)
	for range 12 {
		counts[pool.Next().URL.String()]++
	}
	for _, backend := range backends {
		if counts[backend.Target()] != 4 {
			t.Fatalf("backend %s selected %d times; want each tied backend to rotate", backend.Target(), counts[backend.Target()])
		}
	}
}

func TestLeastConnsSkipsDeadAndQuarantinedBackends(t *testing.T) {
	dead := newControllableBackend(t, "dead")
	quarantined := newControllableBackend(t, "quarantined")
	healthy := newControllableBackend(t, "healthy")
	defer dead.Close()
	defer quarantined.Close()
	defer healthy.Close()

	pool := newLeastConnsPool(t, dead.Target(), quarantined.Target(), healthy.Target())
	if err := pool.registry.SetAlive(dead.Target(), false); err != nil {
		t.Fatalf("SetAlive dead backend: %v", err)
	}
	quarantinedBackend, ok := pool.registry.Get(quarantined.Target())
	if !ok {
		t.Fatal("quarantined backend missing from registry")
	}
	quarantinedBackend.mu.Lock()
	quarantinedBackend.alive = false
	quarantinedBackend.quarantined = true
	quarantinedBackend.mu.Unlock()
	if got := pool.Next().URL.String(); got != healthy.Target() {
		t.Fatalf("Next selected %s; want only selectable backend %s", got, healthy.Target())
	}
}

func TestLeastConnsRoutesAroundBusyBackend(t *testing.T) {
	slow := newBlockingTestBackend(t, "slow")
	var quickHits atomic.Int32
	quick := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		quickHits.Add(1)
		_, _ = fmt.Fprint(w, "quick")
	}))
	t.Cleanup(quick.Close)
	pool := newLeastConnsPool(t, slow.server.URL, quick.URL)
	handler := pool.Handler()
	slowResult := make(chan int, 1)
	go func() {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "http://balancer.test/slow", nil))
		slowResult <- recorder.Code
	}()
	waitForSignal(t, slow.arrived, "slow backend request")

	slowBackend, _ := pool.registry.Get(slow.server.URL)
	if got := slowBackend.activeConns.Load(); got != 1 {
		t.Fatalf("slow backend active connections = %d while held; want 1", got)
	}
	for range 6 {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "http://balancer.test/", nil))
		if recorder.Code != http.StatusOK || strings.TrimSpace(recorder.Body.String()) != "quick" {
			t.Fatalf("request while slow backend was busy returned %d %q; want quick backend", recorder.Code, recorder.Body.String())
		}
	}
	if got := quickHits.Load(); got != 6 {
		t.Fatalf("quick backend handled %d requests while slow backend was held; want 6", got)
	}

	slow.Release()
	if code := waitForResult(t, slowResult, "slow backend response", 5*time.Second); code != http.StatusOK {
		t.Fatalf("slow request returned %d; want 200", code)
	}
	quickBackend, _ := pool.registry.Get(quick.URL)
	if slowBackend.activeConns.Load() != 0 || quickBackend.activeConns.Load() != 0 {
		t.Fatalf("active connections after completion: slow=%d quick=%d; want both zero", slowBackend.activeConns.Load(), quickBackend.activeConns.Load())
	}

	seen := make(map[string]bool)
	for range 2 {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "http://balancer.test/", nil))
		seen[strings.TrimSpace(recorder.Body.String())] = true
	}
	if !seen["slow"] || !seen["quick"] {
		t.Fatalf("after the slow request finished, tied traffic reached %v; want both backends", seen)
	}
}

func TestLeastConnsReservationsReleaseOnFailureAndCancellation(t *testing.T) {
	refused := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	refusedURL := refused.URL
	refused.Close()
	failurePool := newLeastConnsPool(t, refusedURL)
	failureResponse := doRequest(t, failurePool)
	failureStatus := failurePool.registry.List()[0]
	if failureResponse.Code != http.StatusBadGateway || failureStatus.TotalRequests != 1 || failureStatus.TotalFailures != 1 || failureStatus.ActiveConns != 0 {
		t.Fatalf("failed request status=%d counters=%#v; want 502, one request/failure, and no active reservation", failureResponse.Code, failureStatus)
	}

	blocked := newBlockingTestBackend(t, "done")
	cancelPool := newLeastConnsPool(t, blocked.server.URL)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		request := httptest.NewRequest(http.MethodGet, "http://balancer.test/", nil).WithContext(ctx)
		cancelPool.Handler().ServeHTTP(httptest.NewRecorder(), request)
		close(done)
	}()
	waitForSignal(t, blocked.arrived, "cancellable backend request")
	backend, _ := cancelPool.registry.Get(blocked.server.URL)
	if got := backend.activeConns.Load(); got != 1 {
		t.Fatalf("active connections before cancellation = %d; want exactly one reservation", got)
	}
	cancel()
	waitForSignal(t, done, "cancelled request completion")
	if got := backend.activeConns.Load(); got != 0 {
		t.Fatalf("active connections after cancellation = %d; want zero", got)
	}
}

func TestLeastConnsConcurrentRequestsSpreadAcrossBackends(t *testing.T) {
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseAll := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseAll()
	arrivals := make(chan struct{}, 100)
	var hits [3]atomic.Int32
	servers := make([]*httptest.Server, 3)
	targets := make([]string, 3)
	for index := range servers {
		index := index
		servers[index] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			hits[index].Add(1)
			arrivals <- struct{}{}
			<-release
			_, _ = fmt.Fprint(w, "ok")
		}))
		targets[index] = servers[index].URL
		t.Cleanup(servers[index].Close)
	}
	pool := newLeastConnsPool(t, targets...)
	handler := pool.Handler()
	var requests sync.WaitGroup
	responses := make(chan int, 100)
	for range 100 {
		requests.Add(1)
		go func() {
			defer requests.Done()
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "http://balancer.test/", nil))
			responses <- recorder.Code
		}()
	}
	arrivalCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for range 100 {
		select {
		case <-arrivals:
		case <-arrivalCtx.Done():
			t.Fatalf("only %d requests reached backends: %v", 100-len(arrivals), arrivalCtx.Err())
		}
	}
	maximum := int32(0)
	for index := range hits {
		count := hits[index].Load()
		if count == 0 {
			t.Fatalf("backend %d received no concurrent requests", index+1)
		}
		if count > maximum {
			maximum = count
		}
	}
	if maximum > 60 {
		t.Fatalf("least-connections spread was too uneven: backend request counts %d, %d, %d", hits[0].Load(), hits[1].Load(), hits[2].Load())
	}
	releaseAll()
	requests.Wait()
	close(responses)
	for code := range responses {
		if code != http.StatusOK {
			t.Fatalf("concurrent request status = %d; want 200", code)
		}
	}
}

func TestLeastConnsRegistryRaceDuringTraffic(t *testing.T) {
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseAll := func() { releaseOnce.Do(func() { close(release) }) }
	arrived := make(chan struct{}, 100)
	var hits atomic.Int32
	servers := make([]*httptest.Server, 3)
	targets := make([]string, 3)
	for index := range servers {
		servers[index] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			hits.Add(1)
			arrived <- struct{}{}
			<-release
			_, _ = fmt.Fprint(w, "ok")
		}))
		targets[index] = servers[index].URL
		t.Cleanup(servers[index].Close)
	}
	t.Cleanup(releaseAll)
	pool := newLeastConnsPool(t, targets...)
	handler := pool.Handler()
	var requests sync.WaitGroup
	statuses := make(chan int, 100)
	for range 100 {
		requests.Add(1)
		go func() {
			defer requests.Done()
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "http://balancer.test/", nil))
			statuses <- recorder.Code
		}()
	}
	select {
	case <-arrived:
	case <-time.After(10 * time.Second):
		t.Fatal("no request reached a backend")
	}

	mutationServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, "mutation")
	}))
	t.Cleanup(mutationServer.Close)
	for iteration := range 20 {
		if err := pool.registry.SetAlive(targets[1], iteration%2 == 0); err != nil {
			t.Fatalf("SetAlive backend 2: %v", err)
		}
		if err := pool.registry.SetAlive(targets[2], iteration%2 != 0); err != nil {
			t.Fatalf("SetAlive backend 3: %v", err)
		}
		if _, err := pool.registry.Add(mutationServer.URL); err != nil {
			t.Fatalf("Add mutation backend: %v", err)
		}
		_ = pool.registry.List()
		if err := pool.registry.Remove(mutationServer.URL); err != nil {
			t.Fatalf("Remove mutation backend: %v", err)
		}
	}
	releaseAll()
	requests.Wait()
	close(statuses)
	for status := range statuses {
		if status != http.StatusOK && status != http.StatusBadGateway && status != http.StatusServiceUnavailable {
			t.Fatalf("request status = %d; want only 200, 502, or 503", status)
		}
	}
	if hits.Load() == 0 {
		t.Fatal("no traffic reached a backend")
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
	for range 3 {
		pool.CheckOnce()
	}
	backend1, ok := pool.registry.Get(b1.Target())
	if !ok {
		t.Fatal("backend-1 is missing from the registry")
	}
	if backend1.IsAlive() {
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
	for range 2 {
		pool.CheckOnce()
	}
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
	backend1, ok := pool.registry.Get(b1.Target())
	if !ok {
		t.Fatal("backend-1 is missing from the registry")
	}
	if backend1.IsAlive() {
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
	for range 3 {
		pool.CheckOnce()
	}

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

func TestGracefulShutdown_FinishesInFlightRequest(t *testing.T) {
	backend := newBlockingTestBackend(t, "done")
	pool, err := NewPool([]string{backend.server.URL}, 200*time.Millisecond)
	if err != nil {
		t.Fatalf("NewPool returned error: %v", err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen returned error: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	srv := &http.Server{Addr: listener.Addr().String(), Handler: pool.Handler()}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	serverErrCh := make(chan error, 1)
	go func() {
		serverErrCh <- runServerWithListener(ctx, srv, time.Second, listener)
	}()

	addr := listener.Addr().String()
	url := "http://" + addr
	waitForServerReady(t, addr)

	type response struct {
		status int
		body   string
		err    error
	}
	requestDone := make(chan response, 1)
	go func() {
		resp, err := http.Get(url)
		if err != nil {
			requestDone <- response{err: err}
			return
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		requestDone <- response{status: resp.StatusCode, body: string(body)}
	}()

	waitForSignal(t, backend.arrived, "the proxied request to arrive at the backend")
	cancel()
	waitForStoppedListener(t, addr)
	backend.Release()
	got := waitForResult(t, requestDone, "the in-flight request to finish", 5*time.Second)
	if got.err != nil {
		t.Fatalf("in-flight request failed: %v", got.err)
	}
	if got.status != http.StatusOK || got.body != "done" {
		t.Fatalf("in-flight response = %d %q; want 200 %q", got.status, got.body, "done")
	}
	if err := waitForResult(t, serverErrCh, "runServer to return after graceful shutdown", 5*time.Second); err != nil {
		t.Fatalf("runServer returned error during graceful shutdown: %v", err)
	}
}

func TestGracefulShutdown_RejectsNewRequests(t *testing.T) {
	// After shutdown begins, new requests must fail while the earlier request continues to completion.
	backend := newBlockingTestBackend(t, "slow-response")
	pool, err := NewPool([]string{backend.server.URL}, 200*time.Millisecond)
	if err != nil {
		t.Fatalf("NewPool returned error: %v", err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen returned error: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	srv := &http.Server{Addr: listener.Addr().String(), Handler: pool.Handler()}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	serverErrCh := make(chan error, 1)
	go func() {
		serverErrCh <- runServerWithListener(ctx, srv, time.Second, listener)
	}()

	addr := listener.Addr().String()
	url := "http://" + addr
	waitForServerReady(t, addr)

	requestDone := make(chan error, 1)
	go func() {
		resp, err := http.Get(url)
		if err != nil {
			requestDone <- err
			return
		}
		defer resp.Body.Close()
		_, _ = io.ReadAll(resp.Body)
		requestDone <- nil
	}()

	waitForSignal(t, backend.arrived, "the proxied request to arrive at the backend")
	cancel()
	waitForStoppedListener(t, addr)
	backend.Release()
	if err := waitForResult(t, requestDone, "the initial request to finish", 5*time.Second); err != nil {
		t.Fatalf("initial request failed before shutdown completed: %v", err)
	}
	if err := waitForResult(t, serverErrCh, "runServer to return after graceful shutdown", 5*time.Second); err != nil {
		t.Fatalf("runServer returned unexpected error: %v", err)
	}
}

func TestGracefulShutdown_TimeoutForcesClose(t *testing.T) {
	// A blocked backend must not keep the shutdown from returning after the timeout budget is exhausted.
	backend := newBlockingTestBackend(t, "slow-response")
	pool, err := NewPool([]string{backend.server.URL}, 200*time.Millisecond)
	if err != nil {
		t.Fatalf("NewPool returned error: %v", err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen returned error: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	srv := &http.Server{Addr: listener.Addr().String(), Handler: pool.Handler()}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	serverErrCh := make(chan error, 1)
	go func() {
		serverErrCh <- runServerWithListener(ctx, srv, 50*time.Millisecond, listener)
	}()

	addr := listener.Addr().String()
	url := "http://" + addr
	waitForServerReady(t, addr)

	requestDone := make(chan error, 1)
	go func() {
		resp, err := http.Get(url)
		if err != nil {
			requestDone <- err
			return
		}
		defer resp.Body.Close()
		_, _ = io.ReadAll(resp.Body)
		requestDone <- nil
	}()

	waitForSignal(t, backend.arrived, "the proxied request to arrive at the backend")
	cancel()
	err = waitForResult(t, serverErrCh, "runServer to return after the shutdown timeout", 2*time.Second)
	if err == nil {
		t.Fatal("runServer returned nil after timeout; expected shutdown error")
	}
	backend.Release()
	if err := waitForResult(t, requestDone, "the request to finish after forced shutdown", 5*time.Second); err != nil && !strings.Contains(err.Error(), "EOF") {
		t.Fatalf("request ended with unexpected error: %v", err)
	}
}

func TestHealthCheckStopsOnContextCancel(t *testing.T) {
	// Health checks must exit quickly when the context is canceled.
	pool, err := NewPool([]string{"http://127.0.0.1:1"}, 200*time.Millisecond)
	if err != nil {
		t.Fatalf("NewPool returned error: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		pool.HealthCheckContext(ctx, 10*time.Millisecond)
		close(done)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("HealthCheckContext did not exit after the context was canceled")
	}
}

func TestGracefulShutdown_NoRequests(t *testing.T) {
	// With no active traffic, shutdown should return quickly with no errors.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen returned error: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	srv := &http.Server{Addr: listener.Addr().String(), Handler: http.NewServeMux()}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	serverErrCh := make(chan error, 1)
	go func() {
		serverErrCh <- runServerWithListener(ctx, srv, time.Second, listener)
	}()
	waitForServerReady(t, listener.Addr().String())
	cancel()
	if err := waitForResult(t, serverErrCh, "idle server shutdown", 5*time.Second); err != nil {
		t.Fatalf("unexpected error on empty shutdown: %v", err)
	}
}

func TestRace_StateUpdatesDuringRequests(t *testing.T) {
	// Health checks, backend toggles, and request handling must coexist without races or invalid status codes.
	b1 := newControllableBackend(t, "backend-1")
	b2 := newControllableBackend(t, "backend-2")
	b3 := newControllableBackend(t, "backend-3")
	defer b1.Close()
	defer b2.Close()
	defer b3.Close()

	pool := newPoolFromBackends(t, b1, b2, b3)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	var badStatus atomic.Int32
	var wg sync.WaitGroup

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				rec := doRequest(t, pool)
				if rec.Code != http.StatusOK && rec.Code != http.StatusBadGateway && rec.Code != http.StatusServiceUnavailable {
					badStatus.Add(1)
				}
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			pool.CheckOnce()
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		healthy := true
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			b1.SetHealthy(healthy)
			healthy = !healthy
		}
	}()

	wg.Wait()
	if got := badStatus.Load(); got != 0 {
		t.Fatalf("saw %d unexpected status codes during concurrent state updates", got)
	}
}

func TestRace_SetAliveIsAlive(t *testing.T) {
	// SetAlive and IsAlive must be safe when called concurrently from many goroutines.
	u, err := url.Parse("http://example.com")
	if err != nil {
		t.Fatalf("parse URL: %v", err)
	}
	b := NewBackend(u, 200*time.Millisecond)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				if i%2 == 0 {
					b.SetAlive(true)
				} else {
					b.SetAlive(false)
				}
				_ = b.IsAlive()
			}
		}(i)
	}
	wg.Wait()
}

func TestNext_ConcurrentDistribution(t *testing.T) {
	// Next() must distribute requests evenly across healthy backends even under concurrent calls.
	b1 := newControllableBackend(t, "backend-1")
	b2 := newControllableBackend(t, "backend-2")
	b3 := newControllableBackend(t, "backend-3")
	defer b1.Close()
	defer b2.Close()
	defer b3.Close()

	pool := newPoolFromBackends(t, b1, b2, b3)
	var counts [3]atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 300; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b := pool.Next()
			if b == nil {
				return
			}
			switch b.URL.String() {
			case b1.Target():
				counts[0].Add(1)
			case b2.Target():
				counts[1].Add(1)
			case b3.Target():
				counts[2].Add(1)
			}
		}()
	}
	wg.Wait()
	for i := 0; i < len(counts); i++ {
		if got := counts[i].Load(); got != 100 {
			t.Fatalf("backend %d selected %d times; want 100", i+1, got)
		}
	}
}

func TestNext_NeverReturnsDeadBackend(t *testing.T) {
	// A permanently dead backend should never be selected by Next(), even while other backends flip state.
	b1 := newControllableBackend(t, "backend-1")
	b2 := newControllableBackend(t, "backend-2")
	b3 := newControllableBackend(t, "backend-3")
	defer b1.Close()
	defer b2.Close()
	defer b3.Close()

	pool := newPoolFromBackends(t, b1, b2, b3)
	backend1, ok := pool.registry.Get(b1.Target())
	if !ok {
		t.Fatal("backend-1 is missing from the registry")
	}
	if err := pool.registry.SetAlive(b1.Target(), false); err != nil {
		t.Fatalf("SetAlive backend-1: %v", err)
	}
	var deadReturned atomic.Int32
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			if i%2 == 0 {
				_ = pool.registry.SetAlive(b2.Target(), true)
			} else {
				_ = pool.registry.SetAlive(b2.Target(), false)
			}
		}
	}()

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 500; j++ {
				b := pool.Next()
				if b == backend1 {
					deadReturned.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	if got := deadReturned.Load(); got != 0 {
		t.Fatalf("Next returned the permanently dead backend %d times; want 0", got)
	}
}
