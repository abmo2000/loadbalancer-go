package main

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrDuplicateBackend = errors.New("duplicate backend")
	ErrInvalidBackend   = errors.New("invalid backend")
	ErrBackendNotFound  = errors.New("backend not found")
)

type BackendStatus struct {
	URL                  string    `json:"url"`
	Alive                bool      `json:"alive"`
	ConsecutiveFailures  uint64    `json:"consecutiveFailures"`
	ConsecutiveSuccesses uint64    `json:"consecutiveSuccesses"`
	TotalRequests        uint64    `json:"totalRequests"`
	TotalFailures        uint64    `json:"totalFailures"`
	ActiveConns          int64     `json:"activeConns"`
	LastCheck            time.Time `json:"lastCheck"`
	LastError            string    `json:"lastError"`
	LastFailureKind      string    `json:"lastFailureKind"`
	LastStateChange      time.Time `json:"lastStateChange"`
	AddedAt              time.Time `json:"addedAt"`
	State                string    `json:"state"`
	QuarantinedUntil     time.Time `json:"quarantinedUntil"`
	QuarantineLevel      int       `json:"quarantineLevel"`
	RecentTransitions    int       `json:"recentTransitions"`
	HealthPath           string    `json:"healthPath"`
	HealthInterval       string    `json:"healthInterval"`
	HealthTimeout        string    `json:"healthTimeout"`
	NextCheckAt          time.Time `json:"nextCheckAt"`
}

type Registry struct {
	mu                   sync.RWMutex
	byURL                map[string]*Backend
	backends             atomic.Pointer[[]*Backend]
	responseTimeout      time.Duration
	dialTimeout          time.Duration
	failThreshold        int
	riseThreshold        int
	flapWindow           time.Duration
	flapMaxTransitions   int
	quarantineBase       time.Duration
	quarantineMax        time.Duration
	quarantineResetAfter time.Duration
	maxQuarantinePercent int
	now                  func() time.Time
	healthDefaults       BackendHealth
	healthOverrides      map[string]BackendHealth
	healthTransport      *http.Transport
	healthClient         *http.Client
}

func NewRegistry(responseTimeout, dialTimeout time.Duration) *Registry {
	healthTransport := &http.Transport{
		DialContext:         (&net.Dialer{Timeout: dialTimeout}).DialContext,
		IdleConnTimeout:     10 * time.Second,
		DisableKeepAlives:   false,
		MaxIdleConnsPerHost: 1,
	}
	registry := &Registry{
		byURL:                make(map[string]*Backend),
		responseTimeout:      responseTimeout,
		dialTimeout:          dialTimeout,
		failThreshold:        3,
		riseThreshold:        2,
		flapWindow:           time.Minute,
		flapMaxTransitions:   4,
		quarantineBase:       30 * time.Second,
		quarantineMax:        5 * time.Minute,
		quarantineResetAfter: 5 * time.Minute,
		maxQuarantinePercent: 50,
		now:                  time.Now,
		healthDefaults:       BackendHealth{Path: "/", Interval: 5 * time.Second, Timeout: 2 * time.Second},
		healthOverrides:      make(map[string]BackendHealth),
		healthTransport:      healthTransport,
		healthClient: &http.Client{
			Transport: healthTransport,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}
	empty := make([]*Backend, 0)
	registry.backends.Store(&empty)
	return registry
}

func (r *Registry) Add(rawURL string) (*Backend, error) {
	return r.AddWithOptions(rawURL, BackendHealth{})
}

func (r *Registry) AddWithOptions(rawURL string, health BackendHealth) (*Backend, error) {
	normalized, parsed, err := normalizeBackendURL(rawURL)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidBackend, err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.byURL[normalized]; exists {
		return nil, fmt.Errorf("%w: %s", ErrDuplicateBackend, normalized)
	}
	effective := resolveBackendHealth(r.healthDefaults, health)
	backend := NewBackend(parsed, r.dialTimeout, r.responseTimeout)
	backend.healthTransport = r.healthTransport
	backend.healthClient = r.healthClient
	backend.registry = r
	backend.now = r.now
	backendTime := r.now()
	backend.addedAt = backendTime
	backend.lastStateChange = backendTime
	backend.lastAliveChange = backendTime
	backend.healthPath = effective.Path
	backend.healthInterval = effective.Interval
	backend.healthTimeout = effective.Timeout
	backend.nextCheckAt = backendTime
	r.healthOverrides[normalized] = health
	backend.configureFlap(r.flapWindow, r.flapMaxTransitions, r.quarantineBase, r.quarantineMax, r.quarantineResetAfter)
	backend.configureHealth(r.failThreshold, r.riseThreshold)
	backends := append(r.snapshot(), backend)
	r.byURL[normalized] = backend
	r.store(backends)
	return backend, nil
}

func (r *Registry) Remove(rawURL string) error {
	normalized, _, err := normalizeBackendURL(rawURL)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidBackend, err)
	}
	r.mu.Lock()
	backend, exists := r.byURL[normalized]
	if !exists {
		r.mu.Unlock()
		return fmt.Errorf("%w: %s", ErrBackendNotFound, normalized)
	}
	current := r.snapshot()
	updated := make([]*Backend, 0, len(current)-1)
	for _, candidate := range current {
		if candidate != backend {
			updated = append(updated, candidate)
		}
	}
	delete(r.byURL, normalized)
	delete(r.healthOverrides, normalized)
	r.store(updated)
	r.mu.Unlock()

	backend.transport.CloseIdleConnections()
	backend.healthTransport.CloseIdleConnections()
	return nil
}

func resolveBackendHealth(defaults, override BackendHealth) BackendHealth {
	if override.Path == "" {
		override.Path = defaults.Path
	}
	if override.Interval == 0 {
		override.Interval = defaults.Interval
	}
	if override.Timeout == 0 {
		override.Timeout = defaults.Timeout
	}
	return override
}

func (r *Registry) ConfigureHealthDefaults(defaults BackendHealth) {
	r.mu.Lock()
	r.healthDefaults = defaults
	now := r.now()
	for normalized, backend := range r.byURL {
		health := resolveBackendHealth(defaults, r.healthOverrides[normalized])
		backend.mu.Lock()
		backend.healthPath = health.Path
		backend.healthInterval = health.Interval
		backend.healthTimeout = health.Timeout
		backend.nextCheckAt = now
		backend.mu.Unlock()
	}
	r.mu.Unlock()
}

func (r *Registry) healthDefaultsSnapshot() BackendHealth {
	r.mu.RLock()
	defaults := r.healthDefaults
	r.mu.RUnlock()
	return defaults
}

func (r *Registry) reserveChecks(dueOnly bool) []backendCheck {
	r.mu.RLock()
	defer r.mu.RUnlock()
	backends := r.snapshot()
	checks := make([]backendCheck, 0, len(backends))
	for _, backend := range backends {
		backend.mu.Lock()
		started := r.now()
		if dueOnly && backend.nextCheckAt.After(started) {
			backend.mu.Unlock()
			continue
		}
		if !backend.checking.CompareAndSwap(false, true) {
			backend.nextCheckAt = started.Add(backend.healthInterval)
			shouldLog := backend.skipLogged.CompareAndSwap(false, true)
			backend.mu.Unlock()
			if shouldLog {
				log.Printf("health check for %s still running, skipping tick", backend.URL)
			}
			continue
		}
		backend.nextCheckAt = started.Add(backend.healthInterval)
		backend.skipLogged.Store(false)
		checks = append(checks, backendCheck{
			backend: backend,
			started: started,
			path:    backend.healthPath,
			timeout: backend.healthTimeout,
		})
		backend.mu.Unlock()
	}
	return checks
}

func (r *Registry) durationUntilNextCheck() time.Duration {
	r.mu.RLock()
	backends := r.snapshot()
	if len(backends) == 0 {
		r.mu.RUnlock()
		return time.Second
	}
	earliest := time.Time{}
	for _, backend := range backends {
		backend.mu.RLock()
		next := backend.nextCheckAt
		backend.mu.RUnlock()
		if earliest.IsZero() || next.Before(earliest) {
			earliest = next
		}
	}
	now := r.now()
	r.mu.RUnlock()
	wait := earliest.Sub(now)
	if wait <= 0 {
		return time.Millisecond
	}
	return wait
}

func (r *Registry) SetHealthOverride(rawURL string, health BackendHealth) error {
	normalized, _, err := normalizeBackendURL(rawURL)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidBackend, err)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	backend, exists := r.byURL[normalized]
	if !exists {
		return fmt.Errorf("%w: %s", ErrBackendNotFound, normalized)
	}
	r.healthOverrides[normalized] = health
	effective := resolveBackendHealth(r.healthDefaults, health)
	backend.mu.Lock()
	backend.healthPath = effective.Path
	backend.healthInterval = effective.Interval
	backend.healthTimeout = effective.Timeout
	backend.nextCheckAt = r.now()
	backend.mu.Unlock()
	return nil
}

func (r *Registry) Get(rawURL string) (*Backend, bool) {
	normalized, _, err := normalizeBackendURL(rawURL)
	if err != nil {
		return nil, false
	}
	r.mu.RLock()
	backend, exists := r.byURL[normalized]
	r.mu.RUnlock()
	return backend, exists
}

func (r *Registry) List() []BackendStatus {
	backends := r.snapshot()
	statuses := make([]BackendStatus, len(backends))
	for i, backend := range backends {
		statuses[i] = backend.status()
	}
	return statuses
}

func (r *Registry) Healthy() []*Backend {
	backends := r.snapshot()
	healthy := make([]*Backend, 0, len(backends))
	for _, backend := range backends {
		if backend.IsAlive() {
			healthy = append(healthy, backend)
		}
	}
	return healthy
}

func (r *Registry) Len() int {
	return len(r.snapshot())
}

func (r *Registry) configureHealth(failThreshold, riseThreshold int) {
	r.mu.Lock()
	r.failThreshold = failThreshold
	r.riseThreshold = riseThreshold
	for _, backend := range r.snapshot() {
		backend.configureHealth(failThreshold, riseThreshold)
	}
	r.mu.Unlock()
}

func (r *Registry) ConfigureFlapProtection(window time.Duration, maxTransitions int, base, maximum, resetAfter time.Duration, maxPercent int) {
	r.mu.Lock()
	r.flapWindow = window
	r.flapMaxTransitions = maxTransitions
	r.quarantineBase = base
	r.quarantineMax = maximum
	r.quarantineResetAfter = resetAfter
	r.maxQuarantinePercent = maxPercent
	for _, backend := range r.snapshot() {
		backend.mu.Lock()
		backend.flapWindow = window
		backend.flapMaxTransitions = maxTransitions
		backend.quarantineBase = base
		backend.quarantineMax = maximum
		backend.quarantineResetAfter = resetAfter
		backend.mu.Unlock()
	}
	r.mu.Unlock()
}

func (r *Registry) recordProxyFailure(backend *Backend, err error) []healthLogEvent {
	kind := classifyFailure(err)
	hardFailure := isHardProxyFailure(err)
	r.mu.Lock()
	backend.mu.Lock()
	now := r.now()
	backend.totalFailures.Add(1)
	failures := backend.consecutiveFailures.Add(1)
	backend.consecutiveSuccesses.Store(0)
	backend.lastError = err.Error()
	backend.lastFailureKind = kind
	if backend.quarantined {
		backend.mu.Unlock()
		r.mu.Unlock()
		return nil
	}
	if backend.alive && (hardFailure || failures >= uint64(backend.failThreshold)) {
		backend.alive = false
		backend.lastStateChange = now
		backend.lastAliveChange = now
		recent := backend.appendTransitionLocked(now)
		backend.mu.Unlock()
		r.mu.Unlock()
		return []healthLogEvent{{kind: "dead", failureKind: kind, count: failures, transitions: recent, window: r.flapWindow}}
	}
	backend.mu.Unlock()
	r.mu.Unlock()
	return nil
}

// Active checks implement the state machine: alive failures eject at FailThreshold;
// dead backends recover at RiseThreshold; a fresh burst of FlapMaxTransitions
// alive/dead transitions in the bounded 16-entry FlapWindow quarantines that
// recovery. Quarantine level is one-based: level 1 uses QuarantineBase and each
// later level doubles it, capped at QuarantineMax. The level resets after an
// uninterrupted QuarantineResetAfter alive period. A Registry capacity guard
// may reject quarantine, but never normal failure ejection. Hold expiry returns
// the backend to dead; only RiseThreshold active successes make it selectable.
func (r *Registry) recordActiveCheck(backend *Backend, success bool, kind string, checkErr error) []healthLogEvent {
	r.mu.Lock()
	backend.mu.Lock()
	now := r.now()
	backend.resetQuarantineLevelLocked(now)
	backend.lastCheck = now
	if success {
		backend.lastError = ""
		backend.lastFailureKind = ""
	} else {
		backend.lastError = checkErr.Error()
		backend.lastFailureKind = kind
	}

	var events []healthLogEvent
	if backend.quarantined {
		backend.consecutiveFailures.Store(0)
		backend.consecutiveSuccesses.Store(0)
		if now.Before(backend.quarantinedUntil) {
			backend.mu.Unlock()
			r.mu.Unlock()
			return nil
		}
		backend.quarantined = false
		backend.quarantinedUntil = time.Time{}
		backend.lastStateChange = now
		events = append(events, healthLogEvent{kind: "released"})
	}

	if success {
		backend.consecutiveFailures.Store(0)
		if backend.alive {
			backend.consecutiveSuccesses.Store(0)
			backend.mu.Unlock()
			r.mu.Unlock()
			return events
		}
		successes := backend.consecutiveSuccesses.Add(1)
		if successes < uint64(backend.riseThreshold) {
			backend.mu.Unlock()
			r.mu.Unlock()
			return events
		}
		backend.alive = true
		backend.lastStateChange = now
		backend.lastAliveChange = now
		transitions := backend.appendTransitionLocked(now)
		blocked := false
		if backend.flapMaxTransitions > 0 && transitions >= backend.flapMaxTransitions && backend.transitionsSinceQuarantine >= backend.flapMaxTransitions {
			if r.canQuarantineLocked(backend) {
				backend.quarantineLevel++
				duration := quarantineDuration(backend.quarantineBase, backend.quarantineMax, backend.quarantineLevel)
				backend.quarantined = true
				backend.alive = false
				backend.quarantinedUntil = now.Add(duration)
				backend.lastStateChange = now
				backend.consecutiveSuccesses.Store(0)
				backend.quarantineLimitLogged = false
				backend.transitionsSinceQuarantine = 0
				events = append(events,
					healthLogEvent{kind: "recovered", count: successes},
					healthLogEvent{kind: "quarantined", level: backend.quarantineLevel, duration: duration, transitions: transitions, window: backend.flapWindow},
				)
				backend.mu.Unlock()
				r.mu.Unlock()
				return events
			}
			blocked = !backend.quarantineLimitLogged
			backend.quarantineLimitLogged = true
		}
		backend.consecutiveSuccesses.Store(0)
		events = append(events, healthLogEvent{kind: "recovered", count: successes, blocked: blocked})
		backend.mu.Unlock()
		r.mu.Unlock()
		return events
	}

	backend.consecutiveSuccesses.Store(0)
	if backend.alive {
		failures := backend.consecutiveFailures.Add(1)
		if failures >= uint64(backend.failThreshold) {
			backend.alive = false
			backend.lastStateChange = now
			backend.lastAliveChange = now
			transitions := backend.appendTransitionLocked(now)
			events = append(events, healthLogEvent{kind: "dead", failureKind: kind, count: failures, transitions: transitions, window: backend.flapWindow})
		}
	}
	backend.mu.Unlock()
	r.mu.Unlock()
	return events
}

func (r *Registry) canQuarantineLocked(candidate *Backend) bool {
	backends := r.snapshot()
	total := len(backends)
	limit := total * r.maxQuarantinePercent / 100
	quarantined := 0
	healthyRemaining := 0
	for _, backend := range backends {
		if backend == candidate {
			continue
		}
		backend.mu.RLock()
		if backend.quarantined {
			quarantined++
		} else if backend.alive {
			healthyRemaining++
		}
		backend.mu.RUnlock()
	}
	return quarantined+1 <= limit && healthyRemaining > 0
}

func quarantineDuration(base, maximum time.Duration, level int) time.Duration {
	duration := base
	for multiplier := 1; multiplier < level && duration < maximum; multiplier++ {
		if duration > maximum/2 {
			return maximum
		}
		duration *= 2
	}
	if duration > maximum {
		return maximum
	}
	return duration
}

func (r *Registry) SetAlive(rawURL string, alive bool) error {
	normalized, _, err := normalizeBackendURL(rawURL)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidBackend, err)
	}
	r.mu.RLock()
	backend, exists := r.byURL[normalized]
	r.mu.RUnlock()
	if !exists {
		return fmt.Errorf("%w: %s", ErrBackendNotFound, normalized)
	}
	backend.SetAlive(alive)
	return nil
}

func (r *Registry) snapshot() []*Backend {
	backends := r.backends.Load()
	if backends == nil {
		return nil
	}
	return *backends
}

func (r *Registry) store(backends []*Backend) {
	copyOfBackends := append([]*Backend(nil), backends...)
	if copyOfBackends == nil {
		copyOfBackends = make([]*Backend, 0)
	}
	r.backends.Store(&copyOfBackends)
}
