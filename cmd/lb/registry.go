package main

import (
	"errors"
	"fmt"
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
	URL                 string    `json:"url"`
	Alive               bool      `json:"alive"`
	ConsecutiveFailures uint64    `json:"consecutiveFailures"`
	TotalRequests       uint64    `json:"totalRequests"`
	TotalFailures       uint64    `json:"totalFailures"`
	ActiveConns         int64     `json:"activeConns"`
	LastCheck           time.Time `json:"lastCheck"`
	LastError           string    `json:"lastError"`
	AddedAt             time.Time `json:"addedAt"`
}

type Registry struct {
	mu              sync.RWMutex
	byURL           map[string]*Backend
	backends        atomic.Pointer[[]*Backend]
	responseTimeout time.Duration
	dialTimeout     time.Duration
}

func NewRegistry(responseTimeout, dialTimeout time.Duration) *Registry {
	registry := &Registry{
		byURL:           make(map[string]*Backend),
		responseTimeout: responseTimeout,
		dialTimeout:     dialTimeout,
	}
	empty := make([]*Backend, 0)
	registry.backends.Store(&empty)
	return registry
}

func (r *Registry) Add(rawURL string) (*Backend, error) {
	normalized, parsed, err := normalizeBackendURL(rawURL)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidBackend, err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.byURL[normalized]; exists {
		return nil, fmt.Errorf("%w: %s", ErrDuplicateBackend, normalized)
	}
	backend := NewBackend(parsed, r.dialTimeout, r.responseTimeout)
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
	r.store(updated)
	r.mu.Unlock()

	backend.transport.CloseIdleConnections()
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
