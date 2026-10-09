package main

import (
	"errors"
	"flag"
	"strings"
	"testing"
	"time"
)

func envLookup(m map[string]string) func(string) string {
	return func(key string) string {
		if v, ok := m[key]; ok {
			return v
		}
		return missingEnvValue
	}
}

func TestLoadConfig_Defaults(t *testing.T) {
	cfg, err := LoadConfig(envLookup(nil), nil)
	if err != nil {
		t.Fatalf("LoadConfig returned error: %v", err)
	}
	want := DefaultConfig()
	if cfg.ListenAddr != want.ListenAddr {
		t.Fatalf("ListenAddr = %q; want %q", cfg.ListenAddr, want.ListenAddr)
	}
	if len(cfg.Backends) != len(want.Backends) {
		t.Fatalf("len(Backends) = %d; want %d", len(cfg.Backends), len(want.Backends))
	}
	for i := range want.Backends {
		if cfg.Backends[i] != want.Backends[i] {
			t.Fatalf("Backends[%d] = %q; want %q", i, cfg.Backends[i], want.Backends[i])
		}
	}
	if cfg.HealthInterval != want.HealthInterval || cfg.HealthTimeout != want.HealthTimeout || cfg.DialTimeout != want.DialTimeout || cfg.ResponseTimeout != want.ResponseTimeout || cfg.ReadHeaderTimeout != want.ReadHeaderTimeout || cfg.ReadTimeout != want.ReadTimeout || cfg.WriteTimeout != want.WriteTimeout || cfg.IdleTimeout != want.IdleTimeout || cfg.ShutdownTimeout != want.ShutdownTimeout {
		t.Fatalf("unexpected default values: %#v; want %#v", cfg, want)
	}
}

func TestLoadConfig_EnvOverridesDefaults(t *testing.T) {
	env := map[string]string{
		"LB_LISTEN_ADDR":         ":9090",
		"LB_BACKENDS":            "http://10.0.0.5:8000, http://10.0.0.6:8000",
		"LB_HEALTH_INTERVAL":     "250ms",
		"LB_HEALTH_TIMEOUT":      "750ms",
		"LB_DIAL_TIMEOUT":        "2s",
		"LB_RESPONSE_TIMEOUT":    "15s",
		"LB_READ_HEADER_TIMEOUT": "4s",
		"LB_READ_TIMEOUT":        "12s",
		"LB_WRITE_TIMEOUT":       "20s",
		"LB_IDLE_TIMEOUT":        "30s",
		"LB_SHUTDOWN_TIMEOUT":    "18s",
	}
	cfg, err := LoadConfig(envLookup(env), nil)
	if err != nil {
		t.Fatalf("LoadConfig returned error: %v", err)
	}
	if cfg.ListenAddr != ":9090" {
		t.Fatalf("ListenAddr = %q; want :9090", cfg.ListenAddr)
	}
	if got, want := cfg.Backends, []string{"http://10.0.0.5:8000", "http://10.0.0.6:8000"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("Backends = %#v; want %#v", got, want)
	}
	if cfg.HealthInterval != 250*time.Millisecond || cfg.HealthTimeout != 750*time.Millisecond || cfg.DialTimeout != 2*time.Second || cfg.ResponseTimeout != 15*time.Second || cfg.ReadHeaderTimeout != 4*time.Second || cfg.ReadTimeout != 12*time.Second || cfg.WriteTimeout != 20*time.Second || cfg.IdleTimeout != 30*time.Second || cfg.ShutdownTimeout != 18*time.Second {
		t.Fatalf("unexpected env-derived config: %#v", cfg)
	}
}

func TestLoadConfig_FlagsOverrideEnv(t *testing.T) {
	env := map[string]string{
		"LB_LISTEN_ADDR":         ":9090",
		"LB_BACKENDS":            "http://10.0.0.5:8000",
		"LB_HEALTH_INTERVAL":     "250ms",
		"LB_HEALTH_TIMEOUT":      "750ms",
		"LB_DIAL_TIMEOUT":        "2s",
		"LB_RESPONSE_TIMEOUT":    "15s",
		"LB_READ_HEADER_TIMEOUT": "4s",
		"LB_READ_TIMEOUT":        "12s",
		"LB_WRITE_TIMEOUT":       "20s",
		"LB_IDLE_TIMEOUT":        "30s",
		"LB_SHUTDOWN_TIMEOUT":    "18s",
	}
	cfg, err := LoadConfig(envLookup(env), []string{
		"-listen", ":9191",
		"-backends", "http://10.0.0.7:8000,http://10.0.0.8:8000",
		"-health-interval", "300ms",
		"-health-timeout", "900ms",
		"-dial-timeout", "3s",
		"-response-timeout", "16s",
		"-read-header-timeout", "5s",
		"-read-timeout", "13s",
		"-write-timeout", "21s",
		"-idle-timeout", "31s",
		"-shutdown-timeout", "19s",
	})
	if err != nil {
		t.Fatalf("LoadConfig returned error: %v", err)
	}
	if cfg.ListenAddr != ":9191" {
		t.Fatalf("ListenAddr = %q; want :9191", cfg.ListenAddr)
	}
	if got, want := cfg.Backends, []string{"http://10.0.0.7:8000", "http://10.0.0.8:8000"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("Backends = %#v; want %#v", got, want)
	}
	if cfg.HealthInterval != 300*time.Millisecond || cfg.HealthTimeout != 900*time.Millisecond || cfg.DialTimeout != 3*time.Second || cfg.ResponseTimeout != 16*time.Second || cfg.ReadHeaderTimeout != 5*time.Second || cfg.ReadTimeout != 13*time.Second || cfg.WriteTimeout != 21*time.Second || cfg.IdleTimeout != 31*time.Second || cfg.ShutdownTimeout != 19*time.Second {
		t.Fatalf("unexpected flag-derived config: %#v", cfg)
	}
}

func TestLoadConfig_BackendsParsing(t *testing.T) {
	env := map[string]string{
		"LB_BACKENDS": " http://localhost:9001 , ,http://localhost:9002, , http://localhost:9003, ",
	}
	cfg, err := LoadConfig(envLookup(env), nil)
	if err != nil {
		t.Fatalf("LoadConfig returned error: %v", err)
	}
	want := []string{"http://localhost:9001", "http://localhost:9002", "http://localhost:9003"}
	if len(cfg.Backends) != len(want) {
		t.Fatalf("len(Backends) = %d; want %d: %#v", len(cfg.Backends), len(want), cfg.Backends)
	}
	for i := range want {
		if cfg.Backends[i] != want[i] {
			t.Fatalf("Backends[%d] = %q; want %q", i, cfg.Backends[i], want[i])
		}
	}
}

func TestLoadConfig_InvalidDuration(t *testing.T) {
	env := map[string]string{
		"LB_HEALTH_INTERVAL":  "abc",
		"LB_SHUTDOWN_TIMEOUT": "-5s",
	}
	_, err := LoadConfig(envLookup(env), nil)
	if err == nil {
		t.Fatal("LoadConfig returned nil error for invalid duration")
	}
	msg := err.Error()
	if !strings.Contains(msg, "invalid duration") || !strings.Contains(msg, "LB_HEALTH_INTERVAL") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidate_NoBackends(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Backends = nil
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "at least one backend") {
		t.Fatalf("Validate = %v; want at least one backend error", err)
	}
}

func TestValidate_InvalidBackendURL(t *testing.T) {
	cases := []string{"localhost:9001", "ftp://x", "http://"}
	for _, tc := range cases {
		cfg := DefaultConfig()
		cfg.Backends = []string{tc}
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "invalid backend URL") {
			t.Fatalf("Validate(%q) = %v; want invalid backend URL error", tc, err)
		}
	}
}

func TestValidate_DuplicateBackends(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Backends = []string{"http://localhost:9001", "http://localhost:9001"}
	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "duplicate backend URL") {
		t.Fatalf("Validate = %v; want duplicate backend URL error", err)
	}
}

func TestValidate_BadListenAddr(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ListenAddr = "localhost"
	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "listen address") {
		t.Fatalf("Validate = %v; want listen address error", err)
	}
}

func TestValidate_ReportsAllErrors(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Backends = []string{"localhost:9001", "ftp://x"}
	cfg.ListenAddr = "bad"
	cfg.HealthInterval = 0
	err := cfg.Validate()
	if err == nil {
		t.Fatal("Validate returned nil error for multiple invalid values")
	}
	msg := err.Error()
	if !strings.Contains(msg, "invalid backend URL") || !strings.Contains(msg, "health interval") || !strings.Contains(msg, "listen address") {
		t.Fatalf("unexpected multi-error output: %v", err)
	}
}

func TestLoadConfig_HelpFlag(t *testing.T) {
	cfg, err := LoadConfig(envLookup(nil), []string{"-h"})
	if !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("LoadConfig returned error %v; want flag.ErrHelp", err)
	}
	if cfg.ListenAddr == "" {
		t.Fatal("LoadConfig should still return a config on help")
	}
}
