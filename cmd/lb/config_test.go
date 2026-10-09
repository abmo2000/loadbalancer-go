package main

import (
	"errors"
	"flag"
	"net"
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

func TestLoadConfig_CheckOnly(t *testing.T) {
	cfg, err := LoadConfig(envLookup(map[string]string{"LB_CHECK_ONLY": "true"}), nil)
	if err != nil {
		t.Fatalf("LoadConfig returned error: %v", err)
	}
	if !cfg.CheckOnly {
		t.Fatal("LB_CHECK_ONLY=true did not enable check-only mode")
	}

	cfg, err = LoadConfig(envLookup(map[string]string{"LB_CHECK_ONLY": "true"}), []string{"-check=false"})
	if err != nil {
		t.Fatalf("LoadConfig returned error: %v", err)
	}
	if cfg.CheckOnly {
		t.Fatal("-check=false did not override LB_CHECK_ONLY=true")
	}
}

func TestLoadConfig_AggregatesInvalidDurations(t *testing.T) {
	env := map[string]string{
		"LB_HEALTH_INTERVAL":  "abc",
		"LB_RESPONSE_TIMEOUT": "later",
	}
	_, err := LoadConfig(envLookup(env), nil)
	assertConfigError(t, err, "LB_HEALTH_INTERVAL", "abc", "LB_RESPONSE_TIMEOUT", "later")
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
	if !strings.Contains(msg, "not a valid duration") || !strings.Contains(msg, "LB_HEALTH_INTERVAL") || !strings.Contains(msg, `"abc"`) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidate_NoBackends(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Backends = nil
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "LB_BACKENDS") || !strings.Contains(err.Error(), `""`) {
		t.Fatalf("Validate = %v; want at least one backend error", err)
	}
}

func TestValidate_InvalidBackendURL(t *testing.T) {
	cases := []string{"localhost:9001", "ftp://x", "http://"}
	for _, tc := range cases {
		cfg := DefaultConfig()
		cfg.Backends = []string{tc}
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "LB_BACKENDS") || !strings.Contains(err.Error(), tc) {
			t.Fatalf("Validate(%q) = %v; want invalid backend URL error", tc, err)
		}
	}
}

func TestValidate_DuplicateBackends(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Backends = []string{"http://localhost:9001", "http://localhost:9001"}
	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "LB_BACKENDS") || !strings.Contains(err.Error(), "must be unique") {
		t.Fatalf("Validate = %v; want duplicate backend URL error", err)
	}
}

func TestValidate_BadListenAddr(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ListenAddr = "localhost"
	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "LB_LISTEN_ADDR") || !strings.Contains(err.Error(), "localhost") {
		t.Fatalf("Validate = %v; want listen address error", err)
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

func TestValidate_ListenPorts(t *testing.T) {
	tests := []struct {
		name    string
		address string
		valid   bool
	}{
		{"zero", ":0", false},
		{"too high", ":70000", false},
		{"nonnumeric", ":abc", false},
		{"missing colon", "8080", false},
		{"negative", ":-1", false},
		{"port only", ":8080", true},
		{"hostname", "localhost:8080", true},
		{"ipv4 host", "127.0.0.1:8080", true},
		{"ipv6 host", "[::1]:8080", true},
		{"host with spaces", "bad host:8080", false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.ListenAddr = test.address
			err := cfg.Validate()
			if test.valid && err != nil {
				t.Fatalf("Validate returned error for %q: %v", test.address, err)
			}
			if !test.valid {
				assertConfigError(t, err, "LB_LISTEN_ADDR", test.address)
			}
		})
	}
}

func TestValidate_BackendURLs(t *testing.T) {
	tests := []struct {
		name  string
		value string
		valid bool
	}{
		{"missing scheme", "localhost:9001", false},
		{"unsupported scheme", "ftp://x", false},
		{"missing host", "http://", false},
		{"userinfo", "http://user:pass@localhost:9001", false},
		{"query", "http://localhost:9001?x=1", false},
		{"fragment", "http://localhost:9001#frag", false},
		{"empty fragment", "http://localhost:9001#", false},
		{"path", "http://localhost:9001/api", false},
		{"zero port", "http://localhost:0", false},
		{"high port", "http://localhost:99999", false},
		{"nonnumeric port", "http://localhost:abc", false},
		{"trailing slash", "http://localhost:9001/", true},
		{"uppercase host", "http://LOCALHOST:9001", true},
		{"https", "https://example.com:8443", true},
		{"https default port", "https://example.com", true},
		{"ipv6", "http://[::1]:9001", true},
		{"default port", "http://example.com", true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.Backends = []string{test.value}
			err := cfg.Validate()
			if test.valid && err != nil {
				t.Fatalf("Validate rejected %q: %v", test.value, err)
			}
			if !test.valid {
				assertConfigError(t, err, "LB_BACKENDS", test.value)
			}
		})
	}
}

func TestValidate_BackendNormalizationAndDuplicates(t *testing.T) {
	tests := []struct {
		name     string
		backends []string
		want     string
	}{
		{
			name:     "case and slash",
			backends: []string{"http://Localhost:9001/", "http://localhost:9001"},
			want:     "http://localhost:9001",
		},
		{
			name:     "implicit default port",
			backends: []string{"http://example.com", "http://example.com:80"},
			want:     "http://example.com:80",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.Backends = append([]string(nil), test.backends...)
			err := cfg.Validate()
			if err == nil || !strings.Contains(err.Error(), "must be unique") {
				t.Fatalf("Validate = %v; want duplicate backend error", err)
			}
			for _, backend := range cfg.Backends {
				if backend != test.want {
					t.Fatalf("normalized backend = %q; want %q", backend, test.want)
				}
			}
		})
	}
}

func TestValidate_RejectsSelfProxy(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ListenAddr = ":8080"
	cfg.Backends = []string{"http://localhost:8080"}
	err := cfg.Validate()
	assertConfigError(t, err, "LB_BACKENDS", "http://localhost:8080")
	if !strings.Contains(err.Error(), "this balancer") {
		t.Fatalf("Validate error = %v; want self-proxy explanation", err)
	}
}

func TestValidate_DurationBounds(t *testing.T) {
	tests := []struct {
		name  string
		field string
		value time.Duration
	}{
		{"health interval zero", "LB_HEALTH_INTERVAL", 0},
		{"dial timeout negative", "LB_DIAL_TIMEOUT", -time.Second},
		{"response timeout over limit", "LB_RESPONSE_TIMEOUT", 25 * time.Hour},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := DefaultConfig()
			switch test.field {
			case "LB_HEALTH_INTERVAL":
				cfg.HealthInterval = test.value
			case "LB_DIAL_TIMEOUT":
				cfg.DialTimeout = test.value
			case "LB_RESPONSE_TIMEOUT":
				cfg.ResponseTimeout = test.value
			}
			assertConfigError(t, cfg.Validate(), test.field, test.value.String())
		})
	}
}

func TestLoadConfig_InvalidDurationNamesValue(t *testing.T) {
	_, err := LoadConfig(envLookup(map[string]string{"LB_RESPONSE_TIMEOUT": "abc"}), nil)
	assertConfigError(t, err, "LB_RESPONSE_TIMEOUT", "abc")
}

func TestValidate_DurationRelationships(t *testing.T) {
	tests := []struct {
		name   string
		field  string
		value  string
		mutate func(*Config)
	}{
		{"health timeout overlaps interval", "LB_HEALTH_TIMEOUT", "1s", func(c *Config) { c.HealthInterval = time.Second; c.HealthTimeout = time.Second }},
		{"dial exceeds response", "LB_DIAL_TIMEOUT", "11s", func(c *Config) { c.DialTimeout = 11 * time.Second; c.ResponseTimeout = 10 * time.Second }},
		{"header exceeds read", "LB_READ_HEADER_TIMEOUT", "16s", func(c *Config) { c.ReadHeaderTimeout = 16 * time.Second; c.ReadTimeout = 15 * time.Second }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := DefaultConfig()
			test.mutate(&cfg)
			assertConfigError(t, cfg.Validate(), test.field, test.value, "must be")
		})
	}
}

func TestWarnings_ShutdownShorterThanResponse(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ShutdownTimeout = 5 * time.Second
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate returned error: %v", err)
	}
	warnings := cfg.Warnings()
	if len(warnings) != 1 || !strings.Contains(warnings[0], "LB_SHUTDOWN_TIMEOUT") || !strings.Contains(warnings[0], "5s") || !strings.Contains(warnings[0], "LB_RESPONSE_TIMEOUT") || !strings.Contains(warnings[0], "10s") {
		t.Fatalf("Warnings = %#v; want shutdown/response warning", warnings)
	}
}

func TestValidate_ReportsAllErrors(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ListenAddr = ":0"
	cfg.Backends = []string{"ftp://host?query=1"}
	cfg.HealthInterval = 0
	cfg.WriteTimeout = 25 * time.Hour
	err := cfg.Validate()
	for _, expected := range []string{"LB_LISTEN_ADDR", `":0"`, "LB_BACKENDS", `"ftp://host?query=1"`, "LB_HEALTH_INTERVAL", `"0s"`, "LB_WRITE_TIMEOUT", `"25h0m0s"`} {
		if err == nil || !strings.Contains(err.Error(), expected) {
			t.Fatalf("Validate error = %v; want all errors including %s", err, expected)
		}
	}
}

func TestPreflight(t *testing.T) {
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen returned error: %v", err)
	}
	t.Cleanup(func() { _ = occupied.Close() })
	occupiedConfig := DefaultConfig()
	occupiedConfig.ListenAddr = occupied.Addr().String()
	if err := Preflight(occupiedConfig); err == nil || !strings.Contains(err.Error(), "cannot listen on") {
		t.Fatalf("Preflight on occupied address = %v; want listen failure", err)
	}

	free, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen returned error: %v", err)
	}
	t.Cleanup(func() { _ = free.Close() })
	freeConfig := DefaultConfig()
	freeConfig.ListenAddr = free.Addr().String()
	if err := free.Close(); err != nil {
		t.Fatalf("closing reservation listener: %v", err)
	}
	if err := Preflight(freeConfig); err != nil {
		t.Fatalf("Preflight on free address returned error: %v", err)
	}
}

func assertConfigError(t *testing.T, err error, expected ...string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error containing %q", expected)
	}
	message := err.Error()
	for _, value := range expected {
		if !strings.Contains(message, value) {
			t.Fatalf("error %q does not contain %q", message, value)
		}
	}
}
