package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"strings"
	"time"
)

const missingEnvValue = "\x00"

var defaultBackends = []string{
	"http://localhost:9001",
	"http://localhost:9002",
	"http://localhost:9003",
}

type Config struct {
	ListenAddr        string
	Backends          []string
	HealthInterval    time.Duration
	HealthTimeout     time.Duration
	DialTimeout       time.Duration
	ResponseTimeout   time.Duration
	ReadHeaderTimeout time.Duration
	ReadTimeout       time.Duration
	WriteTimeout      time.Duration
	IdleTimeout       time.Duration
	ShutdownTimeout   time.Duration
}

func DefaultConfig() Config {
	return Config{
		ListenAddr:        ":8080",
		Backends:          append([]string(nil), defaultBackends...),
		HealthInterval:    5 * time.Second,
		HealthTimeout:     2 * time.Second,
		DialTimeout:       3 * time.Second,
		ResponseTimeout:   10 * time.Second,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
		ShutdownTimeout:   15 * time.Second,
	}
}

func parseBackends(value string) []string {
	if value == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			continue
		}
		result = append(result, trimmed)
	}
	return result
}

func lookupEnvValue(getenv func(string) string, key string) (string, bool) {
	value := getenv(key)
	if value == missingEnvValue {
		return "", false
	}
	return value, true
}

func LoadConfig(getenv func(string) string, args []string) (Config, error) {
	cfg := DefaultConfig()

	if v, ok := lookupEnvValue(getenv, "LB_LISTEN_ADDR"); ok {
		cfg.ListenAddr = v
	}
	if v, ok := lookupEnvValue(getenv, "LB_BACKENDS"); ok {
		cfg.Backends = parseBackends(v)
	}
	for key, target := range map[string]*time.Duration{
		"LB_HEALTH_INTERVAL":     &cfg.HealthInterval,
		"LB_HEALTH_TIMEOUT":      &cfg.HealthTimeout,
		"LB_DIAL_TIMEOUT":        &cfg.DialTimeout,
		"LB_RESPONSE_TIMEOUT":    &cfg.ResponseTimeout,
		"LB_READ_HEADER_TIMEOUT": &cfg.ReadHeaderTimeout,
		"LB_READ_TIMEOUT":        &cfg.ReadTimeout,
		"LB_WRITE_TIMEOUT":       &cfg.WriteTimeout,
		"LB_IDLE_TIMEOUT":        &cfg.IdleTimeout,
		"LB_SHUTDOWN_TIMEOUT":    &cfg.ShutdownTimeout,
	} {
		if v, ok := lookupEnvValue(getenv, key); ok {
			dur, err := time.ParseDuration(v)
			if err != nil {
				return cfg, fmt.Errorf("invalid duration for %s: %q: %w", key, v, err)
			}
			*target = dur
		}
	}

	fs := flag.NewFlagSet("loadbalancer-go", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	listenAddr := cfg.ListenAddr
	backendsValue := strings.Join(cfg.Backends, ",")
	fs.StringVar(&listenAddr, "listen", cfg.ListenAddr, "listen address")
	fs.StringVar(&backendsValue, "backends", strings.Join(cfg.Backends, ","), "comma-separated backend URLs")
	fs.DurationVar(&cfg.HealthInterval, "health-interval", cfg.HealthInterval, "health check interval")
	fs.DurationVar(&cfg.HealthTimeout, "health-timeout", cfg.HealthTimeout, "health check HTTP client timeout")
	fs.DurationVar(&cfg.DialTimeout, "dial-timeout", cfg.DialTimeout, "backend dial timeout")
	fs.DurationVar(&cfg.ResponseTimeout, "response-timeout", cfg.ResponseTimeout, "backend response-header timeout")
	fs.DurationVar(&cfg.ReadHeaderTimeout, "read-header-timeout", cfg.ReadHeaderTimeout, "server read-header timeout")
	fs.DurationVar(&cfg.ReadTimeout, "read-timeout", cfg.ReadTimeout, "server read timeout")
	fs.DurationVar(&cfg.WriteTimeout, "write-timeout", cfg.WriteTimeout, "server write timeout")
	fs.DurationVar(&cfg.IdleTimeout, "idle-timeout", cfg.IdleTimeout, "server idle timeout")
	fs.DurationVar(&cfg.ShutdownTimeout, "shutdown-timeout", cfg.ShutdownTimeout, "time allowed for graceful shutdown")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", fs.Name())
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			fs.Usage()
			return cfg, err
		}
		return cfg, err
	}

	cfg.ListenAddr = listenAddr
	cfg.Backends = parseBackends(backendsValue)
	return cfg, nil
}

func (c Config) Validate() error {
	var problems []error

	if len(c.Backends) == 0 {
		problems = append(problems, errors.New("at least one backend is required"))
	}

	seen := make(map[string]struct{}, len(c.Backends))
	for _, raw := range c.Backends {
		u, err := url.Parse(raw)
		if err != nil || u.Scheme == "" || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
			problems = append(problems, fmt.Errorf("invalid backend URL %q: must be http(s) with a non-empty host", raw))
			continue
		}
		if _, ok := seen[raw]; ok {
			problems = append(problems, fmt.Errorf("duplicate backend URL %q", raw))
			continue
		}
		seen[raw] = struct{}{}
	}

	for name, value := range map[string]time.Duration{
		"health interval":     c.HealthInterval,
		"health timeout":      c.HealthTimeout,
		"dial timeout":        c.DialTimeout,
		"response timeout":    c.ResponseTimeout,
		"read header timeout": c.ReadHeaderTimeout,
		"read timeout":        c.ReadTimeout,
		"write timeout":       c.WriteTimeout,
		"idle timeout":        c.IdleTimeout,
		"shutdown timeout":    c.ShutdownTimeout,
	} {
		if value <= 0 {
			problems = append(problems, fmt.Errorf("%s must be greater than zero", name))
		}
	}

	if c.ListenAddr == "" {
		problems = append(problems, errors.New("listen address must be non-empty"))
	} else if _, _, err := net.SplitHostPort(c.ListenAddr); err != nil {
		problems = append(problems, fmt.Errorf("listen address %q is invalid: %w", c.ListenAddr, err))
	}

	return errors.Join(problems...)
}

func (c Config) String() string {
	return fmt.Sprintf(
		"listen=%s backends=%v healthInterval=%s healthTimeout=%s dialTimeout=%s responseTimeout=%s readHeaderTimeout=%s readTimeout=%s writeTimeout=%s idleTimeout=%s shutdownTimeout=%s",
		c.ListenAddr,
		c.Backends,
		c.HealthInterval,
		c.HealthTimeout,
		c.DialTimeout,
		c.ResponseTimeout,
		c.ReadHeaderTimeout,
		c.ReadTimeout,
		c.WriteTimeout,
		c.IdleTimeout,
		c.ShutdownTimeout,
	)
}
