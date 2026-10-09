package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
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
	CheckOnly         bool
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
	durationSettings := []struct {
		name   string
		target *time.Duration
	}{
		{"LB_HEALTH_INTERVAL", &cfg.HealthInterval},
		{"LB_HEALTH_TIMEOUT", &cfg.HealthTimeout},
		{"LB_DIAL_TIMEOUT", &cfg.DialTimeout},
		{"LB_RESPONSE_TIMEOUT", &cfg.ResponseTimeout},
		{"LB_READ_HEADER_TIMEOUT", &cfg.ReadHeaderTimeout},
		{"LB_READ_TIMEOUT", &cfg.ReadTimeout},
		{"LB_WRITE_TIMEOUT", &cfg.WriteTimeout},
		{"LB_IDLE_TIMEOUT", &cfg.IdleTimeout},
		{"LB_SHUTDOWN_TIMEOUT", &cfg.ShutdownTimeout},
	}
	var parseProblems []error
	for _, setting := range durationSettings {
		if value, ok := lookupEnvValue(getenv, setting.name); ok {
			duration, err := time.ParseDuration(value)
			if err != nil {
				parseProblems = append(parseProblems, fmt.Errorf("%s: %q is not a valid duration (use e.g. 10s, 500ms)", setting.name, value))
				continue
			}
			*setting.target = duration
		}
	}
	if value, ok := lookupEnvValue(getenv, "LB_CHECK_ONLY"); ok {
		checkOnly, err := strconv.ParseBool(value)
		if err != nil {
			parseProblems = append(parseProblems, fmt.Errorf("LB_CHECK_ONLY: %q must be true or false", value))
		} else {
			cfg.CheckOnly = checkOnly
		}
	}

	fs := flag.NewFlagSet("loadbalancer-go", flag.ContinueOnError)
	var flagOutput bytes.Buffer
	fs.SetOutput(&flagOutput)
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
	fs.BoolVar(&cfg.CheckOnly, "check", cfg.CheckOnly, "validate and preflight configuration, then exit")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", fs.Name())
		fs.SetOutput(os.Stderr)
		fs.PrintDefaults()
		fs.SetOutput(&flagOutput)
	}

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return cfg, err
		}
		return cfg, errors.Join(append(parseProblems, err)...)
	}

	cfg.ListenAddr = listenAddr
	cfg.Backends = parseBackends(backendsValue)
	return cfg, errors.Join(parseProblems...)
}

const maxConfigDuration = 24 * time.Hour

func (c *Config) Validate() error {
	var problems []error

	if len(c.Backends) == 0 {
		problems = append(problems, fmt.Errorf("LB_BACKENDS: %q must contain at least one http(s) backend URL", strings.Join(c.Backends, ",")))
	}

	listenHost, listenPort, listenValid, listenProblems := validateListenAddr(c.ListenAddr)
	problems = append(problems, listenProblems...)

	seen := make(map[string]string, len(c.Backends))
	for index, raw := range c.Backends {
		u, err := url.Parse(raw)
		if err != nil {
			if strings.Contains(err.Error(), "port") {
				problems = append(problems, fmt.Errorf("LB_BACKENDS: %q has an invalid port; use a numeric port from 1 to 65535", raw))
			} else {
				problems = append(problems, fmt.Errorf("LB_BACKENDS: %q is not a valid URL: %v", raw, err))
			}
			continue
		}

		backendProblems := make([]error, 0)
		scheme := strings.ToLower(u.Scheme)
		if scheme != "http" && scheme != "https" {
			backendProblems = append(backendProblems, fmt.Errorf("LB_BACKENDS: %q must use the http or https scheme", raw))
		}
		if u.Hostname() == "" {
			backendProblems = append(backendProblems, fmt.Errorf("LB_BACKENDS: %q must include a non-empty hostname", raw))
		} else if !validHost(u.Hostname()) {
			backendProblems = append(backendProblems, fmt.Errorf("LB_BACKENDS: %q has an invalid hostname; use an IP address or hostname without spaces", raw))
		}
		if u.User != nil {
			backendProblems = append(backendProblems, fmt.Errorf("LB_BACKENDS: %q must not contain user information", raw))
		}
		if u.RawQuery != "" || u.ForceQuery {
			backendProblems = append(backendProblems, fmt.Errorf("LB_BACKENDS: %q must not contain a query string", raw))
		}
		if strings.Contains(raw, "#") || u.Fragment != "" || u.RawFragment != "" {
			backendProblems = append(backendProblems, fmt.Errorf("LB_BACKENDS: %q must not contain a fragment", raw))
		}
		if u.Path != "" && u.Path != "/" {
			backendProblems = append(backendProblems, fmt.Errorf("LB_BACKENDS: %q must be a base address with an empty path or /", raw))
		}

		port := u.Port()
		if strings.HasSuffix(u.Host, ":") {
			backendProblems = append(backendProblems, fmt.Errorf("LB_BACKENDS: %q has an empty port; use a numeric port from 1 to 65535", raw))
		} else if port != "" {
			parsedPort, portErr := parsePort(port)
			if portErr != nil {
				backendProblems = append(backendProblems, fmt.Errorf("LB_BACKENDS: %q has an invalid port %q; use a numeric port from 1 to 65535", raw, port))
			} else {
				port = strconv.Itoa(parsedPort)
			}
		} else if scheme == "http" {
			port = "80"
		} else if scheme == "https" {
			port = "443"
		}

		if len(backendProblems) > 0 {
			problems = append(problems, backendProblems...)
			continue
		}

		host := strings.ToLower(u.Hostname())
		normalized := scheme + "://" + net.JoinHostPort(host, port)
		c.Backends[index] = normalized
		if previous, ok := seen[normalized]; ok {
			problems = append(problems, fmt.Errorf("LB_BACKENDS: %q duplicates %q after URL normalization; each backend must be unique", raw, previous))
		} else {
			seen[normalized] = raw
		}
		if listenValid && (listenHost == "" || localHost(listenHost)) && localHost(host) && port == strconv.Itoa(listenPort) {
			problems = append(problems, fmt.Errorf("LB_BACKENDS: %q points to this balancer on listen port %d; choose a different backend address or port", raw, listenPort))
		}
	}

	durations := []struct {
		name  string
		value time.Duration
	}{
		{"LB_HEALTH_INTERVAL", c.HealthInterval},
		{"LB_HEALTH_TIMEOUT", c.HealthTimeout},
		{"LB_DIAL_TIMEOUT", c.DialTimeout},
		{"LB_RESPONSE_TIMEOUT", c.ResponseTimeout},
		{"LB_READ_HEADER_TIMEOUT", c.ReadHeaderTimeout},
		{"LB_READ_TIMEOUT", c.ReadTimeout},
		{"LB_WRITE_TIMEOUT", c.WriteTimeout},
		{"LB_IDLE_TIMEOUT", c.IdleTimeout},
		{"LB_SHUTDOWN_TIMEOUT", c.ShutdownTimeout},
	}
	durationValid := make(map[string]bool, len(durations))
	for _, setting := range durations {
		durationValid[setting.name] = setting.value > 0 && setting.value <= maxConfigDuration
		if setting.value <= 0 || setting.value > maxConfigDuration {
			problems = append(problems, fmt.Errorf("%s: %q must be greater than 0 and no more than 24h", setting.name, setting.value))
		}
	}

	if durationValid["LB_HEALTH_INTERVAL"] && durationValid["LB_HEALTH_TIMEOUT"] && c.HealthTimeout >= c.HealthInterval {
		problems = append(problems, fmt.Errorf("LB_HEALTH_TIMEOUT: %q must be less than LB_HEALTH_INTERVAL %q to avoid overlapping checks", c.HealthTimeout, c.HealthInterval))
	}
	if durationValid["LB_DIAL_TIMEOUT"] && durationValid["LB_RESPONSE_TIMEOUT"] && c.DialTimeout > c.ResponseTimeout {
		problems = append(problems, fmt.Errorf("LB_DIAL_TIMEOUT: %q must be less than or equal to LB_RESPONSE_TIMEOUT %q", c.DialTimeout, c.ResponseTimeout))
	}
	if durationValid["LB_READ_HEADER_TIMEOUT"] && durationValid["LB_READ_TIMEOUT"] && c.ReadHeaderTimeout > c.ReadTimeout {
		problems = append(problems, fmt.Errorf("LB_READ_HEADER_TIMEOUT: %q must be less than or equal to LB_READ_TIMEOUT %q", c.ReadHeaderTimeout, c.ReadTimeout))
	}

	return errors.Join(problems...)
}

func validateListenAddr(address string) (string, int, bool, []error) {
	const setting = "LB_LISTEN_ADDR"
	host, portText, err := net.SplitHostPort(address)
	if err != nil {
		return "", 0, false, []error{fmt.Errorf("%s: %q must be a host:port address with a numeric port from 1 to 65535: %v", setting, address, err)}
	}
	var problems []error
	if host != "" && !validHost(host) {
		problems = append(problems, fmt.Errorf("%s: %q has an invalid host; use an IP address or hostname without spaces", setting, address))
	}
	port, err := parsePort(portText)
	if err != nil {
		problems = append(problems, fmt.Errorf("%s: %q must use a numeric port from 1 to 65535", setting, address))
		return host, 0, false, problems
	}
	return host, port, len(problems) == 0, problems
}

func parsePort(port string) (int, error) {
	if port == "" {
		return 0, errors.New("empty port")
	}
	for _, digit := range port {
		if digit < '0' || digit > '9' {
			return 0, errors.New("port is not numeric")
		}
	}
	value, err := strconv.Atoi(port)
	if err != nil || value < 1 || value > 65535 {
		return 0, errors.New("port is outside 1..65535")
	}
	return value, nil
}

func validHost(host string) bool {
	if host == "" || strings.ContainsAny(host, " \t\r\n") {
		return false
	}
	if net.ParseIP(host) != nil {
		return true
	}
	if strings.Contains(host, ":") || len(host) > 253 {
		return false
	}
	host = strings.TrimSuffix(host, ".")
	if host == "" {
		return false
	}
	for _, label := range strings.Split(host, ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, char := range label {
			if (char < 'a' || char > 'z') && (char < 'A' || char > 'Z') && (char < '0' || char > '9') && char != '-' {
				return false
			}
		}
	}
	return true
}

func localHost(host string) bool {
	switch strings.ToLower(host) {
	case "localhost", "127.0.0.1", "::1", "::", "0.0.0.0":
		return true
	default:
		return false
	}
}

func (c Config) Warnings() []string {
	if c.ShutdownTimeout > 0 && c.ResponseTimeout > 0 && c.ShutdownTimeout < c.ResponseTimeout {
		return []string{fmt.Sprintf("LB_SHUTDOWN_TIMEOUT %q is shorter than LB_RESPONSE_TIMEOUT %q; in-flight requests may be cut off during shutdown", c.ShutdownTimeout, c.ResponseTimeout)}
	}
	return nil
}

func Preflight(c Config) error {
	listener, err := net.Listen("tcp", c.ListenAddr)
	if err != nil {
		return fmt.Errorf("cannot listen on %s: %w", c.ListenAddr, err)
	}
	return listener.Close()
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
