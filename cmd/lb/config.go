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
	"unicode/utf8"
)

const missingEnvValue = "\x00"

var defaultBackends = []string{
	"http://localhost:9001",
	"http://localhost:9002",
	"http://localhost:9003",
}

type Config struct {
	ListenAddr           string
	Backends             []string
	BackendHealth        map[string]BackendHealth
	HealthInterval       time.Duration
	HealthTimeout        time.Duration
	FailThreshold        int
	RiseThreshold        int
	HealthPath           string
	FlapWindow           time.Duration
	FlapMaxTransitions   int
	QuarantineBase       time.Duration
	QuarantineMax        time.Duration
	QuarantineResetAfter time.Duration
	MaxQuarantinePercent int
	DialTimeout          time.Duration
	ResponseTimeout      time.Duration
	ReadHeaderTimeout    time.Duration
	ReadTimeout          time.Duration
	WriteTimeout         time.Duration
	IdleTimeout          time.Duration
	ShutdownTimeout      time.Duration
	CheckOnly            bool
}

type BackendHealth struct {
	Path     string
	Interval time.Duration
	Timeout  time.Duration
}

func DefaultConfig() Config {
	return Config{
		ListenAddr:           ":8080",
		Backends:             append([]string(nil), defaultBackends...),
		BackendHealth:        make(map[string]BackendHealth),
		HealthInterval:       5 * time.Second,
		HealthTimeout:        2 * time.Second,
		FailThreshold:        3,
		RiseThreshold:        2,
		HealthPath:           "/",
		FlapWindow:           60 * time.Second,
		FlapMaxTransitions:   4,
		QuarantineBase:       30 * time.Second,
		QuarantineMax:        5 * time.Minute,
		QuarantineResetAfter: 5 * time.Minute,
		MaxQuarantinePercent: 50,
		DialTimeout:          3 * time.Second,
		ResponseTimeout:      10 * time.Second,
		ReadHeaderTimeout:    5 * time.Second,
		ReadTimeout:          15 * time.Second,
		WriteTimeout:         30 * time.Second,
		IdleTimeout:          60 * time.Second,
		ShutdownTimeout:      15 * time.Second,
	}
}

func (c Config) EffectiveBackendHealth(normalizedURL string) BackendHealth {
	override := c.BackendHealth[normalizedURL]
	if override.Path == "" {
		override.Path = c.HealthPath
	}
	if override.Interval == 0 {
		override.Interval = c.HealthInterval
	}
	if override.Timeout == 0 {
		override.Timeout = c.HealthTimeout
	}
	return override
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
	backendsValue := strings.Join(cfg.Backends, ",")
	if v, ok := lookupEnvValue(getenv, "LB_BACKENDS"); ok {
		backendsValue = v
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
		{"LB_FLAP_WINDOW", &cfg.FlapWindow},
		{"LB_QUARANTINE_BASE", &cfg.QuarantineBase},
		{"LB_QUARANTINE_MAX", &cfg.QuarantineMax},
		{"LB_QUARANTINE_RESET_AFTER", &cfg.QuarantineResetAfter},
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
	thresholdSettings := []struct {
		name   string
		target *int
	}{
		{"LB_HEALTH_FAIL_THRESHOLD", &cfg.FailThreshold},
		{"LB_HEALTH_RISE_THRESHOLD", &cfg.RiseThreshold},
	}
	for _, setting := range thresholdSettings {
		if value, ok := lookupEnvValue(getenv, setting.name); ok {
			threshold, err := strconv.Atoi(value)
			if err != nil {
				parseProblems = append(parseProblems, fmt.Errorf("%s: %q must be an integer from 1 to 10", setting.name, value))
				continue
			}
			*setting.target = threshold
		}
	}
	if value, ok := lookupEnvValue(getenv, "LB_HEALTH_PATH"); ok {
		cfg.HealthPath = value
	}
	integerSettings := []struct {
		name   string
		target *int
	}{
		{"LB_FLAP_MAX_TRANSITIONS", &cfg.FlapMaxTransitions},
		{"LB_MAX_QUARANTINE_PERCENT", &cfg.MaxQuarantinePercent},
	}
	for _, setting := range integerSettings {
		if value, ok := lookupEnvValue(getenv, setting.name); ok {
			parsed, err := strconv.Atoi(value)
			if err != nil {
				parseProblems = append(parseProblems, fmt.Errorf("%s: %q must be an integer", setting.name, value))
				continue
			}
			*setting.target = parsed
		}
	}

	fs := flag.NewFlagSet("loadbalancer-go", flag.ContinueOnError)
	var flagOutput bytes.Buffer
	fs.SetOutput(&flagOutput)
	listenAddr := cfg.ListenAddr
	fs.StringVar(&listenAddr, "listen", cfg.ListenAddr, "listen address")
	fs.StringVar(&backendsValue, "backends", backendsValue, "comma-separated backend URLs")
	fs.DurationVar(&cfg.HealthInterval, "health-interval", cfg.HealthInterval, "health check interval")
	fs.DurationVar(&cfg.HealthTimeout, "health-timeout", cfg.HealthTimeout, "health check HTTP client timeout")
	fs.IntVar(&cfg.FailThreshold, "health-fail-threshold", cfg.FailThreshold, "consecutive failures before marking a backend dead")
	fs.IntVar(&cfg.RiseThreshold, "health-rise-threshold", cfg.RiseThreshold, "consecutive active successes before recovering a backend")
	fs.StringVar(&cfg.HealthPath, "health-path", cfg.HealthPath, "path used for active health checks")
	fs.DurationVar(&cfg.FlapWindow, "flap-window", cfg.FlapWindow, "sliding window for backend state transitions")
	fs.IntVar(&cfg.FlapMaxTransitions, "flap-max-transitions", cfg.FlapMaxTransitions, "transitions in the flap window before quarantine; 0 disables")
	fs.DurationVar(&cfg.QuarantineBase, "quarantine-base", cfg.QuarantineBase, "initial quarantine hold-down")
	fs.DurationVar(&cfg.QuarantineMax, "quarantine-max", cfg.QuarantineMax, "maximum quarantine hold-down")
	fs.DurationVar(&cfg.QuarantineResetAfter, "quarantine-reset-after", cfg.QuarantineResetAfter, "alive duration after which quarantine level resets")
	fs.IntVar(&cfg.MaxQuarantinePercent, "max-quarantine-percent", cfg.MaxQuarantinePercent, "maximum percentage of registered backends quarantined")
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
	var backendParseProblems []error
	cfg.Backends, cfg.BackendHealth, backendParseProblems = parseBackendSpecs(backendsValue)
	parseProblems = append(parseProblems, backendParseProblems...)
	return cfg, errors.Join(parseProblems...)
}

func parseBackendSpecs(value string) ([]string, map[string]BackendHealth, []error) {
	backends := make([]string, 0)
	health := make(map[string]BackendHealth)
	var problems []error
	allowed := "health_path, health_interval, health_timeout"

	for _, untrimmed := range strings.Split(value, ",") {
		entry := strings.TrimSpace(untrimmed)
		if entry == "" {
			continue
		}
		parts := strings.Split(entry, ";")
		rawURL := strings.TrimSpace(parts[0])
		options := BackendHealth{}
		keys := make(map[string]struct{})
		for _, rawOption := range parts[1:] {
			option := strings.TrimSpace(rawOption)
			if option == "" {
				continue
			}
			keyValue := strings.SplitN(option, "=", 2)
			key := strings.ToLower(strings.TrimSpace(keyValue[0]))
			if key == "" || len(keyValue) != 2 || strings.TrimSpace(keyValue[1]) == "" {
				problems = append(problems, fmt.Errorf("LB_BACKENDS: %q has an empty option value; expected key=value (%s)", entry, allowed))
				continue
			}
			if _, exists := keys[key]; exists {
				problems = append(problems, fmt.Errorf("LB_BACKENDS: %q repeats option %q; each option key may appear once", entry, key))
				continue
			}
			keys[key] = struct{}{}
			optionValue := strings.TrimSpace(keyValue[1])
			switch key {
			case "health_path":
				options.Path = optionValue
			case "health_interval", "health_timeout":
				duration, err := time.ParseDuration(optionValue)
				if err != nil {
					problems = append(problems, fmt.Errorf("LB_BACKENDS: %q has an invalid %s (use e.g. 2s, 500ms)", entry, key))
					continue
				}
				if key == "health_interval" {
					options.Interval = duration
				} else {
					options.Timeout = duration
				}
			default:
				problems = append(problems, fmt.Errorf("LB_BACKENDS: %q has unknown option %q; allowed options are %s", entry, key, allowed))
			}
		}

		normalized, _, err := normalizeBackendURL(rawURL)
		if err != nil {
			problems = append(problems, err)
			backends = append(backends, rawURL)
			continue
		}
		backends = append(backends, normalized)
		if options != (BackendHealth{}) {
			health[normalized] = options
		}
	}
	return backends, health, problems
}

const maxConfigDuration = 24 * time.Hour

func (c *Config) Validate() error {
	var problems []error

	if len(c.Backends) == 0 {
		problems = append(problems, fmt.Errorf("LB_BACKENDS: %q must contain at least one http(s) backend URL", strings.Join(c.Backends, ",")))
	}
	if c.FailThreshold < 1 || c.FailThreshold > 10 {
		problems = append(problems, fmt.Errorf("LB_HEALTH_FAIL_THRESHOLD: %q must be an integer from 1 to 10", strconv.Itoa(c.FailThreshold)))
	}
	if c.RiseThreshold < 1 || c.RiseThreshold > 10 {
		problems = append(problems, fmt.Errorf("LB_HEALTH_RISE_THRESHOLD: %q must be an integer from 1 to 10", strconv.Itoa(c.RiseThreshold)))
	}
	if !strings.HasPrefix(c.HealthPath, "/") || strings.ContainsAny(c.HealthPath, " \t\r\n?#") || utf8.RuneCountInString(c.HealthPath) > 200 {
		problems = append(problems, fmt.Errorf("LB_HEALTH_PATH: %q must start with /, contain no spaces, ? or #, and be at most 200 characters", c.HealthPath))
	}
	if c.FlapMaxTransitions != 0 && (c.FlapMaxTransitions < 2 || c.FlapMaxTransitions > 16) {
		problems = append(problems, fmt.Errorf("LB_FLAP_MAX_TRANSITIONS: %q must be 0 (disabled) or an integer from 2 to 16", strconv.Itoa(c.FlapMaxTransitions)))
	}
	if c.MaxQuarantinePercent < 1 || c.MaxQuarantinePercent > 100 {
		problems = append(problems, fmt.Errorf("LB_MAX_QUARANTINE_PERCENT: %q must be an integer from 1 to 100", strconv.Itoa(c.MaxQuarantinePercent)))
	}
	if c.FlapMaxTransitions != 0 {
		flapDurations := []struct {
			name  string
			value time.Duration
		}{
			{"LB_FLAP_WINDOW", c.FlapWindow},
			{"LB_QUARANTINE_BASE", c.QuarantineBase},
			{"LB_QUARANTINE_MAX", c.QuarantineMax},
			{"LB_QUARANTINE_RESET_AFTER", c.QuarantineResetAfter},
		}
		for _, setting := range flapDurations {
			if setting.value <= 0 || setting.value > maxConfigDuration {
				problems = append(problems, fmt.Errorf("%s: %q must be greater than 0 and no more than 24h", setting.name, setting.value))
			}
		}
		if c.QuarantineBase > c.QuarantineMax {
			problems = append(problems, fmt.Errorf("LB_QUARANTINE_BASE: %q must be less than or equal to LB_QUARANTINE_MAX %q", c.QuarantineBase, c.QuarantineMax))
		}
		largestInterval, largestBackend := c.largestEffectiveInterval()
		minimumWindow := largestInterval * time.Duration(c.FailThreshold+c.RiseThreshold)
		if minimumWindow > 0 && c.FlapWindow < minimumWindow {
			problems = append(problems, fmt.Errorf("LB_FLAP_WINDOW: %q must be at least the largest effective health_interval %q for backend %q times (LB_HEALTH_FAIL_THRESHOLD + LB_HEALTH_RISE_THRESHOLD) = %s", c.FlapWindow, largestInterval, largestBackend, minimumWindow))
		}
	}

	listenHost, listenPort, listenValid, listenProblems := validateListenAddr(c.ListenAddr)
	problems = append(problems, listenProblems...)

	seen := make(map[string]string, len(c.Backends))
	for index, raw := range c.Backends {
		normalized, u, err := normalizeBackendURL(raw)
		if err != nil {
			problems = append(problems, err)
			continue
		}

		c.Backends[index] = normalized
		effective := c.EffectiveBackendHealth(normalized)
		if !validHealthPath(effective.Path) {
			problems = append(problems, fmt.Errorf("LB_BACKENDS: backend %q health_path (LB_HEALTH_PATH) %q must start with /, contain no spaces, ? or #, and be at most 200 characters", normalized, effective.Path))
		}
		if effective.Interval <= 0 || effective.Interval > maxConfigDuration {
			problems = append(problems, fmt.Errorf("LB_BACKENDS: backend %q health_interval (LB_HEALTH_INTERVAL) %q must be greater than 0 and no more than 24h", normalized, effective.Interval))
		} else if override := c.BackendHealth[normalized]; override.Interval != 0 && override.Interval < 100*time.Millisecond {
			problems = append(problems, fmt.Errorf("LB_BACKENDS: backend %q health_interval %q is below the minimum 100ms", normalized, override.Interval))
		}
		if effective.Timeout <= 0 || effective.Timeout > maxConfigDuration {
			problems = append(problems, fmt.Errorf("LB_BACKENDS: backend %q health_timeout (LB_HEALTH_TIMEOUT) %q must be greater than 0 and no more than 24h", normalized, effective.Timeout))
		}
		if effective.Interval > 0 && effective.Timeout >= effective.Interval {
			problems = append(problems, fmt.Errorf("LB_BACKENDS: backend %q health_timeout (LB_HEALTH_TIMEOUT) %q must be strictly less than health_interval (LB_HEALTH_INTERVAL) %q", normalized, effective.Timeout, effective.Interval))
		}
		if previous, ok := seen[normalized]; ok {
			problems = append(problems, fmt.Errorf("LB_BACKENDS: %q duplicates %q after URL normalization; each backend must be unique", raw, previous))
		} else {
			seen[normalized] = raw
		}
		if listenValid && (listenHost == "" || localHost(listenHost)) && localHost(u.Hostname()) && u.Port() == strconv.Itoa(listenPort) {
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

	if durationValid["LB_DIAL_TIMEOUT"] && durationValid["LB_RESPONSE_TIMEOUT"] && c.DialTimeout > c.ResponseTimeout {
		problems = append(problems, fmt.Errorf("LB_DIAL_TIMEOUT: %q must be less than or equal to LB_RESPONSE_TIMEOUT %q", c.DialTimeout, c.ResponseTimeout))
	}
	if durationValid["LB_READ_HEADER_TIMEOUT"] && durationValid["LB_READ_TIMEOUT"] && c.ReadHeaderTimeout > c.ReadTimeout {
		problems = append(problems, fmt.Errorf("LB_READ_HEADER_TIMEOUT: %q must be less than or equal to LB_READ_TIMEOUT %q", c.ReadHeaderTimeout, c.ReadTimeout))
	}

	return errors.Join(problems...)
}

func validHealthPath(path string) bool {
	return strings.HasPrefix(path, "/") && !strings.ContainsAny(path, " \t\r\n?#") && utf8.RuneCountInString(path) <= 200
}

func (c Config) largestEffectiveInterval() (time.Duration, string) {
	largest := time.Duration(0)
	backendURL := ""
	for _, rawURL := range c.Backends {
		normalized, _, err := normalizeBackendURL(rawURL)
		if err != nil {
			continue
		}
		effective := c.EffectiveBackendHealth(normalized)
		if effective.Interval > largest {
			largest = effective.Interval
			backendURL = normalized
		}
	}
	return largest, backendURL
}

func normalizeBackendURL(raw string) (string, *url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil {
		if strings.Contains(err.Error(), "port") {
			return "", nil, fmt.Errorf("LB_BACKENDS: %q has an invalid port; use a numeric port from 1 to 65535", raw)
		}
		return "", nil, fmt.Errorf("LB_BACKENDS: %q is not a valid URL: %v", raw, err)
	}

	problems := make([]error, 0)
	scheme := strings.ToLower(u.Scheme)
	if scheme != "http" && scheme != "https" {
		problems = append(problems, fmt.Errorf("LB_BACKENDS: %q must use the http or https scheme", raw))
	}
	if u.Hostname() == "" {
		problems = append(problems, fmt.Errorf("LB_BACKENDS: %q must include a non-empty hostname", raw))
	} else if !validHost(u.Hostname()) {
		problems = append(problems, fmt.Errorf("LB_BACKENDS: %q has an invalid hostname; use an IP address or hostname without spaces", raw))
	}
	if u.User != nil {
		problems = append(problems, fmt.Errorf("LB_BACKENDS: %q must not contain user information", raw))
	}
	if u.RawQuery != "" || u.ForceQuery {
		problems = append(problems, fmt.Errorf("LB_BACKENDS: %q must not contain a query string", raw))
	}
	if strings.Contains(raw, "#") || u.Fragment != "" || u.RawFragment != "" {
		problems = append(problems, fmt.Errorf("LB_BACKENDS: %q must not contain a fragment", raw))
	}
	if u.Path != "" && u.Path != "/" {
		problems = append(problems, fmt.Errorf("LB_BACKENDS: %q must be a base address with an empty path or /", raw))
	}

	port := u.Port()
	if strings.HasSuffix(u.Host, ":") {
		problems = append(problems, fmt.Errorf("LB_BACKENDS: %q has an empty port; use a numeric port from 1 to 65535", raw))
	} else if port != "" {
		parsedPort, portErr := parsePort(port)
		if portErr != nil {
			problems = append(problems, fmt.Errorf("LB_BACKENDS: %q has an invalid port %q; use a numeric port from 1 to 65535", raw, port))
		} else {
			port = strconv.Itoa(parsedPort)
		}
	} else if scheme == "http" {
		port = "80"
	} else if scheme == "https" {
		port = "443"
	}
	if len(problems) != 0 {
		return "", nil, errors.Join(problems...)
	}

	host := strings.ToLower(u.Hostname())
	normalized := scheme + "://" + net.JoinHostPort(host, port)
	parsed, err := url.Parse(normalized)
	if err != nil {
		return "", nil, fmt.Errorf("LB_BACKENDS: %q could not be normalized: %v", raw, err)
	}
	return normalized, parsed, nil
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
	var warnings []string
	if c.ShutdownTimeout > 0 && c.ResponseTimeout > 0 && c.ShutdownTimeout < c.ResponseTimeout {
		warnings = append(warnings, fmt.Sprintf("LB_SHUTDOWN_TIMEOUT %q is shorter than LB_RESPONSE_TIMEOUT %q; in-flight requests may be cut off during shutdown", c.ShutdownTimeout, c.ResponseTimeout))
	}
	for _, backendURL := range c.Backends {
		normalized, _, err := normalizeBackendURL(backendURL)
		if err != nil {
			normalized = backendURL
		}
		effective := c.EffectiveBackendHealth(normalized)
		if effective.Interval > 0 && c.FailThreshold > 0 {
			detectionTime := effective.Interval * time.Duration(c.FailThreshold)
			if detectionTime > c.ResponseTimeout*3 || detectionTime > 60*time.Second {
				warnings = append(warnings, fmt.Sprintf("backend %q: effective health_interval %q times LB_HEALTH_FAIL_THRESHOLD %d means dead backends may take %s to detect", normalized, effective.Interval, c.FailThreshold, detectionTime))
			}
		}
		if c.FlapMaxTransitions != 0 && effective.Interval > 0 && c.RiseThreshold > 0 && c.QuarantineBase < effective.Interval*time.Duration(c.RiseThreshold) {
			warnings = append(warnings, fmt.Sprintf("backend %q: LB_QUARANTINE_BASE %q is shorter than effective health_interval * LB_HEALTH_RISE_THRESHOLD (%s); the hold-down may end before normal recovery is possible", normalized, c.QuarantineBase, effective.Interval*time.Duration(c.RiseThreshold)))
		}
	}
	return warnings
}

func Preflight(c Config) error {
	listener, err := net.Listen("tcp", c.ListenAddr)
	if err != nil {
		return fmt.Errorf("cannot listen on %s: %w", c.ListenAddr, err)
	}
	return listener.Close()
}

func (c Config) String() string {
	backends := make([]string, 0, len(c.Backends))
	for _, backendURL := range c.Backends {
		entry := backendURL
		if health, exists := c.BackendHealth[backendURL]; exists {
			options := make([]string, 0, 3)
			if health.Path != "" {
				options = append(options, "path="+health.Path)
			}
			if health.Interval != 0 {
				options = append(options, "interval="+health.Interval.String())
			}
			if health.Timeout != 0 {
				options = append(options, "timeout="+health.Timeout.String())
			}
			if len(options) > 0 {
				entry += "(" + strings.Join(options, " ") + ")"
			}
		}
		backends = append(backends, entry)
	}
	return fmt.Sprintf(
		"listen=%s backends=%v healthInterval=%s healthTimeout=%s healthFailThreshold=%d healthRiseThreshold=%d healthPath=%s flapWindow=%s flapMaxTransitions=%d quarantineBase=%s quarantineMax=%s quarantineResetAfter=%s maxQuarantinePercent=%d dialTimeout=%s responseTimeout=%s readHeaderTimeout=%s readTimeout=%s writeTimeout=%s idleTimeout=%s shutdownTimeout=%s",
		c.ListenAddr,
		backends,
		c.HealthInterval,
		c.HealthTimeout,
		c.FailThreshold,
		c.RiseThreshold,
		c.HealthPath,
		c.FlapWindow,
		c.FlapMaxTransitions,
		c.QuarantineBase,
		c.QuarantineMax,
		c.QuarantineResetAfter,
		c.MaxQuarantinePercent,
		c.DialTimeout,
		c.ResponseTimeout,
		c.ReadHeaderTimeout,
		c.ReadTimeout,
		c.WriteTimeout,
		c.IdleTimeout,
		c.ShutdownTimeout,
	)
}
