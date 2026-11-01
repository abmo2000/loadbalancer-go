# Project Map

## Purpose and status
HTTP reverse proxy/load balancer using only the Go standard library. Phase 0 is complete; Phase 1 includes registry, active/passive health checks, flap quarantine, per-backend health settings, least-connections, and safe opt-in retries. A retry budget remains open; the admin API is Phase 2.

## Commands
- Build: `go build ./...`
- Vet: `go vet ./...`
- Tests: `go test ./...`
- Race tests: `go test -race -count=1 -timeout 180s ./cmd/lb`; never use `-count` above 3.
- Dry run: `go run ./cmd/lb -check` (validates and binds the configured listen address).
- Demo: `./start.sh` builds and starts ports 9001-9003 and 8080; check `ss -ltnp` first and do not touch existing processes on protected ports.
- Laptop rule: no CPU stress loops. Live demos only on isolated ports after checking `ss -ltnp`; never touch processes on 8080, 9001-9003, 18080, 18180, 18280, 18380, 19001-19102, 19201-19202, or 19301-19302.

## File map
- `cmd/lb/config.go`: configuration defaults, env/flag loading, parsing, validation, warnings, preflight, effective config string.
- `cmd/lb/config_test.go`: config parsing, precedence, validation, warnings, and preflight tests.
- `cmd/lb/main.go`: backend/proxy state, pool selection, health checks, request handler, server lifecycle, and process entry point.
- `cmd/lb/main_test.go`: round-robin, least-connections, safe retries, failover, requests, shutdown, concurrency, and pool-selection tests.
- `cmd/lb/registry.go`: backend registry, immutable snapshots, status, health-state transitions, failure policy, and quarantine.
- `cmd/lb/registry_test.go`: registry, health-check, passive failure, scheduling, race, and quarantine tests.
- `cmd/backend/main.go`: small demo backend with `/slow` and optional flaky/request logging modes.
- `start.sh`: builds binaries, launches demo backends and balancer, forwards signals, cleans up child backends.
- `.env.example`: documented environment-variable defaults and per-backend health example.
- `README.md`: user-facing architecture, config, behavior, health, quarantine, and shutdown documentation.
- `go.mod`: module path `loadbalancer-go` and required Go version.
- `.gitignore`: ignores generated binaries, local `.env`, editor folders, and `*.out`.
- `AGENTS.md`: concise source-grounded project map, commands, invariants, config, tests, and roadmap.

## Key types and functions
`cmd/lb/config.go`: `Config`, `LoadConfig`, `Validate`, `Warnings`, `Preflight`; `DefaultConfig` defines defaults. `cmd/lb/main.go`: `Backend`, `NewBackend`, `Pool`, `NewPool`, `Pool.Next`, `Pool.Handler`, `Pool.RetryStats`, `Pool.CheckOnce`, `Pool.CheckDue`, `Pool.HealthCheckContext`, `runServer`, `runServerWithListener`; `Strategy`, `roundRobinStrategy`, and `leastConnsStrategy` implement selection. `Pool.SetStrategy` configures one. `shouldRetryRequest` classifies retry eligibility; `Pool.proxyAttempt` wraps each attempt. `cmd/lb/registry.go`: `Registry`; `Add`, `AddWithOptions`, `Remove`, `Get`, `List`, `Healthy`, `SetAlive`. Failure classification is `classifyFailure` / `isHardProxyFailure` in `cmd/lb/main.go`. State transitions are `recordProxyFailure` / `recordActiveCheck` in `cmd/lb/registry.go`; quarantine helpers are `canQuarantineLocked`, `quarantineDuration`, and backend transition helpers in `cmd/lb/main.go`.

## Request path
Client -> `Pool.Handler` -> `Pool.nextExcluding` / `Strategy.Pick` -> `Registry.snapshot` and selectable backends -> `Backend.Proxy.ServeHTTP` -> proxy `ErrorHandler`. `shouldRetryRequest` in `cmd/lb/main.go` is the retry decision point; a per-request tried set excludes prior backends from selection. `Pool.proxyAttempt` counts each selected attempt, reserves `activeConns` once (least-connections in `Pick`, round-robin in the helper), and releases it in one defer even on errors or panic. `ErrorHandler` records failures without writing when an attempt holder is present; `Handler` writes a final 502 only after deciding not to retry. `Pool.RetryStats` exposes atomic retry and exhaustion counters. Public `Pool.Next` remains selection-only.

## Concurrency invariants
`Registry.mu` serializes registry mutations and health state transitions; backend fields guarded by `Backend.mu` are read/written under that mutex. The registry publishes immutable backend-slice copies through `atomic.Pointer`; request-path snapshot reads are lock-free. Request/failure/probe/retry counters, strategy rotation, least-connections reservations, and probe guards use atomics. No global lock is taken on request selection or retry paths. The per-request tried set is local to one handler invocation. `Backend.IsAlive` uses `Backend.mu` (not lock-free). Registry mutations copy and publish the slice; in-flight requests retain the backend pointer. `Registry.now` / `Backend.now` inject the clock used for deterministic health/quarantine tests.

## Behavior contracts
`NewPool` accepts an empty target list; `Next` returns nil and `Handler` returns 503 when no selectable backend exists. `Next` selects only and does not reserve a request; `Handler` owns request counts. Only dial-level errors can retry, and only for bodyless `GET`, `HEAD`, or `OPTIONS` with a live context and no response output; tried backends are excluded. Other methods, bodies, timeouts, response failures, and backend statuses are never retried. `CheckOnce` probes every registered backend regardless of schedule; `CheckDue` probes due backends only. Passive hard dial failures kill immediately; softer proxy failures use the threshold. Dead backends recover only through active probes. Quarantine does not apply when it would leave no healthy backend, so a single backend is never quarantined. Removed backends leave future snapshots but existing requests may finish. Default selection is round-robin. Least-connections rotates ties and reserves active requests during handler selection.

## Config surface
Validation is in `Config.Validate` (`cmd/lb/config.go`); parse-time errors aggregate in `LoadConfig`. All flags have matching env settings except flag-only `-check` also maps to `LB_CHECK_ONLY`.

| Setting | Env | Flag | Default |
| --- | --- | --- | --- |
| Listen address | `LB_LISTEN_ADDR` | `-listen` | `:8080` |
| Backends | `LB_BACKENDS` | `-backends` | localhost:9001,9002,9003 |
| Strategy | `LB_STRATEGY` | `-strategy` | `round_robin` |
| Maximum retries | `LB_MAX_RETRIES` | `-max-retries` | `0` |
| Health interval | `LB_HEALTH_INTERVAL` | `-health-interval` | `5s` |
| Health timeout | `LB_HEALTH_TIMEOUT` | `-health-timeout` | `2s` |
| Health path | `LB_HEALTH_PATH` | `-health-path` | `/` |
| Health fail threshold | `LB_HEALTH_FAIL_THRESHOLD` | `-health-fail-threshold` | `3` |
| Health rise threshold | `LB_HEALTH_RISE_THRESHOLD` | `-health-rise-threshold` | `2` |
| Flap window | `LB_FLAP_WINDOW` | `-flap-window` | `60s` |
| Flap max transitions | `LB_FLAP_MAX_TRANSITIONS` | `-flap-max-transitions` | `4` |
| Quarantine base | `LB_QUARANTINE_BASE` | `-quarantine-base` | `30s` |
| Quarantine max | `LB_QUARANTINE_MAX` | `-quarantine-max` | `5m` |
| Quarantine reset after | `LB_QUARANTINE_RESET_AFTER` | `-quarantine-reset-after` | `5m` |
| Max quarantine percent | `LB_MAX_QUARANTINE_PERCENT` | `-max-quarantine-percent` | `50` |
| Dial timeout | `LB_DIAL_TIMEOUT` | `-dial-timeout` | `3s` |
| Response timeout | `LB_RESPONSE_TIMEOUT` | `-response-timeout` | `10s` |
| Read-header timeout | `LB_READ_HEADER_TIMEOUT` | `-read-header-timeout` | `5s` |
| Read timeout | `LB_READ_TIMEOUT` | `-read-timeout` | `15s` |
| Write timeout | `LB_WRITE_TIMEOUT` | `-write-timeout` | `30s` |
| Idle timeout | `LB_IDLE_TIMEOUT` | `-idle-timeout` | `60s` |
| Shutdown timeout | `LB_SHUTDOWN_TIMEOUT` | `-shutdown-timeout` | `15s` |
| Check-only | `LB_CHECK_ONLY` | `-check` | `false` |

## Test conventions
Tests use the standard library, `httptest` servers, injected clocks, and free listener ports; avoid fixed ports and `time.Sleep` for synchronization. Never weaken existing assertions. `config_test.go` covers config; `main_test.go` covers pool/request/failover/shutdown; `registry_test.go` covers registry/health/quarantine/concurrency. `go test -race` requires `gcc`.

## Known limits and roadmap
Registry state and counters are in memory and reset on restart. No retry budget yet. No admin API; Phase 2. WebSocket hijacked connections are not drained by `http.Server.Shutdown`. Update this map at the end of future tasks if repository structure changes.
