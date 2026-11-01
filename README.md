# loadbalancer-go

A lightweight HTTP load balancer and reverse proxy written in Go, standard library only. It distributes requests across backend servers, checks their health actively and passively, protects against flapping backends, and shuts down gracefully.

**Status:** Phase 0 complete. Phase 1 includes the backend registry, health checks, flap protection, per-backend health settings, optional least-connections, and safe opt-in retries. A retry budget remains open. Admin API is planned for Phase 2.

## Features

- **Selectable load balancing:** round-robin by default, or least-connections across healthy backends.
- **Active health checks** with a configurable interval, timeout and path, set globally or per backend. A probe succeeds on status `200..399`; redirects are not followed.
- **Failure and recovery thresholds:** a backend is marked dead after `N` consecutive failed probes and revived only after `M` consecutive successful probes. Real traffic never revives a dead backend.
- **Passive detection:** hard dial failures (for example connection refused) mark a backend dead immediately. Other proxy failures count toward the failure threshold.
- **Flap protection:** a backend that keeps going up and down is quarantined with an exponentially growing hold-down, with a safety limit on how much capacity can be quarantined.
- **Backend registry:** one component owns backend data and state, with lock-free reads on the request path.
- **Timeouts:** dial, response-header, and server read, write and idle timeouts.
- **Graceful shutdown:** on `SIGINT` or `SIGTERM` it stops accepting connections, waits for in-flight requests up to a deadline, then force-closes.
- **Config validation:** layered configuration (defaults, environment, flags) validated before startup, with a dry-run mode.
- **Demo backend** with a slow endpoint and testing aids.

## Requirements

- Go version declared in [`go.mod`](./go.mod) (`1.26.8` at the time of writing)
- `gcc` (needed by `go test -race`)

## Quick start

```sh
./start.sh
```

The script builds the binaries once, starts three demo backends on ports `9001`, `9002` and `9003`, then starts the load balancer on `:8080`. Press `Ctrl+C` to stop; the balancer drains in-flight requests before the backends are stopped.

Or run each component in its own terminal:

```sh
go run ./cmd/backend -port 9001
go run ./cmd/backend -port 9002
go run ./cmd/backend -port 9003
go run ./cmd/lb
```

Try it:

```sh
for i in 1 2 3 4 5 6; do curl -s localhost:8080; done
```

Each response names the backend port, so you can see the rotation. The demo backend also has a slow endpoint that waits 15 seconds:

```sh
curl -i localhost:8080/slow
```

With the default `LB_RESPONSE_TIMEOUT` of `10s` this returns `502`; raise the timeout to see it succeed.

## Architecture

```text
                          +------------------+
 Client --> Handler --> Pool (selection) --> Registry --> Backends
                                              ^
                  Health scheduler -----------+
                  (probes, thresholds, quarantine)
```

- **Handler** picks a backend from the pool and proxies the request.
- **Pool** selects a healthy backend from the registry snapshot with the configured strategy.
- **Registry** owns every backend and its state (alive, dead or quarantined, counters, last check).
- **Health scheduler** probes backends and updates their state in the registry.

## Load-balancing strategies

`round_robin` is the default and preserves the existing rotating order. Select `least_conns` to prefer the selectable backend with the fewest active requests. Use either the `LB_STRATEGY` environment variable or the `-strategy` flag; flag values override environment values. Values are case-insensitive.

```sh
LB_STRATEGY=least_conns go run ./cmd/lb
go run ./cmd/lb -strategy least_conns
```

When candidates tie, least-connections uses an atomic rotating index so sequential low-volume traffic does not stick to the first backend. It reserves the chosen backend's active-request count with compare-and-swap during selection. This closes the race where concurrent requests all observe the same old count; the handler's defer is the only place that releases the reservation. Round-robin still increments its count in the handler, as before.

Least-connections helps when request durations vary: a backend handling a slow request temporarily receives fewer new requests. With uniformly short requests, counts quickly return to zero, so behavior resembles round-robin. Counts are in memory and local to each balancer instance; backend weights are not supported.

## Configuration

Settings come from three layers, lowest to highest precedence: built-in defaults, environment variables, command-line flags. Effective values are logged at startup.

| Setting | Env var | Flag | Default |
| --- | --- | --- | --- |
| Listen address | `LB_LISTEN_ADDR` | `-listen` | `:8080` |
| Backends | `LB_BACKENDS` | `-backends` | `http://localhost:9001,http://localhost:9002,http://localhost:9003` |
| Load-balancing strategy | `LB_STRATEGY` | `-strategy` | `round_robin` |
| Maximum safe retries | `LB_MAX_RETRIES` | `-max-retries` | `0` |
| Health interval | `LB_HEALTH_INTERVAL` | `-health-interval` | `5s` |
| Health timeout | `LB_HEALTH_TIMEOUT` | `-health-timeout` | `2s` |
| Health path | `LB_HEALTH_PATH` | `-health-path` | `/` |
| Health fail threshold | `LB_HEALTH_FAIL_THRESHOLD` | `-health-fail-threshold` | `3` |
| Health rise threshold | `LB_HEALTH_RISE_THRESHOLD` | `-health-rise-threshold` | `2` |
| Flap window | `LB_FLAP_WINDOW` | `-flap-window` | `60s` |
| Flap max transitions (`0` disables) | `LB_FLAP_MAX_TRANSITIONS` | `-flap-max-transitions` | `4` |
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
| Dry run | `LB_CHECK_ONLY` | `-check` | `false` |

[`.env.example`](./.env.example) lists every variable with its default. The program reads real environment variables, so export them in your shell. Use `go run ./cmd/lb -h` for the built-in usage text.

```sh
LB_LISTEN_ADDR=:9090 go run ./cmd/lb
go run ./cmd/lb -listen :9191 -backends http://localhost:9001,http://localhost:9002
LB_BACKENDS=http://localhost:9001,http://localhost:9002 go run ./cmd/lb -health-timeout 1s
```

### Backend list syntax

`LB_BACKENDS` is a comma-separated list. Whitespace is trimmed and empty entries are ignored. Each entry may carry per-backend health options after the URL, separated by `;`:

```text
http://localhost:9001
http://localhost:9001;health_path=/healthz
http://localhost:9001;health_path=/healthz;health_interval=2s;health_timeout=500ms
```

| Option | Inherits | Rules |
| --- | --- | --- |
| `health_path` | `LB_HEALTH_PATH` | Starts with `/`; no spaces, `?` or `#`; at most 200 characters |
| `health_interval` | `LB_HEALTH_INTERVAL` | Greater than zero, at most `24h`, and at least `100ms` |
| `health_timeout` | `LB_HEALTH_TIMEOUT` | Greater than zero, at most `24h`, strictly less than the backend's effective interval |

Option names are case-insensitive. Unknown or repeated options, empty values and malformed durations are configuration errors. Thresholds and flap settings stay global.

### Validation

Configuration is validated before anything starts, so mistakes fail at startup rather than on the first request. All problems are reported together.

| Setting | Rule |
| --- | --- |
| Listen address | `host:port`; host empty, an IP or a valid hostname; port numeric in `1..65535` (port `0` rejected) |
| Backends | At least one; scheme `http` or `https`; hostname present; no user info, query, fragment or path other than `/`; port numeric in `1..65535` |
| Backend duplicates | Compared after lowercasing scheme and host, removing a trailing `/`, and applying default ports; options do not make duplicates distinct |
| Self proxy | A local backend (`localhost`, `127.0.0.1`, `::1`, `0.0.0.0`) must not use the balancer's own listen port |
| Durations | Must parse, be greater than zero, and be at most `24h` |
| Health timing | Effective timeout strictly less than effective interval, per backend |
| Dial vs response | `LB_DIAL_TIMEOUT` at most `LB_RESPONSE_TIMEOUT` |
| Read timeouts | `LB_READ_HEADER_TIMEOUT` at most `LB_READ_TIMEOUT` |
| Thresholds | Integers in `1..10` |
| `LB_MAX_RETRIES` | Integer in `0..3`; retries are disabled at `0` |
| Health path | Starts with `/`; no spaces, `?` or `#`; at most 200 characters |
| Flap settings | `LB_FLAP_MAX_TRANSITIONS` is `0` or an integer in `2..16`; `LB_MAX_QUARANTINE_PERCENT` in `1..100`; base at most max; flap window at least `interval * (fail + rise thresholds)` using the largest backend interval |
| `LB_CHECK_ONLY` | Must be a boolean |

Exit codes: configuration error `2` (printed with a `config error:` prefix), preflight failure `1` (`preflight error:`), help `0`. Warnings (`warning:` prefix) do not stop startup, for example a shutdown timeout shorter than the response timeout, a long failure-detection time, a quarantine hold shorter than the recovery time, or `LB_MAX_RETRIES` greater than or equal to the configured backend count.

Dry run validates the configuration and checks that the listen address can be bound, without starting the balancer. Use it in CI or before deploying:

```sh
go run ./cmd/lb -check
LB_CHECK_ONLY=true go run ./cmd/lb
```

Backends do not need to be reachable at startup; health checks own reachability. At startup, hostnames are resolved with a 2 second timeout per backend and failures only produce warnings.

## Failure responses

- `503 Service Unavailable` with `no healthy backends` when no backend is selectable, meaning all are dead or quarantined.
- `502 Bad Gateway` with `backend unavailable` when the selected backend cannot be reached or its response times out.

## Request failure policy

Retries are opt-in through `LB_MAX_RETRIES` or `-max-retries`, with a range of `0..3` and default `0`. Each retry selects a different currently selectable backend; the request-local tried set prevents selecting the same backend twice. A retry is permitted only when all of these conditions hold:

- The proxy error proves a dial-level failure: connection refused, DNS failure, connect/dial timeout, or network/host unreachable. Classification uses wrapped error types and syscall values, never error text.
- The method is `GET`, `HEAD`, or `OPTIONS`.
- The request has no body (`nil`, `http.NoBody`, or `ContentLength == 0`).
- The client context is still live and no response output has started.
- Fewer than `LB_MAX_RETRIES + 1` total attempts have been used.

| Failure type | Retried? | Why |
| --- | --- | --- |
| Connection refused, DNS failure, dial timeout, network/host unreachable | Yes, only if all safe-retry conditions hold and another untried backend is selectable | These failures establish that the request was not delivered to the backend. |
| Response-header timeout | No | The backend may already have received the request. |
| Failure during response-body transfer | No | A response may already be partially written. |
| Any backend status, including `5xx` | No | The backend answered; its status and body pass through unchanged. |
| Other methods or requests with a body | No | The request is not eligible for replay. |
| Client cancellation | No | Cancellation is not a backend failure. |
| Other proxy/transport failures | No | Delivery is uncertain, so the policy fails closed. |

No selectable backend at the start still returns `503 no healthy backends`. A failed final attempt, including a timeout, returns `502 backend unavailable`; this policy does not use `504`. If no untried selectable backend remains, the request ends with `502`. `Pool.RetryStats()` exposes atomic counts for retries performed and requests exhausted.

The Go `http.Transport` can itself transparently retry some idempotent requests on reused connections. Those transport-level retries are separate from this application retry policy and are not disabled. There is no retry budget yet, non-idempotent methods are never retried even when an idempotency key is present, and `502` remains the response for timeouts too.

## How it works

### Backend registry

The registry is the single source of truth for backend objects and their runtime state. The backend slice is published through an atomic pointer to an immutable snapshot. Requests and health checks read it without taking a lock; `Add` and `Remove` serialize under a mutex and publish a copied slice. This favors frequent reads over rare updates, at the cost of a copy proportional to the number of backends on each change.

Removing a backend stops future selection and checking and closes its idle connections. A request already in flight on it is allowed to finish.

`BackendStatus` is a detached, JSON-friendly snapshot: `URL`, `Alive`, `State` (`alive`, `dead` or `quarantined`), `ConsecutiveFailures`, `ConsecutiveSuccesses`, `TotalRequests`, `TotalFailures`, `ActiveConns`, `LastCheck`, `LastError`, `LastFailureKind`, `LastStateChange`, `AddedAt`, `QuarantinedUntil`, `QuarantineLevel`, `RecentTransitions`, plus the effective `HealthPath`, `HealthInterval`, `HealthTimeout` and `NextCheckAt`.

Registry changes are in memory only and are lost on restart.

### Health checks

```text
                 FailThreshold consecutive failures
        +-------+ -------------------------------> +------+
        | ALIVE |                                  | DEAD |
        +-------+ <------------------------------- +------+
                 RiseThreshold consecutive successes
                      (active checks only)

ALIVE + active success   -> stay ALIVE, reset failure streak
DEAD  + active failure   -> stay DEAD, reset success streak
passive hard dial failure -> ALIVE to DEAD immediately
```

A dead backend is revived only by consecutive successful active probes, never by production traffic, so clients do not pay for the probing.

Probe failures are classified without matching error strings:

| Kind | Meaning |
| --- | --- |
| `timeout` | The probe or network operation exceeded its deadline |
| `connection_refused` | The target refused the connection |
| `dns` | Name resolution failed |
| `bad_status` | Status outside `200..399` |
| `other` | Any other request, transport or body error |

Detection time is about `interval * FailThreshold` and recovery about `interval * RiseThreshold`, plus up to one interval depending on when the change happens. With the defaults that is roughly `5s * 3 = 15s` to detect and `5s * 2 = 10s` to recover.

Probes are bounded and isolated: each runs with its own timeout covering connect, headers and body drain (at most 4 KB read), backends are probed in parallel so one slow backend does not delay the others, and an atomic per-backend guard skips a probe that would overlap a previous one. Probes use a dedicated transport so they do not share connections with proxy traffic. State changes are logged once per transition, not per check.

### Per-backend scheduling

The scheduler computes the earliest `NextCheckAt` across backends, waits on a timer capped at one second (so backends added at runtime are noticed quickly), then probes only the backends that are due. The next deadline is the probe's start time plus that backend's interval, so a slow probe does not stretch its own period. With `FailThreshold=3`, a backend with a `1s` interval is detected in about `3s`, while one with a `10s` interval takes about `30s`. `CheckOnce()` probes every backend immediately regardless of schedule.

### Flap protection

Thresholds stop fast flapping. Quarantine stops slow flapping: a backend that is down 15 seconds, up 10 seconds and repeats can pass the thresholds every cycle. If alive/dead transitions reach `LB_FLAP_MAX_TRANSITIONS` inside `LB_FLAP_WINDOW`, the backend is quarantined. It is treated as dead, keeps being probed, but cannot return before the hold-down ends, and then still needs the normal `RiseThreshold` successes.

```text
ALIVE --fail threshold--> DEAD --rise threshold--> ALIVE
                                         |
                       flap limit reached on recovery
                                         v
                                   QUARANTINED --hold expires--> DEAD (needs rise threshold)
```

The first hold is `LB_QUARANTINE_BASE`; each following hold doubles up to `LB_QUARANTINE_MAX`. With the defaults: `30s`, `60s`, `120s`, `240s`, then capped at `5m`. The level resets after `LB_QUARANTINE_RESET_AFTER` of stable uptime.

Safety limit: at most `floor(total * LB_MAX_QUARANTINE_PERCENT / 100)` backends may be quarantined, and at least one healthy non-quarantined backend must remain, so a single backend is never quarantined. The limit applies only to quarantine; genuinely failing backends are still ejected normally. Setting `LB_FLAP_MAX_TRANSITIONS=0` disables the feature. The clock is injected so tests drive time deterministically.

### Graceful shutdown

On `SIGINT` or `SIGTERM` the balancer stops accepting new connections and waits for in-flight requests. If `LB_SHUTDOWN_TIMEOUT` is reached it force-closes the rest. The health scheduler also stops through context cancellation. Choose a timeout at least as long as your slowest normal request and shorter than your orchestrator's kill timeout (for Kubernetes, `terminationGracePeriodSeconds`).

`start.sh` runs the compiled binaries directly, avoiding a `go run` wrapper that would sit between the script and the balancer. On a signal it forwards it to the balancer, waits for it to finish draining, and only then stops the backends, so in-flight requests can still reach their upstreams.

Limitation: `http.Server.Shutdown` does not wait for hijacked connections such as WebSockets. In a real deployment the layer in front should stop sending traffic first.

Manual check, with timeouts longer than the 15 second `/slow` endpoint:

```sh
LB_RESPONSE_TIMEOUT=25s LB_SHUTDOWN_TIMEOUT=25s ./start.sh
# in another terminal:
curl -i localhost:8080/slow & sleep 3; pkill -TERM -f bin/lb; wait
```

The request completes with `200`, and the log shows `shutdown signal received` followed by `shutdown complete` roughly 12 seconds later.

## Testing

```sh
go vet ./...
go test ./cmd/lb
go test -race ./...
go test -race -count=3 ./cmd/lb
```

Tests live in `cmd/lb/main_test.go`, `cmd/lb/config_test.go` and `cmd/lb/registry_test.go`, and use only the standard library. Fake backends are `httptest` servers that can be flipped between healthy and unhealthy. Time-dependent logic (health windows, scheduling, quarantine) uses an injected fake clock instead of sleeping. The race detector only reports races that execute, so the concurrency tests deliberately create heavy parallel load.

Test design notes: readiness checks use a TCP dial because an HTTP probe would pass through the balancer to the blocking fake backend. Blocking handlers also watch the request context, and the release channel is closed before the test server so cleanup cannot deadlock. The in-flight shutdown test waits for an "arrived" signal rather than sleeping.

| Area | Tests |
| --- | --- |
| Routing | `TestRoundRobin`, `TestNoBackends`, `TestAllBackendsDown`, `TestConcurrentRequests`, `TestTimeout` |
| Round-robin compatibility | `TestRoundRobinMaintainsRotationWhenBackendUnavailable` verifies the existing counter phase is retained while a backend is dead. |
| Least-connections | `TestLeastConnsPicksLowestActiveCount` verifies lowest active count; `TestLeastConnsTieBreakRotates` verifies rotating ties; `TestLeastConnsSkipsDeadAndQuarantinedBackends` verifies eligibility filtering; `TestLeastConnsRoutesAroundBusyBackend` verifies slow-request avoidance and resumed spreading; `TestLeastConnsReservationsReleaseOnFailureAndCancellation` verifies no counter leak or double increment; `TestLeastConnsConcurrentRequestsSpreadAcrossBackends` verifies bounded parallel distribution; `TestLeastConnsRegistryRaceDuringTraffic` exercises add/remove/toggle/status during traffic and constrains status codes. |
| Request failure policy | Safe retry, exclusions, exhaustion, counters, and races; see the test list below. |
| Failover | `TestFailover`, `TestFailover_PassiveDetection`, `TestPassiveHardFailureRequiresActiveRecovery`, `TestPassiveSoftFailureUsesThreshold` |
| Concurrency | `TestRace_StateUpdatesDuringRequests`, `TestRace_SetAliveIsAlive`, `TestNext_ConcurrentDistribution`, `TestNext_NeverReturnsDeadBackend`, `TestRegistryRaceConcurrentChanges` |
| Graceful shutdown | `TestGracefulShutdown_FinishesInFlightRequest`, `TestGracefulShutdown_RejectsNewRequests`, `TestGracefulShutdown_TimeoutForcesClose`, `TestGracefulShutdown_NoRequests`, `TestHealthCheckStopsOnContextCancel` |
| Registry | `TestRegistryAddAndNormalize`, `TestRegistryRemoveInFlight`, `TestRegistryListSnapshotAndOrder`, `TestRegistrySetAliveAndHealthy`, `TestPoolDynamicRoundRobin`, `TestPoolHealthCheckIncludesAddedBackend`, `TestBackendCountersAndHealthRecovery`, `TestPoolEmptyRegistry` |
| Health checks | `TestHealthFailureThresholdAndReset`, `TestHealthRecoveryThresholdAndFlapping`, `TestHealthFailureKindsAndRedirect`, `TestHealthTimeoutBound`, `TestHealthChecksRunInParallel`, `TestHealthCheckDoesNotOverlap`, `TestHealthCheckLoopCancellationAbortsProbe`, `TestHealthPathIsUsed`, `TestFailureClassification` |
| Per-backend health | `TestBackendHealthSpecParsing`, `TestBackendHealthSpecErrors`, `TestBackendHealthEffectiveValuesAndNormalizedDuplicates`, `TestBackendHealthEffectiveValidationAndLargestFlapInterval`, `TestBackendHealthWarningsArePerBackend`, `TestPerBackendHealthPaths`, `TestPerBackendHealthTimeouts`, `TestCheckDueUsesPerBackendIntervals`, `TestCheckDueDynamicAddRemoveAndCheckOnceAll`, `TestCheckDueNonOverlapAcrossIntervals`, `TestPerBackendStatusShowsEffectiveSchedule`, `TestPerBackendSchedulerCancellation`, `TestPerBackendSchedulerRegistryRace` |
| Flap protection | `TestFlapProtectionDisabledKeepsThresholdBehavior`, `TestFourTransitionsQuarantineAndHoldDown`, `TestExpiredQuarantineStillRequiresRiseThreshold`, `TestQuarantineExponentialGrowthAndCap`, `TestQuarantineLevelResetsAfterStableAlivePeriod`, `TestTransitionsOutsideWindowDoNotQuarantine`, `TestQuarantineCapacitySafety`, `TestCapacityLimitDoesNotBlockRealFailureEjection`, `TestQuarantineTransitionLogsOnlyOnce`, `TestFlapRegistryRace` |
| Configuration | `TestLoadConfig_Defaults`, `TestLoadConfig_EnvOverridesDefaults`, `TestLoadConfig_FlagsOverrideEnv`, `TestLoadConfig_CheckOnly`, `TestLoadConfig_BackendsParsing`, `TestLoadConfig_InvalidDuration`, `TestLoadConfig_InvalidDurationNamesValue`, `TestLoadConfig_AggregatesInvalidDurations`, `TestLoadConfig_HelpFlag`, `TestValidate_*`, `TestHealthCheckConfigValidation`, `TestFlapConfigValidation`, `TestWarnings_*`, `TestPreflight` |

`TestStrategyConfig` verifies the default, env/flag precedence, case-insensitive values, invalid-value diagnostics, and aggregation with another config error.

`TestMaxRetriesConfig` verifies the default, env/flag precedence, invalid values and diagnostics, warning, effective config, and aggregation with another config error.

Retry-policy tests:

- `TestRetriesDisabledByDefault`: confirms the default still returns 502 without retrying.
- `TestRetryFailureClassificationUsesTypedErrors`: accepts typed dial/DNS/network failures and rejects response/read errors and matching error text.
- `TestSafeRetryAfterDialFailure`: retries a refused GET to a healthy distinct backend and verifies the first backend is marked dead.
- `TestSafeRetryHeadAndOptions`: verifies HEAD and OPTIONS can safely retry.
- `TestUnsafeMethodsAndBodyAreNotRetried`: verifies POST, PUT, PATCH, DELETE, and a body-bearing GET do not retry.
- `TestRetryDoesNotRepeatBackendAndTracksExhaustion`: verifies two dead backends receive one attempt each and stats report exhaustion.
- `TestResponseHeaderTimeoutIsNotRetried`: verifies a backend that received but did not answer the request is not retried.
- `TestBackendStatusFiveHundredIsNotRetried`: verifies an upstream 500 and its body pass through unchanged.
- `TestPartialResponseFailureIsNotRetried`: verifies a truncated response is not retried or written twice.
- `TestClientCancellationIsNotRetriedOrCountedAsFailure`: verifies cancellation creates no retry or backend failure.
- `TestRetryLeastConnsUsesUntriedBackend`: verifies least-connections selects the eligible backend not yet tried.
- `TestRetryConnectionAccountingForBothStrategies`: verifies success and exhaustion release every per-attempt active count.
- `TestParallelRetriesWithOneDeadBackend`: verifies 100 concurrent GETs succeed with one refused backend and one retry.
- `TestRetryRegistryRaceDuringTraffic`: exercises retries alongside registry add/remove/toggle and status snapshots.
- `TestMaxRetriesConfig`: verifies default, env/flag precedence, invalid values, diagnostics, warning, effective config, and aggregated errors.

## Test aids

These flags exist for demos and tests only and default to off:

- `cmd/backend -flaky-period 4s`: alternates healthy and `503` responses on `/` in periods measured from process start. `/slow` is unchanged.
- `cmd/backend -log-requests`: logs each request as a timestamp, method and path, which shows probe paths and schedules.

## Design decisions and known limits

- Passive hard dial failures mark a backend dead immediately; every other failure follows the configured threshold.
- Thresholds are consecutive counts, not a rolling window.
- Thresholds and flap settings are global; only health path, interval and timeout can be set per backend.
- No jitter on health checks.
- All state is in memory and resets on restart.
- Least-connections counts are in memory and per balancer instance; backend weights are not supported.
- Configuration is read once at startup; there is no hot reload.
- Quarantine never applies when only one backend is configured.
- Automatic retries are limited to proven dial failures on bodyless `GET`, `HEAD`, and `OPTIONS`; a failed final attempt returns `502`.

## Roadmap

- Completed: explicit request failure policy with opt-in dial-failure retries only for bodyless `GET`, `HEAD`, and `OPTIONS`.
- Future: add a retry budget to bound retry load during backend outages.
- Admin API on a separate port (Phase 2).

## Project layout

```text
cmd/
  backend/            Demo HTTP backend
  lb/
    main.go           Handler, pool strategies, safe retry policy, health scheduler and shutdown
    config.go         Configuration loading, parsing and validation
    registry.go       Backend registry, state machine and quarantine
    main_test.go      Routing, failover, shutdown and race tests
    config_test.go    Configuration and validation tests
    registry_test.go  Registry, health, scheduling and flap tests
start.sh              Builds and starts the demo backends and the balancer
.env.example          Every LB_ variable with its default
AGENTS.md              Concise source-grounded project map for future sessions
go.mod                Go module definition
```