# loadbalancer-go

A lightweight HTTP load balancer and reverse proxy written in Go. It distributes requests across three local backend servers, periodically checks their health, and avoids backends that are unavailable.

## Features

- **Round-robin balancing:** Requests are distributed across healthy backends using an atomic counter.
- **Health checks:** Every five seconds, the load balancer sends an HTTP `GET` to each backend. A backend is considered healthy when the request succeeds with a status code below `500`.
- **Automatic failover:** Unhealthy backends are skipped. A proxy error marks its backend unhealthy; health checks can mark it healthy again.
- **Reverse proxying:** Incoming requests are forwarded to the selected backend, including the request path.
- **Timeouts:** Configured connection, response-header, and server read/write/idle timeouts help bound slow or idle connections.
- **Demo backend:** Each backend identifies its port in responses and offers a slow endpoint for testing.

## Requirements

- Go version `1.26.8` or compatible with the version declared in [`go.mod`](./go.mod)

## Run

The simplest way to start all three backends and the load balancer is:

```sh
./start.sh
```

The script starts backends on ports `9001`, `9002`, and `9003`, then starts the load balancer on port `8080`. Press `Ctrl+C` to stop the processes.

Alternatively, run each component separately in its own terminal:

```sh
go run ./cmd/backend -port 9001
go run ./cmd/backend -port 9002
go run ./cmd/backend -port 9003
go run ./cmd/lb
```

The backend targets and the load balancer listen address are currently defined in code: `http://localhost:9001`, `http://localhost:9002`, `http://localhost:9003`, and `:8080`, respectively.

## Configuration

The load balancer reads configuration at startup from three layers, in precedence order:

1. built-in defaults
2. environment variables
3. command-line flags

The effective values are logged at startup and any invalid configuration exits with a non-zero status before the HTTP server begins accepting traffic.

### Supported settings

| Setting | Env var | Flag | Default | Purpose |
| --- | --- | --- | --- | --- |
| Listen address | `LB_LISTEN_ADDR` | `-listen` | `:8080` | Address the HTTP server binds to |
| Backends | `LB_BACKENDS` | `-backends` | `http://localhost:9001,http://localhost:9002,http://localhost:9003` | Comma-separated backend URLs |
| Health interval | `LB_HEALTH_INTERVAL` | `-health-interval` | `5s` | How often the pool probes backends |
| Health timeout | `LB_HEALTH_TIMEOUT` | `-health-timeout` | `2s` | Timeout for health-check HTTP requests |
| Health fail threshold | `LB_HEALTH_FAIL_THRESHOLD` | `-health-fail-threshold` | `3` | Consecutive failures before marking a backend dead |
| Health rise threshold | `LB_HEALTH_RISE_THRESHOLD` | `-health-rise-threshold` | `2` | Consecutive active successes before recovery |
| Health path | `LB_HEALTH_PATH` | `-health-path` | `/` | Path used for active health checks |
| Dial timeout | `LB_DIAL_TIMEOUT` | `-dial-timeout` | `3s` | Timeout for backend connection attempts |
| Response timeout | `LB_RESPONSE_TIMEOUT` | `-response-timeout` | `10s` | Response-header timeout for proxied requests |
| Read-header timeout | `LB_READ_HEADER_TIMEOUT` | `-read-header-timeout` | `5s` | Server read-header timeout |
| Read timeout | `LB_READ_TIMEOUT` | `-read-timeout` | `15s` | Server read timeout |
| Write timeout | `LB_WRITE_TIMEOUT` | `-write-timeout` | `30s` | Server write timeout |
| Idle timeout | `LB_IDLE_TIMEOUT` | `-idle-timeout` | `60s` | Idle keepalive timeout |
| Shutdown timeout | `LB_SHUTDOWN_TIMEOUT` | `-shutdown-timeout` | `15s` | Graceful shutdown deadline |

The project includes a tracked template at [`.env.example`](./.env.example) with the default values and a matching `.env` that is ignored by Git.

### Examples

```sh
LB_LISTEN_ADDR=:9090 go run ./cmd/lb
go run ./cmd/lb -listen :9191 -backends http://localhost:9001,http://localhost:9002
LB_BACKENDS=http://localhost:9001,http://localhost:9002 go run ./cmd/lb -health-timeout 1s
```

`LB_BACKENDS` accepts a comma-separated list and ignores empty entries, so values like `http://localhost:9001, ,http://localhost:9002` still work.

### Validation behavior

The configuration layer validates the final values before startup:

- at least one backend URL is required
- every backend must parse as a valid URL
- duplicate backend URLs are rejected
- the listen address must be valid for `net.Listen`
- durations must parse cleanly and be positive when required

Use `go run ./cmd/lb -h` to print the built-in usage text and defaults.

## Validation

Configuration is validated before the pool or HTTP server is started. Invalid settings fail fast at startup rather than surfacing on the first client request.

| Setting | Rule | Error condition |
| --- | --- | --- |
| `LB_LISTEN_ADDR` | Must be `host:port`; host is empty, an IP address, or a valid hostname; port is numeric and in `1..65535`. | Invalid address, host, or port; port `0` is not accepted for the application listener. |
| `LB_BACKENDS` | Must contain at least one URL using `http` or `https`, with a hostname and no user information. | Missing backend, unsupported scheme, empty/invalid hostname, or `user:pass@` information. |
| `LB_BACKENDS` | Backend URLs must not have a query, fragment, or path other than empty or `/`. | The URL includes `?query`, `#fragment`, or a path such as `/api`. |
| `LB_BACKENDS` | A supplied port must be numeric and in `1..65535`; omitted ports use HTTP `80` or HTTPS `443`. | Port is malformed or outside the allowed range. |
| `LB_BACKENDS` | URLs are compared after lowercasing scheme/host, removing `/`, and applying default ports. | Duplicate normalized backend URLs are rejected. |
| `LB_BACKENDS` | A local backend must not use the balancer's listen port. | A backend on `localhost`, `127.0.0.1`, `::1`, or `0.0.0.0` would proxy back to this balancer. |
| `LB_HEALTH_INTERVAL`, `LB_HEALTH_TIMEOUT`, `LB_DIAL_TIMEOUT`, `LB_RESPONSE_TIMEOUT`, `LB_READ_HEADER_TIMEOUT`, `LB_READ_TIMEOUT`, `LB_WRITE_TIMEOUT`, `LB_IDLE_TIMEOUT`, `LB_SHUTDOWN_TIMEOUT` | Each duration must parse, be greater than zero, and be no more than `24h`. | Invalid, zero, negative, or excessive duration. |
| `LB_HEALTH_TIMEOUT` | Must be less than `LB_HEALTH_INTERVAL`. | Health checks could overlap. |
| `LB_DIAL_TIMEOUT` | Must be less than or equal to `LB_RESPONSE_TIMEOUT`. | The dial budget exceeds the overall response-header budget. |
| `LB_READ_HEADER_TIMEOUT` | Must be less than or equal to `LB_READ_TIMEOUT`. | The header-read budget exceeds the server read budget. |
| `LB_SHUTDOWN_TIMEOUT` | May be any otherwise-valid duration; a value shorter than `LB_RESPONSE_TIMEOUT` is allowed with a warning. | Warning only: in-flight requests may be cut off during shutdown. |
| `LB_CHECK_ONLY` | Must be a boolean when set. | Value is not `true` or `false`. |

Configuration errors are printed to stderr with the `config error:` prefix and exit with status `2`. Warnings use the `warning:` prefix and the server continues. A listen preflight failure is printed with `preflight error:` and exits with status `1`. Preflight attempts to bind the configured address and closes it immediately; the actual server bind can still fail later if another process takes the address in between.

Use the dry-run mode in CI or before deploying to validate settings and confirm the listen address is currently available without starting the balancer:

```sh
go run ./cmd/lb -check
LB_CHECK_ONLY=true go run ./cmd/lb
```

The process does not require backends to be reachable at startup. Health checks own reachability and failover, so a temporary backend outage does not prevent the balancer from starting. During normal startup, hostname resolution is attempted with a two-second timeout per backend; resolution failures produce warnings and do not stop startup.

The config tests exercise the rules directly without depending on real environment variables or backend services:

- `TestValidate_ListenPorts`: accepts valid IPv4/IPv6 and empty-host listen addresses and rejects missing, nonnumeric, zero, negative, and out-of-range ports.
- `TestValidate_BackendURLs`: checks required URL structure, forbidden URL components, port bounds, and valid HTTP/HTTPS, IPv6, and default-port forms.
- `TestValidate_BackendNormalizationAndDuplicates`: detects case/slash and implicit/explicit default-port duplicates and verifies stored canonical URLs.
- `TestValidate_RejectsSelfProxy`: rejects a local backend that targets the balancer's own listen port.
- `TestValidate_DurationBounds`: rejects zero, negative, and over-24-hour durations.
- `TestLoadConfig_InvalidDurationNamesValue` and `TestLoadConfig_AggregatesInvalidDurations`: check helpful malformed-duration errors and report multiple bad values together.
- `TestValidate_DurationRelationships`: checks the health, dial/response, and read-header/read timeout constraints.
- `TestWarnings_ShutdownShorterThanResponse`: confirms the shutdown warning does not make validation fail.
- `TestValidate_ReportsAllErrors`: confirms unrelated address, URL, and duration errors are returned together.
- `TestPreflight`: confirms an occupied address fails preflight and an available address passes.
- `TestLoadConfig_CheckOnly`: checks the environment setting and command-line override for dry-run mode.

## Try it

Send repeated requests through the load balancer:

```sh
curl http://localhost:8080/
```

Responses identify the backend port, so repeated requests demonstrate distribution across the backends.

The demo backend also has a slow endpoint:

```sh
curl http://localhost:8080/slow
```

It waits 15 seconds before responding. The load balancer's backend transport has a 10-second response-header timeout, so this endpoint can be used to observe proxy timeout handling.

## Failure responses

- `503 Service Unavailable` with `no healthy backends` when every backend is marked unhealthy.
- `502 Bad Gateway` with `backend unavailable` when a selected backend cannot be reached or its response times out.

## Testing

Run the load balancer test suite with the standard Go tooling:

```sh
go test ./cmd/lb
go test -race ./cmd/lb
go vet ./...
```

The tests in [`cmd/lb/main_test.go`](./cmd/lb/main_test.go) cover the following behaviors:

- `TestRoundRobin`: verifies the pool cycles through three healthy backends in a fixed repeating order and distributes traffic evenly.
- `TestFailover`: verifies a backend marked unhealthy by `CheckOnce` is skipped, then receives traffic again after it recovers.
- `TestFailover_PassiveDetection`: verifies a proxy error marks a backend dead immediately even without a health check, and later requests skip it.
- `TestTimeout`: verifies a backend that exceeds the configured response timeout produces `502 Bad Gateway` within a short time.
- `TestNoBackends`: verifies an empty pool returns `503 Service Unavailable` with a no-healthy-backends message.
- `TestAllBackendsDown`: verifies every unhealthy backend causes the pool to return `503 Service Unavailable`.
- `TestConcurrentRequests`: verifies 100 concurrent requests all succeed when the pool has healthy backends available.

The fake backends are `httptest` servers that expose a controllable health flag. Each backend can be flipped between healthy and unhealthy while the tests exercise the real load balancer logic without third-party libraries.

### Test design notes

Readiness checks use a TCP dial rather than an HTTP request because an HTTP probe would pass through the balancer and reach the blocking fake backend. Blocking backend handlers also watch the request context, so cancellation can unblock them; test cleanup closes the release channel before closing the `httptest.Server`, which may wait for active handlers. The in-flight shutdown test waits for the backend's arrival signal instead of sleeping, ensuring shutdown begins only after the proxied request is actually in progress.

## Concurrency and race detection

The race detector is relevant here because the health check goroutine writes the backend `alive` flag while request goroutines read it, and the pool round-robin counter is shared across goroutines. The backend state is protected by a `sync.RWMutex`, while the counter uses `sync/atomic` to avoid lost updates during concurrent selection.

Run the race checks with:

```sh
go test -race ./...
go test -race -count=20 ./cmd/lb
```

The additional concurrency tests cover the following behaviors:

- `TestRace_StateUpdatesDuringRequests`: verifies health checks, backend toggles, and request handling can run at the same time without races or invalid status codes.
- `TestRace_SetAliveIsAlive`: verifies `SetAlive` and `IsAlive` stay safe under heavy concurrent access.
- `TestNext_ConcurrentDistribution`: verifies the atomic counter in `Next()` distributes work evenly across three backends.
- `TestNext_NeverReturnsDeadBackend`: verifies a backend marked dead is never selected again, even while other backends are toggled.

The race detector only reports races that actually execute, so these tests deliberately create heavy concurrent load to exercise the critical paths.

## Refactor notes

`main()` was split into `NewBackend`, `NewPool`, `CheckOnce`, and `Handler` so the load balancer could be constructed and exercised in tests without starting a network listener. This keeps the program behavior the same while making the logic testable as plain Go code. The proxying, health checks, failover behavior, and timeout handling were not changed.

## Graceful shutdown

The load balancer listens for `SIGINT` and `SIGTERM` through `signal.NotifyContext`. On shutdown, it stops accepting new connections, waits for in-flight requests to finish, and then exits cleanly. If the graceful shutdown deadline is reached, it calls `Shutdown` with a timeout and then `Close` to force-close any remaining connections. This prevents the process from exiting immediately while a request is still being served.

The `-shutdown-timeout` flag is defined in `main()` and defaults to `15s`. Choose a value that is longer than the slowest normal request but shorter than the orchestrator’s kill timeout; for Kubernetes, this is usually bounded by `terminationGracePeriodSeconds`.

The health check goroutine also stops through context cancellation. When the main context is canceled, the ticker loop exits cleanly instead of continuing to probe the backends.

`start.sh` builds the backend and load-balancer executables once, then runs those binaries directly. This avoids a `go run` wrapper between the script and the balancer, so the script can signal and wait on the actual server process. On `SIGINT` or `SIGTERM`, the script forwards the signal to the balancer and waits for graceful shutdown before its `EXIT` cleanup stops the backends. Keeping the backends alive during that wait lets in-flight proxied requests finish against their upstreams.

The standard library has a known limitation: `http.Server.Shutdown` does not wait for hijacked connections such as WebSockets. In a real deployment, the load balancer in front should stop sending traffic before the backend process is terminated.

Manual verification:

```sh
./start.sh
curl http://localhost:8080/slow
```

While the request is still in progress, press `Ctrl+C` in the balancer terminal. The request should finish, and the logs should show `shutdown signal received` followed by `shutdown complete`.

The new shutdown tests cover the following behaviors:

- `TestGracefulShutdown_FinishesInFlightRequest`: verifies an in-flight request completes during shutdown.
- `TestGracefulShutdown_RejectsNewRequests`: verifies new requests fail after shutdown begins while the earlier request still completes.
- `TestGracefulShutdown_TimeoutForcesClose`: verifies a shutdown timeout returns an error instead of hanging.
- `TestHealthCheckStopsOnContextCancel`: verifies the health-check loop exits on context cancellation.
- `TestGracefulShutdown_NoRequests`: verifies an idle server shuts down promptly without activity.

## Backend registry (Phase 1)

The registry is the single source of truth for backend objects and their runtime state. `Pool` uses it for selection, while health checks update the same backend records. It provides the foundation for later runtime configuration and an admin API; no admin endpoints are included in this phase.

```text
                 +--> Pool (selection) --> Handler
Config --> Registry
                 +--> HealthCheck
```

The backend slice is published through an atomic pointer to an immutable snapshot. Requests and health checks load the current snapshot without taking a registry lock; Add and Remove serialize under a mutex and publish a copied slice. This favors frequent reads and infrequent updates. The tradeoff is allocation and copying proportional to the number of registered backends on each update.

Removing a backend prevents future snapshots from selecting or checking it and closes its idle transport connections. A request that already selected the backend keeps its pointer and is allowed to finish; removal does not cancel in-flight work.

`BackendStatus` is a copied, JSON-friendly snapshot:

- `URL`: normalized backend base URL.
- `Alive`: current health state used by selection.
- `ConsecutiveFailures`: successive proxy or health-check failures, reset after a successful health check.
- `TotalRequests`: requests sent through the backend proxy.
- `TotalFailures`: proxy failures reported by the reverse proxy error handler.
- `ActiveConns`: requests currently being proxied through the backend.
- `LastCheck`: time of the most recent health check.
- `LastError`: most recent proxy or health-check error, cleared by a successful health check.
- `AddedAt`: time the backend was registered.

Registry changes are in memory only and are lost when the process restarts. Active failures use the configured threshold, and recovery requires consecutive successful active checks. Passive hard dial failures still mark a backend dead immediately.

The registry tests cover these guarantees:

- `TestRegistryAddAndNormalize`: stores normalized URLs and rejects invalid or duplicate backends with sentinel errors.
- `TestRegistryRemoveInFlight`: removes a backend from selection while an already-started request still completes.
- `TestRegistryListSnapshotAndOrder`: proves returned status values are detached copies and retain insertion order.
- `TestRegistrySetAliveAndHealthy`: verifies explicit health updates are reflected by `Healthy()`.
- `TestPoolDynamicRoundRobin`: checks an added backend joins rotation and a removed backend stops receiving new requests.
- `TestPoolHealthCheckIncludesAddedBackend`: verifies a later health check examines a backend added after pool creation.
- `TestBackendCountersAndHealthRecovery`: checks request/failure/active counters and recovery after a successful health check.
- `TestRegistryRaceConcurrentChanges`: exercises concurrent requests, add/remove, status listing, and health checks.
- `TestPoolEmptyRegistry`: confirms an empty registry selects no backend and responds with 503.

## Health checks (Phase 1)

Health checks use explicit failure and recovery thresholds. Ordinary probe failures do not immediately remove a backend, and successful client traffic never probes a backend that is already dead.

```text
          failure below threshold
        +-------------------------------+
        |                               v
      +--------+   threshold failures   +------+
      | ALIVE  | ----------------------> | DEAD |
      +--------+                         +------+
        ^                               |
        +-------------------------------+
          RiseThreshold consecutive active
          successful checks

DEAD + active failure -> remain DEAD; reset success streak
ALIVE + active success -> remain ALIVE; reset failure streak
Passive hard dial failure -> ALIVE to DEAD immediately
```

| Setting | Flag | Environment | Default | Meaning |
| --- | --- | --- | --- | --- |
| Check interval | `-health-interval` | `LB_HEALTH_INTERVAL` | `5s` | Delay between active-check rounds |
| Check timeout | `-health-timeout` | `LB_HEALTH_TIMEOUT` | `2s` | Total time allowed for one probe, including body drain |
| Failure threshold | `-health-fail-threshold` | `LB_HEALTH_FAIL_THRESHOLD` | `3` | Consecutive active failures before marking an alive backend dead |
| Recovery threshold | `-health-rise-threshold` | `LB_HEALTH_RISE_THRESHOLD` | `2` | Consecutive active successes required to recover a dead backend |
| Probe path | `-health-path` | `LB_HEALTH_PATH` | `/` | Path appended to each backend base URL for active checks |

Probe failures are classified as:

| Kind | Meaning |
| --- | --- |
| `timeout` | The check or network operation exceeded its deadline |
| `connection_refused` | The target actively refused a connection |
| `dns` | Name resolution failed |
| `bad_status` | The probe returned outside the accepted `200..399` range |
| `other` | Another request, transport, or response-body error |

An alive backend increments its failure streak on each active failure and is marked dead at `LB_HEALTH_FAIL_THRESHOLD`. A successful active check resets that streak. A dead backend remains unavailable until it reaches `LB_HEALTH_RISE_THRESHOLD` consecutive successful active checks; any failed active check resets the recovery streak. This prevents production requests from accidentally reviving a backend that should still be excluded. Connection-refused and other non-timeout dial failures detected by the proxy remain hard failures and mark the backend dead immediately; proxy timeouts and mid-response failures use the configured failure threshold.

Detection takes roughly `HealthInterval * FailThreshold`; recovery takes roughly `HealthInterval * RiseThreshold`, plus up to one interval depending on where a failure or recovery begins relative to the next tick. With defaults, detection is about `5s * 3 = 15s`, and recovery is about `5s * 2 = 10s`.

Each round checks backends in parallel so one slow target does not add its timeout to every later target. An atomic per-backend guard skips an overlapping probe instead of allowing rounds to pile up; a backend emits at most one skip log while its current check is running. Each backend has a separate health transport with one idle connection per host, so probes do not share proxy traffic connections. Probe requests derive their timeout context from the health-loop context, drain at most 4 KB, and do not follow redirects.

Known limits: checks have no jitter, thresholds and paths cannot be overridden per backend, and all health state is in memory and resets on restart.

Health-check tests cover these behaviors:

- `TestHealthFailureThresholdAndReset`: verifies two failures remain alive, a success resets the streak, and the next third consecutive failure marks the backend dead.
- `TestHealthRecoveryThresholdAndFlapping`: verifies recovery requires two active successes and a failure resets progress.
- `TestHealthFailureKindsAndRedirect`: checks timeout, refused connection, 404/503 classification, and that redirects are not followed.
- `TestHealthTimeoutBound`: confirms a blocked probe is canceled within the configured timeout and records `timeout`.
- `TestHealthChecksRunInParallel`: verifies a round with multiple blocked probes finishes within one timeout budget.
- `TestHealthCheckDoesNotOverlap`: verifies a second round skips a backend already being checked and a later round runs normally.
- `TestHealthCheckLoopCancellationAbortsProbe`: verifies loop cancellation aborts a blocked probe and exits promptly.
- `TestHealthPathIsUsed`: confirms active checks request the configured path.
- `TestPassiveHardFailureRequiresActiveRecovery`: verifies immediate passive hard-failure removal and recovery only through active checks.
- `TestPassiveSoftFailureUsesThreshold`: verifies proxy timeouts count toward the failure threshold rather than killing the backend immediately.
- `TestFailureClassification`: checks error-based classification for timeout, refused connection, DNS, and other errors.
- `TestHealthCheckConfigValidation`: covers threshold bounds and health-path requirements.
- `TestWarnings_HealthDetectionTime`: checks the warning for a long failure-detection window.
- `TestRegistryRaceConcurrentChanges`: exercises traffic, checks, registry updates, and status snapshots concurrently.

## Design notes

- Passive hard dial failures mark a backend dead immediately; all other failures follow the configured consecutive-failure threshold.
- Thresholds are consecutive counts, not a rolling time window, and there is no jitter or per-backend override yet.

## Project layout

```text
cmd/
  backend/  Demo HTTP backend
  lb/       Load balancer and reverse proxy
start.sh    Starts the demo backends and load balancer
go.mod      Go module definition
```
