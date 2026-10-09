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

## Design notes

- A single timeout marks a backend dead until the next health check. This is a deliberate simplification for the demo implementation.
- A production system would usually use a failure threshold or a short rolling error count before marking a backend unhealthy for a longer period.

## Project layout

```text
cmd/
  backend/  Demo HTTP backend
  lb/       Load balancer and reverse proxy
start.sh    Starts the demo backends and load balancer
go.mod      Go module definition
```
