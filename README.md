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

## Project layout

```text
cmd/
  backend/  Demo HTTP backend
  lb/       Load balancer and reverse proxy
start.sh    Starts the demo backends and load balancer
go.mod      Go module definition
```
