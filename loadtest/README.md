# Load test harness

Standalone helpers (stdlib only) for load testing the balancer under download-heavy and compressed-response traffic.

- `be/`: test backend. Endpoints: `/small` (2KB), `/download?mb=N` (N MB stream), `/json` (~1.7MB, gzips if `Accept-Encoding: gzip`), `/slowdl` (flushed chunks with delays).
- `cl/`: load client. Flags: `-url`, `-c` (connections), `-d` (duration), `-gzip`.

Run:

    go run ./loadtest/be -port 9101 &   # also 9102, 9103
    go run ./cmd/lb -listen :18080 -backends http://127.0.0.1:9101,http://127.0.0.1:9102,http://127.0.0.1:9103 &
    go run ./loadtest/cl -url http://127.0.0.1:18080/download?mb=10 -c 20 -d 10s
