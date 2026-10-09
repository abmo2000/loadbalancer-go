#!/bin/bash
# Starts 3 backends + the load balancer.
set -euo pipefail

backend_pids=()
cleanup() {
  for pid in "${backend_pids[@]}"; do
    kill "$pid" 2>/dev/null || true
  done
  wait "${backend_pids[@]}" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

go run ./cmd/backend -port 9001 &
backend_pids+=("$!")
go run ./cmd/backend -port 9002 &
backend_pids+=("$!")
go run ./cmd/backend -port 9003 &
backend_pids+=("$!")

sleep 2
go run ./cmd/lb
