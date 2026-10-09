#!/bin/bash
# Starts 3 backends + the load balancer.
set -euo pipefail

mkdir -p ./bin
go build -o ./bin/backend ./cmd/backend
go build -o ./bin/lb ./cmd/lb

backend_pids=()
balancer_pid=""
balancer_status=0

cleanup_backends() {
  for pid in "${backend_pids[@]}"; do
    kill "$pid" 2>/dev/null || true
  done
  for pid in "${backend_pids[@]}"; do
    wait "$pid" 2>/dev/null || true
  done
}

forward_signal() {
  local signal="$1"
  if [[ -n "$balancer_pid" ]]; then
    kill -s "$signal" "$balancer_pid" 2>/dev/null || true
  fi
}

trap cleanup_backends EXIT
trap 'forward_signal INT' INT
trap 'forward_signal TERM' TERM

./bin/backend -port 9001 &
backend_pids+=("$!")
./bin/backend -port 9002 &
backend_pids+=("$!")
./bin/backend -port 9003 &
backend_pids+=("$!")

sleep 2
./bin/lb &
balancer_pid="$!"

while true; do
  if wait "$balancer_pid"; then
    balancer_status=0
  else
    balancer_status=$?
  fi
  if ! kill -0 "$balancer_pid" 2>/dev/null; then
    break
  fi
done

exit "$balancer_status"