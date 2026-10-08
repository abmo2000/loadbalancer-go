#!/bin/bash
# Starts 3 backends + the load balancer. Ctrl+C stops everything.
trap 'kill 0' EXIT
go run ./cmd/backend -port 9001 &
go run ./cmd/backend -port 9002 &
go run ./cmd/backend -port 9003 &
sleep 2
go run ./cmd/lb
