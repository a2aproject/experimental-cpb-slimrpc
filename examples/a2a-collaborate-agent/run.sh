#!/usr/bin/env bash
# run.sh — build + run the e-commerce scenario with three specialist agents.
#
# Usage:
#   ./run.sh
#   ./run.sh "Customer C-100 says order ORD-1001 never arrived."
#
# Requires: ANTHROPIC_API_KEY, a2a-collaborate on PATH (or go run ../a2a-collaborate)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

MSG="${1:-Customer C-100 says their order ORD-1001 (Wireless Headphones) never arrived. It was placed 14 days ago. Please investigate.}"

echo "==> Building agents..."
go build -o /tmp/collab-orders   ./cmd/orders/
go build -o /tmp/collab-inventory ./cmd/inventory/
go build -o /tmp/collab-shipping  ./cmd/shipping/

PIDS=()
cleanup() {
    echo ""
    echo "==> Stopping agents..."
    for pid in "${PIDS[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
}
trap cleanup EXIT INT TERM

echo "==> Starting orders-agent    (gRPC :50052  HTTP :8081)..."
/tmp/collab-orders    --debug &
PIDS+=($!)

echo "==> Starting inventory-agent (gRPC :50053  HTTP :8082)..."
/tmp/collab-inventory --debug &
PIDS+=($!)

echo "==> Starting shipping-agent  (gRPC :50054  HTTP :8083)..."
/tmp/collab-shipping  --debug &
PIDS+=($!)

echo "==> Waiting for agents to start..."
sleep 2

echo ""
echo "==> Opening collaborative session..."
echo "    Message: $MSG"
echo ""

a2a collaborate \
    --agent-card "http://localhost:8081/.well-known/agent.json" \
    --agent-card "http://localhost:8082/.well-known/agent.json" \
    --agent-card "http://localhost:8083/.well-known/agent.json" \
    "$MSG"
