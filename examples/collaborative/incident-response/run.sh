#!/usr/bin/env bash
# Copyright 2025 The A2A Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Run the incident-response broadcast live messaging example.
#
# Prerequisites:
#   1. A SLIM router must be running on localhost:46357.
#      Start one with: docker run -p 46357:46357 ghcr.io/agntcy/slim:latest
#         OR: slim-router (if installed via cargo)
#
#   2. Python dependencies must be installed:
#      uv sync
#
# Usage:
#   ./run.sh
#
# Output:
#   Each agent prints its received messages and responses to stdout, prefixed
#   with the agent name. The client prints the full session transcript with
#   slim-src attribution for each event.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

cleanup() {
    echo ""
    echo "Shutting down agents..."
    kill "${AGENT_PIDS[@]}" 2>/dev/null || true
    wait "${AGENT_PIDS[@]}" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

echo "Starting agents..."

uv run python -m agents.monitoring_agent &
AGENT_PIDS=($!)

uv run python -m agents.log_agent &
AGENT_PIDS+=($!)

uv run python -m agents.diagnostics_agent &
AGENT_PIDS+=($!)

uv run python -m agents.remediation_agent &
AGENT_PIDS+=($!)

# Wait for all agents to connect and subscribe.
echo "Waiting for agents to connect..."
sleep 2

echo "Running client..."
uv run python client.py

echo ""
echo "Done."
