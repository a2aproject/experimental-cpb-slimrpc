# Incident Response — Collaborative Channel Example

This example demonstrates the [SLIMRPC Collaborative Channel](../../../spec/v1/slimrpc-collaborative-channel.md) extension using a simulated production incident that is detected, diagnosed, and remediated by four agents working together on a shared SLIM group channel.

## Scenario

A spike in the `/api/checkout` error rate is detected. Four agents join a `Collaborate` session and work through the incident in real time, each building on what the others contribute:

| Agent | SLIM Name | Role |
| :---- | :-------- | :--- |
| Monitoring | `mydomain/demo/monitoring-agent` | Receives the anomaly trigger and broadcasts a structured alert |
| Log | `mydomain/demo/log-agent` | Streams relevant log entries from the checkout service |
| Diagnostics | `mydomain/demo/diagnostics-agent` | Correlates log entries and posts a root-cause diagnosis |
| Remediation | `mydomain/demo/remediation-agent` | Proposes actionable fixes once the diagnosis is confident |

Every message carries `metadata["slim-src"]` so each agent knows who sent what, without inspecting the SLIM transport layer.

## Expected output

```
[client]              sending: 'ANOMALY DETECTED: /api/checkout error rate 45% ...'

--- Incident Response Collaborate Session ---

[mydomain/demo/monitoring-agent] ALERT: Error rate spike detected on /api/checkout ...
[mydomain/demo/log-agent]        LOG: 2024-01-15T10:23:01Z ERROR checkout: dial tcp db.prod:5432: connection refused
[mydomain/demo/log-agent]        LOG: 2024-01-15T10:23:02Z ERROR checkout: dial tcp db.prod:5432: connection refused (x47 in 1s)
[mydomain/demo/log-agent]        LOG: 2024-01-15T10:23:03Z WARN  checkout: connection pool exhausted (max=20, waiting=134)
[mydomain/demo/log-agent]        LOG: 2024-01-15T10:23:04Z ERROR checkout: context deadline exceeded after 30s waiting for db conn
[mydomain/demo/log-agent]        LOG: 2024-01-15T10:23:04Z INFO  db.prod: max_connections=100 active=100 idle=0
[mydomain/demo/diagnostics-agent] DIAGNOSIS (confidence: 1.0): DB connection pool exhausted on checkout service ...
[mydomain/demo/remediation-agent] REMEDIATION: DB connection pool exhausted on checkout service. Immediate actions: ...

--- Session complete ---
```

## How it works

1. The client creates a SLIM group channel and invites all four agents
2. The client initiates a `Collaborate` session by calling `CollaborativeChannelServiceGroupStub.Collaborate()`
3. The client's initial message (the anomaly trigger) is broadcast to all agents via the group channel
4. Each agent's `CollaborativeChannelServiceServicer.Collaborate()` implementation receives every message sent by any participant — the client, or any other agent
5. Each agent responds selectively: the monitoring agent responds to `ANOMALY`, the log agent responds to `ALERT`, the diagnostics agent accumulates `LOG:` entries, and the remediation agent acts on `DIAGNOSIS`
6. `metadata["slim-src"]` on each received message tells the recipient who sent it

The result is an emergent workflow where each agent builds on the contributions of the others, without any central coordinator.

## Prerequisites

**1. A running SLIM router**

```bash
# Using Docker
docker run -p 46357:46357 ghcr.io/agntcy/slim:latest

# Or if built from source
slim-router
```

**2. Python dependencies**

```bash
uv sync
```

**3. (Optional) Regenerate protobuf stubs**

The `generated/` directory contains committed stubs. To regenerate from the proto source:

```bash
# Install the SLIMRPC protoc plugin (see https://docs.agntcy.org/slim/slim-slimrpc-compiler/)
# Then from this directory:
buf dep update ../../proto/v1
buf generate
```

## Running

```bash
./run.sh
```

Or start agents and client manually in separate terminals:

```bash
# Terminal 1–4: start each agent
uv run python -m agents.monitoring_agent
uv run python -m agents.log_agent
uv run python -m agents.diagnostics_agent
uv run python -m agents.remediation_agent

# Terminal 5: run the client (after agents are ready)
uv run python client.py
```

## File layout

```
incident-response/
├── README.md                     — this file
├── buf.gen.yaml                  — buf code-generation config (regenerates generated/)
├── pyproject.toml                — Python package dependencies (uv)
├── run.sh                        — convenience script to start all agents + client
├── client.py                     — creates the group channel and initiates Collaborate
├── agents/
│   ├── base.py                   — shared setup_slim_client + AgentCard helpers
│   ├── monitoring_agent.py       — broadcasts structured alert on receiving ANOMALY
│   ├── log_agent.py              — streams log entries on receiving ALERT
│   ├── diagnostics_agent.py      — correlates logs → root-cause diagnosis
│   └── remediation_agent.py      — proposes fix on receiving confident diagnosis
└── generated/
    ├── slimrpc_collaborative_channel_pb2.py         — protobuf file descriptor
    └── slimrpc_collaborative_channel_pb2_slimrpc.py — SLIMRPC stubs
        (CollaborativeChannelServiceServicer,
         CollaborativeChannelServiceGroupStub,
         add_CollaborativeChannelServiceServicer_to_server)
```

## Key spec concepts demonstrated

| Concept | Where |
| :------ | :----- |
| Bidirectional `Collaborate` RPC on a SLIM group channel | `client.py` + each agent's `Collaborate()` method |
| Any member may send at any time | Log agent yields multiple messages; diagnostics and remediation yield one each |
| `slim-src` metadata for A2A-level attribution | `agents/base.py:make_message()`, `get_slim_src()` |
| EOS signals non-participation | Agents that don't recognise a message simply don't yield — the stream handler sends EOS automatically |
| Agent Card `capabilities.extensions` declaration | `agents/base.py:make_agent_card()` |
