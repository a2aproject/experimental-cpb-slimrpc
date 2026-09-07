# Incident Response — Broadcast Live Example

This example demonstrates the [SLIMRPC Broadcast Live](../../../spec/v1/slimrpc-collaborative-channel.md) pattern using a simulated production incident that is detected, diagnosed, and remediated by four agents working together in a shared live session.

## Scenario

A spike in the `/api/checkout` error rate is detected. The client opens a `SendLiveMessage` broadcast session with four agents. Each agent sees every message sent by any participant, and responds selectively based on the content and sender:

| Agent | SLIM Name | Role |
| :---- | :-------- | :--- |
| Monitoring | `mydomain/demo/monitoring-agent` | Reacts to ANOMALY trigger, broadcasts structured METRICS: data |
| Log | `mydomain/demo/log-agent` | Reacts to ANOMALY trigger (in parallel), streams LOG: entries |
| Diagnostics | `mydomain/demo/diagnostics-agent` | Accumulates evidence from METRICS: and LOG:, emits DIAGNOSIS |
| Remediation | `mydomain/demo/remediation-agent` | Proposes plan on DIAGNOSIS, executes on client APPROVED |

Monitoring and log agents both fire on the initial ANOMALY trigger at the same time, exploiting the broadcast channel for true parallel work.

## Message flow

```
client          monitoring-agent    log-agent       diagnostics-agent   remediation-agent
  |                  |                  |                  |                  |
  | ANOMALY -------> | ANOMALY -------> | ANOMALY          |                  |
  |                  |                  |                  |                  |
  |          METRICS: db_pool_wait...   |                  |                  |
  |          --------------------------> (all)             |                  |
  |                  |         LOG: connection refused...   |                  |
  |                  |         --------------------------> (all)             |
  |                  |         LOG: connection pool exhausted...             |
  |                  |         --------------------------> (all)             |
  |                  |            ... (5 log lines total)                   |
  |                             DIAGNOSIS (confidence: 0.7+)                |
  |             <------------------------------------------------- (all)    |
  |                                               REMEDIATION: ... awaiting |
  |             <-----------------------------------------------------------------
  |                                                                          |
  | APPROVED ---------------------------------------------------------------->
  |                                              REMEDIATION EXECUTED: ...  |
  |             <-----------------------------------------------------------------
  | close session                                                            |
```

## Expected output

```
--- Broadcast Live Session ---

[client]            sending: 'ANOMALY DETECTED: /api/checkout error rate 45% ...'

[monitoring-agent]  task='abc123' context='...'
[log-agent]         task='def456' context='...'
[monitoring-agent]  [working] METRICS: error_rate=45% threshold=5% p99_latency=28400ms ...
[log-agent]         [working] LOG: 2024-01-15T10:23:01Z ERROR checkout: dial tcp db.prod:5432: connection refused
[log-agent]         [working] LOG: 2024-01-15T10:23:02Z ERROR checkout: dial tcp db.prod:5432: connection refused (x47 in 1s)
[log-agent]         [working] LOG: 2024-01-15T10:23:03Z WARN  checkout: connection pool exhausted (max=20, waiting=134)
[log-agent]         [working] LOG: 2024-01-15T10:23:04Z ERROR checkout: context deadline exceeded after 30s waiting for db conn
[log-agent]         [working] LOG: 2024-01-15T10:23:04Z INFO  db.prod: max_connections=100 active=100 idle=0
[diagnostics-agent] [working] DIAGNOSIS (confidence: 0.8): DB connection pool exhausted ...
[remediation-agent] [input_required] REMEDIATION: DB connection pool exhausted on checkout service. ...

[client]            sending approval: 'APPROVED: please execute the remediation plan.'

[remediation-agent] [working] REMEDIATION EXECUTED: Restarted checkout-db-pool ...

[client]            remediation confirmed — closing session

--- Session complete ---
```

Each participant prints in a distinct color: white for the client, yellow for monitoring, cyan for log, magenta for diagnostics, green for remediation.

## How it works

### Application-layer broadcast routing

`slim-a2a-python` implements point-to-point `SendLiveMessage` streams (one client to one agent). Broadcast routing — where every agent sees every other agent's messages — is implemented at the application layer by `BroadcastLiveClient` in `broadcast_transport.py`.

`BroadcastLiveClient` wraps N `SRPCTransport` instances and:

1. Fans every `StreamRequest` from the caller out to all agent queues.
2. For each `StreamResponse` from agent X, builds a `StreamRequest` (with `slim-src=X` and the original parts intact) and routes it to every other agent's queue — never back to the originator.
3. Merges all agents' response streams into a single `(slim_name, StreamResponse)` async generator for the caller.
4. Injects `slim-src` into outbound client messages at the transport layer, so agents can identify the sender without application-layer stamps.

The session stays open until the caller's request generator exhausts. Calling `send_queue.put(None)` terminates the generator, which causes `fan_out_client` to send a sentinel to every agent queue, closing the send side of each stream cleanly.

### Two-phase remediation

The remediation agent uses a simple state machine:

- **Phase 1**: waits for `DIAGNOSIS` from `diagnostics-agent` with confidence ≥ 0.7. Emits the remediation plan with `TASK_STATE_INPUT_REQUIRED` ("Awaiting approval to execute.").
- **Phase 2**: waits for `APPROVED` in any message. Executes the plan and emits `REMEDIATION EXECUTED:` with `TASK_STATE_WORKING`.

The client detects `REMEDIATION:` in the first status update, enqueues an approval message into `send_queue`, then detects `REMEDIATION EXECUTED:` and calls `send_queue.put(None)` to close the session.

### Agent executor pattern

All agents use the A2A 1.1 `AgentExecutor.execute(context, event_queue, input_queue)` signature. The loop pattern is:

```python
async def execute(self, context, event_queue, input_queue):
    try:
        while True:
            msg_ctx = await input_queue.get()   # blocks; raises QueueShutDown on close
            sender = get_slim_src(msg_ctx.message)
            text   = get_message_text(msg_ctx.message)
            # ... agent logic ...
    except QueueShutDown:
        pass
    finally:
        if updater:
            await updater.complete()
```

Per-session state (confidence scores, flags) lives in local variables inside `execute()`.

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

Dependencies are sourced from the `feat/send-live-message` branches of `a2a-python` and `slim-a2a-python`:

```toml
[tool.uv.sources]
a2a-sdk  = { git = "https://github.com/Tehsmash/a2a-python",   branch = "feat/send-live-message" }
slima2a  = { git = "https://github.com/agntcy/slim-a2a-python", branch = "feat/send-live-message" }
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
├── README.md                       — this file
├── pyproject.toml                  — Python package dependencies (uv)
├── run.sh                          — convenience script: start all agents + client
├── broadcast_transport.py          — BroadcastLiveClient application-layer broadcast router
├── client.py                       — opens broadcast live session, drives two-phase approval
└── agents/
    ├── base.py                     — shared constants, log(), make_agent_card(), start_agent()
    ├── monitoring_agent.py         — emits METRICS: data on ANOMALY trigger
    ├── log_agent.py                — streams LOG: entries on ANOMALY trigger (parallel)
    ├── diagnostics_agent.py        — accumulates METRICS: + LOG: evidence → DIAGNOSIS
    └── remediation_agent.py        — proposes plan on DIAGNOSIS, executes on APPROVED
```

## Key spec concepts demonstrated

| Concept | Where |
| :------ | :----- |
| `SendLiveMessage(stream StreamRequest) returns (stream StreamResponse)` — A2A 1.1 BiDi streaming | `broadcast_transport.py`, each agent's `execute()` |
| Application-layer broadcast routing (pending transport-layer support) | `BroadcastLiveClient` in `broadcast_transport.py` |
| `slim-src` attribution injected at transport layer | `BroadcastLiveClient.fan_out_client()` |
| Parallel agent activation on single client message | `monitoring_agent.py` + `log_agent.py` both react to ANOMALY |
| `AgentExecutor.execute(context, event_queue, input_queue)` — A2A 1.1 executor API | All four agents |
| `TaskUpdater.update_status()` + `complete()` | All four agents |
| `TASK_STATE_INPUT_REQUIRED` for human-in-the-loop approval | `remediation_agent.py` |
| Mid-session client messages (async `send_queue`) | `client.py` |
| Session teardown via request generator exhaustion | `client.py` `send_queue.put(None)` |
| Agent Card `extensions` field for broadcast-live capability declaration | `agents/base.py:make_agent_card()` |
