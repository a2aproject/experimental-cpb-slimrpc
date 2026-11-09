# a2a-collaborate-agent

A Go agent that participates in collaborative group-channel sessions via the
[`a2a-collaborate`](../a2a-collaborate/) CLI plugin.

## Structure

```
a2a-collaborate-agent/
├── agent.go        — CollabAgentExecutor: LLM loop, tool dispatch
├── fanin.go        — FanInHandler: routes relay messages to running task inbox
├── server.go       — Run() helper, gRPC/HTTP server setup
├── scenario.go     — e-commerce fake data, role tools, tool dispatcher
└── cmd/
    ├── generic/    — generic chat agent (no specialist role)
    ├── orders/     — Orders specialist: look up orders by ID or customer
    ├── inventory/  — Inventory specialist: check stock, spot oversold products
    └── shipping/   — Shipping specialist: look up carrier tracking status
```

## Quick start — e-commerce scenario

### Prerequisites

- `ANTHROPIC_API_KEY` set in your environment
- `a2a-collaborate` CLI plugin built and on your PATH (or use `go run ../a2a-collaborate/`)

### 1 — Build the agents

```bash
go build ./...
```

This produces four binaries under their respective `cmd/` directories.

### 2 — Start the three specialist agents (separate terminals)

```bash
# Terminal 1
./cmd/orders/orders --debug

# Terminal 2
./cmd/inventory/inventory --debug

# Terminal 3
./cmd/shipping/shipping --debug
```

### 3 — Open a collaborative session

```bash
a2a collaborate \
  --agent-card http://localhost:8081/.well-known/agent.json \
  --agent-card http://localhost:8082/.well-known/agent.json \
  --agent-card http://localhost:8083/.well-known/agent.json \
  "Customer C-100 says their order ORD-1001 (Wireless Headphones) never arrived. It was placed 14 days ago. Please investigate."
```

The relay broadcasts your message to all three agents. Each agent calls its domain
tools to retrieve real (fake) data and then `respond`s with its findings:

- **orders-agent** → looks up ORD-1001, finds tracking ID TRK-9001
- **shipping-agent** → looks up TRK-9001, discovers a weather delay in Memphis
- **inventory-agent** → checks P-500 stock, surfaces the oversell problem (reserved=5, stock=3)

You can also use `./run.sh` to do all of the above automatically.

### 4 — Continue the conversation

The relay keeps reading stdin after the initial message. Type follow-up questions
and press Enter; every line is broadcast to all three agents.

```
> Can the customer expect delivery within the week?
> Are there other customers affected by the P-500 inventory issue?
```

Press `Ctrl-C` to end the session.

## Scenario data

All data is hardcoded in `scenario.go`. The scenario is:

| Entity | Key facts |
| :----- | :-------- |
| ORD-1001 | Customer C-100, Wireless Headphones, shipped 14 days ago via TRK-9001 |
| ORD-1002 | Customer C-100, USB-C Cable, already delivered |
| ORD-1003 | Customer C-101, Laptop Stand, on hold (inventory problem) |
| ORD-1004 | Customer C-102, Wireless Headphones, cancelled + refunded |
| P-500 | Wireless Headphones: stock=3, reserved=5 → **oversold by 2** |
| P-501 | USB-C Cable: stock=150, normal |
| P-502 | Laptop Stand: stock=0, backordered |
| TRK-9001 | FedEx, **delayed by weather** in Memphis DC, est. delivery in 3 days |
| TRK-9002 | FedEx, delivered 5 days ago, signed by J. Smith |

## Generic agent

`cmd/generic` is the original agent from before the specialist roles were added.
It has no domain tools — only the `respond` tool. Use it to add a free-form
participant to the channel:

```bash
./cmd/generic/generic --name alice --grpc-port 50055 --http-port 8084
```

## Flags (all commands)

| Flag | Default | Description |
| :--- | :------ | :---------- |
| `--name` | role-specific | Agent name shown in the channel |
| `--grpc-port` | role-specific | gRPC A2A server port |
| `--http-port` | role-specific | HTTP agent-card server port |
| `--model` | `claude-haiku-4-5-20251001` | Anthropic model |
| `--debug` | false | Log LLM reasoning, tool calls, and inbox messages |

Default ports:

| Command | gRPC | HTTP |
| :------ | :--- | :--- |
| generic | 50051 | 8080 |
| orders  | 50052 | 8081 |
| inventory | 50053 | 8082 |
| shipping | 50054 | 8083 |
