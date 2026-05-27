# A2A SLIMRPC Custom Protocol Binding

> **Status: Experimental** — This is a community-contributed custom protocol binding for A2A. It is not part of the core A2A specification.

This repository contains the specification for the **SLIMRPC custom protocol binding** for the [Agent2Agent (A2A) protocol](https://github.com/a2aproject/A2A).

## What is SLIM?

[SLIM](https://github.com/agntcy/slim) (Secure Low-Latency Interactive Messaging) is a next-generation communication framework that provides the secure, scalable transport layer for AI agent protocols like [A2A](https://github.com/a2aproject/A2A) and [MCP](https://modelcontextprotocol.io). An [IETF draft specification](https://datatracker.ietf.org/doc/draft-slim-protocol/) for the protocol is available.

### Why SLIM?

Modern agentic workloads have three properties that create hard transport requirements:

- **Collaborative** — agents share high volumes of context in real time and coordinate complex, multi-step tasks
- **Sensitive** — agents routinely exchange data that must not be readable by infrastructure in between them
- **Distributed** — agents run across multiple networks and organizations, behind NATs and firewalls

Existing messaging solutions don't address all of these requirements at once:

| Solution | Gap |
| :--- | :--- |
| Message queues (SQS, RabbitMQ, Kafka, NATS, etc.) | One-directional; no end-to-end encryption; broker can read all traffic; hard or impossible to expose across organizational boundaries; no native RPC |
| HTTP / gRPC | No group communication; no end-to-end encryption; requires each agent to be directly reachable over the internet |

The result is that developers end up reimplementing the missing capabilities — NAT traversal, encryption, multiparty coordination — at the application layer for every project. SLIM was built to provide all of these as a single, reusable transport layer.

### What SLIM provides

SLIM is built on gRPC over HTTP/2 and combines:

- **No centralized broker** — peer-to-peer architecture; gRPC/HTTP/2 provides straightforward NAT and firewall traversal without special infrastructure
- **Hierarchical name-based addressing** — participants are identified by structured names (`organization/namespace/service`) rather than URLs and ports, enabling location-transparent routing across networks
- **Zero-trust data security** — end-to-end encryption using the [Message Layer Security (MLS)](https://www.rfc-editor.org/rfc/rfc9420) protocol, so even a compromised routing node cannot read message content
- **Native multicast and group communication** — agents can form MLS-encrypted groups identified by shared names, enabling efficient one-to-many and multi-party coordination patterns
- **Bidirectional, low-latency messaging** — native support for both directed channels and group communication, with request-response and streaming patterns
- **Distributed architecture** — a lightweight, pure data-plane routing layer (the SLIM node) handles message forwarding without inspecting application content; a separate control plane manages configuration and monitoring

### SLIMRPC

SLIMRPC (also called SRPC) is the RPC layer built on top of SLIM. Like gRPC is Protobuf RPC over HTTP/2, SLIMRPC is Protobuf RPC over SLIM — it exposes services defined in `.proto` files using a unary-request / streaming-response pattern, and additionally enables multicast RPC patterns via SLIM's group channels. Client stubs and server scaffolding are generated from the proto using the SLIMRPC compiler plugin. Because SLIMRPC uses the same Protocol Buffer service definitions as gRPC, integrating A2A's existing `a2a.proto` is straightforward.

### SLIM Documentation

- [SLIM project on GitHub](https://github.com/agntcy/slim)
- [SLIM overview](https://docs.agntcy.org/slim/overview/)
- [Getting started with SLIM](https://docs.agntcy.org/slim/slim-howto/)
- [SLIMRPC overview](https://docs.agntcy.org/slim/slim-rpc/)
- [SLIMRPC compiler / protoc plugin](https://docs.agntcy.org/slim/slim-slimrpc-compiler/)
- [IETF draft specification](https://datatracker.ietf.org/doc/draft-slim-protocol/)

## Protocol Binding Specification

The `protocolBinding` identifier for this binding is versioned. The current version is `v1`:

```
https://a2a-protocol.org/bindings/experimental-slimrpc/v1
```

### Versioned Specifications

| Version | Document | Description |
| :------ | :------- | :---------- |
| `v1` | [`spec/v1/slimrpc.md`](spec/v1/slimrpc.md) | Core binding specification — protocol requirements, SLIM addressing, service parameters, method inventory, error mapping, streaming, authentication, and Agent Card declaration |
| `v1` | [`spec/v1/slimrpc-multicast.md`](spec/v1/slimrpc-multicast.md) | Multicast RPC — sending a single message to multiple agents simultaneously via SLIM group channels, client discovery, and response collection |

## Reference Implementations

| Language | Repository | A2A versions |
| :------- | :--------- | :----------- |
| Go       | [github.com/agntcy/slim-a2a-go](https://github.com/agntcy/slim-a2a-go) | v0.3.0, v1.0.0 |
| Python   | [github.com/agntcy/slim-a2a-python](https://github.com/agntcy/slim-a2a-python) | v0.3.0, v1.0.0 |
| Rust     | [github.com/a2aproject/a2a-rs](https://github.com/a2aproject/a2a-rs/tree/main/a2a-slimrpc) | |
| Java     | [github.com/agntcy/slim-a2a-java](https://github.com/agntcy/slim-a2a-java) *(early development)* | |
| .NET     | [github.com/agntcy/slim-a2a-dotnet](https://github.com/agntcy/slim-a2a-dotnet) | |

## A2A Protocol

This binding is designed to work with the [A2A protocol specification](https://github.com/a2aproject/A2A). Refer to the main A2A repository for the core specification, proto definitions, and other protocol bindings.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines. This project follows the same process as the main A2A project.

## License

This project is licensed under the [Apache License 2.0](LICENSE), the same license as the main A2A project.

## Code of Conduct

This project follows the A2A [Code of Conduct](CODE_OF_CONDUCT.md).
