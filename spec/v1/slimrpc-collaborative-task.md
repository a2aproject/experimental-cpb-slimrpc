# SLIMRPC Collaborative Task

This document specifies how the [A2A Collaborative Task](a2a-collaborative-task.md) extension is implemented over SLIMRPC. All generic session semantics, stream translation rules, attribution model, and error handling are defined in the base spec. This document adds SLIM-specific transport mechanics and metadata key mappings.

For a transport-neutral description of the protocol, see [A2A Collaborative Task](a2a-collaborative-task.md).

## 1. Overview

SLIMRPC implements the collaborative task extension on SLIM group channels. The `slimrpc-live-routing: collaborative` metadata key is the binding-defined signal that activates collaborative task mode on a `SendLiveMessage` call (see [Section 3.1](#31-routing-signal)).

Three transport modes are available, corresponding to the tiers defined in the base spec (see [Section 3.3](#33-transport-modes)).

## 2. SLIM Group Channels

SLIMRPC collaborative task sessions use the same SLIM group channel mechanism as multicast RPC (see [Section 2 of the Multicast RPC spec](slimrpc-multicast.md#2-slim-group-channels)). No new channel type or naming convention is required.

**Examples:**

| SLIM Channel Name | Description |
| :--- | :--- |
| `mydomain/demo/planning-session` | A collaborative planning session for a group of agents |
| `mydomain/production/incident-response` | A shared incident response channel for agents and human clients |

## 3. SLIMRPC Binding

### 3.1. Routing Signal

`slimrpc-live-routing: collaborative` in the SLIMRPC call metadata activates collaborative task mode on a `SendLiveMessage` call. This is the binding-defined activation signal required by [Section 4.1 of the base spec](a2a-collaborative-task.md#41-session-initiation). When absent, the call follows standard multicast routing (see [slimrpc-multicast.md](slimrpc-multicast.md)).

### 3.2. Metadata Key Names

SLIMRPC uses flat metadata keys rather than nested dictionaries. The only metadata field required by collaborative task sessions is sender identity from the shared-task extension:

| Extension | Field | SLIMRPC metadata key | Value format |
| :--- | :--- | :--- | :--- |
| shared-task | `message-sender` | `slim-src` | SLIM name in `domain/namespace/service` format |
| — | Context map | `slimrpc-context-map` | JSON object `{ "SLIM name" → "contextId" }` |

`slim-src` is populated from the SLIM transport `src` field. Peer task context (`task_id`, `context_id`, state) is carried in `Part.data` on each translated item, not in metadata. Application code **MUST NOT** set or override these keys.

### 3.3. Transport Modes

SLIMRPC supports three transport modes corresponding to the tiers in [Section 2 of the base spec](a2a-collaborative-task.md#2-transport-models).

| Mode | Base spec tier | Mechanism | Relay |
| :--- | :--- | :--- | :--- |
| `nstreams` | [Basic relay](a2a-collaborative-task.md#21-basic-relay-model) | N independent `SRPCTransport` point-to-point streams; relay translates peer events and injects them via `SendMessage(context_id)`. Any A2A 1.0+ transport with task continuation support is sufficient. | Application layer |
| `multicast` | [Live relay](a2a-collaborative-task.md#22-live-relay-model) | `SRPCMulticastTransport` on a SLIM GROUP channel; SLIM delivers fan-out natively, relay re-sends translated peer responses back into the same group stream. | Application layer |
| `native-broadcast` | [Native fan-out](a2a-collaborative-task.md#23-native-fan-out-model) | `SRPCMulticastTransport` on a SLIM GROUP channel with shared-responses enabled; SLIM delivers each agent's `StreamResponse` to all other group members natively. Agents **MUST** be started with `Server.new_with_shared_responses_and_connection`. | None (SLIM handles it) |

`native-broadcast` is the most efficient mode: SLIM handles both fan-out and peer response routing end-to-end with no application-layer relay.

### 3.4. Session Continuation

To continue an existing context, the initiating client **MAY** include a `slimrpc-context-map` metadata entry on the initial `SendLiveMessage` call. The value is a JSON object mapping each agent's SLIM name to its `contextId`. Each agent's SLIMRPC transport reads its own entry from this map, caches the `contextId`, and uses it for session ID rewriting (see [Section 5.3 of the base spec](a2a-collaborative-task.md#53-session-id-rewriting)).

## 4. Message Attribution

The full attribution model is defined in [Section 5.2 of the base spec](a2a-collaborative-task.md#52-message-attribution). SLIMRPC populates `slim-src` from the SLIM transport `src` field on every delivered item. Peer task context is carried in `Part.data`, not in metadata.

**Example — client-originated item:**

```
slim-src: mydomain/demo/client
```

**Example — translated peer item** (`slim-src` set to the peer agent; peer context in `Part.data`):

```
slim-src: mydomain/demo/agent-a
```

Recipients **MUST** use `slim-src` for sender attribution.

## 5. Agent Card Declaration

Agents that support SLIMRPC collaborative task sessions **MUST** declare this using the A2A extension mechanism (see [Section 3 of the base spec](a2a-collaborative-task.md#3-extension-declaration)). The extension URI for the SLIMRPC profile is:

```
https://a2a-protocol.org/bindings/experimental-slimrpc/extensions/collaborative-task/v1
```

Both this URI and the [A2A Shared Task](a2a-shared-task.md) extension URI (`https://a2a-protocol.org/extensions/shared-task/v1`) **MUST** be declared in `capabilities.extensions` in the agent's Agent Card. The existing SLIMRPC binding `supportedInterfaces` entry is sufficient; no new `protocolBinding` identifier is required.

**Example Agent Card fragment:**

```json
{
  "name": "Planning Agent",
  "description": "A collaborative planning agent supporting collaborative task sessions.",
  "version": "1.0.0",
  "supportedInterfaces": [
    {
      "url": "slim://mydomain/demo/planning-agent",
      "protocolBinding": "https://a2a-protocol.org/bindings/experimental-slimrpc/v1",
      "protocolVersion": "1.1"
    }
  ],
  "defaultInputModes": ["application/json"],
  "defaultOutputModes": ["application/json"],
  "capabilities": {
    "streaming": true,
    "extensions": [
      {
        "uri": "https://a2a-protocol.org/extensions/shared-task/v1",
        "description": "Supports multiple clients sending to the same task with per-message sender identity.",
        "required": false
      },
      {
        "uri": "https://a2a-protocol.org/bindings/experimental-slimrpc/extensions/collaborative-task/v1",
        "description": "Supports collaborative task sessions on SLIM group channels (SendLiveMessage with slimrpc-live-routing: collaborative).",
        "required": false
      }
    ]
  },
  "skills": []
}
```

Clients **SHOULD** verify that all target agents declare both extension URIs before initiating a collaborative task session. Agents that do not declare both extensions **SHOULD NOT** be included in a collaborative task session.

## 6. Channel Establishment

1. **Create a group channel** with a SLIM name of the client's choosing, following the `domain/namespace/channel-name` format
2. **Invite members** into the group channel using each agent's and additional client's individual SLIM names (see [Section 6 of the Multicast RPC spec](slimrpc-multicast.md#6-sending-a-multicast-request) for the invitation procedure)
3. **Initiate the session** by invoking `SendLiveMessage` on the group channel with `slimrpc-live-routing: collaborative` in the SLIMRPC call metadata
4. **Collect initial tasks:** receive the first `StreamResponse` from each agent, which carries the initial `Task`; record each agent's SLIM name, task ID, and `contextId` from these responses and build the `slimrpc-context-map` for all subsequent requests

## 7. Channel Lifecycle

### 7.1. Creation

The initiating client creates the SLIM group channel and invites all intended participants at the SLIM transport level before sending `SendLiveMessage`.

### 7.2. Membership Changes

SLIMRPC does not support adding new participants to an active `SendLiveMessage` session. Inviting a new member to the SLIM group channel does not automatically enroll them in the live session. To include new participants, the initiating client **MUST** cancel the active session (see Section 7.3), invite the new members at the SLIM transport level, and restart the session with all intended participants from the beginning.

When a member is removed from the channel, its `SendLiveMessage` stream **MUST** be terminated. Other members' streams and tasks are unaffected.

### 7.3. Teardown

When the group channel is closed, all open `SendLiveMessage` streams **MUST** be terminated. Agents **SHOULD** transition active tasks to a terminal state (`canceled`) and release associated resources.

## 8. Error Handling

Error responses use the SLIMRPC status codes defined in [Section 6 of the binding spec](slimrpc.md#6-error-handling). Per-agent failure rules are defined in [Section 7 of the base spec](a2a-collaborative-task.md#7-error-handling).

The following are channel-level failures:

| Condition | SLIMRPC Status Code |
| :--- | :--- |
| The SLIM group channel does not exist | `NOT_FOUND` |
| The initial `SendLiveMessage` cannot be delivered to the channel | `UNAVAILABLE` |

A collaborative task session is only considered to have failed at the interaction level if the SLIM group channel cannot be created or the initial `SendLiveMessage` cannot be delivered.
