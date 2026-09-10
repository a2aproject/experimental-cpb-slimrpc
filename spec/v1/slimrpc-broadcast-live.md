# SLIMRPC Broadcast Live Messaging

This document specifies how the [A2A Broadcast Live](a2a-broadcast-live.md) extension is implemented over SLIMRPC. All generic session semantics, stream translation rules, attribution model, and error handling are defined in the base spec. This document adds SLIM-specific transport mechanics and metadata key mappings.

For a transport-neutral description of the protocol, see [A2A Broadcast Live Messaging](a2a-broadcast-live.md).

## 1. Overview

SLIMRPC implements broadcast live messaging on SLIM group channels. The `slimrpc-live-routing: broadcast` metadata key is the binding-defined signal that activates broadcast mode on a `SendLiveMessage` call (see [Section 3.1](#31-routing-signal)).

Three transport modes are available, corresponding to the two models defined in the base spec (see [Section 3.3](#33-transport-modes)).

## 2. SLIM Group Channels

SLIMRPC broadcast live messaging uses the same SLIM group channel mechanism as multicast RPC (see [Section 2 of the Multicast RPC spec](slimrpc-multicast.md#2-slim-group-channels)). No new channel type or naming convention is required.

**Examples:**

| SLIM Channel Name | Description |
| :--- | :--- |
| `mydomain/demo/planning-session` | A collaborative planning session for a group of agents |
| `mydomain/production/incident-response` | A shared incident response channel for agents and human clients |

## 3. SLIMRPC Binding

### 3.1. Routing Signal

`slimrpc-live-routing: broadcast` in the SLIMRPC call metadata activates broadcast mode on a `SendLiveMessage` call. This is the binding-defined activation signal required by [Section 4.1 of the base spec](a2a-broadcast-live.md#41-session-initiation). When absent, the call follows standard multicast routing (see [slimrpc-multicast.md](slimrpc-multicast.md)).

### 3.2. Metadata Key Names

The following table maps the abstract attribution fields from [Section 5.2 of the base spec](a2a-broadcast-live.md#52-message-attribution) to their SLIMRPC metadata key names:

| Abstract field | SLIMRPC metadata key | Value format |
| :--- | :--- | :--- |
| `message-sender` | `slim-src` | SLIM name in `domain/namespace/service` format |
| `peer-task-id` | `slim-peer-task-id` | A2A task ID string |
| `peer-state` | `slim-peer-state` | `TaskState` name (lower-case, no `TASK_STATE_` prefix) |
| Context map | `slimrpc-context-map` | JSON object `{ "SLIM name" → "contextId" }` |

`slim-src` is populated from the SLIM transport `src` field. Application code **MUST NOT** set or override any of these keys.

### 3.3. Transport Modes

SLIMRPC supports three transport modes. All satisfy the base spec requirements; they differ in where fan-out and relay are performed.

| Mode | Base spec model | Mechanism | Relay |
| :--- | :--- | :--- | :--- |
| `nstreams` | [Relay model](a2a-broadcast-live.md#21-relay-model) | N independent `SRPCTransport` point-to-point streams; relay translates peer events and injects them via `SendMessage(context_id)`. Any A2A 1.0+ transport with task continuation support is sufficient for this model. | Application layer |
| `multicast` | [Relay model](a2a-broadcast-live.md#21-relay-model) with native fan-out | `SRPCMulticastTransport` on a SLIM GROUP channel; SLIM delivers fan-out natively, relay re-sends translated peer responses back into the same group stream. | Application layer |
| `native-broadcast` | [Native broadcast model](a2a-broadcast-live.md#22-native-broadcast-model) | `SRPCMulticastTransport` on a SLIM GROUP channel with shared-responses enabled; SLIM delivers each agent's `StreamResponse` to all other group members natively. Agents **MUST** be started with `Server.new_with_shared_responses_and_connection`. | None (SLIM handles it) |

`native-broadcast` is the closest implementation to the broadcast-live spec's intent: SLIM handles both fan-out and peer response routing end-to-end with no application-layer relay.

### 3.4. Session Continuation

To continue an existing context, the initiating client **MAY** include a `slimrpc-context-map` metadata entry on the initial `SendLiveMessage` call. The value is a JSON object mapping each agent's SLIM name to its `contextId`. Each agent's SLIMRPC transport reads its own entry from this map, caches the `contextId`, and uses it for session ID rewriting (see [Section 5.3 of the base spec](a2a-broadcast-live.md#53-session-id-rewriting)).

## 4. Message Attribution

The full attribution model is defined in [Section 5.2 of the base spec](a2a-broadcast-live.md#52-message-attribution). SLIMRPC populates `slim-src` from the SLIM transport `src` field; `slim-peer-task-id` and `slim-peer-state` are stamped by the SLIMRPC runtime on translated peer items.

**Example — client-originated item:**

```
slim-src: mydomain/demo/client
```

**Example — translated peer item:**

```
slim-src: mydomain/demo/agent-a
slim-peer-task-id: task-7f3c1b
slim-peer-state: working
```

Recipients **MUST** use `metadata["slim-src"]` for sender attribution at the A2A layer.

## 5. Agent Card Declaration

Agents that support SLIMRPC broadcast live messaging **MUST** declare this using the A2A extension mechanism (see [Section 3 of the base spec](a2a-broadcast-live.md#3-extension-declaration)). The extension URI for the SLIMRPC profile is:

```
https://a2a-protocol.org/bindings/experimental-slimrpc/extensions/broadcast-live/v1
```

This URI **MUST** be declared in `capabilities.extensions` in the agent's Agent Card as the `uri` field of an `AgentExtension` object. The existing SLIMRPC binding `supportedInterfaces` entry is sufficient; no new `protocolBinding` identifier is required.

**Example Agent Card fragment:**

```json
{
  "name": "Planning Agent",
  "description": "A collaborative planning agent supporting broadcast live sessions.",
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
        "uri": "https://a2a-protocol.org/bindings/experimental-slimrpc/extensions/broadcast-live/v1",
        "description": "Supports broadcast live messaging on SLIM group channels (SendLiveMessage with slimrpc-live-routing: broadcast).",
        "required": false
      }
    ]
  },
  "skills": []
}
```

Clients **SHOULD** verify that all target agents declare this extension URI before initiating a broadcast live session. Agents that do not declare the extension **SHOULD NOT** be invited into a broadcast live session.

## 6. Channel Establishment

1. **Create a group channel** with a SLIM name of the client's choosing, following the `domain/namespace/channel-name` format
2. **Invite members** into the group channel using each agent's and additional client's individual SLIM names (see [Section 6 of the Multicast RPC spec](slimrpc-multicast.md#6-sending-a-multicast-request) for the invitation procedure)
3. **Initiate the session** by invoking `SendLiveMessage` on the group channel with `slimrpc-live-routing: broadcast` in the SLIMRPC call metadata
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

Error responses use the SLIMRPC status codes defined in [Section 6 of the binding spec](slimrpc.md#6-error-handling). Member-level failure rules are defined in [Section 7 of the base spec](a2a-broadcast-live.md#7-error-handling).

The following are channel-level failures:

| Condition | SLIMRPC Status Code |
| :--- | :--- |
| The SLIM group channel does not exist | `NOT_FOUND` |
| The initial `SendLiveMessage` cannot be delivered to the channel | `UNAVAILABLE` |

A broadcast live session is only considered to have failed at the interaction level if the SLIM group channel cannot be created or the initial `SendLiveMessage` cannot be delivered.
