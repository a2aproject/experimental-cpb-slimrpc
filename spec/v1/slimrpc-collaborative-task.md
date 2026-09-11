# SLIMRPC Collaborative Task

This document specifies how the [A2A Collaborative Task](a2a-collaborative-task.md) extension is implemented over SLIMRPC. All generic session semantics, stream translation rules, attribution model, and error handling are defined in the base spec. This document specifies how SLIMRPC selects a transport mode based on participant capabilities, and how SLIM group channels can implement the relay in a decentralised way.

For a transport-neutral description of the protocol, see [A2A Collaborative Task](a2a-collaborative-task.md).

## 1. Overview

The relay defined in the base spec can be implemented in two ways under SLIMRPC:

- **Decentralised (native mode):** When all participants support SLIMRPC and the collaborative task extension, the SLIM transport itself acts as the relay. A SLIM group channel with shared-responses enabled delivers each participant's `StreamResponse` to all other group members natively. The SLIMRPC transport layer on each agent performs stream translation and session ID rewriting locally, before items reach the agent executor. No single relay process is required.

- **Application-layer relay (hybrid or full-relay mode):** When some or all participants do not support SLIMRPC, an application-layer relay handles those connections as point-to-point A2A calls. SLIMRPC-capable participants are still grouped on a SLIM group channel where possible; the relay bridges between the SLIM group and the p2p connections.

The client selects the transport mode by inspecting the Agent Cards of all intended participants before initiating the session (see [Section 3](#3-transport-mode-selection)).

## 2. SLIM Group Channels

SLIMRPC collaborative task sessions use the same SLIM group channel mechanism as multicast RPC (see [Section 2 of the Multicast RPC spec](slimrpc-multicast.md#2-slim-group-channels)). No new channel type or naming convention is required.

**Examples:**

| SLIM Channel Name | Description |
| :--- | :--- |
| `mydomain/demo/planning-session` | A collaborative planning session for a group of agents |
| `mydomain/production/incident-response` | A shared incident response channel for agents and human clients |

## 3. Transport Mode Selection

Before initiating a session, the client **MUST** inspect the Agent Card of each intended participant to determine which transport mode to use.

A participant is **SLIMRPC-capable** if its Agent Card declares:

- A `supportedInterfaces` entry with SLIMRPC protocol binding (`https://a2a-protocol.org/bindings/experimental-slimrpc/v1`)
- All three of the following extension URIs in `capabilities.extensions`:
  - [A2A Shared Task](a2a-shared-task.md): `https://a2a-protocol.org/extensions/shared-task/v1`
  - [A2A Collaborative Task](a2a-collaborative-task.md): `https://a2a-protocol.org/extensions/collaborative-task/v1`
  - SLIMRPC Collaborative Task: `https://a2a-protocol.org/bindings/experimental-slimrpc/extensions/collaborative-task/v1`

The client selects the transport mode as follows:

| Participant capabilities | Mode |
| :--- | :--- |
| All participants are SLIMRPC-capable | [Native mode](#31-native-mode) |
| At least one participant is SLIMRPC-capable, at least one is not | [Hybrid mode](#32-hybrid-mode) |
| No participants are SLIMRPC-capable | [Full relay mode](#33-full-relay-mode) |

### 3.1. Native Mode

**Condition:** all participants are SLIMRPC-capable.

The client creates a SLIM group channel with shared-responses enabled and invites all participants. The client sends `SendLiveMessage` on the group channel with `slimrpc-live-routing: collaborative` in the call metadata.

SLIM delivers each participant's `StreamResponse` to all other group members natively. The SLIMRPC transport layer on each agent performs stream translation (peer `StreamResponse` → `StreamRequest`), `message-sender` population, and session ID rewriting before passing items to the agent executor. No application-layer relay is required.

Agents **MUST** be started with shared-responses mode enabled (`Server.new_with_shared_responses_and_connection`) to participate in native mode sessions.

### 3.2. Hybrid Mode

**Condition:** at least one participant is SLIMRPC-capable and at least one is not.

The SLIMRPC-capable participants are connected via a SLIM group channel with shared-responses enabled. Non-SLIMRPC participants are connected via point-to-point A2A calls.

An application-layer relay bridges the two groups:

- The relay subscribes to the SLIM group channel and receives translated peer items from SLIMRPC agents
- The relay injects those items into the non-SLIMRPC agents via p2p calls
- The relay receives responses from non-SLIMRPC agents and injects them back into the SLIM group channel (targeting SLIMRPC agents)

For point-to-point connections to non-SLIMRPC participants, the relay **SHOULD** use `SendLiveMessage` if the agent declares A2A 1.1 support, and **MUST** fall back to `SendStreamingMessage` with `SendMessage(context_id, return_immediately=True)` injection otherwise (see [Sections 2.1 and 2.2 of the base spec](a2a-collaborative-task.md#2-transport-models)).

### 3.3. Full Relay Mode

**Condition:** no participants are SLIMRPC-capable, or the client does not support SLIM group channels.

The client uses the transport tiers defined in the base spec with point-to-point A2A connections only. For each agent, the relay **SHOULD** use `SendLiveMessage` if the agent declares A2A 1.1 support, and **MUST** fall back to `SendStreamingMessage` with `SendMessage(context_id, return_immediately=True)` injection otherwise.

## 4. Activation Signal

`slimrpc-live-routing: collaborative` in the SLIMRPC call metadata is the binding-defined activation signal required by [Section 4.1 of the base spec](a2a-collaborative-task.md#41-session-initiation). It is set on `SendLiveMessage` calls on SLIM group channels only. Point-to-point A2A calls in hybrid or full relay mode do not carry this key.

When absent on a `SendLiveMessage` call, the call follows standard multicast routing (see [slimrpc-multicast.md](slimrpc-multicast.md)).

## 5. Metadata Key Names

SLIMRPC uses flat metadata keys rather than nested dictionaries. The metadata fields required by collaborative task sessions are:

| Extension | Field | SLIMRPC metadata key | Value format |
| :--- | :--- | :--- | :--- |
| shared-task | `message-sender` | `slim-src` | SLIM name in `domain/namespace/service` format |
| — | Context map | `slimrpc-context-map` | JSON object `{ "SLIM name" → "contextId" }` |

`slim-src` is populated from the SLIM transport `src` field on group channel items. On point-to-point connections in hybrid mode, the relay populates `slim-src` based on the identity of the participant whose event is being forwarded. Peer task context (`task_id`, `context_id`, state) is carried in `Part.data` on each translated item, not in metadata. Application code **MUST NOT** set or override these keys.

## 6. Session Continuation

To continue an existing context, the initiating client **MAY** include a `slimrpc-context-map` metadata entry on the initial `SendLiveMessage` call. The value is a JSON object mapping each agent's SLIM name to its `contextId`. Each agent's SLIMRPC transport reads its own entry from this map, caches the `contextId`, and uses it for session ID rewriting (see [Section 5.3 of the base spec](a2a-collaborative-task.md#53-session-id-rewriting)).

## 7. Message Attribution

The full attribution model is defined in [Section 5.2 of the base spec](a2a-collaborative-task.md#52-message-attribution). SLIMRPC populates `slim-src` from the SLIM transport `src` field on every item delivered via the group channel. In hybrid mode, the relay populates `slim-src` with the SLIM name of the originating participant before injecting items across the bridge.

**Example — client-originated item:**

```
slim-src: mydomain/demo/client
```

**Example — translated peer item** (`slim-src` set to the peer agent; peer context in `Part.data`):

```
slim-src: mydomain/demo/agent-a
```

Recipients **MUST** use `slim-src` for sender attribution.

## 8. Agent Card Declaration

Agents that support SLIMRPC collaborative task sessions **MUST** declare this using the A2A extension mechanism (see [Section 3 of the base spec](a2a-collaborative-task.md#3-extension-declaration)). The extension URI for the SLIMRPC profile is:

```
https://a2a-protocol.org/bindings/experimental-slimrpc/extensions/collaborative-task/v1
```

All three extension URIs — [A2A Shared Task](a2a-shared-task.md), [A2A Collaborative Task](a2a-collaborative-task.md), and the SLIMRPC profile URI — **MUST** be declared in `capabilities.extensions` in the agent's Agent Card. The existing SLIMRPC binding `supportedInterfaces` entry is sufficient; no new `protocolBinding` identifier is required.

An agent **MUST NOT** declare the SLIMRPC Collaborative Task extension URI unless it is started with shared-responses mode enabled (`Server.new_with_shared_responses_and_connection`). Declaring this URI is the signal that the client uses to determine native mode eligibility (see [Section 3](#3-transport-mode-selection)); an agent that declares it but is not running in shared-responses mode will fail to participate correctly in native mode sessions.

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
        "uri": "https://a2a-protocol.org/extensions/collaborative-task/v1",
        "description": "Supports collaborative task sessions with peer agents via a relay.",
        "required": false
      },
      {
        "uri": "https://a2a-protocol.org/bindings/experimental-slimrpc/extensions/collaborative-task/v1",
        "description": "Supports native SLIMRPC collaborative task sessions on SLIM group channels (SendLiveMessage with slimrpc-live-routing: collaborative).",
        "required": false
      }
    ]
  },
  "skills": []
}
```

## 9. Channel Establishment

### 9.1. Native Mode

1. **Inspect Agent Cards** of all intended participants to confirm all are SLIMRPC-capable
2. **Create a SLIM group channel** with a name of the client's choosing, following the `domain/namespace/channel-name` format, with shared-responses enabled
3. **Invite members** into the group channel using each participant's individual SLIM name (see [Section 6 of the Multicast RPC spec](slimrpc-multicast.md#6-sending-a-multicast-request) for the invitation procedure)
4. **Initiate the session** by invoking `SendLiveMessage` on the group channel with `slimrpc-live-routing: collaborative` in the SLIMRPC call metadata
5. **Collect initial tasks:** receive the first `StreamResponse` from each agent, which carries the initial `Task`; record each agent's SLIM name, task ID, and `contextId` from these responses and build the `slimrpc-context-map` for all subsequent requests

### 9.2. Hybrid Mode

1. **Inspect Agent Cards** to identify SLIMRPC-capable and non-SLIMRPC participants
2. **Create a SLIM group channel** with shared-responses enabled; invite only the SLIMRPC-capable participants
3. **Open p2p connections** to each non-SLIMRPC participant using `SendLiveMessage` (if A2A 1.1) or `SendStreamingMessage` (otherwise)
4. **Start the application-layer relay** to bridge between the SLIM group and the p2p connections
5. **Collect initial tasks** from all participants and build the `slimrpc-context-map` for SLIMRPC agents

## 10. Channel Lifecycle

### 10.1. Creation

The initiating client creates the SLIM group channel and invites all SLIMRPC-capable participants at the SLIM transport level before sending `SendLiveMessage`.

### 10.2. Membership Changes

SLIMRPC does not support adding new participants to an active `SendLiveMessage` session. To include new participants, the initiating client **MUST** cancel the active session (see Section 10.3), and restart the session with all intended participants from the beginning, re-evaluating transport mode based on the full updated participant set.

When a participant is removed from the channel, its stream **MUST** be terminated. Other participants' streams and tasks are unaffected.

### 10.3. Teardown

When the session ends, all open streams **MUST** be terminated. Agents **SHOULD** transition active tasks to a terminal state (`canceled`) and release associated resources.

## 11. Error Handling

Error responses use the SLIMRPC status codes defined in [Section 6 of the binding spec](slimrpc.md#6-error-handling). Per-agent failure rules are defined in [Section 7 of the base spec](a2a-collaborative-task.md#7-error-handling).

The following are channel-level failures:

| Condition | SLIMRPC Status Code |
| :--- | :--- |
| The SLIM group channel does not exist | `NOT_FOUND` |
| The initial `SendLiveMessage` cannot be delivered to the channel | `UNAVAILABLE` |

A collaborative task session is only considered to have failed at the interaction level if the SLIM group channel cannot be created or the initial `SendLiveMessage` to the channel cannot be delivered. Failure to connect a single p2p participant in hybrid mode is a per-member failure and **MUST NOT** prevent the session from starting with the remaining participants.
