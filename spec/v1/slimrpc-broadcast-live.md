# SLIMRPC Broadcast Live Messaging

This document specifies the **broadcast live messaging** extension for the [SLIMRPC Multicast RPC specification](slimrpc-multicast.md), which is itself built on the [SLIMRPC custom protocol binding](slimrpc.md). It defines how a SLIM group channel can be used as a shared real-time channel in which every `SendLiveMessage` event — from any participant — is delivered to all other participants, enabling collaborative multi-agent workflows.

## 1. Overview

[Multicast `SendLiveMessage`](slimrpc-multicast.md#8-multicast-sendlivemessage-bidirectional-streaming) follows a fan-out model: a single initiating client sends `StreamRequest` items to all agents, and each agent's `StreamResponse` stream flows back to the initiating client only. Agents are unaware of one another's output.

Broadcast live messaging changes only the routing: when the `slimrpc-live-routing: broadcast` metadata key is present on the `SendLiveMessage` call, SLIM delivers every `StreamResponse` item from any channel member to **all** other channel members. Combined with the A2A 1.1 `timeline` semantics, this produces a group-chat model:

- Each agent creates its own `Task` for the session, identified by the shared `context_id`
- Every message sent by any participant — client prompts, agent status updates, artifact events — is received by all other participants on their inbound `StreamRequest` stream
- Each agent records peer messages as `TimelineEntry` items in its own task's timeline, so a peer's output becomes a recorded input in that agent's interaction history
- Agents decide independently whether to respond to any given received message, exactly as in a group chat

Use cases include:

- A pipeline of specialised agents where each agent's output becomes the next agent's input without a central coordinator
- Multiple clients observing a shared evolving workspace in real time
- A coordinating agent that assigns subtasks to peer agents and observes their progress directly on its own task timeline
- Incident response or planning sessions where humans and agents collaborate on a shared channel

## 2. SLIM Group Channels

Broadcast live messaging uses the same SLIM group channel mechanism as multicast RPC (see [Section 2 of the Multicast RPC spec](slimrpc-multicast.md#2-slim-group-channels)). No new channel type or naming convention is required.

**Examples:**

| SLIM Channel Name | Description |
| :--- | :--- |
| `mydomain/demo/planning-session` | A collaborative planning session for a group of agents |
| `mydomain/production/incident-response` | A shared incident response channel for agents and human clients |

## 3. Protocol Requirements

- **Underlying mechanism:** SLIM group channels
- **Prerequisites:** All participating members **MUST** support A2A 1.1 or later and the base SLIMRPC binding (`https://a2a-protocol.org/bindings/experimental-slimrpc/v1`); SLIM group channel support as described in [slimrpc-multicast.md](slimrpc-multicast.md) is also required
- **Method:** `SendLiveMessage` as defined in A2A 1.1; no new RPC method is introduced
- **Routing signal:** `slimrpc-live-routing: broadcast` in SLIMRPC call metadata; absent = standard multicast (see [slimrpc.md reserved metadata keys](slimrpc.md#3-service-parameter-transmission))
- **Message attribution:** the SLIMRPC layer **MUST** populate the `slim-src` key in `StreamRequest` metadata on every item delivered to a receiving member, identifying the original sender (see [Section 6](#6-message-attribution))

## 4. Session Model

### 4.1. Session Initiation

Any channel member **MAY** initiate a broadcast live session by invoking `SendLiveMessage` on the SLIM group channel with `slimrpc-live-routing: broadcast` in the call metadata. The SLIM runtime delivers the call to all current channel members.

Each agent creates its own `Task` independently and assigns its own server-generated `contextId` per the A2A specification (see [Section 3.4.1](https://a2a-protocol.org/v1.1.0/specification/#341-context-identifier-semantics)). Agent-generated `contextId` values are opaque to other participants and are not required to be the same across agents.

Each agent **MUST** create a `Task` in response to the `SendLiveMessage` and return the initial `Task` object as the first `StreamResponse`. Because SLIM broadcasts this response to all channel members, every participant learns every agent's task ID and `contextId` without additional signalling. Participants **MUST** record the per-agent `{ SLIM name → contextId }` mapping from these initial responses.

On all subsequent `StreamRequest` items, participants **MUST** include a `slimrpc-context-map` metadata entry as defined in [Section 8.3 of the Multicast RPC spec](slimrpc-multicast.md#83-task-management). The SLIMRPC transport rewrites the `contextId` field of each outbound message to the destination agent's value from this map before delivery, so each agent always sees its own `contextId` transparently. The `slim-peer-task-id` metadata key (see [Section 6](#6-message-attribution)) allows subsequent events to be attributed to the correct per-agent task.

### 4.2. The Group Chat Model

Once a session is established, the channel operates as a group chat:

- Any participant — client or agent — **MAY** send a `StreamRequest` item at any time
- Every `StreamRequest` item sent by the initiating client is broadcast to all agents
- Every `StreamResponse` item sent by any agent is broadcast to all other channel members (clients and agents)
- Each receiving member's SLIMRPC runtime translates incoming `StreamResponse` items from peers into `StreamRequest` items on its inbound stream (see [Section 5](#5-stream-translation))
- Participants **SHOULD** record received peer messages in their own task's timeline (see [Section 4.3](#43-timeline-integration))
- Participants **MAY** choose to act on or ignore any received message according to their own logic; no response is required

A participant's own reflected messages **MUST NOT** be delivered back to that participant (no echo).

### 4.3. Timeline Integration

The A2A 1.1 `timeline` field on `Task` is the coherent, generation-ordered interaction record (see [Task Timeline Semantics](https://a2a-protocol.org/v1.1.0/specification/#328-task-timeline-semantics)). In broadcast live sessions, each agent **MUST** append received peer messages to its own task's `timeline` as `TimelineEntry(Message)` items. This produces a per-agent record of the full group conversation, in which peer outputs are literally recorded as inputs in the timeline — exactly as if they had been sent by a client in a standard point-to-point interaction.

The agent **SHOULD** preserve the `slim-src` metadata key on `TimelineEntry(Message)` items appended from peer messages, so the sender is identifiable in the persisted timeline.

**Effect on `generation`:** each appended `TimelineEntry` advances the task's `generation` by 1, enabling downstream subscribers to detect peer-message arrivals as generation gaps and reconcile via `GetTask` (standard ADR-002 behaviour).


## 5. Stream Translation

The SLIMRPC runtime is responsible for translating `StreamResponse` items received from peer agents via SLIM broadcast into `StreamRequest` items on the receiving agent's inbound stream. Application code sees a unified inbound stream mixing client prompts and translated peer events; it does not handle the broadcast routing directly.

### 5.1. Translation Rules

| Peer sends (`StreamResponse`) | Translated to (`StreamRequest`) | Metadata added |
| :--- | :--- | :--- |
| Initial `Task` (first response) | `StreamRequest { message: synthetic task-announcement Message }` | `slim-src`, `slim-peer-task-id` |
| `TaskStatusUpdateEvent` with `status.message` | `StreamRequest { message: status.message }` | `slim-src`, `slim-peer-task-id`, `slim-peer-state` |
| `TaskStatusUpdateEvent` without `status.message` | Delivered as `StreamRequest { message: synthetic state-change Message }` | `slim-src`, `slim-peer-task-id`, `slim-peer-state` |
| `TaskArtifactUpdateEvent` | `StreamRequest { artifact_update: artifact_update }` | `slim-src`, `slim-peer-task-id` |
| `TaskMessageUpdateEvent` (client-message entry) | `StreamRequest { message: message }` | `slim-src`, `slim-peer-task-id` |

The synthetic task-announcement `Message` for the initial `Task` response **MUST** carry `role: ROLE_AGENT` and **SHOULD** include the peer's task ID in its text or data part so that receiving agents can record it.

The synthetic state-change `Message` for a `TaskStatusUpdateEvent` without `status.message` **MUST** carry `role: ROLE_AGENT` and **SHOULD** encode the new `TaskState` value so that receiving agents can track peer state without polling.

### 5.2. `contextId` Rewriting

Before delivering a translated `StreamRequest` to a receiving agent, the SLIMRPC runtime **MUST** rewrite its `contextId` field to the value from the session's `slimrpc-context-map` that corresponds to the receiving agent's SLIM name. This ensures that peer-originated messages arrive with the correct `contextId` for that agent's task, exactly as if they had been sent by a direct client.

### 5.3. Echo Suppression

The SLIMRPC runtime **MUST NOT** deliver a translated `StreamRequest` back to the member that originally sent the corresponding `StreamResponse`. SLIM `src`-based identity is used to suppress echoes.

## 6. Message Attribution

Every translated `StreamRequest` item delivered to a receiving member **MUST** carry the following metadata keys:

| Metadata Key | Type | Description |
| :--- | :--- | :--- |
| `slim-src` | string | SLIM name of the originating sender in `domain/namespace/service` format |
| `slim-peer-task-id` | string | Task ID of the peer agent that produced this event |
| `slim-peer-state` | string | Task state of the peer at the time of the event (`working`, `completed`, `failed`, etc.); present on translated `TaskStatusUpdateEvent` items only |

The SLIMRPC layer populates `slim-src` from the SLIM transport `src` field before delivering the translated item to the receiving agent. Application code does not set these keys.

Recipients **MUST** use `metadata["slim-src"]` for sender attribution at the A2A layer.

**Example translated `StreamRequest` metadata:**

```
slim-src: mydomain/demo/agent-a
slim-peer-task-id: task-7f3c1b
slim-peer-state: working
```

## 7. Message Flows

SLIM transport-level operations (channel creation, member invitations, join acknowledgements) are omitted for brevity.

### 7.1. Session Initiation and Task Creation

A client initiates the session. All agents create tasks and announce them. All participants receive all task announcements.

```
Client          Channel         Agent A         Agent B
  |               |               |               |
  |-SendLiveMsg-->|               |               |  (slimrpc-live-routing: broadcast)
  |               |-SendLiveMsg-->|               |  (context_id=ctx-1)
  |               |-SendLiveMsg------------------>|
  |               |               |               |
  |               |<--[Task A]----|               |  (Agent A: initial Task)
  |<--[Task A]----|               |               |
  |               |--[Task A announcement]-------->|  (translated StreamRequest)
  |               |               |               |
  |               |<--[Task B]---------------------|  (Agent B: initial Task)
  |<--[Task B]----|               |               |
  |               |--[Task B announcement]-------->|  (translated StreamRequest)
```

### 7.2. Agent-to-Agent Messaging

Agent A sends a status update with a message. All channel members receive it. Agent B acts on it and responds; its response is likewise broadcast.

```
Client          Channel         Agent A         Agent B
  |               |               |               |
  |               |<--[StatusEvt]-|               |  (Agent A: status update with message)
  |<--[StatusEvt]-|slim-src=AgentA|               |
  |               |--[translated StreamReq]------->|  (Agent B receives peer message)
  |               |               |               |
  |               |<--------[StatusEvt]-----------|  (Agent B: responds)
  |<--[StatusEvt]-|slim-src=AgentB|               |
  |               |--[translated StreamReq]------->|  (Agent A receives peer message)
```

Agent A and Agent B each append the other's message as `TimelineEntry(Message)` in their own task timeline.


## 8. Agent Card Declaration

Agents that support broadcast live messaging **MUST** declare this using the A2A extension mechanism. The extension URI for this specification is:

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

## 9. Channel Establishment

1. **Create a group channel** with a SLIM name of the client's choosing, following the `domain/namespace/channel-name` format
2. **Invite members** into the group channel using each agent's and additional client's individual SLIM names (see [Section 6 of the Multicast RPC spec](slimrpc-multicast.md#6-sending-a-multicast-request) for the invitation procedure)
3. **Initiate the session** by invoking `SendLiveMessage` on the group channel with `slimrpc-live-routing: broadcast` in the SLIMRPC call metadata
4. **Collect initial tasks:** receive the first `StreamResponse` from each agent, which carries the initial `Task`; record each agent's SLIM name, task ID, and `contextId` from these responses and build the `slimrpc-context-map` for all subsequent requests

## 10. Channel Lifecycle

### 10.1. Creation

The initiating client creates the SLIM group channel and invites all intended participants at the SLIM transport level before sending `SendLiveMessage`.

### 10.2. Membership Changes

SLIMRPC does not support adding new participants to an active `SendLiveMessage` session. Inviting a new member to the SLIM group channel does not automatically enroll them in the live session. To include new participants, the initiating client **MUST** cancel the active session (see Section 10.3), invite the new members at the SLIM transport level, and restart the session with all intended participants from the beginning.

When a member is removed from the channel, its `SendLiveMessage` stream **MUST** be terminated. Other members' streams and tasks are unaffected.

### 10.3. Teardown

When the group channel is closed, all open `SendLiveMessage` streams **MUST** be terminated. Agents **SHOULD** transition active tasks to a terminal state (`canceled`) and release associated resources.

## 11. Error Handling

Error responses use the SLIMRPC status codes defined in [Section 6 of the binding spec](slimrpc.md#6-error-handling). No additional error codes are defined by this specification.

The following are member-level failures and **MUST NOT** terminate the channel or affect other members:

- A member's `SendLiveMessage` stream terminates with an error
- A member's task fails (the `Task` transitions to `failed`)
- A member does not respond to a received message (selective participation is valid)
- A member is removed from the channel while a session is active

The following are channel-level failures:

| Condition | SLIMRPC Status Code |
| :--- | :--- |
| The SLIM group channel does not exist | `NOT_FOUND` |
| The initial `SendLiveMessage` cannot be delivered to the channel | `UNAVAILABLE` |

A broadcast live session is only considered to have failed at the interaction level if the SLIM group channel cannot be created or the initial `SendLiveMessage` cannot be delivered.
