# A2A Broadcast Live Messaging

This document specifies the **broadcast live messaging** extension for [A2A 1.1](https://a2a-protocol.org/v1.1.0/specification/). It defines how a group of A2A agents can share a real-time channel in which every event — from any participant — is delivered to all other participants, enabling collaborative multi-agent workflows.

For a concrete implementation over SLIM group channels, see the [SLIMRPC Broadcast Live profile](slimrpc-broadcast-live.md).

## 1. Overview

Standard A2A task interactions follow a client-to-agent model: a client sends a request to one agent and receives a stream of events from that agent only. Each agent's output flows back to the initiating client, not to peer agents.

Broadcast live messaging changes the routing model: when the broadcast-live extension is activated, every event from any participant is delivered to all other participants. Combined with the A2A 1.1 `timeline` semantics, this produces a group-chat model:

- Each agent creates its own `Task` for the session, identified by a shared `context_id`
- Every message sent by any participant — client prompts, agent status updates, artifact events — is received by all other participants on their inbound stream
- Each agent records peer messages as `TimelineEntry` items in its own task's timeline, so a peer's output becomes a recorded input in that agent's interaction history
- Agents decide independently whether to respond to any given received message, exactly as in a group chat

Use cases include:

- A pipeline of specialised agents where each agent's output becomes the next agent's input without a central coordinator
- Multiple clients observing a shared evolving workspace in real time
- A coordinating agent that assigns subtasks to peer agents and observes their progress directly on its own task timeline
- Incident response or planning sessions where humans and agents collaborate on a shared channel

## 2. Transport Requirements

The broadcast-live extension can be implemented with two transport models. Bindings choose which they support.

### 2.1. Relay model

The relay model works over any A2A transport that supports task continuation — the ability to send follow-up messages to a non-terminal task via `context_id` or `task_id`. This is supported by `SendMessage` and `SendStreamingMessage` (A2A 1.0+) as well as `SendLiveMessage` (A2A 1.1).

In this model, an application-layer relay:

1. Opens a stream to each agent (via `SendStreamingMessage` or `SendLiveMessage`)
2. Reads each agent's event stream and translates peer `StreamResponse` items into `StreamRequest` items
3. Injects translated items into each other agent by calling `SendMessage(context_id)` with `return_immediately=True`

The original streaming subscriber for each agent continues to receive that agent's own events. The relay does not process the response to its injected `SendMessage` calls.

### 2.2. Native broadcast model

The native broadcast model delegates fan-out and peer routing to the transport layer, eliminating the application relay entirely. It requires a transport that can deliver a single sender's `StreamResponse` items to all other session participants natively. `SendLiveMessage` (A2A 1.1) is required.

### 2.3. Common requirements

Both models share the following requirements:

- The transport or relay **MUST NOT** deliver a member's own events back to that member (echo suppression)
- The transport or relay **MUST** carry sender identity on every item delivered to a receiving member; the identity format is defined by the binding (see [Section 5](#5-message-attribution))
- The transport or relay **MUST** rewrite both the `contextId` and `taskId` fields in every translated item before delivery to the receiving agent's executor — each agent independently assigns its own `Task` with server-generated IDs; peer messages carry IDs that belong to the originating agent, not the receiving one (see [Section 4.1](#41-session-initiation))

## 3. Extension Declaration

Agents that support broadcast live messaging **MUST** declare the extension in their Agent Card using the A2A `AgentExtension` mechanism. The extension URI is binding-specific. Clients **SHOULD** verify that all target agents declare the broadcast-live extension before initiating a session. Agents that do not declare the extension **SHOULD NOT** be invited into a broadcast live session.

## 4. Session Model

### 4.1. Session Initiation

A broadcast live session is initiated when the extension is activated on a set of participating agents. The activation mechanism — which A2A method is called, which metadata key or parameter signals broadcast mode — is defined by the binding.

Each agent **MUST** create a `Task` independently and assign its own server-generated `contextId` per the A2A specification (see [Section 3.4.1](https://a2a-protocol.org/v1.1.0/specification/#341-context-identifier-semantics)). Agent-generated `contextId` values are opaque to other participants and are not required to match across agents.

Each agent **MUST** return the initial `Task` object as its first response. Because the broadcast routing delivers this response to all channel members, every participant learns every agent's task ID and `contextId` without additional signalling.

Session ID rewriting is performed on the **receive side**: each agent's runtime injects the correct `contextId` **and** `taskId` (for that agent's own task) into every inbound translated item — whether it originates from the client or from a translated peer response — before passing it to the agent executor.

For new sessions each agent's runtime caches its own `contextId` and `taskId` from the `Task` it creates at session initiation. If the client wants all agents to continue prior contexts, it **MAY** include a context map in the session activation; the format is defined by the binding. Each agent's runtime reads its own entry from this map and caches it instead.

### 4.2. The Group Chat Model

Once a session is established, the channel operates as a group chat:

- Any participant — client or agent — **MAY** send a message at any time
- Every item sent by the initiating client is broadcast to all agents
- Every response item sent by any agent is broadcast to all other channel members (clients and agents)
- Each receiving member's runtime translates incoming peer response items into inbound request items (see [Section 5](#5-stream-translation))
- Participants **SHOULD** record received peer messages in their own task's timeline (see [Section 4.3](#43-timeline-integration))
- Participants **MAY** choose to act on or ignore any received message according to their own logic; no response is required

A participant's own reflected messages **MUST NOT** be delivered back to that participant (no echo).

### 4.3. Timeline Integration

The A2A 1.1 `timeline` field on `Task` is the coherent, generation-ordered interaction record (see [Task Timeline Semantics](https://a2a-protocol.org/v1.1.0/specification/#328-task-timeline-semantics)). In broadcast live sessions, each agent **MUST** append received peer messages to its own task's `timeline` as `TimelineEntry(Message)` items. This produces a per-agent record of the full group conversation, in which peer outputs are literally recorded as inputs in the timeline — exactly as if they had been sent by a client in a standard point-to-point interaction.

The agent **SHOULD** preserve the sender attribution metadata on `TimelineEntry(Message)` items appended from peer messages, so the sender is identifiable in the persisted timeline.

**Effect on `generation`:** each appended `TimelineEntry` advances the task's `generation` by 1, enabling downstream subscribers to detect peer-message arrivals as generation gaps and reconcile via `GetTask` (standard ADR-002 behaviour).

## 5. Stream Translation

The runtime (transport layer or application relay) is responsible for translating peer response items into inbound request items on each receiving agent's stream. Application code sees a unified inbound stream mixing client prompts and translated peer events; it does not handle the broadcast routing directly.

### 5.1. Translation Rules

**Client-originated request items** (sent by the initiating client and broadcast to all agents) are delivered directly to each agent's inbound stream without structural modification. The runtime **MUST** inject the sender attribution key (see [Section 5.2](#52-message-attribution)) before delivery.

**Peer response items** (emitted by an agent and broadcast to all other channel members) are translated into request items before delivery:

| Peer sends (`StreamResponse`) | Translated to (`StreamRequest`) | Parts |
| :--- | :--- | :--- |
| Initial `Task` | `StreamRequest { message }` | 1× `Part.data` (Task JSON) |
| `TaskStatusUpdateEvent` with `status.message` | `StreamRequest { message }` | Original `status.message` parts + appended `Part.data` (TaskStatusUpdateEvent JSON) |
| `TaskStatusUpdateEvent` without `status.message` | `StreamRequest { message }` | 1× `Part.data` (TaskStatusUpdateEvent JSON) |
| `TaskArtifactUpdateEvent` | `StreamRequest { artifact_update }` (unchanged) | — |
| `TaskMessageUpdateEvent` | `StreamRequest { message }` (original parts unchanged) | — |

For `Task` and `TaskStatusUpdateEvent` items, the `Part.data` field is a `google.protobuf.Value` containing the JSON-serialised proto event with `preserving_proto_field_name=True` (snake_case field names). The `Part.media_type` **MUST** be set to identify the event type:

| Event | `Part.media_type` |
| :--- | :--- |
| Initial `Task` | `application/vnd.a2a.task+json` |
| `TaskStatusUpdateEvent` | `application/vnd.a2a.task-status-update+json` |

For `TaskStatusUpdateEvent` with `status.message`, the translated message carries the original text parts so receiving agents can directly use the content, and appends a `Part.data` so agents can also inspect the full event envelope (state, task ID, etc.).

`TaskMessageUpdateEvent` is a notification that this agent's task received an external input message from outside the broadcast channel (e.g. a direct `SendMessage` call from another client). It is forwarded as `StreamRequest { message }` with original parts unchanged. The runtime **MUST NOT** overwrite the sender attribution key on these items — the message already carries the original sender's identity from when it was delivered to the agent, and replacing it with the relay agent's identity would misattribute the message. The runtime **MUST** still stamp the peer task ID attribution key so receivers can identify which peer task received the external input. Receiving agents **MUST NOT** treat a forwarded `TaskMessageUpdateEvent` as agent-generated content.

### 5.2. Message Attribution

The runtime populates attribution metadata before delivering any item to a receiving member. Application code **MUST NOT** set or override these keys. The binding defines the actual metadata key names for the three abstract fields:

| Abstract field | Present on | Description |
| :--- | :--- | :--- |
| sender (`broadcast-src`) | All items | Identity of the originating sender; format defined by binding |
| peer task ID (`broadcast-peer-task-id`) | Translated peer items only | Task ID of the peer agent that produced this event |
| peer state (`broadcast-peer-state`) | Translated `TaskStatusUpdateEvent` items only | Task state of the peer at the time of the event |

**`broadcast-src`** **MUST** be present on every item delivered to a receiving member. For client-originated items and all translated peer items except `TaskMessageUpdateEvent`, the runtime sets this to the sender's identity. For translated `TaskMessageUpdateEvent` items, `broadcast-src` **MUST** be preserved from the original message (the external client that sent the out-of-band input) and **MUST NOT** be replaced with the relay agent's identity.

**`broadcast-peer-task-id`** and **`broadcast-peer-state`** are only meaningful for translated peer items and **MUST NOT** be present on client-originated items.

### 5.3. Session ID Rewriting

Before passing a translated item to the receiving agent's executor, the runtime **MUST** inject the agent's cached `contextId` and `taskId` (acquired at session initiation — see [Section 4.1](#41-session-initiation)) into the message. This ensures that peer-originated messages arrive with the correct task identity for that agent, exactly as if they had been sent by a direct client.

### 5.4. Echo Suppression

The runtime **MUST NOT** deliver a translated item back to the member that originally produced it. The mechanism for identifying the originating sender is defined by the binding.

## 6. Message Flows

The following diagrams illustrate generic broadcast-live behaviour. Transport-layer operations (channel creation, membership) are omitted for brevity. Sender identity labels (`src=Client`, `src=AgentA`) use the abstract `broadcast-src` field.

### 6.1. Session Initiation and Task Creation

```
Client          Runtime/Relay   Agent A         Agent B
  |               |               |               |
  |-activate----->|               |               |  (activate broadcast-live extension)
  |               |-send--------->|               |
  |               |-send------------------------->|
  |               |               |               |
  |               |<--[Task A]----|               |  (Agent A: initial Task)
  |<--[Task A]----|               |               |
  |               |--[Task A, src=AgentA]-------->|  (translated StreamRequest)
  |               |               |               |
  |               |<--[Task B]---------------------|  (Agent B: initial Task)
  |<--[Task B]----|               |               |
  |               |--[Task B, src=AgentB]-------->|  (translated StreamRequest)
```

### 6.2. Agent-to-Agent Messaging

```
Client          Runtime/Relay   Agent A         Agent B
  |               |               |               |
  |               |<--[StatusEvt]-|               |  (Agent A: status update with message)
  |<--[StatusEvt]-|src=AgentA     |               |
  |               |--[translated, src=AgentA]---->|  (Agent B receives peer message)
  |               |               |               |
  |               |<--------[StatusEvt]-----------|  (Agent B: responds)
  |<--[StatusEvt]-|src=AgentB     |               |
  |               |--[translated, src=AgentB]---->|  (Agent A receives peer message)
```

Agent A and Agent B each append the other's message as `TimelineEntry(Message)` in their own task timeline.

## 7. Error Handling

The following are member-level failures and **MUST NOT** terminate the session or affect other members:

- A member's stream terminates with an error
- A member's task fails (the `Task` transitions to `failed`)
- A member does not respond to a received message (selective participation is valid)
- A member is removed from the session while it is active

Channel-level failure conditions (e.g. the session cannot be established) and their status codes are defined by the binding.
