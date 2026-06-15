# SLIMRPC Collaborative Channel

This document specifies a collaborative many-to-many channel extension for the [SLIMRPC Multicast RPC specification](slimrpc-multicast.md), which is itself built on the [SLIMRPC custom protocol binding](slimrpc.md). It defines how any member of a SLIM group channel — whether a client or an agent — can originate and receive messages simultaneously, enabling collaborative multi-agent workflows on a shared channel.

## 1. Overview

The [SLIMRPC Multicast RPC](slimrpc-multicast.md) specification follows a one-to-many model: a single client sends one request to a group of agents, each of which responds independently. This works well for fan-out patterns but has two constraints that limit collaborative workloads:

- Only the client may originate messages; agents can only respond to the client that sent the request.
- All responses target the originating client; agents cannot send messages to one another or to other clients on the same channel.

Collaborative agent workloads require more: multiple agents contributing to a shared task, agents that trigger downstream work in peer agents, and multiple clients observing or participating in the same interaction. Use cases include:

- A pipeline of specialised agents where each agent's output becomes the input to a downstream agent
- Multiple clients monitoring an evolving shared workspace in real time
- A coordinating agent that assigns subtasks to peer agents and incorporates their results

This specification defines two interaction modes that can coexist on the same SLIM group channel:

1. **Standard multicast RPC** — unchanged from [slimrpc-multicast.md](slimrpc-multicast.md); one client sends `SendMessage` or `SendStreamingMessage` to the group and receives an independent response from each agent
2. **Collaborative RPC** — a new `Collaborate` bidirectional streaming operation in which any channel member may originate messages; all messages are delivered to every member of the channel

## 2. SLIM Collaborative Channels

### 2.1. Channel Naming

SLIM collaborative channels use the same hierarchical naming scheme as individual agent names and multicast group channels (see [Section 2.1 of the SLIMRPC binding spec](slimrpc.md#21-slim-names)):

```
<domain>/<namespace>/<channel name>
```

No additional naming convention is required beyond that already defined for SLIM group channels.

**Examples:**

| SLIM Channel Name                   | Description                                            |
| :---------------------------------- | :----------------------------------------------------- |
| `mydomain/demo/planning-session`    | A collaborative planning channel for a group of agents |
| `mydomain/production/audit-channel` | A shared audit workspace for multiple agents and clients |

### 2.2. Membership

All members of a SLIM group channel are equal participants. Any member — whether a client or an agent — **MAY** initiate or participate in a `Collaborate` session. Membership management (inviting and removing members) is handled at the SLIM transport level. Join acknowledgement is provided by the SLIM Group Channel protocol; no application-layer signalling is required.

Agents **cannot** subscribe themselves to a group channel. The client that creates the channel **MUST** explicitly invite each agent and each additional client using their individual SLIM names, following the same procedure described in [Section 2 of the Multicast RPC spec](slimrpc-multicast.md#2-slim-group-channels).

## 3. Protocol Requirements

- **Underlying Mechanism:** SLIM group channels (see Section 2)
- **Prerequisites:** All participating members **MUST** support the base SLIMRPC binding (`https://a2a-protocol.org/bindings/experimental-slimrpc/v1`) as described in [slimrpc.md](slimrpc.md); SLIM group channel support as described in [slimrpc-multicast.md](slimrpc-multicast.md) is also required
- **Standard multicast RPC:** `SendMessage` and `SendStreamingMessage` remain available as defined in [slimrpc-multicast.md](slimrpc-multicast.md); their semantics are unchanged
- **Collaborative RPC:** the `Collaborate` method (Section 4) is the new operation introduced by this specification
- **Message attribution:** the SLIM transport includes the sender's identity in the `src` field of every message; no additional SLIMRPC metadata is required for attribution

## 4. The `Collaborate` Operation

`Collaborate` is a bidirectional streaming RPC method added as an extension to the SLIMRPC service. It uses the existing A2A `Message` type for both the request and response streams; no new Protocol Buffer message types are introduced.

```protobuf
rpc Collaborate(stream Message) returns (stream Message);
```

Because the method is invoked on a SLIM group channel, any member may send a `Message` at any time and all other channel members receive it. The output of one participant is the input to all others in the same session, making every participant's send and receive paths structurally identical.

### 4.1. Method Signature

| RPC Type                | Request Stream | Response Stream |
| :---------------------- | :------------- | :-------------- |
| Bidirectional streaming | `Message`      | `Message`       |

The SLIM header metadata carries an **RPC ID** that is assigned when the RPC is initiated. This RPC ID identifies the specific **Collaborate session** and allows recipients to demultiplex concurrent sessions on the same channel.

### 4.2. Collaborate Sessions

A **Collaborate session** is a single `Collaborate` RPC invocation, identified by its RPC ID in the SLIM header metadata.

- Any channel member **MAY** initiate a new session by invoking `Collaborate` on the channel; the RPC is delivered to all channel members via the SLIM Group Channel broadcast
- Any member that receives a `Collaborate` invocation **MAY** choose to participate by sending `Message` objects on its response stream
- A member that does not wish to join a session **SHOULD** send an EOS (End of Stream) on its response stream to signal non-participation to the other members; this closes only that member's stream for this RPC and does not affect the member's participation in the SLIM group channel or in other concurrent sessions
- Multiple concurrent sessions **MAY** exist on the same channel, each distinguished by its RPC ID

### 4.3. Sending Messages

Any member participating in an active `Collaborate` session **MAY** send a `Message` at any time:

- Messages sent by the session initiator are carried on the request stream and delivered to all channel members
- Messages sent by other participants are carried on their individual response streams and delivered to all channel members by the SLIM Group Channel broadcast
- A participant **MAY** additionally use SLIM's native MLS addressed messaging to direct a `Message` to a specific channel member, in which case only the addressed member is expected to respond

### 4.4. Receiving Messages

All channel members receive `Message` objects sent by any other participant in the same session:

- A recipient **SHOULD** process any received `Message` according to its agent logic; the message **SHOULD** be treated as equivalent to a standard inbound request
- A recipient **MAY** respond by sending its own `Message` on the session stream; this response is in turn delivered to all other session members
- Recipients are **NOT REQUIRED** to respond to every received message; selective participation is valid

### 4.5. Stream Lifecycle

- Any member **MAY** close its own half of the stream independently by sending an EOS; this does not affect other members' streams or their membership in the SLIM group channel
- The session continues as long as at least two members have active streams; when only one member remains, that member **SHOULD** close its stream to end the session
- When the SLIM group channel is torn down (see Section 8.3), all open `Collaborate` streams **MUST** be terminated

## 5. Message Flows

This section illustrates representative interaction patterns. SLIM transport-level operations (channel creation, member invitations, join acknowledgements) are omitted for brevity.

### 5.1. Standard Multicast: `SendMessage` Creating a Task

A client sends a `SendMessage` request to the group channel. Each agent independently creates a Task and returns a response. This flow is described in [slimrpc-multicast.md](slimrpc-multicast.md) and is shown here for contrast with collaborative mode.

```
Client          Channel         Agent A         Agent B
  |               |               |               |
  |-SendMessage-->|               |               |
  |               |-SendMessage-->|               |
  |               |-SendMessage------------------>|
  |               |               |               |
  |               |<---(Task A)---|               |
  |<--(Task A)----|               |               |
  |               |<---(Task B)-------------------|
  |<--(Task B)----|               |               |
```

### 5.2. Standard Multicast: `SendStreamingMessage`

Same fan-out as Section 5.1 but each agent returns a stream of events rather than a single Task.

```
Client          Channel         Agent A         Agent B
  |               |               |               |
  |-SendStreaming->|               |               |
  |               |-SendStreaming->|               |
  |               |-SendStreaming----------------->|
  |               |               |               |
  |               |<--(events)----|               |
  |<--(events)----|               |               |
  |               |<--(events)---------------------|
  |<--(events)----|               |               |
  :               :               :               :
  (all streams complete)
```

### 5.3. Collaborative RPC: Agent-to-Agent

A client initiates a `Collaborate` session. The session is delivered to all channel members. Agent A sends a `Message`; all channel members receive it. Agent B responds; all channel members receive the response.

```
Client          Channel         Agent A         Agent B
  |               |               |               |
  |-Collaborate-->|               |               |
  |               |-Collaborate-->|               |
  |               |-Collaborate------------------>|
  |               |               |               |
  |               |<--[Message]---|               |  (Agent A sends)
  |<--[Message]---|  src=Agent A  |               |
  |               |--[Message]-------------------->|
  |               |               |               |
  |               |<--------[Message]-------------|  (Agent B responds)
  |<--[Message]---|  src=Agent B  |               |
  |               |--[Message]---->               |
```

### 5.4. Collaborative RPC: Multi-Client Observation

Two clients and an agent are all members of the channel. Client 1 initiates a `Collaborate` session and sends a `Message`. The agent processes it and responds. Client 2 receives all messages in the session.

```
Client 1        Channel           Agent         Client 2
  |               |               |               |
  |-Collaborate-->|               |               |
  |               |-Collaborate-->|               |
  |               |-Collaborate------------------>|
  |               |               |               |
  |-[Message]---->|               |               |  (Client 1 sends)
  |               |-[Message]---->|               |
  |               |-[Message]-------------------->|
  |               |               |               |
  |               |<--[Message]---|               |  (Agent responds)
  |<--[Message]---|  src=Agent    |               |
  |               |--[Message]-------------------->|
```

## 6. Agent Card Declaration

Agents that support the `Collaborate` operation **MUST** declare this using the A2A extension mechanism. The extension URI for this specification is:

```
https://a2a-protocol.org/bindings/experimental-slimrpc/extensions/collaborate/v1
```

This URI **MUST** be declared in `capabilities.extensions` in the agent's Agent Card, as the `uri` field of an `AgentExtension` object. No new `protocolBinding` identifier or additional `supportedInterfaces` entry is required; the existing SLIMRPC binding entry is sufficient.

**Example Agent Card fragment with `Collaborate` support:**

```json
{
  "name": "Planning Agent",
  "description": "A collaborative planning agent that participates in multi-agent SLIM group channel sessions.",
  "version": "1.0.0",
  "supportedInterfaces": [
    {
      "url": "slim://mydomain/demo/planning-agent",
      "protocolBinding": "https://a2a-protocol.org/bindings/experimental-slimrpc/v1",
      "protocolVersion": "1.0"
    }
  ],
  "defaultInputModes": ["application/json"],
  "defaultOutputModes": ["application/json"],
  "capabilities": {
    "streaming": true,
    "extensions": [
      {
        "uri": "https://a2a-protocol.org/bindings/experimental-slimrpc/extensions/collaborate/v1",
        "description": "Supports bidirectional collaborative messaging on SLIM group channels.",
        "required": false
      }
    ]
  },
  "skills": []
}
```

Clients **SHOULD** verify that all target agents declare this extension URI before initiating a `Collaborate` session. Agents that do not declare the extension **SHOULD NOT** be invited into a collaborative session.

## 7. Channel Establishment

1. **Create a group channel** with a SLIM name of the client's choosing, following the `domain/namespace/channel-name` format
2. **Invite members** into the group channel using each agent's and additional client's individual SLIM names; membership management is handled at the SLIM transport level (see [Section 6 of the Multicast RPC spec](slimrpc-multicast.md#6-sending-a-multicast-request) for the invitation procedure)
3. **Use standard multicast RPC** (optional) — members **MAY** issue `SendMessage` or `SendStreamingMessage` on the channel at any time
4. **Initiate a Collaborate session** (optional) — any member **MAY** invoke `Collaborate` on the channel at any time; multiple sessions **MAY** run concurrently

## 8. Channel Lifecycle

### 8.1. Creation

A client creates the SLIM group channel and invites all intended participants at the SLIM transport level before any interaction begins.

### 8.2. Membership Changes

Membership management is handled entirely at the SLIM transport level. When a member is removed from the channel, any `Collaborate` streams that member holds **MUST** be terminated.

### 8.3. Teardown

When the group channel is closed, all open `Collaborate` streams **MUST** be terminated. Agents **SHOULD** release any task state associated with active collaborative sessions on the channel.

## 9. Error Handling

Error responses use the same SLIMRPC status codes and error structure defined in [Section 6 of the binding spec](slimrpc.md#6-error-handling). No additional error codes are defined by this specification.

The following conditions are member-level failures. They **MUST NOT** terminate the channel or affect other members:

- A member's `Collaborate` stream terminates with an error
- A member does not respond to a message within an application-defined timeout
- A member is removed from the channel while a `Collaborate` session is active

The following conditions are channel-level failures:

| Condition                                          | SLIMRPC Status Code |
| :------------------------------------------------- | :------------------ |
| The SLIM group channel does not exist              | `NOT_FOUND`         |
| An addressed message targets a non-existent member | `NOT_FOUND`         |

A collaborative interaction is only considered to have failed at the interaction level if the SLIM group channel cannot be created or the initial `Collaborate` RPC cannot be delivered (for example, the SLIM node is unreachable).
