# SLIMRPC Channel Moderator Extension

This document specifies the Channel Moderator extension for the [SLIMRPC custom protocol binding](slimrpc.md). It defines how an agent can expose A2A skills to advertise, create, and manage SLIM group channels, solving the bootstrapping problem that arises because SLIM group channels are invite-only and have no built-in discovery or join-request mechanism at the transport level.

## 1. Overview

[SLIM group channels](slimrpc-multicast.md#2-slim-group-channels) are invite-only: the agent that creates a channel must explicitly invite each participant by their individual SLIM name. There is no built-in way for other agents to discover that a channel exists or to request an invitation through the SLIM protocol itself. This creates a bootstrapping problem for collaborative multi-agent workloads.

This extension solves the problem at the A2A layer by allowing any agent to advertise and manage its SLIM channels as A2A skills. Two deployment profiles are supported:

- **Minimal profile** — any agent exposes just `list-channels` and `invite-to-channel` so that peer agents can discover which channels it owns and request to join them.
- **Dedicated moderator profile** — a specialised moderator agent exposes the full skill set and holds channel ownership centrally. Individual participants can leave without destroying the channel, because the moderator remains the permanent owner.

In both profiles, the underlying SLIM channel mechanics are unchanged: invite and membership operations still happen at the SLIM transport level via the `ChannelManagerService` proto API. This extension is the A2A-layer coordination interface on top of that transport-level mechanism.

## 2. Extension Declaration

### 2.1. Extension URI

The extension URI for this specification is:

```
https://a2a-protocol.org/bindings/experimental-slimrpc/extensions/channel-moderator/v1
```

This URI **MUST** be declared in `capabilities.extensions` in the agent's Agent Card, as the `uri` field of an `AgentExtension` object, whenever the agent implements any skill defined by this extension.

### 2.2. Skill Routing

Skills defined by this extension are delivered over the agent's existing point-to-point SLIMRPC interface. Clients **MUST** target the moderator agent's individual SLIM name (not a group channel) when invoking these skills.

Skill routing uses the standard A2A mechanism: each skill declares a unique `inputModes` media type in the Agent Card, and the client sends a `Message` whose `DataPart` carries that media type in its `mediaType` field. The moderator agent routes the request to the correct skill handler by matching the incoming `DataPart.mediaType` against the declared `inputModes` of its skills.

This extension defines the following per-skill media types:

| Skill ID            | Input Media Type |
| :------------------ | :--------------- |
| `list-channels`     | `application/vnd.a2a.channel-moderator.list-channels+json` |
| `get-channel-info`  | `application/vnd.a2a.channel-moderator.get-channel-info+json` |
| `create-channel`    | `application/vnd.a2a.channel-moderator.create-channel+json` |
| `delete-channel`    | `application/vnd.a2a.channel-moderator.delete-channel+json` |
| `invite-to-channel` | `application/vnd.a2a.channel-moderator.invite-to-channel+json` |

### 2.3. Agent Card Example

An agent that implements the full dedicated moderator profile **MUST** declare the extension in `capabilities.extensions` and **SHOULD** include a `skills` entry for each skill it supports.

```json
{
  "name": "Channel Moderator Agent",
  "description": "A dedicated moderator agent that manages SLIM group channels on behalf of participants via A2A skills.",
  "version": "1.0.0",
  "supportedInterfaces": [
    {
      "url": "slim://mydomain/demo/moderator",
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
        "uri": "https://a2a-protocol.org/bindings/experimental-slimrpc/extensions/channel-moderator/v1",
        "description": "Exposes SLIM group channel management as A2A skills, enabling peer agents to discover, create, and request access to channels.",
        "required": false
      }
    ]
  },
  "skills": [
    {
      "id": "list-channels",
      "name": "List Channels",
      "description": "Returns the names of all SLIM group channels currently managed by this moderator.",
      "tags": ["channel", "discovery"],
      "examples": ["List all available channels"],
      "inputModes": ["application/vnd.a2a.channel-moderator.list-channels+json"],
      "outputModes": ["application/json"]
    },
    {
      "id": "get-channel-info",
      "name": "Get Channel Info",
      "description": "Returns metadata and the current participant list for a named SLIM group channel.",
      "tags": ["channel", "discovery"],
      "examples": ["Get info for channel mydomain/demo/planning-session"],
      "inputModes": ["application/vnd.a2a.channel-moderator.get-channel-info+json"],
      "outputModes": ["application/json"]
    },
    {
      "id": "create-channel",
      "name": "Create Channel",
      "description": "Creates a new SLIM group channel and registers it with this moderator.",
      "tags": ["channel", "management"],
      "examples": ["Create a new channel mydomain/demo/new-channel with MLS enabled"],
      "inputModes": ["application/vnd.a2a.channel-moderator.create-channel+json"],
      "outputModes": ["application/json"]
    },
    {
      "id": "delete-channel",
      "name": "Delete Channel",
      "description": "Tears down a managed SLIM group channel and removes all participants.",
      "tags": ["channel", "management"],
      "examples": ["Delete channel mydomain/demo/old-channel"],
      "inputModes": ["application/vnd.a2a.channel-moderator.delete-channel+json"],
      "outputModes": ["application/json"]
    },
    {
      "id": "invite-to-channel",
      "name": "Invite to Channel",
      "description": "Requests that the moderator invite a participant into a managed SLIM group channel. If no participant is specified, the caller's own SLIM identity is used.",
      "tags": ["channel", "membership"],
      "examples": [
        "Invite mydomain/demo/agent-c to channel mydomain/demo/planning-session",
        "Add me to channel mydomain/demo/planning-session"
      ],
      "inputModes": ["application/vnd.a2a.channel-moderator.invite-to-channel+json"],
      "outputModes": ["application/json"]
    }
  ]
}
```

An agent implementing only the minimal profile omits `create-channel`, `delete-channel`, and `get-channel-info` from the `skills` array, but still declares the extension in `capabilities.extensions`.

## 3. Message Format

### 3.1. Request

Clients invoke skills by calling `SendMessage` on the moderator agent's point-to-point SLIM name. The `Message` **MUST** carry exactly one `DataPart` whose `mediaType` is set to the skill's declared input media type (see [Section 2.2](#22-skill-routing)) and whose `data` field contains the skill-specific JSON input object.

**Example `SendMessage` request for `list-channels`:**

```json
{
  "messageId": "msg-001",
  "role": "user",
  "parts": [
    {
      "mediaType": "application/vnd.a2a.channel-moderator.list-channels+json",
      "data": {}
    }
  ]
}
```

### 3.2. Response

All skills return an A2A `Task`. The moderator agent responds to `SendMessage` with a `Task` object. Clients **MUST** follow the standard A2A Task flow: after receiving the initial `Task`, call `SubscribeToTask` to receive status updates and wait for the Task to reach a terminal state (`completed` or `failed`). The task artifact carries the JSON output object for the skill once the Task completes (see [Section 6](#6-invite-to-channel-flow)).

### 3.3. Error Convention

Skill-level errors (channel not found, bad input, permission denied, etc.) are returned as **completed** Task artifacts with `"success": false`. The `Task.status` field **MUST NOT** be set to `failed` for business-logic outcomes. `Task.status = failed` is reserved exclusively for agent infrastructure failures such as SLIM transport errors.

This convention mirrors the SLIM `CommandResponse { success, error_msg }` pattern used by the underlying `ChannelManagerService`.

## 4. Data Model

### 4.1. `Channel` Object

The `Channel` object represents a single SLIM group channel managed by the moderator. Field names are aligned with the SLIM [`ChannelManagerService` proto](https://github.com/agntcy/slim/blob/main/proto/channel-manager/v1/commands.proto).

| Field          | Type     | Proto Origin              | Description |
| :------------- | :------- | :------------------------ | :---------- |
| `channel_name` | string   | `channel_name`            | SLIM channel name in `domain/namespace/channel` format |
| `mls_enabled`  | boolean  | `mls_enabled`             | Whether MLS end-to-end encryption is enabled on this channel |
| `participants` | string[] | `participant_name` (list) | SLIM names of current channel participants |

**Example:**

```json
{
  "channel_name": "mydomain/demo/planning-session",
  "mls_enabled": true,
  "participants": [
    "mydomain/demo/agent-a",
    "mydomain/demo/agent-b"
  ]
}
```

## 5. Skills

All skills use the standard A2A Task flow: `SendMessage` returns a `Task`; results and errors are delivered as Task artifacts. SLIMRPC transport status codes are not used for skill-level outcomes.

| Skill ID            | Description |
| :------------------ | :---------- |
| `list-channels`     | List all channels managed by this moderator |
| `get-channel-info`  | Get metadata and current participant list for a channel |
| `create-channel`    | Create a new SLIM channel managed by this moderator |
| `delete-channel`    | Tear down a managed channel |
| `invite-to-channel` | Invite a participant to a channel; `participant_name` defaults to the caller's SLIM identity |

### 5.1. `list-channels`

Returns all channels currently managed by this moderator. Maps to `ListChannelsRequest {}`.

**Input:** `{}` (empty object)

**Output (success):**

```json
{
  "success": true,
  "channels": ["mydomain/demo/planning-session", "mydomain/demo/audit-channel"]
}
```

**Output (error):**

```json
{
  "success": false,
  "error": "internal error: channel store unavailable"
}
```

**Input example:**

```json
{
  "messageId": "msg-001",
  "role": "user",
  "parts": [
    {
      "mediaType": "application/vnd.a2a.channel-moderator.list-channels+json",
      "data": {}
    }
  ]
}
```

### 5.2. `get-channel-info`

Returns the `Channel` object (including participant list) for a named channel. Maps to `ListParticipantsRequest`.

**Input:**

| Field          | Type   | Required | Description |
| :------------- | :----- | :------- | :---------- |
| `channel_name` | string | yes      | SLIM channel name to query |

**Output (success):**

```json
{
  "success": true,
  "channel": {
    "channel_name": "mydomain/demo/planning-session",
    "mls_enabled": true,
    "participants": ["mydomain/demo/agent-a", "mydomain/demo/agent-b"]
  }
}
```

**Output (error):**

```json
{
  "success": false,
  "error": "channel not found: mydomain/demo/planning-session"
}
```

**Input example:**

```json
{
  "messageId": "msg-002",
  "role": "user",
  "parts": [
    {
      "mediaType": "application/vnd.a2a.channel-moderator.get-channel-info+json",
      "data": { "channel_name": "mydomain/demo/planning-session" }
    }
  ]
}
```

### 5.3. `create-channel`

Creates a new SLIM channel and registers it with the moderator. Maps to `CreateChannelRequest`.

**Input:**

| Field          | Type    | Required | Description |
| :------------- | :------ | :------- | :---------- |
| `channel_name` | string  | yes      | SLIM name for the new channel in `domain/namespace/channel` format |
| `mls_enabled`  | boolean | yes      | Whether to enable MLS end-to-end encryption |

**Output (success):**

```json
{
  "success": true,
  "channel": {
    "channel_name": "mydomain/demo/new-channel",
    "mls_enabled": false,
    "participants": []
  }
}
```

**Output (error):**

```json
{
  "success": false,
  "error": "channel already exists: mydomain/demo/new-channel"
}
```

**Input example:**

```json
{
  "messageId": "msg-003",
  "role": "user",
  "parts": [
    {
      "mediaType": "application/vnd.a2a.channel-moderator.create-channel+json",
      "data": {
        "channel_name": "mydomain/demo/new-channel",
        "mls_enabled": false
      }
    }
  ]
}
```

### 5.4. `delete-channel`

Tears down a managed channel. All participants are removed at the SLIM transport level before the channel is destroyed. Maps to `DeleteChannelRequest`.

**Input:**

| Field          | Type   | Required | Description |
| :------------- | :----- | :------- | :---------- |
| `channel_name` | string | yes      | SLIM name of the channel to delete |

**Output (success):**

```json
{
  "success": true,
  "channel_name": "mydomain/demo/old-channel"
}
```

**Output (error):**

```json
{
  "success": false,
  "error": "channel not found: mydomain/demo/old-channel"
}
```

**Input example:**

```json
{
  "messageId": "msg-004",
  "role": "user",
  "parts": [
    {
      "mediaType": "application/vnd.a2a.channel-moderator.delete-channel+json",
      "data": { "channel_name": "mydomain/demo/old-channel" }
    }
  ]
}
```

### 5.5. `invite-to-channel`

Requests that the moderator invite a participant to a channel. Maps to `AddParticipantRequest`.

If `participant_name` is omitted, the moderator **MUST** default to the caller's SLIM identity as conveyed by the SLIM transport `src` field. This allows an agent to request an invitation for itself without explicitly stating its own name.

**Input:**

| Field              | Type   | Required | Description |
| :----------------- | :----- | :------- | :---------- |
| `channel_name`     | string | yes      | Channel to invite the participant into |
| `participant_name` | string | no       | SLIM name of the participant to invite; defaults to the caller's SLIM identity |

**Output (grant):** Task `completed` with artifact:

```json
{
  "channel_name": "mydomain/demo/planning-session",
  "participant_name": "mydomain/demo/agent-c",
  "granted": true
}
```

**Output (deny):** Task `completed` with artifact:

```json
{
  "channel_name": "mydomain/demo/planning-session",
  "participant_name": "mydomain/demo/agent-c",
  "granted": false,
  "reason": "channel is closed to new participants"
}
```

A denial is a business-logic outcome. Task.status **MUST** remain `completed` and **MUST NOT** be set to `failed` when the invite is denied.

**Input example:**

```json
{
  "messageId": "msg-005",
  "role": "user",
  "parts": [
    {
      "mediaType": "application/vnd.a2a.channel-moderator.invite-to-channel+json",
      "data": {
        "channel_name": "mydomain/demo/planning-session",
        "participant_name": "mydomain/demo/agent-c"
      }
    }
  ]
}
```

## 6. `invite-to-channel` Flow

The `invite-to-channel` skill requires the moderator to evaluate policy and call the SLIM `ChannelManagerService` before the outcome is known. The following sequence illustrates the full Task flow.

```
Client                Moderator              SLIM ChannelManagerService
  |                      |                              |
  |--SendMessage-------->|                              |
  |  mediaType:           |                              |
  |  invite-to-channel   |                              |
  |  {channel_name,      |                              |
  |   participant_name}  |                              |
  |                      |                              |
  |<--(Task: submitted)--|                              |
  |   taskId: "task-99"  |                              |
  |                      |                              |
  |--SubscribeToTask---->|                              |
  |  taskId: "task-99"   |                              |
  |                      |                              |
  |                      |--AddParticipant(channel,---->|
  |                      |   participant_name)          |
  |                      |                              |
  |                      |           [grant path]       |
  |                      |<--(CommandResponse: ok)------|
  |                      |                              |
  |<--(Task: completed)--|                              |
  |  artifact: {         |                              |
  |   granted: true,     |                              |
  |   channel_name,      |                              |
  |   participant_name}  |                              |
  |                      |                              |
  |                      |           [deny path]        |
  |                      | (policy check fails or       |
  |                      |  SLIM returns error)         |
  |                      |                              |
  |<--(Task: completed)--|                              |
  |  artifact: {         |                              |
  |   granted: false,    |                              |
  |   channel_name,      |                              |
  |   participant_name,  |                              |
  |   reason: "..."}     |                              |
```

**Step-by-step:**

1. Client sends `SendMessage` with a `DataPart` whose `mediaType` is `application/vnd.a2a.channel-moderator.invite-to-channel+json` and `data` is `{ "channel_name": "...", "participant_name": "..." }`.
2. Moderator responds with a `Task` and a `taskId`.
3. Client calls `SubscribeToTask(taskId)` to receive status updates.
4. Moderator evaluates the request against its local policy and authorization rules.
5. Moderator calls `AddParticipant` on the SLIM `ChannelManagerService`.
   - **Grant path:** SLIM adds the participant; the moderator transitions the Task to `completed` with artifact `{ "granted": true, ... }`.
   - **Deny path:** Policy check fails or SLIM returns an error; the moderator transitions the Task to `completed` with artifact `{ "granted": false, "reason": "...", ... }`. The Task does **not** fail; a denial is a business-logic outcome.

## 7. Error Handling

All skill-level errors are returned as payload responses in Task artifacts — not as SLIMRPC transport status codes. This extension operates at the A2A application layer; SLIMRPC status codes are reserved for transport or infrastructure failures outside the agent's control.

Every skill output includes a `success` boolean, mirroring the SLIM `CommandResponse { success, error_msg }` pattern. When `success` is `false`, an `error` string describes the reason.

| Condition | Task Status | Artifact |
| :--- | :--- | :--- |
| Normal outcome (any skill) | `completed` | `{ "success": true, ... }` |
| Skill-level error (not found, bad input, unauthorized, etc.) | `completed` | `{ "success": false, "error": "..." }` |
| Invite granted | `completed` | `{ ..., "granted": true }` |
| Invite denied by moderator policy | `completed` | `{ ..., "granted": false, "reason": "..." }` |
| Agent infrastructure failure (SLIM transport error, crash) | `failed` | `Task.status.message` describes the agent-level fault |

`Task.status = failed` **MUST NOT** be used for skill-level business-logic outcomes. It is reserved for cases where the moderator agent itself cannot process the request due to an infrastructure failure.
