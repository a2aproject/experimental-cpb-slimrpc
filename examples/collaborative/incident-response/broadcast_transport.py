# Copyright 2025 The A2A Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Application-layer broadcast routing for SendLiveMessage.

BroadcastLiveClient wraps N point-to-point SRPCTransport instances and
implements the broadcast-live spec pattern: each StreamResponse from any
agent is forwarded as a StreamRequest to all other agents, so every
participant sees every message in the session.

This mirrors what will eventually live in slim-a2a-python as a first-class
BroadcastSRPCTransport once the SLIM transport layer adds broadcast routing.
"""

import asyncio
import uuid
from collections.abc import AsyncGenerator

from a2a.types.a2a_pb2 import (
    Message,
    Part,
    ROLE_USER,
    StreamRequest,
    StreamResponse,
    TaskState,
)

from slima2a.client_transport import SRPCTransport


def _task_state_name(state: TaskState) -> str:
    """Return a lower-case state name string from a TaskState enum value."""
    return TaskState.Name(state).removeprefix("TASK_STATE_").lower()


def _peer_message(text: str, slim_src: str, peer_task_id: str) -> StreamRequest:
    """Build a synthetic ROLE_USER StreamRequest carrying a peer agent's message."""
    msg = Message(
        message_id=str(uuid.uuid4()),
        role=ROLE_USER,
        parts=[Part(text=text)],
    )
    msg.metadata.fields["slim-src"].string_value = slim_src
    msg.metadata.fields["slim-peer-task-id"].string_value = peer_task_id
    return StreamRequest(message=msg)


def _peer_status_message(
    text: str,
    slim_src: str,
    peer_task_id: str,
    state: TaskState,
) -> StreamRequest:
    """Build a synthetic ROLE_USER StreamRequest carrying a peer status update."""
    msg = Message(
        message_id=str(uuid.uuid4()),
        role=ROLE_USER,
        parts=[Part(text=text)],
    )
    msg.metadata.fields["slim-src"].string_value = slim_src
    msg.metadata.fields["slim-peer-task-id"].string_value = peer_task_id
    msg.metadata.fields["slim-peer-state"].string_value = _task_state_name(state)
    return StreamRequest(message=msg)


class BroadcastLiveClient:
    """Application-layer broadcast routing over N SRPCTransport instances.

    Opens one SendLiveMessage stream per agent and forwards each agent's
    StreamResponse items as StreamRequest items to every other agent, producing
    the group-chat semantics described in the SLIMRPC broadcast-live spec.

    Usage::

        client = BroadcastLiveClient([
            ("mydomain/demo/agent-a", transport_a),
            ("mydomain/demo/agent-b", transport_b),
        ])
        async for slim_name, response in client.send_live_message(initial_request):
            print(slim_name, response)
    """

    def __init__(self, agents: list[tuple[str, SRPCTransport]]) -> None:
        self._agents = agents

    async def send_live_message(
        self,
        initial_request: StreamRequest,
        metadata: dict[str, str] | None = None,
    ) -> AsyncGenerator[tuple[str, StreamResponse], None]:
        """Open N SendLiveMessage streams and broadcast peer responses.

        Yields (slim_name, StreamResponse) tuples from all agents as they arrive.
        Each agent's responses are also forwarded as StreamRequest items to all
        other agents so every participant sees the full conversation.

        Args:
            initial_request: The first StreamRequest (must have message set).
            metadata:         SLIMRPC call metadata (e.g. slimrpc-live-routing).
        """
        # Per-agent queue for forwarded StreamRequest items from peers.
        queues: dict[str, asyncio.Queue] = {
            name: asyncio.Queue() for name, _ in self._agents
        }

        # Merged output queue: (slim_name, StreamResponse)
        output_queue: asyncio.Queue[tuple[str, StreamResponse] | None] = asyncio.Queue()

        sentinel = object()

        async def agent_send_stream(slim_name: str) -> AsyncGenerator[StreamRequest, None]:
            """Yield initial_request first, then forward items from peer queue."""
            yield initial_request
            q = queues[slim_name]
            while True:
                item = await q.get()
                if item is sentinel:
                    return
                yield item

        async def read_agent(slim_name: str, transport: SRPCTransport) -> None:
            """Read one agent's stream, forward responses to peers, push to output."""
            try:
                async for response in transport.send_live_message(
                    agent_send_stream(slim_name),
                    context=None,
                ):
                    await output_queue.put((slim_name, response))

                    # Determine the peer task ID for attribution.
                    peer_task_id = ""
                    if response.HasField("task"):
                        peer_task_id = response.task.id
                    elif response.HasField("status_update"):
                        peer_task_id = response.status_update.task_id
                    elif response.HasField("artifact_update"):
                        peer_task_id = response.artifact_update.task_id
                    elif response.HasField("message_update"):
                        peer_task_id = response.message_update.task_id

                    # Build forwarded StreamRequest for peers.
                    forwarded: StreamRequest | None = None

                    if response.HasField("task"):
                        # Task announcement: inform peers of new task.
                        task = response.task
                        forwarded = _peer_message(
                            text=f"Agent {slim_name} started task {task.id}",
                            slim_src=slim_name,
                            peer_task_id=task.id,
                        )

                    elif response.HasField("status_update"):
                        update = response.status_update
                        if update.status.HasField("message"):
                            forwarded = _peer_status_message(
                                text=_text_from_message(update.status.message),
                                slim_src=slim_name,
                                peer_task_id=peer_task_id,
                                state=update.status.state,
                            )
                        else:
                            state_name = _task_state_name(update.status.state)
                            forwarded = _peer_status_message(
                                text=f"Agent {slim_name} state: {state_name}",
                                slim_src=slim_name,
                                peer_task_id=peer_task_id,
                                state=update.status.state,
                            )

                    elif response.HasField("message_update"):
                        update = response.message_update
                        forwarded = _peer_message(
                            text=_text_from_message(update.message),
                            slim_src=slim_name,
                            peer_task_id=peer_task_id,
                        )

                    elif response.HasField("artifact_update"):
                        # Forward artifact updates as StreamRequest artifact_update.
                        forwarded_artifact = StreamRequest(
                            artifact_update=response.artifact_update
                        )
                        # Metadata not supported on StreamRequest.artifact_update;
                        # send as-is to all other agents.
                        for other_name, _ in self._agents:
                            if other_name != slim_name:
                                await queues[other_name].put(forwarded_artifact)
                        continue

                    if forwarded is not None:
                        for other_name, _ in self._agents:
                            if other_name != slim_name:
                                await queues[other_name].put(forwarded)

            except Exception as exc:
                print(f"[broadcast] agent {slim_name} stream error: {exc}")
            finally:
                await output_queue.put(None)  # signal this agent is done
                # Unblock any agents waiting on this sender's queue entry.
                for other_name, _ in self._agents:
                    if other_name != slim_name:
                        await queues[other_name].put(sentinel)

        # Launch all agent reader tasks concurrently.
        reader_tasks = [
            asyncio.create_task(read_agent(name, transport), name=f"broadcast-reader-{name}")
            for name, transport in self._agents
        ]

        agent_count = len(self._agents)
        done_count = 0
        try:
            while done_count < agent_count:
                item = await output_queue.get()
                if item is None:
                    done_count += 1
                else:
                    yield item
        finally:
            for task in reader_tasks:
                task.cancel()
            # Drain all queues so reader tasks can exit.
            for q in queues.values():
                await q.put(sentinel)


def _text_from_message(msg: Message) -> str:
    """Extract the first text content from a Message."""
    for part in msg.parts:
        if part.HasField("text"):
            return part.text
    return ""
