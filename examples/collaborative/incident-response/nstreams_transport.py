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

"""N-streams application-layer relay for collaborative task sessions (SendLiveMessage).

NStreamsBroadcastTransport wraps N point-to-point SRPCTransport instances and
implements the collaborative task basic relay pattern: each StreamResponse from any
agent is forwarded as a StreamRequest to all other agents, so every
participant sees every message in the session.

This uses N independent point-to-point streams.  See multicast_transport.py
for an equivalent that uses a single SLIM GROUP channel bidi stream.
"""

import asyncio
import uuid
from collections.abc import AsyncGenerator

from google.protobuf.json_format import MessageToDict
from google.protobuf.struct_pb2 import Value

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


def _forward_message(
    msg: Message,
    slim_src: str,
    peer_task_id: str,
    event=None,
    media_type: str = "",
    state: TaskState | None = None,
) -> StreamRequest:
    """Translate a peer Message into a ROLE_USER StreamRequest, preserving all parts.

    If event is provided, appends a Part.data carrying the JSON-serialised event
    so receiving agents have both the text content and the structured event envelope.
    """
    forwarded = Message()
    forwarded.CopyFrom(msg)
    forwarded.role = ROLE_USER
    if event is not None:
        d = MessageToDict(event, preserving_proto_field_name=True)
        val = Value()
        val.struct_value.update(d)
        forwarded.parts.append(Part(data=val, media_type=media_type))
    forwarded.metadata.fields["slim-src"].string_value = slim_src
    forwarded.metadata.fields["slim-peer-task-id"].string_value = peer_task_id
    if state is not None:
        forwarded.metadata.fields["slim-peer-state"].string_value = _task_state_name(state)
    return StreamRequest(message=forwarded)


def _event_data_message(
    event,
    media_type: str,
    slim_src: str,
    peer_task_id: str,
    state: TaskState | None = None,
) -> StreamRequest:
    """Build a ROLE_USER StreamRequest carrying the serialised proto event as Part.data."""
    d = MessageToDict(event, preserving_proto_field_name=True)
    val = Value()
    val.struct_value.update(d)
    msg = Message(
        message_id=str(uuid.uuid4()),
        role=ROLE_USER,
        parts=[Part(data=val, media_type=media_type)],
    )
    msg.metadata.fields["slim-src"].string_value = slim_src
    msg.metadata.fields["slim-peer-task-id"].string_value = peer_task_id
    if state is not None:
        msg.metadata.fields["slim-peer-state"].string_value = _task_state_name(state)
    return StreamRequest(message=msg)


class NStreamsBroadcastTransport:
    """Application-layer broadcast routing over N SRPCTransport instances.

    Opens one SendLiveMessage stream per agent and implements a bidirectional
    broadcast session:
    - Every StreamRequest from the caller is fanned out to all agents.
    - Every StreamResponse from any agent is forwarded as a StreamRequest to
      all other agents, producing the group-chat semantics in the spec.
    - The caller can yield StreamRequest items asynchronously at any point
      during the session; the streams stay open until the caller's generator
      exhausts or the session is cancelled.

    Usage::

        async def requests():
            yield first_message
            await asyncio.sleep(5)
            yield follow_up_message  # sent while agents are still responding

        client = NStreamsBroadcastTransport([
            ("mydomain/demo/agent-a", transport_a),
            ("mydomain/demo/agent-b", transport_b),
        ])
        async for slim_name, response in client.send_live_message(requests()):
            print(slim_name, response)
    """

    def __init__(self, agents: list[tuple[str, SRPCTransport]], source_slim_name: str = "") -> None:
        self._agents = agents
        self._source_slim_name = source_slim_name

    async def send_live_message(
        self,
        request_stream: AsyncGenerator[StreamRequest, None],
        metadata: dict[str, str] | None = None,
    ) -> AsyncGenerator[tuple[str, StreamResponse], None]:
        """Open N SendLiveMessage streams and broadcast between client and agents.

        Yields (slim_name, StreamResponse) tuples from all agents as they arrive.

        The caller drives the session via request_stream: items are fanned out
        to every agent as they are yielded. When request_stream exhausts, the
        send side of every agent stream is closed; agents will complete their
        tasks and close their response streams naturally.

        Args:
            request_stream: Async generator of StreamRequest items from the caller.
            metadata:        SLIMRPC call metadata (e.g. slimrpc-live-routing).
        """
        sentinel = object()

        # Per-agent queue receives both client items (from fan_out_client) and
        # peer-forwarded items (from read_agent tasks for other agents).
        # Sentinel closes the send side — only fan_out_client sends it.
        queues: dict[str, asyncio.Queue] = {
            name: asyncio.Queue() for name, _ in self._agents
        }

        # Merged output: (slim_name, StreamResponse) or None (agent done signal).
        output_queue: asyncio.Queue[tuple[str, StreamResponse] | None] = asyncio.Queue()

        async def fan_out_client() -> None:
            """Distribute every client StreamRequest to all agent queues.

            Injects slim-src from source_slim_name into each item's message
            metadata — mirroring what the real SLIM transport would do by
            reading context.src() before passing items to the executor.

            When the client stream exhausts, puts sentinel into every agent
            queue to close the send side of each SendLiveMessage stream.
            """
            try:
                async for req in request_stream:
                    if self._source_slim_name and req.HasField("message"):
                        req.message.metadata.fields["slim-src"].string_value = self._source_slim_name
                    for q in queues.values():
                        await q.put(req)
            finally:
                for q in queues.values():
                    await q.put(sentinel)

        async def agent_send_stream(slim_name: str) -> AsyncGenerator[StreamRequest, None]:
            """Yield items from this agent's queue until sentinel."""
            q = queues[slim_name]
            while True:
                item = await q.get()
                if item is sentinel:
                    return
                yield item

        async def read_agent(slim_name: str, transport: SRPCTransport) -> None:
            """Read one agent's response stream, forward to peers, push to output."""
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

                    # Build forwarded StreamRequest for all other agents.
                    forwarded: StreamRequest | None = None

                    if response.HasField("task"):
                        task = response.task
                        forwarded = _event_data_message(
                            task,
                            "application/vnd.a2a.task+json",
                            slim_src=slim_name,
                            peer_task_id=task.id,
                        )

                    elif response.HasField("status_update"):
                        update = response.status_update
                        if update.status.HasField("message"):
                            forwarded = _forward_message(
                                msg=update.status.message,
                                slim_src=slim_name,
                                peer_task_id=peer_task_id,
                                event=update,
                                media_type="application/vnd.a2a.task-status-update+json",
                                state=update.status.state,
                            )
                        else:
                            forwarded = _event_data_message(
                                update,
                                "application/vnd.a2a.task-status-update+json",
                                slim_src=slim_name,
                                peer_task_id=peer_task_id,
                                state=update.status.state,
                            )

                    elif response.HasField("message_update"):
                        update = response.message_update
                        # Preserve the original slim-src from the out-of-band client;
                        # only stamp slim-peer-task-id so agents know which peer task
                        # received the external input.
                        fwd_msg = Message()
                        fwd_msg.CopyFrom(update.message)
                        fwd_msg.role = ROLE_USER
                        fwd_msg.metadata.fields["slim-peer-task-id"].string_value = peer_task_id
                        forwarded = StreamRequest(message=fwd_msg)

                    elif response.HasField("artifact_update"):
                        forwarded_artifact = StreamRequest(
                            artifact_update=response.artifact_update
                        )
                        for other_name, _ in self._agents:
                            if other_name != slim_name:
                                await queues[other_name].put(forwarded_artifact)
                        continue

                    if forwarded is not None:
                        for other_name, _ in self._agents:
                            if other_name != slim_name:
                                await queues[other_name].put(forwarded)

            except Exception as exc:
                short = slim_name.rsplit("/", 1)[-1]
                print(f"\033[1;31m[broadcast:{short}]\033[0m stream error: {exc}")
            finally:
                # Signal output collector that this agent is done.
                # Do NOT put sentinel into peer queues — their send streams are
                # only closed by fan_out_client when the caller's stream exhausts.
                await output_queue.put(None)

        fan_out_task = asyncio.create_task(fan_out_client(), name="broadcast-fan-out")
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
            fan_out_task.cancel()
            for task in reader_tasks:
                task.cancel()
            # Ensure agent send streams can unblock and exit.
            for q in queues.values():
                await q.put(sentinel)
