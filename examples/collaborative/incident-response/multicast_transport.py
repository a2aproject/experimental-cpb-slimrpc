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

"""Multicast broadcast routing for SendLiveMessage using a SLIM GROUP channel.

MulticastBroadcastTransport wraps a single SRPCMulticastTransport on a GROUP
channel.  SLIM delivers each outbound StreamRequest to all N agents natively —
no application-level fan-out loop needed.  Peer responses are relayed back
into the same group channel so every participant sees every message, just as
NStreamsBroadcastTransport does via N separate point-to-point streams.

See nstreams_transport.py for the equivalent that uses N independent streams.
"""

import asyncio
import uuid
from collections.abc import AsyncGenerator

import slim_bindings
from a2a.types.a2a_pb2 import (
    Message,
    Part,
    ROLE_USER,
    StreamRequest,
    StreamResponse,
    TaskState,
)

from slima2a.client_transport import SRPCMulticastTransport


def _task_state_name(state: TaskState) -> str:
    return TaskState.Name(state).removeprefix("TASK_STATE_").lower()


def _forward_message(
    msg: Message,
    slim_src: str,
    peer_task_id: str,
    state: TaskState | None = None,
) -> StreamRequest:
    forwarded = Message()
    forwarded.CopyFrom(msg)
    forwarded.role = ROLE_USER
    forwarded.metadata.fields["slim-src"].string_value = slim_src
    forwarded.metadata.fields["slim-peer-task-id"].string_value = peer_task_id
    if state is not None:
        forwarded.metadata.fields["slim-peer-state"].string_value = _task_state_name(state)
    return StreamRequest(message=forwarded)


def _synthetic_message(text: str, slim_src: str, peer_task_id: str, state: TaskState | None = None) -> StreamRequest:
    msg = Message(
        message_id=str(uuid.uuid4()),
        role=ROLE_USER,
        parts=[Part(text=text)],
    )
    msg.metadata.fields["slim-src"].string_value = slim_src
    msg.metadata.fields["slim-peer-task-id"].string_value = peer_task_id
    if state is not None:
        msg.metadata.fields["slim-peer-state"].string_value = _task_state_name(state)
    return StreamRequest(message=msg)


class MulticastBroadcastTransport:
    """Broadcast routing over a single SLIM GROUP channel bidi stream.

    Opens one SendLiveMessage stream to the group channel. SLIM fans each
    outbound StreamRequest to all N agents natively. Incoming StreamResponse
    items from any agent are translated to StreamRequest items and re-sent
    into the same group channel, so every agent sees every peer's output.

    Agents are responsible for filtering echoes of their own messages
    (sender != FULL_SLIM_NAME guards already in place in each agent).

    Usage::

        async def requests():
            yield first_message
            await asyncio.sleep(5)
            yield follow_up_message

        transport = MulticastBroadcastTransport(group_channel, source_slim_name="ns/grp/client")
        async for slim_name, response in transport.send_live_message(requests()):
            print(slim_name, response)
    """

    def __init__(self, channel: slim_bindings.Channel, source_slim_name: str = "") -> None:
        self._transport = SRPCMulticastTransport(channel)
        self._source_slim_name = source_slim_name

    async def send_live_message(
        self,
        request_stream: AsyncGenerator[StreamRequest, None],
        metadata: dict[str, str] | None = None,
    ) -> AsyncGenerator[tuple[str, StreamResponse], None]:
        """Open a GROUP SendLiveMessage stream and relay peer responses to all agents.

        Yields (slim_name, StreamResponse) tuples from all agents as they arrive.

        Args:
            request_stream: Async generator of StreamRequest items from the caller.
            metadata:        SLIMRPC call metadata (e.g. slimrpc-live-routing).
        """
        sentinel = object()

        # Merged send queue: receives items from both the caller's request_stream
        # and forwarded peer responses.  Sentinel closes the send side.
        merged_send_queue: asyncio.Queue = asyncio.Queue()

        async def fan_out_client() -> None:
            """Drain the caller's request_stream into merged_send_queue.

            Injects slim-src so agents can identify the client sender.
            When the caller's stream exhausts, sends the sentinel to close
            the group channel send side.
            """
            try:
                async for req in request_stream:
                    if self._source_slim_name and req.HasField("message"):
                        req.message.metadata.fields["slim-src"].string_value = self._source_slim_name
                    await merged_send_queue.put(req)
            finally:
                await merged_send_queue.put(sentinel)

        async def merged_stream() -> AsyncGenerator[StreamRequest, None]:
            """Yield items from merged_send_queue until sentinel."""
            while True:
                item = await merged_send_queue.get()
                if item is sentinel:
                    return
                yield item

        fan_out_task = asyncio.create_task(fan_out_client(), name="multicast-fan-out")

        try:
            async for source, response in self._transport.send_live_message(
                merged_stream(),
                context=None,
            ):
                slim_name = "/".join(source.source.components())
                yield slim_name, response

                # Translate the response to a StreamRequest and relay it back
                # into the group channel so all other agents see it.
                peer_task_id = ""
                if response.HasField("task"):
                    peer_task_id = response.task.id
                elif response.HasField("status_update"):
                    peer_task_id = response.status_update.task_id
                elif response.HasField("artifact_update"):
                    peer_task_id = response.artifact_update.task_id
                elif response.HasField("message_update"):
                    peer_task_id = response.message_update.task_id

                forwarded: StreamRequest | None = None

                if response.HasField("task"):
                    task = response.task
                    forwarded = _synthetic_message(
                        text=f"Agent {slim_name} started task {task.id}",
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
                            state=update.status.state,
                        )
                    else:
                        forwarded = _synthetic_message(
                            text=f"Agent {slim_name} state: {_task_state_name(update.status.state)}",
                            slim_src=slim_name,
                            peer_task_id=peer_task_id,
                            state=update.status.state,
                        )

                elif response.HasField("message_update"):
                    forwarded = _forward_message(
                        msg=response.message_update.message,
                        slim_src=slim_name,
                        peer_task_id=peer_task_id,
                    )

                elif response.HasField("artifact_update"):
                    forwarded = StreamRequest(artifact_update=response.artifact_update)

                if forwarded is not None:
                    try:
                        await merged_send_queue.put(forwarded)
                    except Exception:
                        pass  # send stream may already be closing

        except Exception as exc:
            print(f"\033[1;31m[multicast]\033[0m stream error: {exc}")
        finally:
            fan_out_task.cancel()
            # Unblock merged_stream if it's still waiting.
            await merged_send_queue.put(sentinel)
