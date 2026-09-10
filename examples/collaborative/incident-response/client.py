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

"""Incident-response broadcast live session client.

Connects to agents over SLIM and initiates a SendLiveMessage broadcast session.
Three transport modes are available via --transport:

  nstreams          (default) NStreamsBroadcastTransport — N point-to-point SRPCTransport
                    instances, application-layer fan-out and cross-agent relay.

  multicast         MulticastBroadcastTransport — single SRPCMulticastTransport on a
                    SLIM GROUP channel; SLIM delivers fan-out natively and the relay
                    re-sends peer responses back into the same group stream.

  native-broadcast  SRPCMulticastTransport on a SLIM GROUP channel with shared-responses
                    enabled; SLIM handles broadcast routing natively end-to-end with no
                    application-layer relay. Requires agents started with --shared-responses.
"""

import argparse
import asyncio
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import slim_bindings
from a2a.types.a2a_pb2 import Message, Part, ROLE_USER, StreamRequest

from agents.base import (
    NAMESPACE,
    GROUP,
    SLIM_URL,
    SLIM_SECRET,
    get_message_text,
    log,
)
from slima2a import setup_slim_client
from slima2a.client_transport import SRPCTransport

CLIENT_NAME = "client"
CLIENT_SLIM_NAME = f"{NAMESPACE}/{GROUP}/{CLIENT_NAME}"

AGENT_NAMES = [
    "monitoring-agent",
    "log-agent",
    "diagnostics-agent",
    "remediation-agent",
]


def _make_request(text: str) -> StreamRequest:
    msg = Message(
        message_id=str(uuid.uuid4()),
        role=ROLE_USER,
        parts=[Part(text=text)],
    )
    return StreamRequest(message=msg)


def _channel_factory(local_app: slim_bindings.App, conn_id: int):
    def factory(remote: str) -> slim_bindings.Channel:
        parts = remote.split("/")
        return slim_bindings.Channel.new_with_connection(
            local_app,
            slim_bindings.Name(parts[0], parts[1], parts[2]),
            conn_id,
        )
    return factory


async def main(transport_mode: str = "nstreams") -> None:
    _service, local_app, _local_name, conn_id = await setup_slim_client(
        namespace=NAMESPACE,
        group=GROUP,
        name=CLIENT_NAME,
        slim_url=SLIM_URL,
        secret=SLIM_SECRET,
    )

    if transport_mode == "native-broadcast":
        from slima2a import slimrpc_group_shared_channel_factory
        from slima2a.client_transport import SRPCMulticastTransport

        factory = slimrpc_group_shared_channel_factory(local_app, conn_id)
        channel = factory([f"{NAMESPACE}/{GROUP}/{name}" for name in AGENT_NAMES])
        _transport = SRPCMulticastTransport(channel)

        async def send_live_message(self, request_stream, metadata=None):
            async for source, response in _transport.send_live_message(
                request_stream, context=None
            ):
                slim_name = "/".join(source.source.components())
                yield slim_name, response

        broadcast_client = type("_NativeBroadcast", (), {"send_live_message": send_live_message})()
        log("client", "transport: native-broadcast (SLIM shared-responses GROUP channel)")

    elif transport_mode == "multicast":
        from multicast_transport import MulticastBroadcastTransport
        from slima2a.client_transport import slimrpc_group_channel_factory

        factory = slimrpc_group_channel_factory(local_app, conn_id)
        channel = factory([f"{NAMESPACE}/{GROUP}/{name}" for name in AGENT_NAMES])
        broadcast_client = MulticastBroadcastTransport(channel, source_slim_name=CLIENT_SLIM_NAME)
        log("client", f"transport: multicast (GROUP channel)")
    else:
        from nstreams_transport import NStreamsBroadcastTransport

        p2p_factory = _channel_factory(local_app, conn_id)
        agents = []
        for agent_name in AGENT_NAMES:
            slim_name = f"{NAMESPACE}/{GROUP}/{agent_name}"
            channel = p2p_factory(slim_name)
            transport = SRPCTransport(channel=channel, agent_card=None)
            agents.append((slim_name, transport))
        broadcast_client = NStreamsBroadcastTransport(agents, source_slim_name=CLIENT_SLIM_NAME)
        log("client", f"transport: nstreams ({len(AGENT_NAMES)} point-to-point streams)")

    from a2a.types.a2a_pb2 import TaskState

    trigger = (
        "ANOMALY DETECTED: /api/checkout error rate 45% (threshold: 5%). "
        "Duration: 90s. Affected region: us-east-1."
    )
    print(f"\n--- Broadcast Live Session ---\n")
    log("client", f"sending: {trigger!r}\n")

    # Queue lets the response loop inject follow-up requests into the send stream.
    # None is the close signal.
    send_queue: asyncio.Queue[StreamRequest | None] = asyncio.Queue()

    async def requests():
        yield _make_request(trigger)
        while True:
            item = await send_queue.get()
            if item is None:
                return
            yield item

    remediation_agent = f"{NAMESPACE}/{GROUP}/remediation-agent"
    approved = False

    async for slim_name, response in broadcast_client.send_live_message(
        requests(),
        metadata={"slimrpc-live-routing": "broadcast"},
    ):
        # Extract short agent name for log coloring (last path component).
        short_name = slim_name.rsplit("/", 1)[-1]

        if response.HasField("task"):
            task = response.task
            log(short_name, f"task={task.id!r} context={task.context_id!r}")

        elif response.HasField("status_update"):
            update = response.status_update
            msg_text = get_message_text(update.status.message) if update.status.HasField("message") else ""
            state_name = TaskState.Name(update.status.state).removeprefix("TASK_STATE_").lower()
            if msg_text:
                log(short_name, f"[{state_name}] {msg_text}")
            else:
                log(short_name, f"state={state_name}")

            # Approve the remediation plan on first REMEDIATION message from the
            # remediation agent. The approval is broadcast to all agents.
            if (
                not approved
                and slim_name == remediation_agent
                and "REMEDIATION:" in msg_text
            ):
                approved = True
                approval = "APPROVED: please execute the remediation plan."
                print()
                log("client", f"sending approval: {approval!r}")
                print()
                await send_queue.put(_make_request(approval))

            # Close the session once the remediation agent confirms execution.
            if (
                slim_name == remediation_agent
                and "REMEDIATION EXECUTED:" in msg_text
            ):
                print()
                log("client", "remediation confirmed — closing session")
                print()
                await send_queue.put(None)

        elif response.HasField("message_update"):
            text = get_message_text(response.message_update.message)
            log(short_name, text)

        elif response.HasField("artifact_update"):
            log(short_name, "artifact update")

    print(f"\n--- Session complete ---")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Incident-response broadcast live client")
    parser.add_argument(
        "--transport",
        choices=["nstreams", "multicast", "native-broadcast"],
        default="nstreams",
        help="Transport mode: nstreams, multicast, or native-broadcast (requires --shared-responses agents)",
    )
    args = parser.parse_args()
    asyncio.run(main(transport_mode=args.transport))
