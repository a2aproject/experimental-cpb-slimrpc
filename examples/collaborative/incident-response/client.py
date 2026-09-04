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

Connects to each of the four agents over point-to-point SLIM channels,
initiates a SendLiveMessage broadcast session via BroadcastLiveClient,
and prints every (agent, event) tuple received during the session.

The BroadcastLiveClient implements application-layer broadcast routing:
each agent's StreamResponse items are forwarded as StreamRequest items to
all other agents, so every participant sees the full conversation.
"""

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
)
from broadcast_transport import BroadcastLiveClient
from slima2a import setup_slim_client
from slima2a.client_transport import SRPCTransport

CLIENT_NAME = "client"

AGENT_NAMES = [
    "monitoring-agent",
    "log-agent",
    "diagnostics-agent",
    "remediation-agent",
]


def _make_initial_request(text: str) -> StreamRequest:
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


async def main() -> None:
    _service, local_app, _local_name, conn_id = await setup_slim_client(
        namespace=NAMESPACE,
        group=GROUP,
        name=CLIENT_NAME,
        slim_url=SLIM_URL,
        secret=SLIM_SECRET,
    )

    factory = _channel_factory(local_app, conn_id)

    agents = []
    for agent_name in AGENT_NAMES:
        slim_name = f"{NAMESPACE}/{GROUP}/{agent_name}"
        channel = factory(slim_name)
        transport = SRPCTransport(channel=channel, agent_card=None)
        agents.append((slim_name, transport))

    broadcast_client = BroadcastLiveClient(agents)

    trigger = (
        "ANOMALY DETECTED: /api/checkout error rate 45% (threshold: 5%). "
        "Duration: 90s. Affected region: us-east-1."
    )
    print(f"\n--- Broadcast Live Session ---\n")
    print(f"[client] sending: {trigger!r}\n")

    initial_request = _make_initial_request(trigger)

    async for slim_name, response in broadcast_client.send_live_message(
        initial_request,
        metadata={"slimrpc-live-routing": "broadcast"},
    ):
        if response.HasField("task"):
            task = response.task
            print(f"[{slim_name}] task={task.id!r} context={task.context_id!r}")
        elif response.HasField("status_update"):
            update = response.status_update
            msg_text = get_message_text(update.status.message) if update.status.HasField("message") else ""
            state = update.status.state
            from a2a.types.a2a_pb2 import TaskState
            state_name = TaskState.Name(state).removeprefix("TASK_STATE_").lower()
            if msg_text:
                print(f"[{slim_name}] [{state_name}] {msg_text}")
            else:
                print(f"[{slim_name}] state={state_name}")
        elif response.HasField("message_update"):
            update = response.message_update
            text = get_message_text(update.message)
            print(f"[{slim_name}] {text}")
        elif response.HasField("artifact_update"):
            print(f"[{slim_name}] artifact update")

    print(f"\n--- Session complete ---")


if __name__ == "__main__":
    asyncio.run(main())
