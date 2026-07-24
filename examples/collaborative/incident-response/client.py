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

"""Incident-response client.

Creates a SLIM group channel, invites all four agents, initiates a Collaborate
session with a stable context_id (passed as SLIM RPC metadata), and sends an
anomaly trigger. Prints every message received from any channel member, with
slim-src attribution.

The context_id is forwarded to every agent via SLIM metadata under the key
"context-id". Agents use it to look up (or create) a shared AgentSession, so
a future SendMessage call with the same context_id would join the same session
and see the same history.
"""

import asyncio
import sys
import uuid
from datetime import timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import slim_bindings
from a2a.types.a2a_pb2 import Message, Part, ROLE_USER

from agents.base import (
    NAMESPACE,
    GROUP,
    SLIM_URL,
    SLIM_SECRET,
    CONTEXT_ID_METADATA_KEY,
    get_message_text,
    get_slim_src,
)
from generated.slimrpc_collaborative_channel_pb2_slimrpc import (
    CollaborativeChannelServiceGroupStub,
)
from slima2a import setup_slim_client

CLIENT_NAME = "client"

AGENT_NAMES = [
    "monitoring-agent",
    "log-agent",
    "diagnostics-agent",
    "remediation-agent",
]

CHANNEL_NAME = "incident-response-channel"


def make_user_message(text: str) -> Message:
    return Message(
        message_id=str(uuid.uuid4()),
        role=ROLE_USER,
        parts=[Part(text=text)],
    )


async def main():
    # Connect to SLIM as the client.
    _service, local_app, local_name, conn_id = await setup_slim_client(
        namespace=NAMESPACE,
        group=GROUP,
        name=CLIENT_NAME,
        slim_url=SLIM_URL,
        secret=SLIM_SECRET,
    )

    # Build the list of server SLIM Names (one per agent).
    server_names = [
        slim_bindings.Name(NAMESPACE, GROUP, agent_name)
        for agent_name in AGENT_NAMES
    ]

    # Create a SLIM group channel that includes all four agents.
    # The channel broadcasts every message to all members.
    channel = slim_bindings.Channel.new_group_with_connection(
        local_app, server_names, conn_id
    )

    stub = CollaborativeChannelServiceGroupStub(channel)

    # Prepare the initial anomaly trigger.
    initial_trigger = (
        "ANOMALY DETECTED: /api/checkout error rate 45% (threshold: 5%). "
        "Duration: 90s. Affected region: us-east-1."
    )

    async def messages():
        print(f"[{CLIENT_NAME}] sending: {initial_trigger!r}")
        yield make_user_message(initial_trigger)
        # Keep the send side open so agents can exchange messages freely.
        # The session ends when all agents close their streams (EOS).
        await asyncio.sleep(10)

    # Generate a stable context_id for this incident session.
    # All agents will use this to look up the shared AgentSession, so a future
    # SendMessage call with the same context_id joins the same history.
    context_id = str(uuid.uuid4())
    print(f"\n--- Incident Response Collaborate Session (context_id={context_id}) ---\n")

    try:
        async for ctx, msg in stub.Collaborate(
            messages(),
            timeout=timedelta(seconds=15),
            metadata={CONTEXT_ID_METADATA_KEY: context_id},
        ):
            sender = get_slim_src(msg)
            text = get_message_text(msg)
            print(f"[{sender}] {text}")
    finally:
        await channel.close_async(timeout=None)

    print(f"\n--- Session complete (context_id={context_id}) ---")


if __name__ == "__main__":
    asyncio.run(main())
