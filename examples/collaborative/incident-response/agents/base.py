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

"""Shared helpers for the incident-response broadcast-live example."""

import uuid

import slim_bindings
from a2a.server.agent_execution import AgentExecutor
from a2a.server.request_handlers import DefaultRequestHandler
from a2a.server.tasks import InMemoryTaskStore
from a2a.types.a2a_pb2 import (
    AgentCapabilities,
    AgentCard,
    AgentExtension,
    AgentInterface,
    Message,
    Part,
    ROLE_AGENT,
)

from slima2a import setup_slim_client
from slima2a.handler import SRPCHandler
from slima2a.types.v1.a2a_pb2_slimrpc import (  # type: ignore[import]
    add_A2AServiceServicer_to_server as _add_a2a,
)

SLIM_URL = "http://localhost:46357"
SLIM_SECRET = "secretsecretsecretsecretsecretsecret"
NAMESPACE = "mydomain"
GROUP = "demo"

BROADCAST_LIVE_EXTENSION_URI = (
    "https://a2a-protocol.org/bindings/experimental-slimrpc/extensions/broadcast-live/v1"
)


# ---------------------------------------------------------------------------
# Message helpers
# ---------------------------------------------------------------------------


def make_agent_card(name: str, description: str, slim_name: str, skills: list) -> AgentCard:
    return AgentCard(
        name=name,
        description=description,
        version="1.0.0",
        supported_interfaces=[
            AgentInterface(
                url=f"slim://{slim_name}",
                protocol_binding="https://a2a-protocol.org/bindings/experimental-slimrpc/v1",
                protocol_version="1.1",
            )
        ],
        default_input_modes=["application/json"],
        default_output_modes=["application/json"],
        capabilities=AgentCapabilities(
            streaming=True,
            extensions=[
                AgentExtension(
                    uri=BROADCAST_LIVE_EXTENSION_URI,
                    description="Participates in broadcast live incident-response sessions.",
                    required=False,
                )
            ],
        ),
        skills=skills,
    )


def make_agent_message(text: str, slim_name: str, context_id: str = "", task_id: str = "") -> Message:
    """Build a ROLE_AGENT Message for emitting from an agent."""
    msg = Message(
        message_id=str(uuid.uuid4()),
        context_id=context_id,
        task_id=task_id,
        role=ROLE_AGENT,
        parts=[Part(text=text)],
    )
    msg.metadata.fields["slim-src"].string_value = slim_name
    return msg


def get_message_text(msg: Message) -> str:
    """Extract the first text content from an A2A Message."""
    for part in msg.parts:
        if part.HasField("text"):
            return part.text
    return ""


def get_slim_src(msg: Message) -> str:
    """Read slim-src metadata from a Message."""
    try:
        return msg.metadata["slim-src"]
    except (KeyError, ValueError):
        return "unknown"


# ---------------------------------------------------------------------------
# Agent startup helper
# ---------------------------------------------------------------------------


async def start_agent(
    slim_name: str,
    agent_card: AgentCard,
    agent_executor: AgentExecutor,
    slim_url: str = SLIM_URL,
    secret: str = SLIM_SECRET,
) -> None:
    """Connect to SLIM, register A2A service handler, and start serving."""
    _service, local_app, local_name, conn_id = await setup_slim_client(
        namespace=NAMESPACE,
        group=GROUP,
        name=slim_name,
        slim_url=slim_url,
        secret=secret,
        log_level="warn",
    )

    request_handler = DefaultRequestHandler(
        agent_executor=agent_executor,
        task_store=InMemoryTaskStore(),
        agent_card=agent_card,
    )
    srpc_handler = SRPCHandler(agent_card, request_handler)

    server = slim_bindings.Server.new_with_connection(local_app, local_name, conn_id)
    _add_a2a(srpc_handler, server)

    print(f"[{slim_name}] ready at {NAMESPACE}/{GROUP}/{slim_name}")
    await server.serve_async()
