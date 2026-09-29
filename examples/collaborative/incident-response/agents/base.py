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

"""Shared helpers for the incident-response collaborative task example."""

import uuid

import slim_bindings
from google.protobuf.struct_pb2 import Struct
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
from slima2a.handler import SRPCHandler, SRPCSharedHandler
from slima2a.types.v1.a2a_pb2_slimrpc import (  # type: ignore[import]
    add_A2AServiceServicer_to_server as _add_a2a,
    add_A2AServiceServicer_to_server_shared as _add_a2a_shared,
)

SLIM_URL = "http://localhost:46357"
SLIM_SECRET = "secretsecretsecretsecretsecretsecret"
NAMESPACE = "mydomain"
GROUP = "demo"

A2A_SHARED_TASK_EXTENSION_URI = "https://a2a-protocol.org/extensions/shared-task/v1"
A2A_COLLABORATIVE_TASK_EXTENSION_URI = (
    "https://a2a-protocol.org/extensions/collaborative-task/v1"
)
SLIMRPC_COLLABORATIVE_TASK_EXTENSION_URI = (
    "https://a2a-protocol.org/bindings/experimental-slimrpc/extensions/collaborative-task/v1"
)

# ANSI color codes for log output — one per named participant.
_COLORS = {
    "client":             "\033[1;37m",   # bold white
    "monitoring-agent":   "\033[1;33m",   # bold yellow
    "log-agent":          "\033[1;36m",   # bold cyan
    "diagnostics-agent":  "\033[1;35m",   # bold magenta
    "remediation-agent":  "\033[1;32m",   # bold green
}
_RESET = "\033[0m"


def log(name: str, msg: str) -> None:
    """Print a color-coded log line prefixed with the participant name."""
    color = _COLORS.get(name, "\033[1m")
    print(f"{color}[{name}]{_RESET} {msg}")


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
                    uri="https://a2a-protocol.org/extensions/shared-task/v1",
                    description="Supports multiple clients sending to the same task with per-message sender identity.",
                    required=False,
                ),
                AgentExtension(
                    uri=A2A_COLLABORATIVE_TASK_EXTENSION_URI,
                    description="Supports collaborative task sessions with peer agents via a relay.",
                    required=False,
                ),
                AgentExtension(
                    uri=SLIMRPC_COLLABORATIVE_TASK_EXTENSION_URI,
                    description="Supports native SLIMRPC collaborative task sessions on SLIM group channels.",
                    required=False,
                ),
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
    set_message_sender(msg, slim_name)
    return msg


def set_message_sender(msg: Message, sender: str) -> None:
    """Set message-sender in Message.metadata under the shared-task extension URI."""
    inner = Struct()
    inner.fields["message-sender"].string_value = sender
    msg.metadata.fields[A2A_SHARED_TASK_EXTENSION_URI].struct_value.CopyFrom(inner)


def get_message_sender(msg: Message) -> str:
    """Read message-sender from Message.metadata under the shared-task extension URI."""
    try:
        return (
            msg.metadata.fields[A2A_SHARED_TASK_EXTENSION_URI]
            .struct_value.fields["message-sender"]
            .string_value
        )
    except (KeyError, ValueError):
        return "unknown"


def get_message_text(msg: Message) -> str:
    """Extract the first text content from an A2A Message."""
    for part in msg.parts:
        if part.HasField("text"):
            return part.text
    return ""


# ---------------------------------------------------------------------------
# Agent startup helper
# ---------------------------------------------------------------------------


async def start_agent(
    slim_name: str,
    agent_card: AgentCard,
    agent_executor: AgentExecutor,
    slim_url: str = SLIM_URL,
    secret: str = SLIM_SECRET,
    shared_responses: bool = False,
) -> None:
    """Connect to SLIM, register A2A service handler, and start serving.

    Pass shared_responses=True for native fan-out mode: registers both
    SRPCHandler and SRPCSharedHandler so the agent accepts both point-to-point
    and collaborative task SendLiveMessage calls.
    """
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

    if shared_responses:
        server = slim_bindings.Server.new_with_shared_responses_and_connection(
            local_app, local_name, conn_id
        )
        _add_a2a(SRPCHandler(agent_card, request_handler), server)
        _add_a2a_shared(SRPCSharedHandler(agent_card, request_handler), server)
    else:
        server = slim_bindings.Server.new_with_connection(local_app, local_name, conn_id)
        _add_a2a(SRPCHandler(agent_card, request_handler), server)

    log(slim_name, f"ready at {NAMESPACE}/{GROUP}/{slim_name}")
    await server.serve_async()
