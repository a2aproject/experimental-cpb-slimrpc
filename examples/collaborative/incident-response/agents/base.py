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

"""Shared helpers and extension classes for the incident-response Collaborate example.

Architecture
------------
Both SendMessage and Collaborate flows route through the same AgentSession keyed
by context_id, giving agents a unified history and a set of OutputChannels.

                SendMessage (HTTP)         Collaborate (SLIM stream)
                      │                           │
                      ▼                           ▼
            DefaultRequestHandler    CollaborativeSRPCHandler
                      │                 extract "context-id" from SLIM metadata
                      ▼                           │
            SessionAwareAgentExecutor.execute()◄──┘
                      │
              session = SessionRegistry.get_or_create(context_id)
              channel = TaskOutputChannel | CollaborateOutputChannel
              session.enqueue(ChannelMessage(msg, channel_id))
                      │
              session._loop_task  (one background task per context_id)
                      │
              on_session_message(session, channel_message)
                      │
              agent reads session.history, session.channels
              agent writes via channel.send() / channel.close()

This mirrors portable-agent (github.com/Tehsmash/portable-agent):
  one goroutine per session, sequential processing, injectPending support.
"""

import asyncio
import sys
import uuid
from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator, AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# Allow imports from the example root (for `generated/`)
sys.path.insert(0, str(Path(__file__).parents[1]))

import slim_bindings
from a2a.server.agent_execution import AgentExecutor, RequestContext
from a2a.server.context import ServerCallContext
from a2a.server.events import EventQueue
from a2a.server.request_handlers import DefaultRequestHandler
from a2a.server.tasks import InMemoryTaskStore
from a2a.types.a2a_pb2 import (
    AgentCapabilities,
    AgentCard,
    AgentExtension,
    AgentInterface,
    AgentSkill,
    Message,
    Part,
    ROLE_AGENT,
    ROLE_USER,
)

from slima2a import setup_slim_client
from slima2a.handler import SRPCHandler
from slima2a.types.v1.a2a_pb2_slimrpc import (  # type: ignore[import]
    add_A2AServiceServicer_to_server as _add_a2a,
)

from generated.slimrpc_collaborative_channel_pb2_slimrpc import (
    CollaborativeChannelServiceServicer,
    add_CollaborativeChannelServiceServicer_to_server,
)

SLIM_URL = "http://localhost:46357"
SLIM_SECRET = "secretsecretsecretsecretsecretsecret"
NAMESPACE = "mydomain"
GROUP = "demo"

COLLABORATE_EXTENSION_URI = (
    "https://a2a-protocol.org/bindings/experimental-slimrpc/extensions/collaborate/v1"
)

# SLIM metadata key used to carry context_id on the Collaborate RPC call.
CONTEXT_ID_METADATA_KEY = "context-id"


# ---------------------------------------------------------------------------
# OutputChannel hierarchy
# ---------------------------------------------------------------------------


class OutputChannel(ABC):
    """Abstract output target — either a Task event queue or a Collaborate stream.

    Agents write to channels via send() and signal completion via close().
    Multiple channels can be registered on a session simultaneously, letting
    the agent choose which channel(s) to address.
    """

    @property
    @abstractmethod
    def channel_id(self) -> str:
        """Unique ID: task_id for SendMessage, session_id for Collaborate."""
        ...

    @property
    def is_closed(self) -> bool:
        return self._closed_event.is_set()

    @abstractmethod
    async def send(self, message: Message) -> None:
        """Enqueue a message without closing the channel."""
        ...

    @abstractmethod
    async def close(self, final_message: Message | None = None) -> None:
        """Optionally enqueue a final message and mark this channel as closed."""
        ...

    async def wait_closed(self) -> None:
        """Await until close() has been called."""
        await self._closed_event.wait()


class TaskOutputChannel(OutputChannel):
    """OutputChannel for a standard A2A SendMessage task.

    Wraps an EventQueue. The A2A EventConsumer treats a plain Message event as
    the terminal signal that ends the SSE/HTTP stream, so agents call close()
    with the final response when they are done with this task.

    NOTE: close() does NOT call queue.close(); the DefaultRequestHandler's
    _run_event_stream() does that after execute() returns.
    """

    def __init__(self, task_id: str, event_queue: EventQueue) -> None:
        self._task_id = task_id
        self._queue = event_queue
        self._closed_event = asyncio.Event()

    @property
    def channel_id(self) -> str:
        return self._task_id

    async def send(self, message: Message) -> None:
        """Enqueue a non-terminal streaming update."""
        await self._queue.enqueue_event(message)

    async def close(self, final_message: Message | None = None) -> None:
        """Enqueue the final response and unblock SessionAwareAgentExecutor.execute().

        A plain Message event is treated as terminal by EventConsumer.consume_all(),
        which ends the HTTP stream. The queue itself is closed by the handler
        after execute() returns — not here.
        """
        if self._closed_event.is_set():
            return
        if final_message is not None:
            await self._queue.enqueue_event(final_message)
        self._closed_event.set()


class CollaborateOutputChannel(OutputChannel):
    """OutputChannel for a SLIM Collaborate bidirectional stream.

    Wraps an EventQueue. Messages are yielded back to the SLIM transport by
    CollaborativeRequestHandler's drain loop. The stream stays open until
    execute() returns (SLIM disconnect), at which point the queue is closed.
    """

    def __init__(self, session_id: str, event_queue: EventQueue) -> None:
        self._session_id = session_id
        self._queue = event_queue
        self._closed_event = asyncio.Event()

    @property
    def channel_id(self) -> str:
        return self._session_id

    async def send(self, message: Message) -> None:
        """Enqueue a message to the Collaborate stream."""
        await self._queue.enqueue_event(message)

    async def close(self, final_message: Message | None = None) -> None:
        """Optionally send a final message and mark as closed.

        The Collaborate execute() path closes the queue when the SLIM message
        stream ends — agents do not need to call this explicitly.
        """
        if self._closed_event.is_set():
            return
        if final_message is not None:
            await self._queue.enqueue_event(final_message)
        self._closed_event.set()


# ---------------------------------------------------------------------------
# ChannelMessage
# ---------------------------------------------------------------------------


@dataclass
class ChannelMessage:
    """An incoming message plus the channel it arrived on."""

    message: Message
    source_channel_id: str  # task_id (SendMessage) or session_id (Collaborate)


# ---------------------------------------------------------------------------
# AgentSession
# ---------------------------------------------------------------------------


class AgentSession:
    """Persistent session for one context_id.

    Manages a shared history, per-session agent state, and a set of active
    output channels. A single background asyncio.Task processes incoming
    messages sequentially — mirroring portable-agent's one-goroutine-per-session
    model, which prevents race conditions on session state and history.

    Both SendMessage tasks and Collaborate streams register channels and enqueue
    messages here; the agent sees a unified inbox and can write to any channel.

    Usage::

        session = AgentSession("ctx-123")
        await session.register_channel(channel)
        session.start(my_agent.on_session_message)
        await session.enqueue(ChannelMessage(msg, channel.channel_id))
    """

    def __init__(self, context_id: str) -> None:
        self.context_id = context_id
        self.history: list[Message] = []
        self.agent_state: dict[str, Any] = {}
        self._inbox: asyncio.Queue[ChannelMessage] = asyncio.Queue(maxsize=100)
        self._channels: dict[str, OutputChannel] = {}
        self._channels_lock = asyncio.Lock()
        self._loop_task: asyncio.Task | None = None

    @property
    def channels(self) -> dict[str, OutputChannel]:
        """Snapshot of currently registered output channels."""
        return dict(self._channels)

    async def register_channel(self, channel: OutputChannel) -> None:
        async with self._channels_lock:
            self._channels[channel.channel_id] = channel

    async def unregister_channel(self, channel_id: str) -> None:
        async with self._channels_lock:
            self._channels.pop(channel_id, None)

    async def enqueue(self, cm: ChannelMessage) -> None:
        """Append message to history and submit to inbox for processing."""
        self.history.append(cm.message)
        await self._inbox.put(cm)

    async def drain_pending(self) -> list[ChannelMessage]:
        """Non-blocking drain of all currently queued inbox messages.

        Equivalent to portable-agent's injectPending() — call this between
        LLM tool-use iterations to check for superseding messages and inject
        them into history before continuing.
        """
        pending: list[ChannelMessage] = []
        while True:
            try:
                cm = self._inbox.get_nowait()
                self._inbox.task_done()
                self.history.append(cm.message)
                pending.append(cm)
            except asyncio.QueueEmpty:
                break
        return pending

    def start(
        self,
        processor: Callable[["AgentSession", ChannelMessage], Awaitable[None]],
    ) -> None:
        """Start the background session loop if not already running."""
        if self._loop_task is None or self._loop_task.done():
            self._loop_task = asyncio.create_task(
                self._run(processor),
                name=f"session-loop-{self.context_id}",
            )

    async def _run(
        self,
        processor: Callable[["AgentSession", ChannelMessage], Awaitable[None]],
    ) -> None:
        """Background loop: dequeue messages and call processor sequentially.

        Stops automatically when no channels are registered and the inbox is
        empty — the session can be restarted via start() if needed.
        """
        while True:
            try:
                cm = await asyncio.wait_for(self._inbox.get(), timeout=1.0)
                self._inbox.task_done()
                await processor(self, cm)
            except asyncio.TimeoutError:
                async with self._channels_lock:
                    has_channels = bool(self._channels)
                if not has_channels and self._inbox.empty():
                    break
            except Exception as e:
                print(f"[session:{self.context_id}] loop error: {e}")


# ---------------------------------------------------------------------------
# SessionRegistry
# ---------------------------------------------------------------------------


class SessionRegistry:
    """Registry of active AgentSessions keyed by context_id."""

    def __init__(self) -> None:
        self._sessions: dict[str, AgentSession] = {}
        self._lock = asyncio.Lock()

    async def get_or_create(self, context_id: str) -> AgentSession:
        async with self._lock:
            if context_id not in self._sessions:
                self._sessions[context_id] = AgentSession(context_id)
            return self._sessions[context_id]

    async def get(self, context_id: str) -> AgentSession | None:
        async with self._lock:
            return self._sessions.get(context_id)


# ---------------------------------------------------------------------------
# CollaborateRequestContext
# ---------------------------------------------------------------------------


class CollaborateRequestContext(RequestContext):
    """RequestContext for a Collaborate session.

    Carries the context_id extracted from SLIM RPC metadata so that
    SessionAwareAgentExecutor can map Collaborate streams and SendMessage tasks
    to the same AgentSession.

    Agents detect this context via isinstance(context, CollaborateRequestContext).
    They read incoming messages from context.message_stream and write outbound
    messages via session.channels[id].send() / .close().
    """

    def __init__(
        self,
        message_stream: AsyncIterator[Message],
        session_id: str,
        context_id: str,
        call_context: ServerCallContext | None = None,
    ):
        super().__init__(request=None, call_context=call_context)
        self._message_stream = message_stream
        self._session_id = session_id
        self._collaborate_context_id = context_id

    @property
    def context_id(self) -> str:
        """Context ID from SLIM metadata (overrides auto-generated UUID)."""
        return self._collaborate_context_id

    @property
    def session_id(self) -> str:
        """RPC session ID (UUID generated per Collaborate invocation)."""
        return self._session_id

    @property
    def message_stream(self) -> AsyncIterator[Message]:
        """Async iterator of incoming Collaborate Messages from the channel."""
        return self._message_stream

    @property
    def is_collaborate(self) -> bool:
        return True


# ---------------------------------------------------------------------------
# SessionAwareAgentExecutor
# ---------------------------------------------------------------------------


class SessionAwareAgentExecutor(AgentExecutor, ABC):
    """AgentExecutor that routes both SendMessage and Collaborate through a shared session.

    Both call types for the same context_id map to one AgentSession, giving the
    agent a unified history and a dict of OutputChannels to write to.

    Subclasses implement on_session_message() instead of execute().

    For Collaborate channels: call ``await channel.send(message)`` to emit messages
    back to the stream.

    For Task channels: call ``await channel.close(final_message)`` when done —
    this enqueues the final response and unblocks execute() so the HTTP handler
    can return it to the client.
    """

    def __init__(self) -> None:
        self._registry = SessionRegistry()

    async def execute(self, context: RequestContext, event_queue: EventQueue) -> None:
        if isinstance(context, CollaborateRequestContext):
            await self._execute_collaborate(context, event_queue)
        else:
            await self._execute_task(context, event_queue)

    async def _execute_collaborate(
        self,
        context: CollaborateRequestContext,
        event_queue: EventQueue,
    ) -> None:
        """Feed incoming Collaborate messages into the session inbox."""
        channel = CollaborateOutputChannel(context.session_id, event_queue)
        session = await self._registry.get_or_create(context.context_id)
        await session.register_channel(channel)
        session.start(self.on_session_message)

        async for msg in context.message_stream:
            # Drop messages whose context_id explicitly mismatches this session.
            if msg.context_id and msg.context_id != context.context_id:
                print(
                    f"[session:{context.context_id}] dropping message with "
                    f"mismatched context_id={msg.context_id!r}"
                )
                continue
            await session.enqueue(ChannelMessage(msg, context.session_id))

        await session.unregister_channel(context.session_id)
        if not event_queue.is_closed():
            await event_queue.close()

    async def _execute_task(
        self,
        context: RequestContext,
        event_queue: EventQueue,
    ) -> None:
        """Inject a SendMessage request into the session and wait for the response."""
        ctx_id = context.context_id or context.task_id
        channel = TaskOutputChannel(context.task_id, event_queue)
        session = await self._registry.get_or_create(ctx_id)
        await session.register_channel(channel)
        session.start(self.on_session_message)

        if context.message:
            await session.enqueue(ChannelMessage(context.message, context.task_id))

        # Block until the agent calls channel.close() with the final response.
        await channel.wait_closed()
        await session.unregister_channel(context.task_id)

    async def cancel(self, context: RequestContext, event_queue: EventQueue) -> None:
        pass

    @abstractmethod
    async def on_session_message(
        self,
        session: AgentSession,
        incoming: ChannelMessage,
    ) -> None:
        """Called once per incoming message in the session loop.

        Args:
            session:  The AgentSession for this context_id.
                      - session.history: all messages seen so far (including incoming)
                      - session.channels: snapshot of active OutputChannels
                      - session.agent_state: dict for per-session persistent state
                      - session.drain_pending(): inject any queued messages (LLM loops)
            incoming: The message and the channel it arrived on.

        To respond:
            - Collaborate channel: ``await channel.send(make_message(...))``
            - Task channel: ``await channel.close(make_message(...))``  (final only)
        """
        ...


# ---------------------------------------------------------------------------
# CollaborativeRequestHandler
# ---------------------------------------------------------------------------


class CollaborativeRequestHandler(DefaultRequestHandler):
    """DefaultRequestHandler extended with on_collaborate().

    on_collaborate() mirrors on_message_send_stream(): creates an EventQueue,
    runs agent_executor.execute() in a background task, and drains the queue —
    yielding every Message event back to the SLIM transport.

    All other A2A methods are inherited unchanged from DefaultRequestHandler.
    """

    async def on_collaborate(
        self,
        request_iterator: AsyncIterator[Message],
        slim_context,
        context_id: str,
    ) -> AsyncGenerator[Message]:
        """Run agent logic for a Collaborate session and yield outbound Messages.

        Args:
            request_iterator: Async iterator of incoming channel Messages.
            slim_context:     slim_bindings.Context from the SLIM transport layer.
            context_id:       Context ID extracted from SLIM RPC metadata
                              (key: CONTEXT_ID_METADATA_KEY).
        """
        ctx = CollaborateRequestContext(
            message_stream=request_iterator,
            session_id=str(uuid.uuid4()),
            context_id=context_id,
        )
        queue = EventQueue()

        async def _run() -> None:
            await self.agent_executor.execute(ctx, queue)
            # execute() closes the queue for Collaborate; this is a safety net.
            if not queue.is_closed():
                await queue.close()

        execute_task = asyncio.create_task(_run())

        # Drain the queue: yield every Message event back to the SLIM transport.
        # Uses the same timeout pattern as EventConsumer.consume_all() but does
        # NOT stop on the first Message — Collaborate sessions produce many.
        while True:
            try:
                event = await asyncio.wait_for(queue.dequeue_event(), timeout=0.5)
                queue.task_done()
                if isinstance(event, Message):
                    yield event
            except (asyncio.TimeoutError, TimeoutError):
                if queue.is_closed():
                    break
            except Exception:
                # QueueShutDown (Python 3.13+) or QueueEmpty after close
                break

        await execute_task


# ---------------------------------------------------------------------------
# CollaborativeSRPCHandler
# ---------------------------------------------------------------------------


class CollaborativeSRPCHandler(SRPCHandler, CollaborativeChannelServiceServicer):
    """SRPCHandler extended with CollaborativeChannelService.Collaborate().

    Extracts the context_id from SLIM RPC metadata (key: CONTEXT_ID_METADATA_KEY)
    and forwards it to on_collaborate() so both Collaborate streams and SendMessage
    tasks for the same context_id share one AgentSession.

    Registration::

        _add_a2a(srpc_handler, server)
        add_CollaborativeChannelServiceServicer_to_server(srpc_handler, server)
    """

    async def Collaborate(self, request_iterator, context):
        # context is slim_bindings.Context; metadata() returns dict[str, str].
        context_id = (
            context.metadata().get(CONTEXT_ID_METADATA_KEY) or context.session_id()
        )
        async for msg in self.request_handler.on_collaborate(
            request_iterator, context, context_id
        ):
            yield msg


# ---------------------------------------------------------------------------
# Shared message helpers
# ---------------------------------------------------------------------------


def make_agent_card(name: str, description: str, slim_name: str, skills: list) -> AgentCard:
    """Build a minimal AgentCard for an incident-response agent."""
    return AgentCard(
        name=name,
        description=description,
        version="1.0.0",
        supported_interfaces=[
            AgentInterface(
                url=f"slim://{slim_name}",
                protocol_binding="https://a2a-protocol.org/bindings/experimental-slimrpc/v1",
                protocol_version="1.0",
            )
        ],
        default_input_modes=["application/json"],
        default_output_modes=["application/json"],
        capabilities=AgentCapabilities(
            streaming=True,
            extensions=[
                AgentExtension(
                    uri=COLLABORATE_EXTENSION_URI,
                    description="Participates in collaborative incident-response sessions.",
                    required=False,
                )
            ],
        ),
        skills=skills,
    )


def make_message(text: str, slim_src: str) -> Message:
    """Create an A2A agent Message with slim-src attribution set.

    In a production SLIMRPC implementation, the SLIMRPC layer populates
    slim-src automatically from the SLIM transport src field. In this example
    we set it explicitly so the output is observable without the full transport.
    """
    msg = Message(
        message_id=str(uuid.uuid4()),
        role=ROLE_AGENT,
        parts=[Part(text=text)],
    )
    # Message.metadata is a google.protobuf.Struct; use .fields for mutation.
    msg.metadata.fields["slim-src"].string_value = slim_src
    return msg


def get_message_text(msg: Message) -> str:
    """Extract the first text content from an A2A Message, or empty string."""
    for part in msg.parts:
        if part.HasField("text"):
            return part.text  # text is a plain string inside a oneof
    return ""


def get_slim_src(msg: Message) -> str:
    """Read the slim-src metadata key from a Message, or 'unknown'.

    The Struct __getitem__ unwraps google.protobuf.Value to the native Python type.
    """
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
    agent_executor,
    slim_url: str = SLIM_URL,
    secret: str = SLIM_SECRET,
):
    """Connect to SLIM, register A2A + Collaborate handlers, and start serving.

    Args:
        slim_name:      The last component of the SLIM name (e.g. "log-agent").
        agent_card:     AgentCard for this agent.
        agent_executor: SessionAwareAgentExecutor subclass whose on_session_message()
                        handles both standard A2A requests and Collaborate sessions.
        slim_url:       SLIM node URL.
        secret:         Shared secret for SLIM identity.
    """
    _service, local_app, local_name, conn_id = await setup_slim_client(
        namespace=NAMESPACE,
        group=GROUP,
        name=slim_name,
        slim_url=slim_url,
        secret=secret,
        log_level="warn",
    )

    request_handler = CollaborativeRequestHandler(
        agent_executor=agent_executor,
        task_store=InMemoryTaskStore(),
    )
    srpc_handler = CollaborativeSRPCHandler(agent_card, request_handler)

    server = slim_bindings.Server.new_with_connection(local_app, local_name, conn_id)
    _add_a2a(srpc_handler, server)
    add_CollaborativeChannelServiceServicer_to_server(srpc_handler, server)

    print(f"[{slim_name}] ready at {NAMESPACE}/{GROUP}/{slim_name}")
    await server.serve_async()
