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

"""Log agent — surfaces relevant log entries when an alert is broadcast.

When the input_queue delivers an ALERT message (forwarded from the monitoring
agent by BroadcastLiveClient), this agent streams simulated log lines as
working-state status updates. Each update is broadcast to all other agents.
"""

import asyncio

from a2a.helpers.proto_helpers import new_task_from_user_message
from a2a.server.agent_execution import AgentExecutor, AgentInputQueue, RequestContext
from a2a.server.agent_execution.agent_input_queue import QueueShutDown
from a2a.server.events import EventQueue
from a2a.server.tasks import TaskUpdater
from a2a.types.a2a_pb2 import AgentSkill, TaskState

from agents.base import (
    NAMESPACE,
    GROUP,
    get_message_text,
    get_slim_src,
    make_agent_card,
    make_agent_message,
    start_agent,
)

SLIM_NAME = "log-agent"
FULL_SLIM_NAME = f"{NAMESPACE}/{GROUP}/{SLIM_NAME}"

SIMULATED_LOGS = [
    "2024-01-15T10:23:01Z ERROR checkout: dial tcp db.prod:5432: connection refused",
    "2024-01-15T10:23:02Z ERROR checkout: dial tcp db.prod:5432: connection refused (x47 in 1s)",
    "2024-01-15T10:23:03Z WARN  checkout: connection pool exhausted (max=20, waiting=134)",
    "2024-01-15T10:23:04Z ERROR checkout: context deadline exceeded after 30s waiting for db conn",
    "2024-01-15T10:23:04Z INFO  db.prod: max_connections=100 active=100 idle=0",
]


class LogAgentExecutor(AgentExecutor):
    async def execute(
        self,
        context: RequestContext,
        event_queue: EventQueue,
        input_queue: AgentInputQueue,
    ) -> None:
        task = None
        updater = None
        logs_sent = False
        try:
            while True:
                msg_ctx = await input_queue.get()
                if not msg_ctx.message:
                    continue

                if task is None:
                    task = new_task_from_user_message(msg_ctx.message)
                    await event_queue.enqueue_event(task)
                    updater = TaskUpdater(event_queue, task.id, task.context_id)

                sender = get_slim_src(msg_ctx.message)
                text = get_message_text(msg_ctx.message)

                if not logs_sent and "ALERT" in text.upper() and sender != FULL_SLIM_NAME:
                    logs_sent = True
                    print(f"[{SLIM_NAME}] received alert from {sender}, streaming logs...")
                    for log_line in SIMULATED_LOGS:
                        log_msg = f"LOG: {log_line}"
                        print(f"[{SLIM_NAME}] sending: {log_msg!r}")
                        await updater.update_status(
                            state=TaskState.TASK_STATE_WORKING,
                            message=make_agent_message(log_msg, FULL_SLIM_NAME, task.context_id, task.id),
                        )
        except QueueShutDown:
            pass
        finally:
            if updater:
                await updater.complete()

    async def cancel(self, context: RequestContext, event_queue: EventQueue) -> None:
        pass


def build_agent_card():
    return make_agent_card(
        name="Log Agent",
        description=(
            "Surfaces relevant log entries from service logs in response to an "
            "incident alert. Log entries are streamed to the broadcast live session "
            "for analysis by other participants."
        ),
        slim_name=FULL_SLIM_NAME,
        skills=[
            AgentSkill(
                id="stream-logs",
                name="Stream Logs",
                description="Streams relevant log entries for a service around the incident window.",
                tags=["logs", "observability", "incident-response"],
            )
        ],
    )


async def main():
    await start_agent(
        slim_name=SLIM_NAME,
        agent_card=build_agent_card(),
        agent_executor=LogAgentExecutor(),
    )


if __name__ == "__main__":
    asyncio.run(main())
