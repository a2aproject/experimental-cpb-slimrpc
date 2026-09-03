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

"""Monitoring agent — receives an anomaly trigger and emits a structured alert.

When SendLiveMessage delivers an ANOMALY message via the input_queue, this
agent publishes an ALERT status update. The BroadcastLiveClient forwards that
update to all other agents as a StreamRequest so they can react.
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

SLIM_NAME = "monitoring-agent"
FULL_SLIM_NAME = f"{NAMESPACE}/{GROUP}/{SLIM_NAME}"


class MonitoringAgentExecutor(AgentExecutor):
    async def execute(
        self,
        context: RequestContext,
        event_queue: EventQueue,
        input_queue: AgentInputQueue,
    ) -> None:
        task = None
        updater = None
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

                if "ANOMALY" in text.upper():
                    print(f"[{SLIM_NAME}] received trigger from {sender}: {text!r}")
                    alert = (
                        "ALERT: Error rate spike detected on /api/checkout — "
                        "45% (threshold: 5%). Timestamp: 2024-01-15T10:23:00Z. "
                        "Requesting log analysis and diagnostics."
                    )
                    print(f"[{SLIM_NAME}] broadcasting: {alert!r}")
                    await updater.update_status(
                        state=TaskState.TASK_STATE_WORKING,
                        message=make_agent_message(alert, FULL_SLIM_NAME, task.context_id, task.id),
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
        name="Monitoring Agent",
        description=(
            "Detects anomalies in service metrics and broadcasts structured alerts "
            "to the broadcast live incident-response session."
        ),
        slim_name=FULL_SLIM_NAME,
        skills=[
            AgentSkill(
                id="detect-anomaly",
                name="Detect Anomaly",
                description="Broadcasts a structured alert when an anomaly trigger is received.",
                tags=["monitoring", "alerting", "incident-response"],
            )
        ],
    )


async def main():
    await start_agent(
        slim_name=SLIM_NAME,
        agent_card=build_agent_card(),
        agent_executor=MonitoringAgentExecutor(),
    )


if __name__ == "__main__":
    asyncio.run(main())
