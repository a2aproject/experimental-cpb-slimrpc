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

"""Diagnostics agent — correlates log entries to identify the root cause.

When the input_queue delivers LOG messages (forwarded from the log agent by
BroadcastLiveClient), this agent accumulates evidence and emits a DIAGNOSIS
status update once the confidence threshold is crossed.
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

SLIM_NAME = "diagnostics-agent"
FULL_SLIM_NAME = f"{NAMESPACE}/{GROUP}/{SLIM_NAME}"

EVIDENCE_SIGNALS: list[tuple[str, float, str]] = [
    ("connection refused", 0.4, "DB connections being refused"),
    ("connection pool exhausted", 0.3, "DB connection pool is full"),
    ("deadline exceeded", 0.2, "requests timing out waiting for DB"),
    ("max_connections=100 active=100", 0.1, "DB at maximum connection limit"),
]
CONFIDENCE_THRESHOLD = 0.7


class DiagnosticsAgentExecutor(AgentExecutor):
    async def execute(
        self,
        context: RequestContext,
        event_queue: EventQueue,
        input_queue: AgentInputQueue,
    ) -> None:
        task = None
        updater = None
        evidence: list[str] = []
        confidence = 0.0
        diagnosis_sent = False
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

                if not text.startswith("LOG:") or sender != f"{NAMESPACE}/{GROUP}/log-agent":
                    continue

                log_line = text[4:].strip()
                print(f"[{SLIM_NAME}] processing log from {sender}: {log_line!r}")

                for keyword, weight, evidence_desc in EVIDENCE_SIGNALS:
                    if keyword.lower() in log_line.lower():
                        evidence.append(evidence_desc)
                        confidence = min(1.0, confidence + weight)

                if confidence >= CONFIDENCE_THRESHOLD and not diagnosis_sent:
                    diagnosis_sent = True
                    evidence_summary = "; ".join(dict.fromkeys(evidence))
                    diagnosis = (
                        f"DIAGNOSIS (confidence: {confidence:.1f}): "
                        f"DB connection pool exhausted on checkout service — "
                        f"db.prod is not accepting new connections. "
                        f"Evidence: {evidence_summary}."
                    )
                    print(f"[{SLIM_NAME}] sending: {diagnosis!r}")
                    await updater.update_status(
                        state=TaskState.TASK_STATE_WORKING,
                        message=make_agent_message(diagnosis, FULL_SLIM_NAME, task.context_id, task.id),
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
        name="Diagnostics Agent",
        description=(
            "Correlates log entries to identify the root cause of a service incident. "
            "Posts a diagnosis with a confidence score once sufficient evidence has "
            "been gathered from the broadcast live session."
        ),
        slim_name=FULL_SLIM_NAME,
        skills=[
            AgentSkill(
                id="diagnose-incident",
                name="Diagnose Incident",
                description="Correlates log evidence to form a root-cause hypothesis.",
                tags=["diagnostics", "root-cause-analysis", "incident-response"],
            )
        ],
    )


async def main():
    await start_agent(
        slim_name=SLIM_NAME,
        agent_card=build_agent_card(),
        agent_executor=DiagnosticsAgentExecutor(),
    )


if __name__ == "__main__":
    asyncio.run(main())
