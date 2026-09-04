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

"""Remediation agent — proposes a fix once the diagnostics agent is confident.

When the input_queue delivers a DIAGNOSIS message (forwarded from the
diagnostics agent by BroadcastLiveClient), this agent emits a REMEDIATION
status update with actionable steps.
"""

import asyncio
import re

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

SLIM_NAME = "remediation-agent"
FULL_SLIM_NAME = f"{NAMESPACE}/{GROUP}/{SLIM_NAME}"

MIN_CONFIDENCE = 0.7

_CONFIDENCE_RE = re.compile(r"confidence:\s*([0-9.]+)", re.IGNORECASE)


class RemediationAgentExecutor(AgentExecutor):
    async def execute(
        self,
        context: RequestContext,
        event_queue: EventQueue,
        input_queue: AgentInputQueue,
    ) -> None:
        task = None
        updater = None
        remediation_sent = False
        try:
            while True:
                msg_ctx = await input_queue.get()
                if not msg_ctx.message:
                    continue

                if task is None:
                    task = new_task_from_user_message(msg_ctx.message)
                    await event_queue.enqueue_event(task)
                    updater = TaskUpdater(event_queue, task.id, task.context_id)

                if remediation_sent:
                    continue

                sender = get_slim_src(msg_ctx.message)
                text = get_message_text(msg_ctx.message)

                if (
                    not text.startswith("DIAGNOSIS")
                    or sender != f"{NAMESPACE}/{GROUP}/diagnostics-agent"
                ):
                    continue

                match = _CONFIDENCE_RE.search(text)
                confidence = float(match.group(1)) if match else 0.0

                if confidence < MIN_CONFIDENCE:
                    print(
                        f"[{SLIM_NAME}] diagnosis confidence {confidence:.1f} < "
                        f"{MIN_CONFIDENCE} — waiting for more evidence"
                    )
                    continue

                remediation_sent = True
                print(f"[{SLIM_NAME}] acting on diagnosis from {sender}: {text!r}")
                remediation = (
                    "REMEDIATION: DB connection pool exhausted on checkout service. "
                    "Immediate actions: "
                    "(1) Restart checkout-db-pool: `systemctl restart checkout-db-pool`. "
                    "(2) Increase max_connections on db.prod from 100 → 200 "
                    "(edit /etc/postgresql/postgresql.conf, then reload). "
                    "Expected recovery time: ~30s after step 1. "
                    "Post-incident: add connection-pool alerting at 80% utilisation."
                )
                print(f"[{SLIM_NAME}] sending: {remediation!r}")
                await updater.update_status(
                    state=TaskState.TASK_STATE_WORKING,
                    message=make_agent_message(remediation, FULL_SLIM_NAME, task.context_id, task.id),
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
        name="Remediation Agent",
        description=(
            "Proposes remediation actions based on a root-cause diagnosis. "
            "Acts only when the diagnostics agent reports sufficient confidence, "
            "ensuring remediation steps are grounded in evidence."
        ),
        slim_name=FULL_SLIM_NAME,
        skills=[
            AgentSkill(
                id="propose-remediation",
                name="Propose Remediation",
                description="Proposes actionable remediation steps for a diagnosed incident.",
                tags=["remediation", "runbook", "incident-response"],
            )
        ],
    )


async def main():
    await start_agent(
        slim_name=SLIM_NAME,
        agent_card=build_agent_card(),
        agent_executor=RemediationAgentExecutor(),
    )


if __name__ == "__main__":
    asyncio.run(main())
