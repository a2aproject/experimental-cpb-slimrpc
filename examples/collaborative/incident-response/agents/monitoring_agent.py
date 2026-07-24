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

"""Monitoring agent — receives an anomaly trigger and broadcasts a structured alert.

Role in the scenario:
  When on_session_message() is called with a ChannelMessage, it checks the
  message text for an ANOMALY keyword and broadcasts a formatted ALERT to every
  registered output channel so all session participants see a consistent signal.
"""

import asyncio

from a2a.types.a2a_pb2 import AgentSkill

from agents.base import (
    NAMESPACE,
    GROUP,
    AgentSession,
    ChannelMessage,
    SessionAwareAgentExecutor,
    get_message_text,
    get_slim_src,
    make_agent_card,
    make_message,
    start_agent,
)

SLIM_NAME = "monitoring-agent"
FULL_SLIM_NAME = f"{NAMESPACE}/{GROUP}/{SLIM_NAME}"


class MonitoringAgentExecutor(SessionAwareAgentExecutor):
    async def on_session_message(
        self,
        session: AgentSession,
        incoming: ChannelMessage,
    ) -> None:
        sender = get_slim_src(incoming.message)
        text = get_message_text(incoming.message)

        # Re-broadcast anomaly trigger as a structured alert to all channels.
        if "ANOMALY" in text.upper():
            print(f"[{SLIM_NAME}] received trigger from {sender}: {text!r}")
            alert = (
                f"ALERT: Error rate spike detected on /api/checkout — "
                f"45% (threshold: 5%). Timestamp: 2024-01-15T10:23:00Z. "
                f"Requesting log analysis and diagnostics."
            )
            print(f"[{SLIM_NAME}] broadcasting: {alert!r}")
            msg = make_message(alert, FULL_SLIM_NAME)
            for channel in session.channels.values():
                await channel.send(msg)


def build_agent_card():
    return make_agent_card(
        name="Monitoring Agent",
        description=(
            "Detects anomalies in service metrics and broadcasts structured alerts "
            "to the collaborative incident-response channel."
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
