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

Role in the scenario:
  When on_session_message() is called with a ChannelMessage containing an ALERT,
  it streams simulated log entries from the checkout service to all registered
  channels. The diagnostics agent uses these log entries to form a hypothesis.
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

SLIM_NAME = "log-agent"
FULL_SLIM_NAME = f"{NAMESPACE}/{GROUP}/{SLIM_NAME}"

# Simulated log lines from the checkout service around the incident window.
SIMULATED_LOGS = [
    "2024-01-15T10:23:01Z ERROR checkout: dial tcp db.prod:5432: connection refused",
    "2024-01-15T10:23:02Z ERROR checkout: dial tcp db.prod:5432: connection refused (x47 in 1s)",
    "2024-01-15T10:23:03Z WARN  checkout: connection pool exhausted (max=20, waiting=134)",
    "2024-01-15T10:23:04Z ERROR checkout: context deadline exceeded after 30s waiting for db conn",
    "2024-01-15T10:23:04Z INFO  db.prod: max_connections=100 active=100 idle=0",
]


class LogAgentExecutor(SessionAwareAgentExecutor):
    async def on_session_message(
        self,
        session: AgentSession,
        incoming: ChannelMessage,
    ) -> None:
        sender = get_slim_src(incoming.message)
        text = get_message_text(incoming.message)

        # Respond to an alert from any sender (except ourselves) with log entries.
        if "ALERT" in text.upper() and sender != FULL_SLIM_NAME:
            print(f"[{SLIM_NAME}] received alert from {sender}, streaming logs...")
            for log_line in SIMULATED_LOGS:
                log_msg = f"LOG: {log_line}"
                print(f"[{SLIM_NAME}] sending: {log_msg!r}")
                msg = make_message(log_msg, FULL_SLIM_NAME)
                for channel in session.channels.values():
                    await channel.send(msg)


def build_agent_card():
    return make_agent_card(
        name="Log Agent",
        description=(
            "Surfaces relevant log entries from service logs in response to an "
            "incident alert. Log entries are streamed to the collaborative channel "
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
