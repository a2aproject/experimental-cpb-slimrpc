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

Role in the scenario:
  When on_session_message() is called with a ChannelMessage, it checks whether
  the message is a confident DIAGNOSIS from the diagnostics agent and, if so,
  broadcasts a REMEDIATION action to all registered channels.
"""

import asyncio
import re

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

SLIM_NAME = "remediation-agent"
FULL_SLIM_NAME = f"{NAMESPACE}/{GROUP}/{SLIM_NAME}"

# Only act on diagnoses with at least this confidence.
MIN_CONFIDENCE = 0.7

_CONFIDENCE_RE = re.compile(r"confidence:\s*([0-9.]+)", re.IGNORECASE)


class RemediationAgentExecutor(SessionAwareAgentExecutor):
    async def on_session_message(
        self,
        session: AgentSession,
        incoming: ChannelMessage,
    ) -> None:
        sender = get_slim_src(incoming.message)
        text = get_message_text(incoming.message)

        # Only act on DIAGNOSIS messages from the diagnostics agent.
        if (
            not text.startswith("DIAGNOSIS")
            or sender != f"{NAMESPACE}/{GROUP}/diagnostics-agent"
        ):
            return

        match = _CONFIDENCE_RE.search(text)
        confidence = float(match.group(1)) if match else 0.0

        if confidence < MIN_CONFIDENCE:
            print(
                f"[{SLIM_NAME}] diagnosis confidence {confidence:.1f} < "
                f"{MIN_CONFIDENCE} — waiting for more evidence"
            )
            return

        print(f"[{SLIM_NAME}] acting on diagnosis from {sender}: {text!r}")
        remediation = (
            f"REMEDIATION: DB connection pool exhausted on checkout service. "
            f"Immediate actions: "
            f"(1) Restart checkout-db-pool: `systemctl restart checkout-db-pool`. "
            f"(2) Increase max_connections on db.prod from 100 → 200 "
            f"(edit /etc/postgresql/postgresql.conf, then reload). "
            f"Expected recovery time: ~30s after step 1. "
            f"Post-incident: add connection-pool alerting at 80% utilisation."
        )
        print(f"[{SLIM_NAME}] sending: {remediation!r}")
        msg = make_message(remediation, FULL_SLIM_NAME)
        for channel in session.channels.values():
            await channel.send(msg)


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
