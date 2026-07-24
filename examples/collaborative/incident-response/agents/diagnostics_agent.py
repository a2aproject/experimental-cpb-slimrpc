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

Role in the scenario:
  When on_session_message() is called with a ChannelMessage containing a LOG
  entry from the log agent, it accumulates evidence and broadcasts a DIAGNOSIS
  message to all channels once the confidence threshold is crossed.

  Diagnostic state (evidence, confidence, diagnosis_sent) is stored in
  session.agent_state["diagnostics"] so it persists for the lifetime of the
  AgentSession and is shared across all active channels for that context_id.
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

SLIM_NAME = "diagnostics-agent"
FULL_SLIM_NAME = f"{NAMESPACE}/{GROUP}/{SLIM_NAME}"

# Diagnostic signals: keyword → (evidence_weight, description)
EVIDENCE_SIGNALS: list[tuple[str, float, str]] = [
    ("connection refused", 0.4, "DB connections being refused"),
    ("connection pool exhausted", 0.3, "DB connection pool is full"),
    ("deadline exceeded", 0.2, "requests timing out waiting for DB"),
    ("max_connections=100 active=100", 0.1, "DB at maximum connection limit"),
]
CONFIDENCE_THRESHOLD = 0.7


class DiagnosticsAgentExecutor(SessionAwareAgentExecutor):
    async def on_session_message(
        self,
        session: AgentSession,
        incoming: ChannelMessage,
    ) -> None:
        sender = get_slim_src(incoming.message)
        text = get_message_text(incoming.message)

        # Only process LOG entries from the log agent.
        if not text.startswith("LOG:") or sender != f"{NAMESPACE}/{GROUP}/log-agent":
            return

        # Get or create per-session diagnostics state.
        state = session.agent_state.setdefault(
            "diagnostics",
            {"evidence": [], "confidence": 0.0, "diagnosis_sent": False},
        )

        log_line = text[4:].strip()
        print(f"[{SLIM_NAME}] processing log from {sender}: {log_line!r}")

        # Accumulate evidence.
        for keyword, weight, evidence_desc in EVIDENCE_SIGNALS:
            if keyword.lower() in log_line.lower():
                state["evidence"].append(evidence_desc)
                state["confidence"] = min(1.0, state["confidence"] + weight)

        # Broadcast a diagnosis once we cross the confidence threshold.
        if state["confidence"] >= CONFIDENCE_THRESHOLD and not state["diagnosis_sent"]:
            state["diagnosis_sent"] = True
            evidence_summary = "; ".join(dict.fromkeys(state["evidence"]))
            diagnosis = (
                f"DIAGNOSIS (confidence: {state['confidence']:.1f}): "
                f"DB connection pool exhausted on checkout service — "
                f"db.prod is not accepting new connections. "
                f"Evidence: {evidence_summary}."
            )
            print(f"[{SLIM_NAME}] sending: {diagnosis!r}")
            msg = make_message(diagnosis, FULL_SLIM_NAME)
            for channel in session.channels.values():
                await channel.send(msg)


def build_agent_card():
    return make_agent_card(
        name="Diagnostics Agent",
        description=(
            "Correlates log entries to identify the root cause of a service incident. "
            "Posts a diagnosis with a confidence score once sufficient evidence has "
            "been gathered from the collaborative channel."
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
