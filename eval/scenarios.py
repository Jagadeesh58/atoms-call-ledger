"""
WHY:
Defines evaluation scenarios that map fault types to synthetic call sets.
Exists to test the ledger's detection capabilities against the documented
failure modes of Atoms webhooks under a deterministic, seeded synthetic fault suite.
Must never use real data, real credentials, or real timestamps.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List


@dataclass
class Scenario:
    """A single evaluation scenario."""

    name: str
    fault_type: str
    count: int
    description: str


def build_scenarios(total_calls: int, seed: int = 7) -> List[Scenario]:
    """
    Build the list of evaluation scenarios.

    The scenario distribution is fixed (proportional to total_calls).
    Counts are deterministic given total_calls.
    """
    import math

    # Fixed proportions for each fault type (must sum to ~1.0)
    # These are illustrative distributions, not production recommendations.
    proportions = [
        ("clean", 0.25, "All 3 events delivered in order"),
        ("duplicate_identical", 0.06, "post-conversation sent twice with identical body"),
        ("duplicate_conflict", 0.04, "post-conversation sent twice with different body"),
        ("out_of_order", 0.07, "analytics-completed arrives before post-conversation"),
        ("dropped_post", 0.08, "post-conversation never delivered"),
        ("dropped_analytics", 0.08, "analytics-completed never delivered"),
        ("dropped_pre", 0.05, "pre-conversation never delivered"),
        ("outage_window", 0.06, "All events lost in a contiguous outage window"),
        ("tampered_body", 0.04, "Valid-looking JSON payload with bad signature"),
        ("wrong_secret", 0.03, "Signed with wrong signing secret"),
        ("missing_signature", 0.03, "No X-Signature header"),
        ("schema_drift", 0.06, "Extra fields, no-answer hyphen, int duration, Z-less timestamp"),
        ("authenticated_malformed", 0.04, "Valid sig over truncated/garbage JSON"),
        ("unicode_content", 0.04, "Devanagari and emoji in transcript fields"),
        (
            "no_answer_status",
            0.05,
            "no-answer call, analytics not expected (UNVALIDATED assumption)",
        ),
        ("large_transcript", 0.02, "Transcript near max body size limit"),
    ]

    scenarios = []
    assigned = 0
    items = [(name, p, desc) for name, p, desc in proportions]

    for i, (name, proportion, desc) in enumerate(items):
        if i == len(items) - 1:
            # Last scenario gets the remainder
            count = total_calls - assigned
        else:
            count = max(1, math.floor(total_calls * proportion))
        assigned += count
        scenarios.append(Scenario(name=name, fault_type=name, count=count, description=desc))

    return scenarios
