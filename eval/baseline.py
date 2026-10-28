"""
WHY:
Deliberately naive reference handler for comparison purposes. An assumption about
a common first implementation, NOT an observation of any real customer's code.

DELIBERATELY NAIVE: No signature check, no dedupe, no raw persistence, no normalization.
Exists as a comparison baseline for the evaluation. NOT a real implementation.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List


class NaiveBaseline:
    """
    DELIBERATELY NAIVE: No signature check, no dedupe, no raw persistence, no normalization.
    Exists as a comparison baseline for the evaluation. NOT a real implementation.
    """

    def __init__(self) -> None:
        self._events: List[Dict[str, Any]] = []  # in-memory, no deduplication

    def handle(self, raw_bytes: bytes, headers: dict) -> dict:
        """
        NAIVE handler: parses JSON, inserts one row per event into memory, returns 200 always.
        - No signature verification
        - No deduplication
        - No normalization
        - No raw-byte persistence
        """
        if not raw_bytes:
            return {"status": 200, "outcome": "accepted", "call_id": None}

        try:
            parsed = json.loads(raw_bytes)
        except Exception:
            # NAIVE: Accepts garbage as if it were a valid event
            parsed = {}

        call_id = parsed.get("callId") or parsed.get("call_id") or None
        event_type = parsed.get("eventType") or parsed.get("event_type") or "unknown"

        # NAIVE: Appends without checking for duplicates or conflicts
        self._events.append(
            {
                "call_id": call_id,
                "event_type": event_type,
                "parsed": parsed,
            }
        )

        return {"status": 200, "outcome": "accepted", "call_id": call_id}

    def get_events_for_call(self, call_id: str) -> List[Dict[str, Any]]:
        return [e for e in self._events if e["call_id"] == call_id]

    def unique_call_count(self) -> int:
        return len({e["call_id"] for e in self._events if e["call_id"]})

    def total_event_count(self) -> int:
        return len(self._events)

    def reset(self) -> None:
        self._events = []
