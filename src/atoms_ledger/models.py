"""
WHY:
Declares data models, schemas, and type definitions for Atoms Call Ledger.
Exists to provide strict, predictable internal representations for inconsistent
inbound webhook envelopes, canonical call aggregation, and backfill tracking.
It must never raise validation errors on unexpected dynamic fields or variable keys.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional


@dataclass
class NormalizedEvent:
    """Canonical internal representation of an Atoms webhook or API event."""

    call_id: str
    agent_id: str
    event_type: str  # pre-conversation, post-conversation, analytics-completed, unknown
    delivery_id: Optional[str] = None
    from_number: Optional[str] = None
    to_number: Optional[str] = None
    status: Optional[str] = None
    direction: str = "unknown"  # inbound, outbound, chat, unknown
    duration_seconds: Optional[float] = None
    answered_at: Optional[datetime] = None
    ended_at: Optional[datetime] = None
    transcript: List[Dict[str, Any]] = field(default_factory=list)
    recording_ref: Optional[str] = None
    variables: Dict[str, Any] = field(default_factory=dict)
    analytics: Optional[Dict[str, Any]] = None
    source: str = "webhook"  # webhook, api_backfill
    flags: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to JSON-serializable dictionary."""
        return {
            "call_id": self.call_id,
            "agent_id": self.agent_id,
            "event_type": self.event_type,
            "delivery_id": self.delivery_id,
            "from_number": self.from_number,
            "to_number": self.to_number,
            "status": self.status,
            "direction": self.direction,
            "duration_seconds": self.duration_seconds,
            "answered_at": self.answered_at.isoformat() if self.answered_at else None,
            "ended_at": self.ended_at.isoformat() if self.ended_at else None,
            "transcript": self.transcript,
            "recording_ref": self.recording_ref,
            "variables": self.variables,
            "analytics": self.analytics,
            "source": self.source,
            "flags": self.flags,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> NormalizedEvent:
        """Reconstruct NormalizedEvent from dictionary representation."""
        answered_at = None
        if data.get("answered_at"):
            try:
                answered_at = datetime.fromisoformat(data["answered_at"])
            except Exception:
                answered_at = None

        ended_at = None
        if data.get("ended_at"):
            try:
                ended_at = datetime.fromisoformat(data["ended_at"])
            except Exception:
                ended_at = None

        return cls(
            call_id=str(data.get("call_id", "")),
            agent_id=str(data.get("agent_id", "")),
            event_type=str(data.get("event_type", "unknown")),
            delivery_id=data.get("delivery_id"),
            from_number=data.get("from_number"),
            to_number=data.get("to_number"),
            status=data.get("status"),
            direction=str(data.get("direction", "unknown")),
            duration_seconds=data.get("duration_seconds"),
            answered_at=answered_at,
            ended_at=ended_at,
            transcript=data.get("transcript", []) or [],
            recording_ref=data.get("recording_ref"),
            variables=data.get("variables", {}) or {},
            analytics=data.get("analytics"),
            source=str(data.get("source", "webhook")),
            flags=list(data.get("flags", []) or []),
        )


@dataclass
class IngestResult:
    """Outcome of an ingestion attempt returned to HTTP handlers and callers."""

    outcome: str  # accepted, duplicate, conflict, quarantined, rejected_signature, rejected_size, rejected_empty
    http_status: int
    call_id: Optional[str] = None
    event_type: Optional[str] = None
    delivery_id: Optional[str] = None
    anomalies: List[str] = field(default_factory=list)


@dataclass
class CallView:
    """Aggregated, deadline-aware lifecycle view for a single callId."""

    call_id: str
    agent_id: Optional[str] = None
    state: str = "IN_PROGRESS"  # COMPLETE, IN_PROGRESS, AWAITING_ANALYTICS, OVERDUE_ANALYTICS, OVERDUE_POST, QUARANTINED_ONLY
    risk_tier: str = "OK"  # OK, WARNING, CRITICAL, EXPIRED
    anchor_at: Optional[datetime] = None
    deadline_at: Optional[datetime] = None
    remaining_seconds: Optional[float] = None
    events_received: List[str] = field(default_factory=list)
    missing_required_events: List[str] = field(default_factory=list)
    flags: List[str] = field(default_factory=list)
    from_number: Optional[str] = None
    to_number: Optional[str] = None
    status: Optional[str] = None
    direction: str = "unknown"
    duration_seconds: Optional[float] = None
    transcript: List[Dict[str, Any]] = field(default_factory=list)
    variables: Dict[str, Any] = field(default_factory=dict)
    analytics: Optional[Dict[str, Any]] = None
    recording_ref: Optional[str] = None
    backfill_state: Optional[str] = None
    raw_deliveries: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class BackfillTask:
    """Task specification for recovering an overdue or at-risk call."""

    call_id: str
    urgency_rank: int
    remaining_seconds: float
    missing_events: List[str]
    agent_id: Optional[str] = None
