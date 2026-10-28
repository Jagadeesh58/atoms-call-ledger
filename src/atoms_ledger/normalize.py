"""
WHY:
Normalizes heterogeneous Atoms webhook envelopes and REST structures into a single schema.
Exists because Atoms documentation exhibits documented field and enum inconsistencies
(toPhone vs toNumber, no-answer vs no_answer, camelCase vs snake_case directions).
It must never raise an exception on authenticated payloads, preserving unknown fields and keys.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from atoms_ledger.models import NormalizedEvent

CALL_ID_REGEX = re.compile(r"^CALL-[0-9]{13}-[0-9a-fA-F]{6}$")

KNOWN_STATUSES = {
    "completed": "completed",
    "busy": "busy",
    "failed": "failed",
    "canceled": "canceled",
    "cancelled": "canceled",
    "in_progress": "in_progress",
    "in-progress": "in_progress",
    "no-answer": "no_answer",
    "no_answer": "no_answer",
    "noanswer": "no_answer",
    "pending": "pending",
    "in_queue": "in_queue",
    "processing": "processing",
    "active": "active",
}

KNOWN_DIRECTIONS = {
    "telephonyoutbound": "outbound",
    "telephony_outbound": "outbound",
    "outbound": "outbound",
    "telephonyinbound": "inbound",
    "telephony_inbound": "inbound",
    "inbound": "inbound",
    "webcall": "chat",
    "web_call": "chat",
    "chat": "chat",
}

KNOWN_ROLES = {"agent", "user", "system", "assistant", "bot"}


def _parse_timestamp(val: Any) -> Tuple[Optional[datetime], Optional[str]]:
    """Parse various timestamp representations into timezone-aware UTC datetime."""
    if val is None or val == "":
        return None, None

    if isinstance(val, (int, float)):
        # Epoch seconds vs milliseconds
        try:
            # 13-digit numbers are epoch milliseconds
            if val > 1e11:
                return datetime.fromtimestamp(val / 1000.0, tz=timezone.utc), None
            return datetime.fromtimestamp(float(val), tz=timezone.utc), None
        except Exception:
            return None, "bad_timestamp"

    if isinstance(val, str):
        clean_str = val.strip()
        if not clean_str:
            return None, None

        # Try parsing ISO 8601 with or without trailing Z
        try:
            if clean_str.endswith("Z") or clean_str.endswith("z"):
                iso_clean = clean_str[:-1] + "+00:00"
            else:
                iso_clean = clean_str
            dt = datetime.fromisoformat(iso_clean)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)
            return dt, None
        except Exception:
            pass

        # Try numeric string (epoch)
        try:
            num = float(clean_str)
            if num > 1e11:
                return datetime.fromtimestamp(num / 1000.0, tz=timezone.utc), None
            return datetime.fromtimestamp(num, tz=timezone.utc), None
        except Exception:
            return None, "bad_timestamp"

    return None, "bad_timestamp"


def _normalize_phone(val: Any) -> Optional[str]:
    """Normalize phone string, stripping whitespace."""
    if val is None:
        return None
    s = str(val).strip()
    return s if s else None


def _normalize_status(val: Any) -> Tuple[Optional[str], Optional[str]]:
    """Canonicalize status strings (no-answer -> no_answer)."""
    if val is None:
        return None, None

    raw = str(val).strip().lower()
    if not raw:
        return None, None

    if raw in KNOWN_STATUSES:
        return KNOWN_STATUSES[raw], None

    return raw, "status_unknown"


def _normalize_direction(val: Any) -> Tuple[str, Optional[str]]:
    """Map conversationType, callDirection, or type to canonical direction."""
    if val is None:
        return "unknown", None

    raw = str(val).strip().lower().replace("-", "_")
    if not raw:
        return "unknown", None

    if raw in KNOWN_DIRECTIONS:
        return KNOWN_DIRECTIONS[raw], None

    return "unknown", "direction_unknown"


def normalize_event(
    payload: Dict[str, Any],
    delivery_id: Optional[str] = None,
    source: str = "webhook",
) -> NormalizedEvent:
    """Normalize an inbound Atoms webhook or API payload into NormalizedEvent.

    Never raises an exception on validly parsed dict input.
    Accumulates diagnostic flags for deviations from expected contracts.
    """
    flags: List[str] = []

    if not isinstance(payload, dict):
        return NormalizedEvent(
            call_id="",
            agent_id="",
            event_type="unknown",
            source=source,
            flags=["payload_not_dict"],
        )

    # 1. Extract metadata and container sub-dicts
    metadata: Dict[str, Any] = {}
    if isinstance(payload.get("metadata"), dict):
        metadata = payload["metadata"]
    elif isinstance(payload.get("data"), dict):
        metadata = payload["data"]

    # 2. Extract delivery ID
    deliv_id = delivery_id or payload.get("id") or metadata.get("id")
    if deliv_id is not None:
        deliv_id = str(deliv_id)

    # 3. Extract callId
    raw_call_id = (
        metadata.get("callId")
        or metadata.get("call_id")
        or payload.get("callId")
        or payload.get("call_id")
        or ""
    )
    call_id = str(raw_call_id).strip()

    if not call_id:
        flags.append("callid_missing")
    elif not CALL_ID_REGEX.match(call_id):
        flags.append("callid_format")

    # 4. Extract agentId and eventType
    # Webhook envelope top-level: event: "<agentId>.<eventType>"
    event_str = payload.get("event")
    agent_from_event, type_from_event = None, None
    if isinstance(event_str, str) and "." in event_str:
        parts = event_str.split(".", 1)
        agent_from_event, type_from_event = parts[0], parts[1]

    agent_id = str(
        metadata.get("agentId")
        or metadata.get("agent_id")
        or payload.get("agentId")
        or agent_from_event
        or ""
    ).strip()

    raw_event_type = (
        metadata.get("eventType")
        or metadata.get("event_type")
        or payload.get("eventType")
        or type_from_event
        or payload.get("event")
        or "unknown"
    )
    event_type = str(raw_event_type).strip().lower()
    # Normalize variants
    if "pre" in event_type and "conversation" in event_type:
        event_type = "pre-conversation"
    elif "post" in event_type and "conversation" in event_type:
        event_type = "post-conversation"
    elif "analytics" in event_type:
        event_type = "analytics-completed"

    # 5. Normalize phone numbers (toPhone vs toNumber, fromPhone vs fromNumber)
    from_num = _normalize_phone(
        metadata.get("fromPhone")
        or metadata.get("fromNumber")
        or metadata.get("from")
        or payload.get("fromPhone")
        or payload.get("fromNumber")
        or payload.get("from")
    )
    to_num = _normalize_phone(
        metadata.get("toPhone")
        or metadata.get("toNumber")
        or metadata.get("to")
        or payload.get("toPhone")
        or payload.get("toNumber")
        or payload.get("to")
    )

    # 6. Normalize status
    raw_status = (
        metadata.get("callStatus")
        or metadata.get("status")
        or payload.get("callStatus")
        or payload.get("status")
    )
    status, status_flag = _normalize_status(raw_status)
    if status_flag:
        flags.append(status_flag)

    # 7. Normalize direction
    raw_direction = (
        metadata.get("conversationType")
        or metadata.get("callDirection")
        or payload.get("conversationType")
        or payload.get("callDirection")
        or metadata.get("type")
        or payload.get("type")
    )
    direction, direction_flag = _normalize_direction(raw_direction)
    if direction_flag:
        flags.append(direction_flag)

    # 8. Normalize duration
    raw_duration = (
        metadata.get("callDuration")
        or metadata.get("duration")
        or payload.get("callDuration")
        or payload.get("duration")
    )
    duration_seconds: Optional[float] = None
    if raw_duration is not None and raw_duration != "":
        try:
            dur = float(raw_duration)
            if dur < 0:
                flags.append("duration_negative")
            else:
                duration_seconds = dur
        except Exception:
            flags.append("duration_invalid")

    # 9. Normalize timestamps (answered_at, ended_at)
    raw_answered = (
        metadata.get("answeredAt")
        or metadata.get("answered_at")
        or payload.get("answeredAt")
        or payload.get("answered_at")
        or metadata.get("startTime")
        or payload.get("startTime")
    )
    answered_at, ans_flag = _parse_timestamp(raw_answered)
    if ans_flag:
        flags.append(f"answered_at_{ans_flag}")

    raw_ended = (
        metadata.get("endedAt")
        or metadata.get("ended_at")
        or payload.get("endedAt")
        or payload.get("ended_at")
        or metadata.get("endTime")
        or payload.get("endTime")
    )
    ended_at, end_flag = _parse_timestamp(raw_ended)
    if end_flag:
        flags.append(f"ended_at_{end_flag}")

    # 10. Normalize transcript
    raw_transcript = (
        metadata.get("transcript")
        or payload.get("transcript")
        or metadata.get("callTranscript")
        or payload.get("callTranscript")
    )
    transcript_list: List[Dict[str, Any]] = []
    if raw_transcript is not None:
        if isinstance(raw_transcript, list):
            for turn in raw_transcript:
                if isinstance(turn, dict):
                    role = str(turn.get("role", "")).strip().lower()
                    if role and role not in KNOWN_ROLES:
                        flags.append("transcript_unknown_role")
                    content = turn.get("content") or turn.get("message") or turn.get("text") or ""
                    ts = turn.get("timestamp") or turn.get("time")
                    transcript_list.append(
                        {
                            "role": role or "unknown",
                            "content": str(content),
                            "timestamp": ts,
                        }
                    )
                else:
                    flags.append("transcript_turn_not_dict")
        else:
            flags.append("transcript_not_list")

    # 11. Normalize variables (preserve dynamic keys verbatim)
    raw_variables = (
        metadata.get("variables")
        or payload.get("variables")
        or metadata.get("callVariables")
        or payload.get("callVariables")
    )
    variables_dict: Dict[str, Any] = {}
    if raw_variables is not None:
        if isinstance(raw_variables, dict):
            variables_dict = dict(raw_variables)
        else:
            flags.append("variables_not_dict")

    # 12. Normalize analytics
    raw_analytics = (
        metadata.get("analytics")
        or payload.get("analytics")
        or metadata.get("postCallAnalytics")
        or payload.get("postCallAnalytics")
    )
    analytics_dict: Optional[Dict[str, Any]] = None
    if raw_analytics is not None:
        if isinstance(raw_analytics, dict):
            analytics_dict = dict(raw_analytics)
        else:
            flags.append("analytics_not_dict")

    # 13. Recording reference
    recording_ref = (
        metadata.get("recordingUrl")
        or metadata.get("recording_url")
        or payload.get("recordingUrl")
        or payload.get("recording_url")
    )
    if recording_ref is not None:
        recording_ref = str(recording_ref).strip()

    # Deduplicate flags while preserving order
    deduped_flags = list(dict.fromkeys(flags))

    return NormalizedEvent(
        call_id=call_id,
        agent_id=agent_id,
        event_type=event_type,
        delivery_id=deliv_id,
        from_number=from_num,
        to_number=to_num,
        status=status,
        direction=direction,
        duration_seconds=duration_seconds,
        answered_at=answered_at,
        ended_at=ended_at,
        transcript=transcript_list,
        recording_ref=recording_ref,
        variables=variables_dict,
        analytics=analytics_dict,
        source=source,
        flags=deduped_flags,
    )
