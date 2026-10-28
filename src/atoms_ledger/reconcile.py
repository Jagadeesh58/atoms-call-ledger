"""
WHY:
Evaluates per-call lifecycle state and ZDR deadline risk for all known calls.
Exists because Atoms delivers events best-effort and in any order; the state
machine must detect missing required events before the retention window expires.
Must be a pure function of DB state + config + now; must never mutate state.
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import List, Optional

from atoms_ledger.config import LedgerConfig
from atoms_ledger.models import CallView, NormalizedEvent
from atoms_ledger.store import LedgerStore

# State labels
STATE_COMPLETE = "COMPLETE"
STATE_IN_PROGRESS = "IN_PROGRESS"
STATE_AWAITING_ANALYTICS = "AWAITING_ANALYTICS"
STATE_OVERDUE_ANALYTICS = "OVERDUE_ANALYTICS"
STATE_OVERDUE_POST = "OVERDUE_POST"
STATE_QUARANTINED_ONLY = "QUARANTINED_ONLY"
STATE_NEEDS_REVIEW = "NEEDS_REVIEW"

# Risk tier labels
RISK_OK = "OK"
RISK_WARNING = "WARNING"
RISK_CRITICAL = "CRITICAL"
RISK_EXPIRED = "EXPIRED"

# Event type constants
PRE = "pre-conversation"
POST = "post-conversation"
ANALYTICS = "analytics-completed"

# Status validation sets
TERMINAL_NON_ANALYTICS_STATUSES = {
    "busy",
    "failed",
    "canceled",
    "cancelled",
    "no_answer",
    "no-answer",
}
KNOWN_RECONCILE_STATUSES = {
    "completed",
    "busy",
    "failed",
    "canceled",
    "cancelled",
    "no_answer",
    "no-answer",
    "in_progress",
    "in-progress",
    "pending",
    "in_queue",
    "processing",
    "active",
}


def _risk_tier(remaining: float, window: int) -> str:
    """
    Compute risk tier from remaining seconds.

    Note: Risk tier is an estimate; conservative; docs say 'never later than
    your retention window' without defining the start anchor precisely.
    """
    if remaining <= 0:
        return RISK_EXPIRED
    critical_threshold = min(3600, window * 0.10)
    if remaining <= critical_threshold:
        return RISK_CRITICAL
    if remaining <= window * 0.50:
        return RISK_WARNING
    return RISK_OK


def _earliest_ts(*timestamps: Optional[float]) -> Optional[float]:
    valid = [t for t in timestamps if t is not None]
    return min(valid) if valid else None


def _parse_normalized(normalized_json: str) -> Optional[NormalizedEvent]:
    try:
        data = json.loads(normalized_json)
        return NormalizedEvent.from_dict(data)
    except Exception:
        return None


def evaluate(
    store: LedgerStore,
    config: LedgerConfig,
    now: datetime,
) -> List[CallView]:
    """
    Return a CallView for every known call_id, reflecting current lifecycle
    state and deadline risk.

    This is a pure read — it never writes to the store.
    """
    now_ts = now.timestamp()
    all_call_ids = store.list_all_calls()
    views: List[CallView] = []

    for call_id in all_call_ids:
        view = _evaluate_call(call_id, store, config, now_ts)
        views.append(view)

    return views


def _evaluate_call(
    call_id: str,
    store: LedgerStore,
    config: LedgerConfig,
    now_ts: float,
) -> CallView:
    # ----------------------------------------------------------------
    # Gather all event rows for this call
    # ----------------------------------------------------------------
    event_rows = store.get_all_events_for_call(call_id)
    deliveries = store.get_deliveries_for_call(call_id)
    backfill_row = store.get_backfill(call_id)
    _anomalies = store.get_anomalies_for_call(call_id)  # noqa: F841 (available for future use)

    raw_deliveries_list = [dict(d) for d in deliveries]
    is_discovered_missing = any(
        d.get("outcome") == "discovered_missing" for d in raw_deliveries_list
    )

    # ----------------------------------------------------------------
    # Check for QUARANTINED_ONLY: only quarantined deliveries, no events
    # Skip if this call was discovered via API diff (it genuinely has no webhooks)
    # ----------------------------------------------------------------
    has_events = len(event_rows) > 0
    has_quarantine_deliveries = any(d.get("outcome") == "quarantined" for d in deliveries)

    if not has_events and not is_discovered_missing and has_quarantine_deliveries:
        view = CallView(call_id=call_id, state=STATE_QUARANTINED_ONLY)
        view.backfill_state = backfill_row["state"] if backfill_row else None
        return view

    if not has_events and not is_discovered_missing:
        # Deliveries reference this call (e.g. rejected_signature) but no events
        view = CallView(call_id=call_id, state=STATE_QUARANTINED_ONLY)
        view.backfill_state = backfill_row["state"] if backfill_row else None
        return view

    # ----------------------------------------------------------------
    # Parse normalized events
    # ----------------------------------------------------------------
    normalized_by_type: dict[str, NormalizedEvent] = {}
    latest_agent_id: Optional[str] = None

    for row in event_rows:
        ne = _parse_normalized(row["normalized_json"])
        if ne:
            normalized_by_type[row["event_type"]] = ne
            if ne.agent_id:
                latest_agent_id = ne.agent_id

    events_received = [row["event_type"] for row in event_rows]
    has_pre = PRE in normalized_by_type
    has_post = POST in normalized_by_type
    has_analytics = ANALYTICS in normalized_by_type

    # ----------------------------------------------------------------
    # Determine call status (from post or pre event, fallback unknown)
    # ----------------------------------------------------------------
    call_status: Optional[str] = None
    has_unknown_status_flag = False
    for et in (POST, ANALYTICS, PRE):
        ne = normalized_by_type.get(et)
        if ne:
            if "status_unknown" in ne.flags:
                has_unknown_status_flag = True
            if ne.status and call_status is None:
                call_status = ne.status

    is_unknown_status = False
    if call_status is not None:
        if call_status.lower() not in KNOWN_RECONCILE_STATUSES:
            is_unknown_status = True
    elif has_post:
        # Post event delivered without any recognizable status field
        is_unknown_status = True

    if has_unknown_status_flag:
        is_unknown_status = True

    # Determine if analytics is required for this call
    analytics_required = call_status in config.analytics_expected_statuses

    # ----------------------------------------------------------------
    # Collect all timestamps for anchor computation
    # ----------------------------------------------------------------
    ts_candidates: list[Optional[float]] = []
    for row in event_rows:
        try:
            ts_candidates.append(datetime.fromisoformat(row["received_at"]).timestamp())
        except Exception:
            pass
    for ne in normalized_by_type.values():
        if ne.answered_at:
            ts_candidates.append(ne.answered_at.timestamp())
        if ne.ended_at:
            ts_candidates.append(ne.ended_at.timestamp())
    for d in raw_deliveries_list:
        try:
            ts_candidates.append(datetime.fromisoformat(d["received_at"]).timestamp())
        except Exception:
            pass
    # Parse timestamp from call_id if no timestamps found (CALL-<13-digit-epoch>-<suffix>)
    if not ts_candidates:
        m = re.search(r"CALL-(\d{13})-", call_id)
        if m:
            ts_candidates.append(int(m.group(1)) / 1000.0)

    anchor_ts = _earliest_ts(*ts_candidates)
    if anchor_ts is not None and anchor_ts > now_ts:
        # Clock skew: clamp to now
        anchor_ts = now_ts

    deadline_ts = (anchor_ts + config.retention_window_seconds) if anchor_ts is not None else None
    remaining = (deadline_ts - now_ts) if deadline_ts is not None else None

    # ----------------------------------------------------------------
    # Determine call age (from anchor)
    # ----------------------------------------------------------------
    call_age = (now_ts - anchor_ts) if anchor_ts is not None else 0.0

    # ----------------------------------------------------------------
    # State machine
    # ----------------------------------------------------------------
    flags: List[str] = []
    state: str

    # is_discovered_missing computed at top of function

    # Check if recovered via API backfill
    if backfill_row and backfill_row.get("state") == "RECOVERED":
        state = STATE_COMPLETE
    elif is_unknown_status:
        flags.append("unknown_status")
        state = STATE_NEEDS_REVIEW
    elif is_discovered_missing and not has_post:
        # Discovered via platform API; webhooks completely missed
        state = STATE_OVERDUE_POST
    elif has_post and (not analytics_required or has_analytics):
        if not analytics_required and call_status not in TERMINAL_NON_ANALYTICS_STATUSES:
            # Unrecognized status outside terminal non-analytics set -> needs review
            flags.append("unknown_status")
            state = STATE_NEEDS_REVIEW
        else:
            state = STATE_COMPLETE
            if not has_pre:
                flags.append("pre_missing")
    elif has_pre and not has_post:
        # Only pre received
        if call_age <= config.max_call_seconds:
            state = STATE_IN_PROGRESS
        else:
            state = STATE_OVERDUE_POST
    elif has_analytics and not has_post:
        # Analytics seen without post
        if call_age > config.post_reorder_grace_seconds:
            state = STATE_OVERDUE_POST
        else:
            state = STATE_IN_PROGRESS
    elif has_post and analytics_required and not has_analytics:
        # Post received but analytics missing
        if call_age > config.analytics_grace_seconds:
            state = STATE_OVERDUE_ANALYTICS
        else:
            state = STATE_AWAITING_ANALYTICS
    elif not has_pre and not has_post and not has_analytics:
        state = STATE_QUARANTINED_ONLY
    else:
        state = STATE_IN_PROGRESS

    # ----------------------------------------------------------------
    # Risk tier (only for incomplete states missing a required event)
    # ----------------------------------------------------------------
    is_incomplete = state not in (STATE_COMPLETE, STATE_QUARANTINED_ONLY, STATE_IN_PROGRESS)
    risk_tier = RISK_OK
    if is_incomplete and remaining is not None:
        risk_tier = _risk_tier(remaining, config.retention_window_seconds)

    # ----------------------------------------------------------------
    # Aggregate call fields from normalized events
    # ----------------------------------------------------------------
    ne_post = normalized_by_type.get(POST)
    ne_pre = normalized_by_type.get(PRE)
    ne_analytics = normalized_by_type.get(ANALYTICS)
    ne_primary = ne_post or ne_pre or ne_analytics

    anchor_dt = datetime.fromtimestamp(anchor_ts, tz=timezone.utc) if anchor_ts else None
    deadline_dt = datetime.fromtimestamp(deadline_ts, tz=timezone.utc) if deadline_ts else None

    view = CallView(
        call_id=call_id,
        agent_id=latest_agent_id,
        state=state,
        risk_tier=risk_tier,
        anchor_at=anchor_dt,
        deadline_at=deadline_dt,
        remaining_seconds=remaining,
        events_received=events_received,
        missing_required_events=_compute_missing(
            has_pre, has_post, has_analytics, analytics_required, is_discovered_missing
        ),
        flags=flags,
        from_number=ne_primary.from_number if ne_primary else None,
        to_number=ne_primary.to_number if ne_primary else None,
        status=call_status,
        direction=ne_primary.direction if ne_primary else "unknown",
        duration_seconds=ne_primary.duration_seconds if ne_primary else None,
        transcript=ne_post.transcript if ne_post else (ne_pre.transcript if ne_pre else []),
        variables=ne_primary.variables if ne_primary else {},
        analytics=ne_analytics.analytics if ne_analytics else None,
        recording_ref=ne_post.recording_ref if ne_post else None,
        backfill_state=backfill_row["state"] if backfill_row else None,
        raw_deliveries=raw_deliveries_list,
    )
    return view


def _compute_missing(
    has_pre: bool,
    has_post: bool,
    has_analytics: bool,
    analytics_required: bool,
    is_discovered_missing: bool = False,
) -> List[str]:
    missing = []
    if not has_post or is_discovered_missing:
        missing.append(POST)
    if analytics_required and not has_analytics:
        missing.append(ANALYTICS)
    return missing
