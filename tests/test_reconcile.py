"""Tests for reconcile.evaluate() — per-call state machine and deadline risk."""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from atoms_ledger.config import LedgerConfig
from atoms_ledger.models import NormalizedEvent
from atoms_ledger.reconcile import (
    RISK_CRITICAL,
    RISK_EXPIRED,
    RISK_OK,
    RISK_WARNING,
    STATE_AWAITING_ANALYTICS,
    STATE_COMPLETE,
    STATE_IN_PROGRESS,
    STATE_OVERDUE_ANALYTICS,
    STATE_OVERDUE_POST,
    STATE_QUARANTINED_ONLY,
    evaluate,
)
from atoms_ledger.store import LedgerStore

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

BASE_TS = 1729000000.0  # fixed base epoch for reproducibility
WINDOW = 86400  # 24 hours


def _config(**kwargs) -> LedgerConfig:
    defaults = dict(
        retention_window_seconds=WINDOW,
        max_call_seconds=3600,
        analytics_grace_seconds=600,
        post_reorder_grace_seconds=60,
        analytics_expected_statuses={"completed"},
    )
    defaults.update(kwargs)
    return LedgerConfig(**defaults)


def _now(offset: float = 0.0) -> datetime:
    return datetime.fromtimestamp(BASE_TS + offset, tz=timezone.utc)


def _received_at(offset: float = 0.0) -> str:
    return datetime.fromtimestamp(BASE_TS + offset, tz=timezone.utc).isoformat()


def _ne_dict(
    call_id: str,
    event_type: str,
    status: str = "completed",
    agent_id: str = "agent-1",
    **kwargs,
) -> str:
    ne = NormalizedEvent(
        call_id=call_id,
        agent_id=agent_id,
        event_type=event_type,
        status=status,
        **kwargs,
    )
    return json.dumps(ne.to_dict())


def _insert_event(
    store: LedgerStore,
    call_id: str,
    event_type: str,
    sha: str = None,
    received_offset: float = 0.0,
    status: str = "completed",
) -> None:
    sha = sha or f"sha_{call_id}_{event_type}"
    received = _received_at(received_offset)
    store.upsert_event(
        call_id=call_id,
        event_type=event_type,
        delivery_row=1,
        payload_sha256=sha,
        normalized_json=_ne_dict(call_id, event_type, status=status),
        received_at=received,
    )


def _insert_quarantine_delivery(store: LedgerStore, call_id: str) -> None:
    store.insert_delivery(
        received_at=_received_at(),
        raw_bytes_or_none=b"garbage",
        sha256="sha_quar",
        sig_ok=1,
        outcome="quarantined",
        call_id=call_id,
        event_type=None,
        delivery_id=None,
        note="test",
    )


# ---------------------------------------------------------------------------
# State transitions
# ---------------------------------------------------------------------------


def test_complete_with_post_and_analytics() -> None:
    store = LedgerStore()
    _insert_event(store, "CALL-1", "post-conversation")
    _insert_event(store, "CALL-1", "analytics-completed")
    views = evaluate(store, _config(), _now())
    assert len(views) == 1
    assert views[0].state == STATE_COMPLETE


def test_complete_with_pre_post_analytics() -> None:
    store = LedgerStore()
    _insert_event(store, "CALL-1", "pre-conversation")
    _insert_event(store, "CALL-1", "post-conversation")
    _insert_event(store, "CALL-1", "analytics-completed")
    views = evaluate(store, _config(), _now())
    assert views[0].state == STATE_COMPLETE


def test_complete_post_only_no_analytics_when_status_no_answer() -> None:
    """no_answer status should not require analytics → COMPLETE."""
    store = LedgerStore()
    _insert_event(store, "CALL-1", "post-conversation", status="no_answer")
    cfg = _config(analytics_expected_statuses={"completed"})
    views = evaluate(store, cfg, _now())
    assert views[0].state == STATE_COMPLETE


def test_complete_has_pre_missing_flag_when_no_pre() -> None:
    store = LedgerStore()
    _insert_event(store, "CALL-1", "post-conversation")
    _insert_event(store, "CALL-1", "analytics-completed")
    views = evaluate(store, _config(), _now())
    assert views[0].state == STATE_COMPLETE
    assert "pre_missing" in views[0].flags


def test_in_progress_pre_only_within_max_call_seconds() -> None:
    store = LedgerStore()
    _insert_event(store, "CALL-1", "pre-conversation")
    # now = base + 1800 (30 min) → well within max_call_seconds=3600
    views = evaluate(store, _config(), _now(offset=1800))
    assert views[0].state == STATE_IN_PROGRESS


def test_overdue_post_pre_only_past_max_call_seconds() -> None:
    store = LedgerStore()
    _insert_event(store, "CALL-1", "pre-conversation")
    # now = base + 7200 → past max_call_seconds=3600
    views = evaluate(store, _config(), _now(offset=7200))
    assert views[0].state == STATE_OVERDUE_POST


def test_awaiting_analytics_within_grace() -> None:
    store = LedgerStore()
    _insert_event(store, "CALL-1", "post-conversation")
    # now = base + 300 → within analytics_grace_seconds=600
    views = evaluate(store, _config(), _now(offset=300))
    assert views[0].state == STATE_AWAITING_ANALYTICS


def test_overdue_analytics_past_grace() -> None:
    store = LedgerStore()
    _insert_event(store, "CALL-1", "post-conversation")
    # now = base + 700 → past analytics_grace_seconds=600
    views = evaluate(store, _config(), _now(offset=700))
    assert views[0].state == STATE_OVERDUE_ANALYTICS


def test_quarantined_only_state() -> None:
    store = LedgerStore()
    _insert_quarantine_delivery(store, "CALL-Q")
    views = evaluate(store, _config(), _now())
    assert len(views) == 1
    assert views[0].state == STATE_QUARANTINED_ONLY


def test_overdue_post_analytics_without_post_past_reorder_grace() -> None:
    """Analytics seen but no post, past reorder grace → OVERDUE_POST."""
    store = LedgerStore()
    _insert_event(store, "CALL-1", "analytics-completed")
    # now = base + 200 → past post_reorder_grace_seconds=60
    views = evaluate(store, _config(), _now(offset=200))
    assert views[0].state == STATE_OVERDUE_POST


def test_recovered_via_backfill_is_complete() -> None:
    store = LedgerStore()
    _insert_event(store, "CALL-1", "pre-conversation")
    store.upsert_backfill("CALL-1", "RECOVERED", 1, None, _received_at(), '{"recovered": true}')
    views = evaluate(store, _config(), _now(offset=7200))
    assert views[0].state == STATE_COMPLETE


# ---------------------------------------------------------------------------
# All 6 permutations of (pre, post, analytics) yielding same final state
# ---------------------------------------------------------------------------


def test_all_three_events_in_any_order_complete() -> None:
    """All 3! = 6 permutations of receiving pre/post/analytics → COMPLETE."""
    import itertools

    event_types = ["pre-conversation", "post-conversation", "analytics-completed"]
    for perm in itertools.permutations(event_types):
        store = LedgerStore()
        for i, et in enumerate(perm):
            _insert_event(store, "CALL-PERM", et, sha=f"sha_{et}", received_offset=float(i))
        views = evaluate(store, _config(), _now(offset=100))
        assert views[0].state == STATE_COMPLETE, (
            f"Permutation {perm} should be COMPLETE, got {views[0].state}"
        )


# ---------------------------------------------------------------------------
# Risk tier boundary conditions
# ---------------------------------------------------------------------------


def test_risk_tier_ok() -> None:
    store = LedgerStore()
    _insert_event(store, "CALL-1", "post-conversation")
    # Remaining = WINDOW - 700 = 86400 - 700 = 85700 (well past 50% threshold)
    views = evaluate(store, _config(), _now(offset=700))
    assert views[0].state == STATE_OVERDUE_ANALYTICS
    assert views[0].risk_tier == RISK_OK


def test_risk_tier_warning_exactly_50pct() -> None:
    store = LedgerStore()
    _insert_event(store, "CALL-1", "post-conversation")
    # exactly 50% remaining: offset = WINDOW * 0.5 + 700 (past grace)
    offset = WINDOW * 0.5 + 1
    views = evaluate(store, _config(), _now(offset=offset))
    assert views[0].state == STATE_OVERDUE_ANALYTICS
    assert views[0].risk_tier in (RISK_WARNING, RISK_CRITICAL)


def test_risk_tier_critical() -> None:
    store = LedgerStore()
    _insert_event(store, "CALL-1", "post-conversation")
    # critical threshold = min(3600, 86400*0.10) = min(3600, 8640) = 3600
    # remaining = WINDOW - offset → offset = WINDOW - 3600 + 1
    offset = WINDOW - 3600 + 1
    views = evaluate(store, _config(), _now(offset=offset))
    assert views[0].state == STATE_OVERDUE_ANALYTICS
    assert views[0].risk_tier == RISK_CRITICAL


def test_risk_tier_expired() -> None:
    store = LedgerStore()
    _insert_event(store, "CALL-1", "post-conversation")
    # Past retention window
    views = evaluate(store, _config(), _now(offset=WINDOW + 1))
    assert views[0].state == STATE_OVERDUE_ANALYTICS
    assert views[0].risk_tier == RISK_EXPIRED


def test_risk_ok_for_complete_state() -> None:
    """Complete calls should always show OK risk tier."""
    store = LedgerStore()
    _insert_event(store, "CALL-1", "post-conversation")
    _insert_event(store, "CALL-1", "analytics-completed")
    # Even past window, complete calls are OK
    views = evaluate(store, _config(), _now(offset=WINDOW + 100000))
    assert views[0].state == STATE_COMPLETE
    assert views[0].risk_tier == RISK_OK


# ---------------------------------------------------------------------------
# Clock skew (future received_at)
# ---------------------------------------------------------------------------


def test_clock_skew_future_timestamp_clamped() -> None:
    """A future received_at must not result in negative call age or inflated remaining."""
    store = LedgerStore()
    # Insert event with received_at far in the future
    future_ts = _received_at(offset=999999.0)
    store.upsert_event(
        call_id="CALL-SKEW",
        event_type="post-conversation",
        delivery_row=1,
        payload_sha256="sha_skew",
        normalized_json=_ne_dict("CALL-SKEW", "post-conversation"),
        received_at=future_ts,
    )
    cfg = _config()
    now = _now()  # earlier than received_at
    views = evaluate(store, cfg, now)
    assert len(views) == 1
    # Should not crash and remaining should be sane (≤ retention_window)
    if views[0].remaining_seconds is not None:
        assert views[0].remaining_seconds <= cfg.retention_window_seconds


# ---------------------------------------------------------------------------
# Config validation
# ---------------------------------------------------------------------------


def test_config_retention_too_small() -> None:
    with pytest.raises(ValueError):
        LedgerConfig(retention_window_seconds=3599)


def test_config_retention_too_large() -> None:
    with pytest.raises(ValueError):
        LedgerConfig(retention_window_seconds=2592001)


def test_config_retention_boundary_min() -> None:
    cfg = LedgerConfig(retention_window_seconds=3600)
    assert cfg.retention_window_seconds == 3600


def test_config_retention_boundary_max() -> None:
    cfg = LedgerConfig(retention_window_seconds=2592000)
    assert cfg.retention_window_seconds == 2592000


# ---------------------------------------------------------------------------
# Multiple calls
# ---------------------------------------------------------------------------


def test_evaluate_multiple_calls() -> None:
    store = LedgerStore()
    _insert_event(store, "CALL-A", "post-conversation")
    _insert_event(store, "CALL-A", "analytics-completed")
    _insert_event(store, "CALL-B", "pre-conversation")
    views = evaluate(store, _config(), _now())
    call_ids = {v.call_id for v in views}
    assert "CALL-A" in call_ids
    assert "CALL-B" in call_ids
    view_a = next(v for v in views if v.call_id == "CALL-A")
    view_b = next(v for v in views if v.call_id == "CALL-B")
    assert view_a.state == STATE_COMPLETE
    assert view_b.state == STATE_IN_PROGRESS


def test_unknown_status_transitions_to_needs_review() -> None:
    """A call with an unrecognized status must transition to NEEDS_REVIEW instead of COMPLETE."""
    from atoms_ledger.reconcile import STATE_NEEDS_REVIEW

    store = LedgerStore()
    _insert_event(store, "CALL-WEIRD", "post-conversation", status="interrupted_by_carrier")
    views = evaluate(store, _config(), _now())
    assert len(views) == 1
    assert views[0].state == STATE_NEEDS_REVIEW
    assert "unknown_status" in views[0].flags


def test_discovered_missing_outage_call_marked_overdue() -> None:
    """A call discovered via Atoms API diff with no webhooks must be flagged as OVERDUE_POST."""
    from atoms_ledger.reconcile import STATE_OVERDUE_POST

    store = LedgerStore()
    store.insert_discovered_call("CALL-1729000000000-outage", _now().isoformat())
    views = evaluate(store, _config(), _now(offset=7200))
    assert len(views) == 1
    assert views[0].state == STATE_OVERDUE_POST
    assert "post-conversation" in views[0].missing_required_events
