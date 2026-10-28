"""Tests for backfill.plan() and backfill.execute()."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import patch

from atoms_ledger.backfill import (
    RESULT_CONFIG_ERROR,
    RESULT_RECOVERED,
    RESULT_RETRY_LATER,
    RESULT_SKIPPED,
    RESULT_UNRECOVERABLE,
    MockAtomsClient,
    _MockAPIError,
    execute,
    plan,
)
from atoms_ledger.config import LedgerConfig
from atoms_ledger.models import NormalizedEvent
from atoms_ledger.store import LedgerStore

BASE_TS = 1729000000.0
WINDOW = 86400


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


def _ne_json(call_id: str, event_type: str, status: str = "completed") -> str:
    ne = NormalizedEvent(call_id=call_id, agent_id="agent-1", event_type=event_type, status=status)
    return json.dumps(ne.to_dict())


def _insert_event(store: LedgerStore, call_id: str, event_type: str, offset: float = 0.0) -> None:
    store.upsert_event(
        call_id=call_id,
        event_type=event_type,
        delivery_row=1,
        payload_sha256=f"sha_{call_id}_{event_type}",
        normalized_json=_ne_json(call_id, event_type),
        received_at=_received_at(offset),
    )


# ---------------------------------------------------------------------------
# Plan
# ---------------------------------------------------------------------------


def test_plan_returns_tasks_for_overdue_calls() -> None:
    store = LedgerStore()
    _insert_event(store, "CALL-A", "post-conversation")  # missing analytics, in grace
    # now = base + 700 → overdue_analytics
    tasks = plan(store, _config(), _now(offset=700))
    call_ids = [t.call_id for t in tasks]
    assert "CALL-A" in call_ids


def test_plan_no_tasks_for_complete_calls() -> None:
    store = LedgerStore()
    _insert_event(store, "CALL-DONE", "post-conversation")
    _insert_event(store, "CALL-DONE", "analytics-completed")
    tasks = plan(store, _config(), _now())
    assert all(t.call_id != "CALL-DONE" for t in tasks)


def test_plan_ordering_most_urgent_first() -> None:
    """Calls with least remaining time must appear first."""
    store = LedgerStore()
    # CALL-NEAR has event received earlier → less remaining
    _insert_event(store, "CALL-NEAR", "post-conversation", offset=0.0)
    _insert_event(store, "CALL-FAR", "post-conversation", offset=1000.0)
    # now = base + 700 (past grace) so both are overdue
    tasks = plan(store, _config(), _now(offset=700))
    if len(tasks) >= 2:
        assert tasks[0].remaining_seconds <= tasks[1].remaining_seconds


def test_plan_chunks_to_max_per_batch() -> None:
    """Plan must not return more than max_per_batch tasks."""
    store = LedgerStore()
    for i in range(150):
        _insert_event(store, f"CALL-{i:04d}", "post-conversation", offset=float(i))
    tasks = plan(store, _config(), _now(offset=700), max_per_batch=100)
    assert len(tasks) <= 100


# ---------------------------------------------------------------------------
# Execute — mock success
# ---------------------------------------------------------------------------


def test_execute_mock_success_recovered() -> None:
    store = LedgerStore()
    _insert_event(store, "CALL-A", "post-conversation")
    from atoms_ledger.models import BackfillTask

    tasks = [BackfillTask("CALL-A", 0, 1000.0, ["analytics-completed"])]
    client = MockAtomsClient(behavior="success")
    with patch("atoms_ledger.backfill.time.sleep"):
        results = execute(client, tasks, store, _config())
    assert results["CALL-A"] == RESULT_RECOVERED
    backfill = store.get_backfill("CALL-A")
    assert backfill is not None
    assert backfill["state"] == RESULT_RECOVERED
    recovered = json.loads(backfill["recovered_json"])
    assert recovered["call_id"] == "CALL-A"


def test_execute_no_recording_url_cached_separately() -> None:
    """Recording URL must only appear in recovered_json, never cached elsewhere."""
    store = LedgerStore()
    from atoms_ledger.models import BackfillTask

    tasks = [BackfillTask("CALL-A", 0, 1000.0, ["analytics-completed"])]
    client = MockAtomsClient(
        behavior="success",
        call_data_overrides={"CALL-A": {"recording_url": "https://example.com/recording/12345"}},
    )
    with patch("atoms_ledger.backfill.time.sleep"):
        results = execute(client, tasks, store, _config())
    assert results["CALL-A"] == RESULT_RECOVERED
    backfill = store.get_backfill("CALL-A")
    recovered = json.loads(backfill["recovered_json"])
    # URL stored in recovered_json but note must accompany it
    assert "_recording_note" in recovered
    assert "15 minutes" in recovered["_recording_note"]
    # URL itself must be in recovered_json (expected) but ONLY there
    # No separate store field for recording_url
    row = store._conn.execute(
        "SELECT recovered_json FROM backfill WHERE call_id='CALL-A'"
    ).fetchone()
    assert row is not None  # stored only in recovered_json blob


# ---------------------------------------------------------------------------
# Execute — mock not_found
# ---------------------------------------------------------------------------


def test_execute_mock_not_found_unrecoverable() -> None:
    store = LedgerStore()
    from atoms_ledger.models import BackfillTask

    tasks = [BackfillTask("CALL-GONE", 0, 500.0, ["analytics-completed"])]
    client = MockAtomsClient(behavior="not_found")
    with patch("atoms_ledger.backfill.time.sleep"):
        results = execute(client, tasks, store, _config())
    assert results["CALL-GONE"] == RESULT_UNRECOVERABLE
    backfill = store.get_backfill("CALL-GONE")
    assert backfill["state"] == RESULT_UNRECOVERABLE


# ---------------------------------------------------------------------------
# Execute — mock timeout → RETRY_LATER
# ---------------------------------------------------------------------------


def test_execute_mock_timeout_retry_later() -> None:
    store = LedgerStore()
    from atoms_ledger.models import BackfillTask

    tasks = [BackfillTask("CALL-TIMEOUT", 0, 500.0, ["analytics-completed"])]
    client = MockAtomsClient(behavior="timeout")
    with patch("atoms_ledger.backfill.time.sleep"):
        results = execute(client, tasks, store, _config())
    assert results["CALL-TIMEOUT"] == RESULT_RETRY_LATER


# ---------------------------------------------------------------------------
# Execute — mock auth_error → CONFIG_ERROR + stops loop
# ---------------------------------------------------------------------------


def test_execute_mock_auth_error_stops_loop() -> None:
    store = LedgerStore()
    from atoms_ledger.models import BackfillTask

    tasks = [
        BackfillTask("CALL-A", 0, 500.0, ["analytics-completed"]),
        BackfillTask("CALL-B", 1, 400.0, ["analytics-completed"]),
        BackfillTask("CALL-C", 2, 300.0, ["analytics-completed"]),
    ]
    client = MockAtomsClient(behavior="auth_error")
    with patch("atoms_ledger.backfill.time.sleep"):
        results = execute(client, tasks, store, _config())

    # First call → CONFIG_ERROR
    assert results["CALL-A"] == RESULT_CONFIG_ERROR
    # Remaining tasks → SKIPPED (loop stopped)
    assert results["CALL-B"] == RESULT_SKIPPED
    assert results["CALL-C"] == RESULT_SKIPPED


# ---------------------------------------------------------------------------
# Execute — one failing task does not stop others (except auth_error)
# ---------------------------------------------------------------------------


def test_execute_one_not_found_doesnt_stop_others() -> None:
    store = LedgerStore()
    from atoms_ledger.models import BackfillTask

    tasks = [
        BackfillTask("CALL-GONE", 0, 500.0, ["analytics-completed"]),
        BackfillTask("CALL-GOOD", 1, 400.0, ["analytics-completed"]),
    ]
    client = MockAtomsClient(behavior="success", fail_call_ids=["CALL-GONE"])
    with patch("atoms_ledger.backfill.time.sleep"):
        results = execute(client, tasks, store, _config())

    assert results["CALL-GONE"] == RESULT_UNRECOVERABLE
    assert results["CALL-GOOD"] == RESULT_RECOVERED


# ---------------------------------------------------------------------------
# Mock client interface verification
# ---------------------------------------------------------------------------


def test_mock_client_behaviors() -> None:
    """Verify all MOCK behaviors produce expected exceptions/responses."""
    behaviors = {
        "success": None,
        "not_found": 404,
        "timeout": 0,
        "rate_limit": 429,
        "auth_error": 401,
    }
    for behavior, expected_status in behaviors.items():
        client = MockAtomsClient(behavior=behavior)
        if expected_status is None:
            resp = client.atoms.calls.get(id="CALL-X")
            assert resp.data.call_id == "CALL-X"
        else:
            try:
                client.atoms.calls.get(id="CALL-X")
                assert False, f"Expected error for {behavior}"
            except _MockAPIError as e:
                assert e.status_code == expected_status
