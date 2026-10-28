"""Tests for LedgerStore (SQLite persistence layer)."""

from __future__ import annotations

import os
import tempfile
import threading
from datetime import datetime, timezone

import pytest

from atoms_ledger.store import LedgerStore

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def store() -> LedgerStore:
    return LedgerStore(db_path=":memory:")


def _ts() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Schema creation
# ---------------------------------------------------------------------------


def test_schema_creation_idempotent() -> None:
    """Creating store twice on the same DB should not raise."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        path = f.name
    try:
        s1 = LedgerStore(db_path=path)
        s1.close()
        s2 = LedgerStore(db_path=path)
        s2.close()
    finally:
        os.unlink(path)


# ---------------------------------------------------------------------------
# Deliveries
# ---------------------------------------------------------------------------


def test_insert_delivery_returns_rowid(store: LedgerStore) -> None:
    row_id = store.insert_delivery(
        received_at=_ts(),
        raw_bytes_or_none=b"hello",
        sha256="abc123",
        sig_ok=1,
        outcome="accepted",
        call_id="CALL-1",
        event_type="post-conversation",
        delivery_id="D1",
        note=None,
    )
    assert row_id == 1


def test_get_delivery_roundtrip(store: LedgerStore) -> None:
    ts = _ts()
    row_id = store.insert_delivery(
        received_at=ts,
        raw_bytes_or_none=b"rawbytes",
        sha256="deadbeef",
        sig_ok=1,
        outcome="accepted",
        call_id="CALL-X",
        event_type="pre-conversation",
        delivery_id="DX",
        note="test note",
    )
    row = store.get_delivery(row_id)
    assert row is not None
    assert row["call_id"] == "CALL-X"
    assert row["sha256"] == "deadbeef"
    assert row["raw"] == b"rawbytes"
    assert row["sig_ok"] == 1


def test_raw_bytes_only_stored_when_sig_ok(store: LedgerStore) -> None:
    """sig_ok=0 must result in NULL raw — never persist unauthenticated bytes."""
    row_id = store.insert_delivery(
        received_at=_ts(),
        raw_bytes_or_none=b"tampered",
        sha256="bad123",
        sig_ok=0,
        outcome="rejected_signature",
        call_id=None,
        event_type=None,
        delivery_id=None,
        note="sig_mismatch",
    )
    row = store.get_delivery(row_id)
    assert row is not None
    assert row["raw"] is None, "Unauthenticated raw bytes must never be stored"


def test_delivery_autoincrement_pk(store: LedgerStore) -> None:
    ids = []
    for i in range(5):
        rid = store.insert_delivery(
            received_at=_ts(),
            raw_bytes_or_none=b"x",
            sha256=f"sha{i}",
            sig_ok=1,
            outcome=None,
            call_id=f"CALL-{i}",
            event_type=None,
            delivery_id=None,
            note=None,
        )
        ids.append(rid)
    assert ids == [1, 2, 3, 4, 5]


def test_get_delivery_not_found(store: LedgerStore) -> None:
    assert store.get_delivery(9999) is None


def test_get_deliveries_for_call(store: LedgerStore) -> None:
    store.insert_delivery(_ts(), b"a", "sha_a", 1, "accepted", "CALL-Z", "pre", "D1", None)
    store.insert_delivery(_ts(), b"b", "sha_b", 1, "accepted", "CALL-Z", "post", "D2", None)
    store.insert_delivery(_ts(), b"c", "sha_c", 1, "accepted", "CALL-OTHER", "post", "D3", None)
    rows = store.get_deliveries_for_call("CALL-Z")
    assert len(rows) == 2


def test_update_delivery_outcome(store: LedgerStore) -> None:
    row_id = store.insert_delivery(_ts(), b"x", "sha_x", 1, None, "CALL-1", "pre", None, None)
    store.update_delivery_outcome(row_id, "accepted")
    row = store.get_delivery(row_id)
    assert row["outcome"] == "accepted"


# ---------------------------------------------------------------------------
# Events
# ---------------------------------------------------------------------------


def test_upsert_event_inserted(store: LedgerStore) -> None:
    result = store.upsert_event(
        call_id="CALL-A",
        event_type="post-conversation",
        delivery_row=1,
        payload_sha256="aaa111",
        normalized_json='{"k": "v"}',
        received_at=_ts(),
    )
    assert result == "inserted"


def test_upsert_event_duplicate(store: LedgerStore) -> None:
    ts = _ts()
    store.upsert_event("CALL-A", "post-conversation", 1, "aaa111", '{"k":"v"}', ts)
    result2 = store.upsert_event("CALL-A", "post-conversation", 2, "aaa111", '{"k":"v"}', ts)
    assert result2 == "duplicate"


def test_upsert_event_conflict(store: LedgerStore) -> None:
    ts = _ts()
    store.upsert_event("CALL-A", "post-conversation", 1, "sha_original", '{"k":"v1"}', ts)
    result = store.upsert_event("CALL-A", "post-conversation", 2, "sha_different", '{"k":"v2"}', ts)
    assert result == "conflict"
    # Original must NOT be overwritten
    ev = store.get_event("CALL-A", "post-conversation")
    assert ev is not None
    assert ev["payload_sha256"] == "sha_original"
    # Anomaly must be recorded
    anomalies = store.get_anomalies_for_call("CALL-A")
    assert any(a["kind"] == "duplicate_conflict" for a in anomalies)


def test_get_all_events_for_call(store: LedgerStore) -> None:
    ts = _ts()
    store.upsert_event("CALL-B", "pre-conversation", 1, "pre_sha", '{"pre": true}', ts)
    store.upsert_event("CALL-B", "post-conversation", 2, "post_sha", '{"post": true}', ts)
    events = store.get_all_events_for_call("CALL-B")
    assert len(events) == 2
    etypes = {e["event_type"] for e in events}
    assert etypes == {"pre-conversation", "post-conversation"}


def test_get_event_not_found(store: LedgerStore) -> None:
    assert store.get_event("NO-SUCH", "post-conversation") is None


# ---------------------------------------------------------------------------
# Anomalies
# ---------------------------------------------------------------------------


def test_insert_anomaly_returns_rowid(store: LedgerStore) -> None:
    rid = store.insert_anomaly("CALL-A", "quarantined", "bad json", _ts())
    assert rid == 1


def test_get_anomalies_for_call(store: LedgerStore) -> None:
    store.insert_anomaly("CALL-X", "quarantined", "detail1", _ts())
    store.insert_anomaly("CALL-X", "schema_drift", "detail2", _ts())
    store.insert_anomaly("CALL-Y", "quarantined", "other", _ts())
    results = store.get_anomalies_for_call("CALL-X")
    assert len(results) == 2
    kinds = {r["kind"] for r in results}
    assert kinds == {"quarantined", "schema_drift"}


# ---------------------------------------------------------------------------
# Backfill
# ---------------------------------------------------------------------------


def test_upsert_backfill_insert_and_update(store: LedgerStore) -> None:
    store.upsert_backfill("CALL-A", "PENDING", 0, None, None, None)
    row = store.get_backfill("CALL-A")
    assert row is not None
    assert row["state"] == "PENDING"
    assert row["attempts"] == 0

    store.upsert_backfill("CALL-A", "RECOVERED", 1, None, _ts(), '{"recovered": true}')
    row2 = store.get_backfill("CALL-A")
    assert row2["state"] == "RECOVERED"
    assert row2["attempts"] == 1


def test_get_backfill_not_found(store: LedgerStore) -> None:
    assert store.get_backfill("NO-SUCH") is None


# ---------------------------------------------------------------------------
# list_all_calls
# ---------------------------------------------------------------------------


def test_list_all_calls_merged_from_both_tables(store: LedgerStore) -> None:
    store.insert_delivery(_ts(), b"x", "sha1", 1, None, "CALL-D1", "pre", None, None)
    store.upsert_event("CALL-E1", "post-conversation", 1, "sha2", "{}", _ts())
    store.insert_delivery(_ts(), b"y", "sha3", 1, None, "CALL-D1", "post", None, None)
    all_calls = store.list_all_calls()
    assert "CALL-D1" in all_calls
    assert "CALL-E1" in all_calls
    assert len(set(all_calls)) == len(all_calls), "No duplicates expected"


def test_list_all_calls_excludes_null_call_id(store: LedgerStore) -> None:
    store.insert_delivery(_ts(), None, "sha_bad", 0, "rejected", None, None, None, None)
    all_calls = store.list_all_calls()
    assert None not in all_calls


# ---------------------------------------------------------------------------
# File-based store
# ---------------------------------------------------------------------------


def test_file_store_persists_across_instances(tmp_path: pytest.TempPathFactory) -> None:
    db = str(tmp_path / "test.db")
    s1 = LedgerStore(db_path=db)
    s1.insert_delivery(_ts(), b"bytes", "shafile", 1, "accepted", "CALL-F", "post", "DX", None)
    s1.close()

    s2 = LedgerStore(db_path=db)
    row = s2.get_delivery(1)
    assert row is not None
    assert row["call_id"] == "CALL-F"
    s2.close()


# ---------------------------------------------------------------------------
# Thread safety
# ---------------------------------------------------------------------------


def test_concurrent_inserts_no_corruption(store: LedgerStore) -> None:
    """Concurrent inserts from multiple threads must not corrupt the DB."""
    errors: list[str] = []
    results: list[int] = []
    lock = threading.Lock()

    def worker(idx: int) -> None:
        try:
            rid = store.insert_delivery(
                received_at=_ts(),
                raw_bytes_or_none=b"x",
                sha256=f"sha_{idx}",
                sig_ok=1,
                outcome="accepted",
                call_id=f"CALL-{idx}",
                event_type="post",
                delivery_id=None,
                note=None,
            )
            with lock:
                results.append(rid)
        except Exception as exc:
            with lock:
                errors.append(str(exc))

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors, f"Errors during concurrent inserts: {errors}"
    assert len(results) == 20
    assert len(set(results)) == 20, "Row IDs must be unique"


def test_concurrent_upsert_event_no_data_race(store: LedgerStore) -> None:
    """Concurrent upserts for the same key must not corrupt; one wins, rest return duplicate/conflict."""
    outcomes: list[str] = []
    lock = threading.Lock()

    def worker(idx: int) -> None:
        sha = "sha_common" if idx % 2 == 0 else f"sha_{idx}"
        result = store.upsert_event(
            call_id="CALL-RACE",
            event_type="post-conversation",
            delivery_row=idx,
            payload_sha256=sha,
            normalized_json="{}",
            received_at=_ts(),
        )
        with lock:
            outcomes.append(result)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert "inserted" in outcomes
    assert all(o in {"inserted", "duplicate", "conflict"} for o in outcomes)
    # Only one event row must exist for (CALL-RACE, post-conversation)
    events = store.get_all_events_for_call("CALL-RACE")
    assert len(events) == 1
