"""Tests for ingest.handle() — persist-before-ack ingestion pipeline."""

from __future__ import annotations

import hashlib
import hmac
import json
from datetime import datetime, timezone

from atoms_ledger.config import LedgerConfig
from atoms_ledger.ingest import handle
from atoms_ledger.store import LedgerStore

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SECRET = "test-webhook-secret"


def _config(**kwargs) -> LedgerConfig:
    defaults = dict(
        webhook_secrets=[SECRET],
        retention_window_seconds=86400,
    )
    defaults.update(kwargs)
    return LedgerConfig(**defaults)


def _sign(body: bytes, secret: str = SECRET) -> str:
    return hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


def _headers(body: bytes, secret: str = SECRET) -> dict:
    return {"x-signature": _sign(body, secret)}


def _now() -> datetime:
    return datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)


def _valid_payload(
    call_id: str = "CALL-1729000000000-abcdef",
    event_type: str = "post-conversation",
    **extra,
) -> bytes:
    payload = {
        "callId": call_id,
        "eventType": event_type,
        "agentId": "agent-1",
        "status": "completed",
    }
    payload.update(extra)
    return json.dumps(payload).encode()


# ---------------------------------------------------------------------------
# Step 1: Empty body → rejected_empty
# ---------------------------------------------------------------------------


def test_empty_body_rejected() -> None:
    store = LedgerStore()
    result = handle(b"", {}, _config(), store, _now())
    assert result.outcome == "rejected_empty"
    assert result.http_status == 400
    # Nothing should be persisted
    assert store.list_all_calls() == []


# ---------------------------------------------------------------------------
# Step 2: Oversize body → rejected_size
# ---------------------------------------------------------------------------


def test_oversize_body_rejected() -> None:
    store = LedgerStore()
    cfg = _config(max_body_bytes=10)
    body = b"x" * 11
    result = handle(body, _headers(body), cfg, store, _now())
    assert result.outcome == "rejected_size"
    assert result.http_status == 413


# ---------------------------------------------------------------------------
# Step 3: Bad/missing signature → rejected_signature, no raw stored
# ---------------------------------------------------------------------------


def test_missing_signature_rejected() -> None:
    store = LedgerStore()
    body = _valid_payload()
    result = handle(body, {}, _config(), store, _now())
    assert result.outcome == "rejected_signature"
    assert result.http_status == 401
    # Delivery must be recorded but raw must be NULL
    rows = store._conn.execute("SELECT raw, sig_ok FROM deliveries").fetchall()
    assert len(rows) == 1
    assert rows[0][0] is None, "Unauthenticated raw must not be stored"
    assert rows[0][1] == 0


def test_bad_signature_rejected() -> None:
    store = LedgerStore()
    body = _valid_payload()
    result = handle(body, {"x-signature": "a" * 64}, _config(), store, _now())
    assert result.outcome == "rejected_signature"
    assert result.http_status == 401
    rows = store._conn.execute("SELECT raw FROM deliveries").fetchall()
    assert rows[0][0] is None


def test_wrong_secret_rejected() -> None:
    store = LedgerStore()
    body = _valid_payload()
    headers = {"x-signature": _sign(body, "wrong-secret")}
    result = handle(body, headers, _config(), store, _now())
    assert result.outcome == "rejected_signature"
    assert result.http_status == 401


# ---------------------------------------------------------------------------
# Case-insensitive header lookup
# ---------------------------------------------------------------------------


def test_header_case_insensitive_uppercase() -> None:
    store = LedgerStore()
    body = _valid_payload()
    result = handle(body, {"X-SIGNATURE": _sign(body)}, _config(), store, _now())
    assert result.outcome == "accepted"


def test_header_case_insensitive_mixed() -> None:
    store = LedgerStore()
    body = _valid_payload()
    result = handle(body, {"X-Signature": _sign(body)}, _config(), store, _now())
    assert result.outcome == "accepted"


def test_header_case_insensitive_lowercase() -> None:
    store = LedgerStore()
    body = _valid_payload()
    result = handle(body, {"x-signature": _sign(body)}, _config(), store, _now())
    assert result.outcome == "accepted"


# ---------------------------------------------------------------------------
# Step 4: Raw bytes persisted BEFORE result returned (insert-order test)
# ---------------------------------------------------------------------------


def test_raw_persisted_before_result_returned() -> None:
    """Verify raw bytes are stored before handle() returns — use a patched store."""

    class OrderCapturingStore(LedgerStore):
        ops: list[str] = []

        def insert_delivery(self, *args, **kwargs) -> int:  # type: ignore[override]
            OrderCapturingStore.ops.append("insert_delivery")
            return super().insert_delivery(*args, **kwargs)

    store = OrderCapturingStore()
    body = _valid_payload()
    result = handle(body, _headers(body), _config(), store, _now())
    assert result.outcome == "accepted"
    # insert_delivery must be first op (before normalization result returned)
    assert OrderCapturingStore.ops[0] == "insert_delivery"


# ---------------------------------------------------------------------------
# Step 5: Authenticated garbage JSON → quarantined + 200 + raw retained
# ---------------------------------------------------------------------------


def test_authenticated_malformed_json_quarantined() -> None:
    store = LedgerStore()
    body = b"not-json-at-all{{{garbage"
    result = handle(body, _headers(body), _config(), store, _now())
    assert result.outcome == "quarantined"
    assert result.http_status == 200
    # Raw bytes MUST be retained (authenticated)
    rows = store._conn.execute("SELECT raw, sig_ok FROM deliveries").fetchall()
    assert len(rows) == 1
    assert rows[0][0] == body, "Authenticated raw must be stored even when quarantined"
    assert rows[0][1] == 1


def test_authenticated_non_object_json_quarantined() -> None:
    store = LedgerStore()
    body = json.dumps([1, 2, 3]).encode()
    result = handle(body, _headers(body), _config(), store, _now())
    assert result.outcome == "quarantined"
    assert result.http_status == 200


# ---------------------------------------------------------------------------
# Step 6: Missing callId → quarantined
# ---------------------------------------------------------------------------


def test_missing_call_id_quarantined() -> None:
    store = LedgerStore()
    payload = {"eventType": "post-conversation", "agentId": "agent-1"}
    body = json.dumps(payload).encode()
    result = handle(body, _headers(body), _config(), store, _now())
    assert result.outcome == "quarantined"
    assert result.http_status == 200


def test_missing_event_type_quarantined() -> None:
    store = LedgerStore()
    payload = {"callId": "CALL-1729000000000-abcdef", "agentId": "agent-1"}
    body = json.dumps(payload).encode()
    result = handle(body, _headers(body), _config(), store, _now())
    assert result.outcome == "quarantined"
    assert result.http_status == 200


# ---------------------------------------------------------------------------
# Step 7: Duplicate identical → duplicate + 200
# ---------------------------------------------------------------------------


def test_duplicate_identical_payload() -> None:
    store = LedgerStore()
    body = _valid_payload()
    r1 = handle(body, _headers(body), _config(), store, _now())
    assert r1.outcome == "accepted"
    r2 = handle(body, _headers(body), _config(), store, _now())
    assert r2.outcome == "duplicate"
    assert r2.http_status == 200


# ---------------------------------------------------------------------------
# Step 7: Duplicate different body → conflict + 200
# ---------------------------------------------------------------------------


def test_duplicate_conflict_payload() -> None:
    store = LedgerStore()
    body1 = _valid_payload(status="completed")
    body2 = _valid_payload(status="failed")  # same call_id/event_type, different body
    r1 = handle(body1, _headers(body1), _config(), store, _now())
    assert r1.outcome == "accepted"
    r2 = handle(body2, _headers(body2), _config(), store, _now())
    assert r2.outcome == "conflict"
    assert r2.http_status == 200


# ---------------------------------------------------------------------------
# Step 8: Valid full payload → accepted
# ---------------------------------------------------------------------------


def test_valid_full_payload_accepted() -> None:
    store = LedgerStore()
    body = _valid_payload()
    result = handle(body, _headers(body), _config(), store, _now())
    assert result.outcome == "accepted"
    assert result.http_status == 200
    assert result.call_id is not None
    assert result.event_type is not None
    # Verify event is persisted
    events = store.get_all_events_for_call(result.call_id)
    assert len(events) == 1
    assert events[0]["event_type"] == result.event_type


def test_accepted_delivery_has_raw_bytes() -> None:
    store = LedgerStore()
    body = _valid_payload()
    result = handle(body, _headers(body), _config(), store, _now())
    assert result.outcome == "accepted"
    deliveries = store.get_deliveries_for_call(result.call_id)
    assert len(deliveries) == 1
    assert deliveries[0]["raw"] == body
    assert deliveries[0]["sig_ok"] == 1
