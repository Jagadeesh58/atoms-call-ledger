"""
WHY:
Implements persist-before-ack ingestion: verify → persist raw bytes → dedupe →
normalize → upsert call. Exists because Atoms webhooks are at-most-once; every
authenticated byte is irreplaceable under ZDR. Must never drop authenticated
bytes; must never store unauthenticated bodies; must persist before returning a result.
"""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import Optional

from atoms_ledger.config import LedgerConfig
from atoms_ledger.models import IngestResult
from atoms_ledger.normalize import normalize_event
from atoms_ledger.signature import verify_signature
from atoms_ledger.store import LedgerStore

logger = logging.getLogger(__name__)

_CALL_ID_FIELDS = ("callId", "call_id")
_EVENT_TYPE_FIELDS = ("eventType", "event_type", "type")


def _sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _get_header(headers: dict, name: str) -> Optional[str]:
    """Case-insensitive header lookup."""
    name_lower = name.lower()
    for k, v in headers.items():
        if k.lower() == name_lower:
            return v
    return None


def _extract_call_id(parsed: dict) -> Optional[str]:
    for field in _CALL_ID_FIELDS:
        val = parsed.get(field)
        if val and isinstance(val, str) and val.strip():
            return val.strip()
        # Also check nested metadata / data objects
    for sub_key in ("metadata", "data", "payload"):
        sub = parsed.get(sub_key)
        if isinstance(sub, dict):
            for field in _CALL_ID_FIELDS:
                val = sub.get(field)
                if val and isinstance(val, str) and val.strip():
                    return val.strip()
    return None


def _extract_event_type(parsed: dict) -> Optional[str]:
    for field in _EVENT_TYPE_FIELDS:
        val = parsed.get(field)
        if val and isinstance(val, str) and val.strip():
            return val.strip()
    for sub_key in ("metadata", "data", "payload"):
        sub = parsed.get(sub_key)
        if isinstance(sub, dict):
            for field in _EVENT_TYPE_FIELDS:
                val = sub.get(field)
                if val and isinstance(val, str) and val.strip():
                    return val.strip()
    return None


def handle(
    raw_bytes: bytes,
    headers: dict,  # e.g. {"x-signature": "..."}
    config: LedgerConfig,
    store: LedgerStore,
    now: Optional[datetime] = None,
) -> IngestResult:
    """
    Persist-before-ack ingestion pipeline.

    Order of operations (invariant — do not reorder):
      1. Empty body check → 400
      2. Size limit check → 413
      3. Signature verification → 401 (persists delivery row without raw)
      4. Persist raw authenticated bytes to deliveries (sig_ok=1)
      5. Parse JSON → quarantine on failure (200)
      6. Extract call_id / event_type → quarantine on missing
      7. Deduplicate via sha256 → duplicate/conflict (200)
      8. Normalize and upsert event → accepted (200)

    Never logs transcripts or phone numbers. Logs call_id and outcome only.
    """
    if now is None:
        now = datetime.now(tz=timezone.utc)
    received_at = now.isoformat()

    # ------------------------------------------------------------------
    # Step 1: Empty body
    # ------------------------------------------------------------------
    if not raw_bytes:
        return IngestResult(outcome="rejected_empty", http_status=400)

    # ------------------------------------------------------------------
    # Step 2: Size limit
    # ------------------------------------------------------------------
    if len(raw_bytes) > config.max_body_bytes:
        return IngestResult(outcome="rejected_size", http_status=413)

    # ------------------------------------------------------------------
    # Step 3: Signature verification
    # ------------------------------------------------------------------
    sig_header = _get_header(headers, "x-signature")
    sig_ok_bool, reason = verify_signature(raw_bytes, sig_header, config.webhook_secrets)

    if not sig_ok_bool:
        # Persist delivery record WITHOUT raw bytes (sig_ok=0)
        store.insert_delivery(
            received_at=received_at,
            raw_bytes_or_none=None,
            sha256=_sha256_hex(raw_bytes),
            sig_ok=0,
            outcome="rejected_signature",
            call_id=None,
            event_type=None,
            delivery_id=None,
            note=reason,
        )
        return IngestResult(outcome="rejected_signature", http_status=401)

    # ------------------------------------------------------------------
    # Step 4: Persist raw authenticated bytes BEFORE any further processing
    # ------------------------------------------------------------------
    body_sha = _sha256_hex(raw_bytes)
    delivery_row_id = store.insert_delivery(
        received_at=received_at,
        raw_bytes_or_none=raw_bytes,
        sha256=body_sha,
        sig_ok=1,
        outcome=None,  # updated below
        call_id=None,  # updated below
        event_type=None,  # updated below
        delivery_id=None,
        note=None,
    )

    # ------------------------------------------------------------------
    # Step 5: Parse JSON
    # ------------------------------------------------------------------
    try:
        parsed = json.loads(raw_bytes)
        if not isinstance(parsed, dict):
            raise ValueError("Top-level payload must be a JSON object")
    except Exception as exc:
        store.insert_anomaly(
            call_id=None,
            kind="quarantined",
            detail=f"json_parse_error: {exc!s}",
            at=received_at,
        )
        store.update_delivery_outcome(delivery_row_id, "quarantined")
        return IngestResult(outcome="quarantined", http_status=200)

    # ------------------------------------------------------------------
    # Step 6: Extract call_id and event_type
    # ------------------------------------------------------------------
    call_id = _extract_call_id(parsed)
    event_type = _extract_event_type(parsed)

    if not call_id or not event_type:
        missing = []
        if not call_id:
            missing.append("call_id")
        if not event_type:
            missing.append("event_type")
        store.insert_anomaly(
            call_id=call_id,
            kind="quarantined",
            detail=f"missing_fields: {missing}",
            at=received_at,
        )
        store.update_delivery_outcome(delivery_row_id, "quarantined")
        logger.info("quarantined delivery_row=%d missing=%s", delivery_row_id, missing)
        return IngestResult(outcome="quarantined", http_status=200)

    # Update delivery row with extracted identifiers
    store.update_delivery_outcome(delivery_row_id, None)  # will be set at end
    # Patch call_id / event_type into the delivery row
    with store._lock:
        store._conn.execute(
            "UPDATE deliveries SET call_id = ?, event_type = ? WHERE id = ?",
            (call_id, event_type, delivery_row_id),
        )
        store._conn.commit()

    # ------------------------------------------------------------------
    # Step 7: Deduplicate
    # ------------------------------------------------------------------
    upsert_result = store.upsert_event(
        call_id=call_id,
        event_type=event_type,
        delivery_row=delivery_row_id,
        payload_sha256=body_sha,
        normalized_json="{}",  # placeholder; overwritten on "inserted"
        received_at=received_at,
    )

    if upsert_result == "duplicate":
        store.update_delivery_outcome(delivery_row_id, "duplicate")
        logger.info("duplicate call_id=%s event_type=%s", call_id, event_type)
        return IngestResult(
            outcome="duplicate",
            http_status=200,
            call_id=call_id,
            event_type=event_type,
        )

    if upsert_result == "conflict":
        store.update_delivery_outcome(delivery_row_id, "conflict")
        logger.info("conflict call_id=%s event_type=%s", call_id, event_type)
        return IngestResult(
            outcome="conflict",
            http_status=200,
            call_id=call_id,
            event_type=event_type,
        )

    # ------------------------------------------------------------------
    # Step 8: Normalize and upsert event with real normalized_json
    # ------------------------------------------------------------------
    normalized = normalize_event(parsed, source="webhook")
    anomaly_flags = normalized.flags

    normalized_json_str = json.dumps(normalized.to_dict())
    # Overwrite the event row we just inserted with real normalized JSON
    with store._lock:
        store._conn.execute(
            "UPDATE events SET normalized_json = ? WHERE call_id = ? AND event_type = ?",
            (normalized_json_str, call_id, event_type),
        )
        store._conn.commit()

    store.update_delivery_outcome(delivery_row_id, "accepted")
    # Patch delivery_id from parsed payload
    delivery_id = parsed.get("deliveryId") or parsed.get("delivery_id")
    if delivery_id:
        with store._lock:
            store._conn.execute(
                "UPDATE deliveries SET delivery_id = ? WHERE id = ?",
                (str(delivery_id), delivery_row_id),
            )
            store._conn.commit()

    logger.info("accepted call_id=%s event_type=%s", call_id, event_type)
    return IngestResult(
        outcome="accepted",
        http_status=200,
        call_id=call_id,
        event_type=event_type,
        delivery_id=str(delivery_id) if delivery_id else None,
        anomalies=anomaly_flags,
    )
