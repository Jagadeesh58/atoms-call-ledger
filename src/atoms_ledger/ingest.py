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
import threading
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


class _UnauthenticatedWriteLimiter:
    """Thread-safe sliding-window rate limiter for unauthenticated DB audit writes.

    Protects SQLite from write-lock starvation and disk space exhaustion during
    DDoS or credential-stuffing attacks. Unauthenticated requests are always rejected (401),
    but audit writes to SQLite are throttled to a configurable rate.
    """

    def __init__(self, limit: int = 120, window_seconds: float = 60.0) -> None:
        self.limit = limit
        self.window_seconds = window_seconds
        self._lock = threading.Lock()
        self._timestamps: list[float] = []

    def allow(self, now_ts: float) -> bool:
        cutoff = now_ts - self.window_seconds
        with self._lock:
            self._timestamps = [ts for ts in self._timestamps if ts > cutoff]
            if len(self._timestamps) >= self.limit:
                return False
            self._timestamps.append(now_ts)
            return True


_unauth_write_limiter = _UnauthenticatedWriteLimiter()


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
      3. Signature verification → 401 (atomic delivery row without raw)
      4. Parse JSON → quarantine on failure (200, atomic delivery + anomaly)
      5. Extract call_id / event_type → quarantine on missing (200, atomic delivery + anomaly)
      6. Normalize payload into canonical representation (prevents empty placeholder states)
      7. Atomic commit: persist raw delivery + deduplicate + insert event (200, single tx)

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
        # Reject immediately. Throttle audit writes to SQLite to prevent lock exhaustion.
        if _unauth_write_limiter.allow(now.timestamp()):
            store.ingest_delivery_atomic(
                received_at=received_at,
                raw_bytes_or_none=None,
                sha256=_sha256_hex(raw_bytes),
                sig_ok=0,
                call_id=None,
                event_type=None,
                delivery_id=None,
                normalized_json=None,
                outcome="rejected_signature",
                note=reason,
            )
        else:
            logger.warning(
                "Unauthenticated delivery write dropped by rate limiter (DDoS protection)"
            )
        return IngestResult(outcome="rejected_signature", http_status=401)

    # ------------------------------------------------------------------
    # Step 4: Parse JSON (authenticated payload)
    # ------------------------------------------------------------------
    body_sha = _sha256_hex(raw_bytes)
    try:
        parsed = json.loads(raw_bytes)
        if not isinstance(parsed, dict):
            raise ValueError("Top-level payload must be a JSON object")
    except Exception as exc:
        store.ingest_delivery_atomic(
            received_at=received_at,
            raw_bytes_or_none=raw_bytes,
            sha256=body_sha,
            sig_ok=1,
            call_id=None,
            event_type=None,
            delivery_id=None,
            normalized_json=None,
            outcome="quarantined",
            anomaly_kind="quarantined",
            anomaly_detail=f"json_parse_error: {exc!s}",
        )
        return IngestResult(outcome="quarantined", http_status=200)

    # ------------------------------------------------------------------
    # Step 5: Extract call_id and event_type
    # ------------------------------------------------------------------
    call_id = _extract_call_id(parsed)
    event_type = _extract_event_type(parsed)
    raw_delivery_id = parsed.get("deliveryId") or parsed.get("delivery_id")
    deliv_id_str = str(raw_delivery_id) if raw_delivery_id else None

    if not call_id or not event_type:
        missing = []
        if not call_id:
            missing.append("call_id")
        if not event_type:
            missing.append("event_type")
        store.ingest_delivery_atomic(
            received_at=received_at,
            raw_bytes_or_none=raw_bytes,
            sha256=body_sha,
            sig_ok=1,
            call_id=call_id,
            event_type=event_type,
            delivery_id=deliv_id_str,
            normalized_json=None,
            outcome="quarantined",
            anomaly_kind="quarantined",
            anomaly_detail=f"missing_fields: {missing}",
        )
        logger.info("quarantined payload missing=%s", missing)
        return IngestResult(outcome="quarantined", http_status=200)

    # ------------------------------------------------------------------
    # Step 6: Normalize payload FIRST to prevent placeholder states
    # ------------------------------------------------------------------
    normalized = normalize_event(parsed, source="webhook")
    anomaly_flags = normalized.flags
    normalized_json_str = json.dumps(normalized.to_dict())

    # ------------------------------------------------------------------
    # Step 7: Atomic persist (delivery + deduplication + event insert)
    # ------------------------------------------------------------------
    _, final_outcome = store.ingest_delivery_atomic(
        received_at=received_at,
        raw_bytes_or_none=raw_bytes,
        sha256=body_sha,
        sig_ok=1,
        call_id=call_id,
        event_type=event_type,
        delivery_id=deliv_id_str,
        normalized_json=normalized_json_str,
        outcome="accepted",
        anomaly_kind="normalization_flag" if anomaly_flags else None,
        anomaly_detail=",".join(anomaly_flags) if anomaly_flags else None,
    )

    logger.info("outcome=%s call_id=%s event_type=%s", final_outcome, call_id, event_type)
    return IngestResult(
        outcome=final_outcome,
        http_status=200,
        call_id=call_id,
        event_type=event_type,
        delivery_id=deliv_id_str,
        anomalies=anomaly_flags,
    )
