"""
WHY:
Persists raw delivery bytes, deduplicated canonical events, anomalies, and backfill
state in SQLite. Exists because Atoms does not retry; raw bytes must be committed
before acknowledgement is returned. Must never lose authenticated bytes and must
never store unauthenticated body bytes.
"""

from __future__ import annotations

import sqlite3
import threading
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS deliveries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    received_at TEXT NOT NULL,
    raw BLOB,  -- only stored when sig_ok=1
    sha256 TEXT NOT NULL,
    sig_ok INTEGER NOT NULL DEFAULT 0,
    outcome TEXT,
    call_id TEXT,
    event_type TEXT,
    delivery_id TEXT,
    note TEXT
);

CREATE TABLE IF NOT EXISTS events (
    call_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    delivery_row INTEGER NOT NULL,
    payload_sha256 TEXT NOT NULL,
    normalized_json TEXT NOT NULL,
    received_at TEXT NOT NULL,
    PRIMARY KEY (call_id, event_type)
);

CREATE TABLE IF NOT EXISTS anomalies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    call_id TEXT,
    kind TEXT NOT NULL,
    detail TEXT,
    at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS backfill (
    call_id TEXT PRIMARY KEY,
    state TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    last_attempt_at TEXT,
    recovered_json TEXT
);
"""


def _now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def _row_to_dict(cursor: sqlite3.Cursor, row: sqlite3.Row) -> Dict[str, Any]:
    return {col[0]: row[idx] for idx, col in enumerate(cursor.description)}


class LedgerStore:
    """Thread-safe SQLite-backed persistence layer for the Atoms Call Ledger."""

    def __init__(self, db_path: str = ":memory:") -> None:
        self._db_path = db_path
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.Lock()
        self._create_schema()

    def _create_schema(self) -> None:
        with self._lock:
            self._conn.executescript(SCHEMA_SQL)
            self._conn.commit()

    # ------------------------------------------------------------------
    # Deliveries
    # ------------------------------------------------------------------

    def insert_delivery(
        self,
        received_at: str,
        raw_bytes_or_none: Optional[bytes],
        sha256: str,
        sig_ok: int,
        outcome: Optional[str],
        call_id: Optional[str],
        event_type: Optional[str],
        delivery_id: Optional[str],
        note: Optional[str],
    ) -> int:
        """Insert a delivery row. Returns the new rowid."""
        # Raw bytes must only be stored for authenticated deliveries (sig_ok=1).
        raw = raw_bytes_or_none if sig_ok == 1 else None
        with self._lock:
            cur = self._conn.execute(
                """INSERT INTO deliveries
                   (received_at, raw, sha256, sig_ok, outcome, call_id, event_type, delivery_id, note)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (received_at, raw, sha256, sig_ok, outcome, call_id, event_type, delivery_id, note),
            )
            self._conn.commit()
            return cur.lastrowid  # type: ignore[return-value]

    def get_delivery(self, delivery_row_id: int) -> Optional[Dict[str, Any]]:
        cur = self._conn.execute("SELECT * FROM deliveries WHERE id = ?", (delivery_row_id,))
        row = cur.fetchone()
        if row is None:
            return None
        return dict(row)

    def update_delivery_outcome(self, delivery_row_id: int, outcome: str) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE deliveries SET outcome = ? WHERE id = ?",
                (outcome, delivery_row_id),
            )
            self._conn.commit()

    def get_deliveries_for_call(self, call_id: str) -> List[Dict[str, Any]]:
        cur = self._conn.execute(
            "SELECT * FROM deliveries WHERE call_id = ? ORDER BY id ASC",
            (call_id,),
        )
        return [dict(row) for row in cur.fetchall()]

    # ------------------------------------------------------------------
    # Events
    # ------------------------------------------------------------------

    def upsert_event(
        self,
        call_id: str,
        event_type: str,
        delivery_row: int,
        payload_sha256: str,
        normalized_json: str,
        received_at: str,
    ) -> str:
        """
        Upsert an event by (call_id, event_type) PK.

        Returns:
            "inserted"  — new event stored
            "duplicate" — same sha256 already stored; idempotent, ignored
            "conflict"  — different sha256 for same event; anomaly recorded, not overwritten
        """
        with self._lock:
            cur = self._conn.execute(
                "SELECT payload_sha256 FROM events WHERE call_id = ? AND event_type = ?",
                (call_id, event_type),
            )
            existing = cur.fetchone()

            if existing is None:
                self._conn.execute(
                    """INSERT INTO events
                       (call_id, event_type, delivery_row, payload_sha256, normalized_json, received_at)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    (
                        call_id,
                        event_type,
                        delivery_row,
                        payload_sha256,
                        normalized_json,
                        received_at,
                    ),
                )
                self._conn.commit()
                return "inserted"

            stored_sha = existing[0]
            if stored_sha == payload_sha256:
                return "duplicate"

            # Different sha256 — conflict. Record anomaly but do NOT overwrite.
            self._conn.execute(
                """INSERT INTO anomalies (call_id, kind, detail, at)
                   VALUES (?, ?, ?, ?)""",
                (
                    call_id,
                    "duplicate_conflict",
                    f"event_type={event_type} stored_sha={stored_sha[:8]} new_sha={payload_sha256[:8]}",
                    _now_iso(),
                ),
            )
            self._conn.commit()
            return "conflict"

    def get_event(self, call_id: str, event_type: str) -> Optional[Dict[str, Any]]:
        cur = self._conn.execute(
            "SELECT * FROM events WHERE call_id = ? AND event_type = ?",
            (call_id, event_type),
        )
        row = cur.fetchone()
        return dict(row) if row else None

    def get_all_events_for_call(self, call_id: str) -> List[Dict[str, Any]]:
        cur = self._conn.execute(
            "SELECT * FROM events WHERE call_id = ? ORDER BY received_at ASC",
            (call_id,),
        )
        return [dict(row) for row in cur.fetchall()]

    # ------------------------------------------------------------------
    # Anomalies
    # ------------------------------------------------------------------

    def insert_anomaly(
        self,
        call_id: Optional[str],
        kind: str,
        detail: Optional[str],
        at: str,
    ) -> int:
        with self._lock:
            cur = self._conn.execute(
                "INSERT INTO anomalies (call_id, kind, detail, at) VALUES (?, ?, ?, ?)",
                (call_id, kind, detail, at),
            )
            self._conn.commit()
            return cur.lastrowid  # type: ignore[return-value]

    def get_anomalies_for_call(self, call_id: str) -> List[Dict[str, Any]]:
        cur = self._conn.execute(
            "SELECT * FROM anomalies WHERE call_id = ? ORDER BY id ASC",
            (call_id,),
        )
        return [dict(row) for row in cur.fetchall()]

    # ------------------------------------------------------------------
    # Backfill
    # ------------------------------------------------------------------

    def upsert_backfill(
        self,
        call_id: str,
        state: str,
        attempts: int,
        last_error: Optional[str],
        last_attempt_at: Optional[str],
        recovered_json: Optional[str],
    ) -> None:
        with self._lock:
            self._conn.execute(
                """INSERT INTO backfill
                   (call_id, state, attempts, last_error, last_attempt_at, recovered_json)
                   VALUES (?, ?, ?, ?, ?, ?)
                   ON CONFLICT(call_id) DO UPDATE SET
                       state = excluded.state,
                       attempts = excluded.attempts,
                       last_error = excluded.last_error,
                       last_attempt_at = excluded.last_attempt_at,
                       recovered_json = excluded.recovered_json""",
                (call_id, state, attempts, last_error, last_attempt_at, recovered_json),
            )
            self._conn.commit()

    def get_backfill(self, call_id: str) -> Optional[Dict[str, Any]]:
        cur = self._conn.execute("SELECT * FROM backfill WHERE call_id = ?", (call_id,))
        row = cur.fetchone()
        return dict(row) if row else None

    # ------------------------------------------------------------------
    # Cross-table utilities
    # ------------------------------------------------------------------

    def list_all_calls(self) -> List[str]:
        """Return distinct call_ids across deliveries + events tables."""
        cur = self._conn.execute(
            """SELECT DISTINCT call_id FROM (
                SELECT call_id FROM deliveries WHERE call_id IS NOT NULL
                UNION
                SELECT call_id FROM events WHERE call_id IS NOT NULL
            ) ORDER BY call_id"""
        )
        return [row[0] for row in cur.fetchall()]

    def close(self) -> None:
        self._conn.close()
