#!/usr/bin/env python3
"""
WHY:
The receiver spools payloads to disk when the SQLite write fails.
Atoms does NOT retry failed deliveries, so spooled entries are the only copy.
This script replays them into the ledger, verifying signatures before inserting.

Usage:
    python scripts/replay_spool.py [--spool spool.jsonl] [--db ledger.db] [--dry-run]

Exits with code 1 if any entry fails to replay (so CI catches it).
"""

from __future__ import annotations

import argparse
import base64
import json
import sys
from pathlib import Path

# Ensure src is importable when run from the project root
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from atoms_ledger.config import LedgerConfig
from atoms_ledger.ingest import handle
from atoms_ledger.store import LedgerStore


def replay(spool_path: Path, db_path: str, dry_run: bool) -> int:
    """Replay entries from spool. Returns number of failures."""
    if not spool_path.exists():
        print(f"[replay_spool] No spool file at {spool_path} — nothing to replay.")
        return 0

    lines = spool_path.read_text(encoding="utf-8").splitlines()
    if not lines:
        print("[replay_spool] Spool file is empty.")
        return 0

    config = LedgerConfig.from_env()
    store = LedgerStore(db_path)

    ok = 0
    fail = 0

    for i, line in enumerate(lines):
        line = line.strip()
        if not line:
            continue
        try:
            entry = json.loads(line)
        except json.JSONDecodeError as exc:
            print(f"[replay_spool] Line {i + 1}: JSON parse error — {exc}")
            fail += 1
            continue

        raw_b64 = entry.get("raw_base64")
        if not raw_b64:
            print(f"[replay_spool] Line {i + 1}: missing raw_base64 — skipping")
            fail += 1
            continue

        try:
            raw_bytes = base64.b64decode(raw_b64)
        except Exception as exc:
            print(f"[replay_spool] Line {i + 1}: base64 decode error — {exc}")
            fail += 1
            continue

        headers = entry.get("headers", {})
        original_error = entry.get("error", "")
        spooled_at = entry.get("spooled_at", "?")

        print(
            f"[replay_spool] Line {i + 1}: replaying {len(raw_bytes)} bytes (spooled {spooled_at}, original error: {original_error!r})"
        )

        if dry_run:
            print("[replay_spool]   DRY RUN — skipping actual ingest")
            ok += 1
            continue

        try:
            result = handle(raw_bytes, headers, config, store)
            print(
                f"[replay_spool]   outcome={result.outcome} call_id={result.call_id} event_type={result.event_type}"
            )
            ok += 1
        except Exception as exc:
            print(f"[replay_spool]   FAIL: {exc}")
            fail += 1

    store.close()
    print(f"\n[replay_spool] Done. replayed={ok} failed={fail} total={ok + fail}")
    return fail


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay spooled Atoms webhook payloads")
    parser.add_argument(
        "--spool",
        default="spool.jsonl",
        help="Path to spool file (default: spool.jsonl)",
    )
    parser.add_argument(
        "--db",
        default=None,
        help="SQLite DB path override (defaults to LEDGER_DB_PATH env or :memory:)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse and log entries without writing to the DB",
    )
    args = parser.parse_args()

    import os

    db_path = args.db or os.getenv("LEDGER_DB_PATH", ":memory:")

    failures = replay(Path(args.spool), db_path, args.dry_run)
    sys.exit(1 if failures > 0 else 0)


if __name__ == "__main__":
    main()
