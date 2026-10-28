"""
WHY:
Lightweight standalone FastAPI receiver for production deployment behind reverse proxies.
Reads unparsed request bytes to guarantee HMAC validity before acknowledging.
Must never parse JSON prior to raw delivery persistence and must never log sensitive transcripts.

Startup policy (fail-closed):
  - ATOMS_WEBHOOK_SECRETS must be set to a non-demo value in production.
  - LEDGER_DB_PATH must be set to a real file path (not :memory:) in production.
  Set ALLOW_INSECURE_DEMO=true to override these checks (local dev / CI only).

Spool policy:
  When the SQLite write fails (disk full, WAL contention), the raw payload is
  spooled to an absolute path set by SPOOL_FILE env var (defaults to spool.jsonl
  in the CWD). Replay it later with scripts/replay_spool.py.
  Atoms does NOT retry, so 503 means the payload is spooled — not lost.
"""

from __future__ import annotations

import base64
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, Request, Response

from atoms_ledger.config import DEMO_WEBHOOK_SECRET, LedgerConfig
from atoms_ledger.ingest import handle
from atoms_ledger.store import LedgerStore

logger = logging.getLogger("atoms_receiver")

# ---------------------------------------------------------------------------
# Startup validation (fail-closed by default)
# ---------------------------------------------------------------------------

_ALLOW_INSECURE_DEMO = os.getenv("ALLOW_INSECURE_DEMO", "").lower() in ("1", "true", "yes")

config = LedgerConfig.from_env()

if not _ALLOW_INSECURE_DEMO:
    # Refuse to start without a real webhook secret
    if not os.getenv("ATOMS_WEBHOOK_SECRETS"):
        logger.critical(
            "STARTUP ABORTED: ATOMS_WEBHOOK_SECRETS is not set. "
            "Set it to your real webhook signing secret(s). "
            "To run in demo mode (insecure), set ALLOW_INSECURE_DEMO=true."
        )
        sys.exit(1)

    # Check the secret is not the demo value
    if any(s == DEMO_WEBHOOK_SECRET for s in config.webhook_secrets):
        logger.critical(
            "STARTUP ABORTED: ATOMS_WEBHOOK_SECRETS contains the demo placeholder value. "
            "Replace it with your real webhook signing secret. "
            "To run in demo mode (insecure), set ALLOW_INSECURE_DEMO=true."
        )
        sys.exit(1)

    # Refuse to start without a real database path
    if config.ledger_db_path == ":memory:" or not os.getenv("LEDGER_DB_PATH"):
        logger.critical(
            "STARTUP ABORTED: LEDGER_DB_PATH is not set or defaults to :memory:. "
            "Set it to a real file path for durable persistence. "
            "To run in demo mode (insecure), set ALLOW_INSECURE_DEMO=true."
        )
        sys.exit(1)

store = LedgerStore(config.ledger_db_path)

# Spool file: use absolute path to avoid CWD ambiguity
_default_spool = str(Path(__file__).parent.parent / "spool.jsonl")
spool_file = Path(os.getenv("SPOOL_FILE", _default_spool))

# ---------------------------------------------------------------------------
# FastAPI application
# ---------------------------------------------------------------------------

app = FastAPI(title="Atoms Webhook Receiver", version="0.2.0")


@app.get("/healthz")
async def healthz():
    return {"status": "ok", "service": "atoms_receiver", "db": config.ledger_db_path}


@app.post("/webhooks/atoms")
async def receive_webhook(request: Request):
    raw_body = await request.body()
    headers = dict(request.headers)

    try:
        result = handle(raw_body, headers, config, store)
        return Response(
            content=json.dumps(
                {
                    "outcome": result.outcome,
                    "call_id": result.call_id,
                    "event_type": result.event_type,
                    "anomalies": result.anomalies,
                }
            ),
            status_code=result.http_status,
            media_type="application/json",
        )
    except Exception as exc:
        logger.critical("Receiver database ingestion error; spooling raw delivery: %s", exc)
        entry = {
            "spooled_at": datetime.now(timezone.utc).isoformat(),
            "raw_base64": base64.b64encode(raw_body).decode("ascii"),
            "headers": {
                k: v for k, v in headers.items() if k.lower() in ("x-signature", "content-type")
            },
            "error": str(exc),
        }
        spool_file.parent.mkdir(parents=True, exist_ok=True)
        with open(spool_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry) + "\n")

        return Response(
            content=json.dumps(
                {"error": "Storage failure; payload spooled for replay", "detail": str(exc)}
            ),
            status_code=503,
            media_type="application/json",
        )
