"""
WHY:
Lightweight standalone FastAPI receiver for production deployment behind reverse proxies.
Reads unparsed request bytes to guarantee HMAC validity before acknowledging.
Must never parse JSON prior to raw delivery persistence and must never log sensitive transcripts.
"""

from __future__ import annotations

import base64
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, Request, Response

from atoms_ledger.config import LedgerConfig
from atoms_ledger.ingest import handle
from atoms_ledger.store import LedgerStore

logger = logging.getLogger("atoms_receiver")
app = FastAPI(title="Atoms Webhook Receiver", version="0.1.0")

config = LedgerConfig.from_env()
store = LedgerStore(config.ledger_db_path)
spool_file = Path("spool.jsonl")


@app.get("/healthz")
async def healthz():
    return {"status": "ok", "service": "atoms_receiver"}


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
        with open(spool_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry) + "\n")

        return Response(
            content=json.dumps(
                {"error": "Storage failure; payload safely spooled", "detail": str(exc)}
            ),
            status_code=503,
            media_type="application/json",
        )
