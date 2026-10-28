"""Unit tests for standalone FastAPI webhook receiver."""

import json

from fastapi.testclient import TestClient

from atoms_ledger.config import DEMO_WEBHOOK_SECRET
from atoms_ledger.signature import compute_signature
from receiver.app import app

client = TestClient(app)


def test_receiver_healthz():
    resp = client.get("/healthz")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


def test_receiver_valid_delivery():
    payload = {
        "event": "agent_test.post-conversation",
        "id": "deliv-rec-01",
        "metadata": {
            "callId": "CALL-1729000000000-a1b2c3",
            "agentId": "agent_test",
            "eventType": "post-conversation",
            "callStatus": "completed",
            "toNumber": "+910000001111",
            "fromNumber": "+910000002222",
        },
    }
    raw = json.dumps(payload).encode("utf-8")
    sig = compute_signature(raw, DEMO_WEBHOOK_SECRET)

    resp = client.post(
        "/webhooks/atoms",
        content=raw,
        headers={"x-signature": sig, "content-type": "application/json"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["outcome"] in ("accepted", "duplicate")
    assert data["call_id"] == "CALL-1729000000000-a1b2c3"


def test_receiver_bad_signature():
    raw = b'{"status":"completed"}'
    resp = client.post(
        "/webhooks/atoms",
        content=raw,
        headers={"x-signature": "00" * 32, "content-type": "application/json"},
    )
    assert resp.status_code == 401


def test_receiver_spool_fallback(monkeypatch, tmp_path):
    """When store throws an unhandled error, raw payload is spooled and 503 is returned."""
    spool_path = tmp_path / "spool.jsonl"
    monkeypatch.setattr("receiver.app.spool_file", spool_path)

    import receiver.app as rec_mod

    def broken_handle(*args, **kwargs):
        raise RuntimeError("Simulated database failure")

    monkeypatch.setattr(rec_mod, "handle", broken_handle)

    body = b'{"important":"data"}'
    resp = client.post(
        "/webhooks/atoms",
        content=body,
        headers={"x-signature": "some-sig", "content-type": "application/json"},
    )
    assert resp.status_code == 503
    assert spool_path.exists()
    content = spool_path.read_text(encoding="utf-8")
    assert "Simulated database failure" in content
