"""Integration smoke tests for FastAPI and Vercel entrypoint."""

import json

from fastapi.testclient import TestClient

from app.main import app
from atoms_ledger.config import DEMO_WEBHOOK_SECRET
from atoms_ledger.signature import compute_signature

client = TestClient(app)


def test_healthz():
    resp = client.get("/healthz")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


def test_index_page():
    resp = client.get("/")
    assert resp.status_code == 200
    assert "Atoms Call Ledger" in resp.text
    assert "DEMO MODE" in resp.text


def test_compute_signature_api():
    body = '{"test": 123}'
    resp = client.post("/api/compute-signature", json={"secret": "my-secret", "body": body})
    assert resp.status_code == 200
    sig = resp.json()["signature"]
    assert len(sig) == 64


def test_reserialize_payload_demonstrates_raw_byte_mismatch():
    body = '{"b": 2, "a": 1}'
    resp = client.post(
        "/api/reserialize-payload",
        json={"secret": DEMO_WEBHOOK_SECRET, "body": body, "signature": ""},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["verified"] is False
    assert "proving why Atoms requires signatures calculated over raw bytes" in data["message"]


def test_webhook_receiver_valid_and_invalid():
    payload = {
        "event": "agent.post-conversation",
        "id": "deliv-smoke-01",
        "metadata": {
            "callId": "CALL-1729000000000-a1b2c3",
            "agentId": "agent-test",
            "eventType": "post-conversation",
            "callStatus": "completed",
            "toNumber": "+910000001234",
            "fromNumber": "+910000005678",
        },
    }
    raw = json.dumps(payload).encode("utf-8")
    valid_sig = compute_signature(raw, DEMO_WEBHOOK_SECRET)

    # Valid delivery
    resp = client.post(
        "/webhooks/atoms",
        content=raw,
        headers={"x-signature": valid_sig, "content-type": "application/json"},
    )
    assert resp.status_code == 200
    assert resp.json()["outcome"] in ("accepted", "duplicate")

    # Invalid signature
    bad_resp = client.post(
        "/webhooks/atoms",
        content=raw,
        headers={"x-signature": "bad" * 16, "content-type": "application/json"},
    )
    assert bad_resp.status_code == 401
