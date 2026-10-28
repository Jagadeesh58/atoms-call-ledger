"""
WHY:
Generates seeded synthetic Atoms webhook payloads and fault injectors for
evaluation. SYNTHETIC: not derived from real customer data. Phone numbers use
the obviously fake +91000000xxxx form. Must be fully deterministic given a seed.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import random
import string
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from atoms_ledger.config import DEMO_WEBHOOK_SECRET

BASE_EPOCH = 1729000000.0  # Fixed base epoch for reproducibility

# Synthetic phone prefix — ALWAYS fake (SYNTHETIC)
SYNTHETIC_PHONE_PREFIX = "+91000000"

FAULT_TYPES = [
    "clean",
    "duplicate_identical",
    "duplicate_conflict",
    "out_of_order",
    "dropped_post",
    "dropped_analytics",
    "dropped_pre",
    "outage_window",
    "tampered_body",
    "wrong_secret",
    "missing_signature",
    "schema_drift",
    "authenticated_malformed",
    "unicode_content",
    "no_answer_status",
    "large_transcript",
]

DEFAULT_FAULT_MIX: Dict[str, float] = {
    "clean": 0.40,
    "duplicate_identical": 0.06,
    "duplicate_conflict": 0.04,
    "out_of_order": 0.06,
    "dropped_post": 0.06,
    "dropped_analytics": 0.06,
    "dropped_pre": 0.04,
    "outage_window": 0.03,
    "tampered_body": 0.04,
    "wrong_secret": 0.03,
    "missing_signature": 0.03,
    "schema_drift": 0.04,
    "authenticated_malformed": 0.03,
    "unicode_content": 0.03,
    "no_answer_status": 0.03,
    "large_transcript": 0.02,
}


def _sign(body: bytes, secret: str = DEMO_WEBHOOK_SECRET) -> str:
    return hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()


def _fake_call_id(rng: random.Random, epoch: float) -> str:
    epoch_ms = int(epoch * 1000)
    suffix = "".join(rng.choices(string.hexdigits[:16], k=6)).lower()
    return f"CALL-{epoch_ms}-{suffix}"


def _fake_phone(rng: random.Random) -> str:
    """SYNTHETIC phone number using +91000000xxxx prefix."""
    suffix = f"{rng.randint(0, 9999):04d}"
    return f"{SYNTHETIC_PHONE_PREFIX}{suffix}"


def _fake_agent_id(rng: random.Random) -> str:
    suffix = "".join(rng.choices(string.ascii_lowercase, k=6))
    return f"agent-{suffix}"


def _build_pre_event(call_id: str, agent_id: str, from_num: str, to_num: str, ts: float) -> dict:
    return {
        "callId": call_id,
        "eventType": "pre-conversation",
        "agentId": agent_id,
        "status": "in_progress",
        "from": from_num,
        "toPhone": to_num,
        "answeredAt": ts,
        "conversationType": "telephonyOutbound",
    }


def _build_post_event(
    call_id: str,
    agent_id: str,
    from_num: str,
    to_num: str,
    ts: float,
    status: str = "completed",
    duration: float = 90.0,
    transcript: Optional[list] = None,
) -> dict:
    return {
        "callId": call_id,
        "eventType": "post-conversation",
        "agentId": agent_id,
        "status": status,
        "from": from_num,
        "toPhone": to_num,
        "answeredAt": ts,
        "endedAt": ts + duration,
        "callDuration": duration,
        "conversationType": "telephonyOutbound",
        "transcript": transcript or [],
    }


def _build_analytics_event(call_id: str, agent_id: str, ts: float) -> dict:
    return {
        "callId": call_id,
        "eventType": "analytics-completed",
        "agentId": agent_id,
        "status": "completed",
        "analytics": {
            "sentiment": "positive",
            "summary": "SYNTHETIC: demo analytics",
        },
        "timestamp": ts,
    }


def _make_signed_delivery(payload: dict, secret: str = DEMO_WEBHOOK_SECRET) -> tuple[bytes, dict]:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    headers = {"x-signature": _sign(body, secret)}
    return body, headers


@dataclass
class CallScenario:
    """Represents a synthetic call scenario with fault injection."""

    call_id: str
    events: List[Dict[str, Any]]  # list of {"body": bytes, "headers": dict, "label": str}
    ground_truth: Dict[str, Any]  # which events were "sent", which were "lost"
    faults_applied: List[str]


def generate_call_set(
    n_calls: int,
    seed: int,
    fault_mix: Optional[Dict[str, float]] = None,
    base_epoch: float = BASE_EPOCH,
) -> List[CallScenario]:
    """
    Generate n_calls synthetic call scenarios with seeded RNG.

    SYNTHETIC: All data is generated from seed. Not derived from any customer data.
    Phone numbers use +91000000xxxx prefix.
    Must be byte-for-byte deterministic given the same seed.
    """
    rng = random.Random(seed)
    mix = fault_mix if fault_mix is not None else DEFAULT_FAULT_MIX

    # Normalize mix to cumulative weights
    fault_names = sorted(mix.keys())
    fault_weights = [mix[k] for k in fault_names]

    scenarios: List[CallScenario] = []
    for i in range(n_calls):
        fault = rng.choices(fault_names, weights=fault_weights, k=1)[0]
        ts = base_epoch + i * 120.0  # 2-minute spacing

        call_id = _fake_call_id(rng, ts)
        agent_id = _fake_agent_id(rng)
        from_num = _fake_phone(rng)
        to_num = _fake_phone(rng)

        scenario = _generate_scenario(rng, call_id, agent_id, from_num, to_num, ts, fault)
        scenarios.append(scenario)

    return scenarios


def _generate_scenario(
    rng: random.Random,
    call_id: str,
    agent_id: str,
    from_num: str,
    to_num: str,
    ts: float,
    fault: str,
) -> CallScenario:
    """Generate a single scenario with the specified fault type."""

    pre = _build_pre_event(call_id, agent_id, from_num, to_num, ts)
    post = _build_post_event(call_id, agent_id, from_num, to_num, ts)
    analytics = _build_analytics_event(call_id, agent_id, ts + 95.0)

    events: List[Dict[str, Any]] = []
    ground_truth: Dict[str, Any] = {
        "pre_sent": True,
        "post_sent": True,
        "analytics_sent": True,
        "pre_lost": False,
        "post_lost": False,
        "analytics_lost": False,
    }
    faults_applied = [fault]

    if fault == "clean":
        for label, payload in [("pre", pre), ("post", post), ("analytics", analytics)]:
            body, headers = _make_signed_delivery(payload)
            events.append({"body": body, "headers": headers, "label": label})

    elif fault == "duplicate_identical":
        for label, payload in [("pre", pre), ("post", post), ("analytics", analytics)]:
            body, headers = _make_signed_delivery(payload)
            events.append({"body": body, "headers": headers, "label": label})
        # Duplicate post (identical)
        body, headers = _make_signed_delivery(post)
        events.append({"body": body, "headers": headers, "label": "post_dup"})

    elif fault == "duplicate_conflict":
        for label, payload in [("pre", pre), ("post", post), ("analytics", analytics)]:
            body, headers = _make_signed_delivery(payload)
            events.append({"body": body, "headers": headers, "label": label})
        # Conflicting post (different body — add extra field)
        conflict_post = dict(post, extra_field="conflict")
        body, headers = _make_signed_delivery(conflict_post)
        events.append({"body": body, "headers": headers, "label": "post_conflict"})

    elif fault == "out_of_order":
        # Random permutation of all 3 events
        deliveries = [("pre", pre), ("post", post), ("analytics", analytics)]
        rng.shuffle(deliveries)
        for label, payload in deliveries:
            body, headers = _make_signed_delivery(payload)
            events.append({"body": body, "headers": headers, "label": label})

    elif fault == "dropped_post":
        for label, payload in [("pre", pre), ("analytics", analytics)]:
            body, headers = _make_signed_delivery(payload)
            events.append({"body": body, "headers": headers, "label": label})
        ground_truth["post_sent"] = False
        ground_truth["post_lost"] = True

    elif fault == "dropped_analytics":
        for label, payload in [("pre", pre), ("post", post)]:
            body, headers = _make_signed_delivery(payload)
            events.append({"body": body, "headers": headers, "label": label})
        ground_truth["analytics_sent"] = False
        ground_truth["analytics_lost"] = True

    elif fault == "dropped_pre":
        for label, payload in [("post", post), ("analytics", analytics)]:
            body, headers = _make_signed_delivery(payload)
            events.append({"body": body, "headers": headers, "label": label})
        ground_truth["pre_sent"] = False
        ground_truth["pre_lost"] = True

    elif fault == "outage_window":
        # All events dropped during a contiguous outage window — nothing sent
        ground_truth["pre_sent"] = False
        ground_truth["post_sent"] = False
        ground_truth["analytics_sent"] = False
        ground_truth["pre_lost"] = True
        ground_truth["post_lost"] = True
        ground_truth["analytics_lost"] = True

    elif fault == "tampered_body":
        # Valid-looking JSON but signature is bad (tampered)
        body = json.dumps(post).encode("utf-8")
        tampered = body + b"TAMPERED"
        sig = _sign(body)  # sign original, send tampered
        events.append(
            {
                "body": tampered,
                "headers": {"x-signature": sig},
                "label": "post_tampered",
            }
        )

    elif fault == "wrong_secret":
        for label, payload in [("pre", pre), ("post", post), ("analytics", analytics)]:
            body, headers = _make_signed_delivery(payload, secret="wrong-secret-SYNTHETIC")
            events.append({"body": body, "headers": headers, "label": label})

    elif fault == "missing_signature":
        for label, payload in [("pre", pre), ("post", post), ("analytics", analytics)]:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            events.append({"body": body, "headers": {}, "label": label})

    elif fault == "schema_drift":
        # Extra fields, no-answer spelling, int duration, Z-less timestamp
        drift_post = {
            "callId": call_id,
            "eventType": "post-conversation",
            "agentId": agent_id,
            "status": "no-answer",  # hyphen variant
            "from": from_num,
            "toPhone": to_num,
            "callDuration": 0,  # int instead of float
            "answeredAt": "2024-10-15T12:00:00",  # no Z
            "extra_unknown_field": "SYNTHETIC",
            "nested_extras": {"foo": "bar", "count": 42},
        }
        drift_analytics = dict(analytics, extra="drift_SYNTHETIC", version=2)
        for label, payload in [("pre", pre), ("post", drift_post), ("analytics", drift_analytics)]:
            body, headers = _make_signed_delivery(payload)
            events.append({"body": body, "headers": headers, "label": label})

    elif fault == "authenticated_malformed":
        # Valid signature over garbage/truncated JSON
        garbage = b"{\x00\x01\x02incomplete"
        body = garbage
        headers = {"x-signature": _sign(body)}
        events.append({"body": body, "headers": headers, "label": "malformed"})

    elif fault == "unicode_content":
        # Devanagari and emoji in transcript (SYNTHETIC)
        unicode_post = _build_post_event(
            call_id,
            agent_id,
            from_num,
            to_num,
            ts,
            transcript=[
                {"role": "agent", "content": "नमस्ते! 🙏 SYNTHETIC transcript"},
                {"role": "user", "content": "हाँ, ठीक है 👍"},
            ],
        )
        for label, payload in [("pre", pre), ("post", unicode_post), ("analytics", analytics)]:
            body, headers = _make_signed_delivery(payload)
            events.append({"body": body, "headers": headers, "label": label})

    elif fault == "no_answer_status":
        # no-answer status → no analytics required (UNVALIDATED HYPOTHESIS)
        no_ans_post = _build_post_event(
            call_id, agent_id, from_num, to_num, ts, status="no-answer", duration=0.0
        )
        for label, payload in [("pre", pre), ("post", no_ans_post)]:
            body, headers = _make_signed_delivery(payload)
            events.append({"body": body, "headers": headers, "label": label})
        ground_truth["analytics_sent"] = False
        ground_truth["analytics_lost"] = False  # not expected

    elif fault == "large_transcript":
        # Transcript near size limit (SYNTHETIC: generated content)
        large_turns = [
            {"role": "agent" if j % 2 == 0 else "user", "content": "A" * 512} for j in range(200)
        ]
        large_post = _build_post_event(
            call_id, agent_id, from_num, to_num, ts, transcript=large_turns
        )
        for label, payload in [("pre", pre), ("post", large_post), ("analytics", analytics)]:
            body, headers = _make_signed_delivery(payload)
            events.append({"body": body, "headers": headers, "label": label})

    else:
        # Unknown fault type — treat as clean
        for label, payload in [("pre", pre), ("post", post), ("analytics", analytics)]:
            body, headers = _make_signed_delivery(payload)
            events.append({"body": body, "headers": headers, "label": label})
        faults_applied = ["unknown"]

    return CallScenario(
        call_id=call_id,
        events=events,
        ground_truth=ground_truth,
        faults_applied=faults_applied,
    )
