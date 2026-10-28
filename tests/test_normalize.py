"""Unit tests and fuzz testing for inbound payload normalization."""

import random
from datetime import datetime, timezone

from atoms_ledger.normalize import normalize_event


def test_phone_number_mappings():
    # pre-conversation style: toPhone, fromPhone
    payload_pre = {
        "metadata": {
            "callId": "CALL-1729000000000-a1b2c3",
            "toPhone": "+910000001111",
            "fromPhone": "+910000002222",
        }
    }
    event_pre = normalize_event(payload_pre)
    assert event_pre.to_number == "+910000001111"
    assert event_pre.from_number == "+910000002222"

    # post-conversation style: toNumber, fromNumber
    payload_post = {
        "metadata": {
            "callId": "CALL-1729000000000-a1b2c3",
            "toNumber": "+910000003333",
            "fromNumber": "+910000004444",
        }
    }
    event_post = normalize_event(payload_post)
    assert event_post.to_number == "+910000003333"
    assert event_post.from_number == "+910000004444"


def test_status_normalization_and_variants():
    # Kebab-case from webhooks
    assert (
        normalize_event(
            {"metadata": {"callId": "CALL-1729000000000-a1b2c3", "callStatus": "no-answer"}}
        ).status
        == "no_answer"
    )

    # Snake-case from REST
    assert (
        normalize_event(
            {"metadata": {"callId": "CALL-1729000000000-a1b2c3", "callStatus": "no_answer"}}
        ).status
        == "no_answer"
    )

    # Concatenated or uppercase
    assert (
        normalize_event(
            {"metadata": {"callId": "CALL-1729000000000-a1b2c3", "callStatus": "NoAnswer"}}
        ).status
        == "no_answer"
    )

    # Known statuses
    for s in ["completed", "busy", "failed", "canceled", "cancelled", "in_progress"]:
        expected = "canceled" if s == "cancelled" else s
        assert (
            normalize_event(
                {"metadata": {"callId": "CALL-1729000000000-a1b2c3", "callStatus": s}}
            ).status
            == expected
        )

    # Unknown status
    ev_unknown = normalize_event(
        {"metadata": {"callId": "CALL-1729000000000-a1b2c3", "callStatus": "operator_intercept"}}
    )
    assert ev_unknown.status == "operator_intercept"
    assert "status_unknown" in ev_unknown.flags


def test_direction_normalization():
    # camelCase conversationType
    ev1 = normalize_event(
        {
            "metadata": {
                "callId": "CALL-1729000000000-a1b2c3",
                "conversationType": "telephonyOutbound",
            }
        }
    )
    assert ev1.direction == "outbound"

    # snake_case callDirection
    ev2 = normalize_event(
        {"metadata": {"callId": "CALL-1729000000000-a1b2c3", "callDirection": "telephony_inbound"}}
    )
    assert ev2.direction == "inbound"

    # webcall
    ev3 = normalize_event({"metadata": {"callId": "CALL-1729000000000-a1b2c3", "type": "webcall"}})
    assert ev3.direction == "chat"

    # Absent direction
    ev4 = normalize_event({"metadata": {"callId": "CALL-1729000000000-a1b2c3"}})
    assert ev4.direction == "unknown"


def test_timestamp_parsing():
    # ISO with Z
    ev1 = normalize_event(
        {"metadata": {"callId": "CALL-1729000000000-a1b2c3", "answeredAt": "2026-10-28T10:00:00Z"}}
    )
    assert ev1.answered_at == datetime(2026, 10, 28, 10, 0, 0, tzinfo=timezone.utc)
    assert "answered_at_bad_timestamp" not in ev1.flags

    # Epoch milliseconds
    ev2 = normalize_event(
        {"metadata": {"callId": "CALL-1729000000000-a1b2c3", "endedAt": 1729000000000}}
    )
    assert ev2.ended_at is not None
    assert "ended_at_bad_timestamp" not in ev2.flags

    # Bad timestamp: must not raise, sets None and adds flag
    ev3 = normalize_event(
        {"metadata": {"callId": "CALL-1729000000000-a1b2c3", "answeredAt": "invalid-time"}}
    )
    assert ev3.answered_at is None
    assert "answered_at_bad_timestamp" in ev3.flags


def test_duration_parsing():
    # int duration
    ev1 = normalize_event({"metadata": {"callId": "CALL-1729000000000-a1b2c3", "callDuration": 42}})
    assert ev1.duration_seconds == 42.0

    # float duration
    ev2 = normalize_event(
        {"metadata": {"callId": "CALL-1729000000000-a1b2c3", "callDuration": "15.5"}}
    )
    assert ev2.duration_seconds == 15.5

    # negative duration
    ev3 = normalize_event({"metadata": {"callId": "CALL-1729000000000-a1b2c3", "callDuration": -5}})
    assert ev3.duration_seconds is None
    assert "duration_negative" in ev3.flags


def test_variables_preserved_verbatim():
    dynamic_vars = {
        "customer_id": 9999,
        "debt_amount": "₹45,000",
        "custom_nested": {"tier": "gold", "tags": ["high_priority"]},
    }
    ev = normalize_event(
        {"metadata": {"callId": "CALL-1729000000000-a1b2c3", "variables": dynamic_vars}}
    )
    assert ev.variables == dynamic_vars
    assert ev.variables["custom_nested"]["tier"] == "gold"


def test_transcript_normalization():
    turns = [
        {"role": "agent", "content": "Hello, how can I help you?", "timestamp": 1.2},
        {"role": "user", "message": "I want to settle my account.", "timestamp": 3.4},
        {"role": "supervisor_bot", "text": "Monitoring call", "timestamp": 3.5},
    ]
    ev = normalize_event({"metadata": {"callId": "CALL-1729000000000-a1b2c3", "transcript": turns}})
    assert len(ev.transcript) == 3
    assert ev.transcript[0]["content"] == "Hello, how can I help you?"
    assert ev.transcript[1]["content"] == "I want to settle my account."
    assert "transcript_unknown_role" in ev.flags


def test_call_id_format_regex_flag():
    # Valid callId format: CALL-<13-digit-timestamp>-<6-hex>
    ev_valid = normalize_event({"metadata": {"callId": "CALL-1729000000000-a1b2c3"}})
    assert "callid_format" not in ev_valid.flags
    assert "callid_missing" not in ev_valid.flags

    # Invalid callId format: still ingested, flagged
    ev_bad_format = normalize_event({"metadata": {"callId": "call-invalid-format"}})
    assert ev_bad_format.call_id == "call-invalid-format"
    assert "callid_format" in ev_bad_format.flags

    # Missing callId
    ev_missing = normalize_event({"metadata": {}})
    assert ev_missing.call_id == ""
    assert "callid_missing" in ev_missing.flags


def test_fuzz_random_mutations_never_raises():
    """Fuzz 500 seeded random mutations of a valid envelope: must never raise."""
    random.seed(42)
    base_envelope = {
        "url": "https://api.smallest.ai/webhook",
        "description": "Debt Collection Call Event",
        "event": "agent_xyz.post-conversation",
        "id": "deliv-12345",
        "metadata": {
            "agentId": "agent_xyz",
            "eventType": "post-conversation",
            "conversationType": "telephonyOutbound",
            "callId": "CALL-1729000000000-abcdef",
            "toNumber": "+910000001234",
            "fromNumber": "+910000005678",
            "callStatus": "completed",
            "callDuration": 78.5,
            "answeredAt": "2026-10-28T10:00:00Z",
            "endedAt": "2026-10-28T10:01:18Z",
            "transcript": [
                {"role": "agent", "content": "Hello", "timestamp": 1.0},
                {"role": "user", "content": "Hi", "timestamp": 2.0},
            ],
            "variables": {"account": "A123", "balance": 500},
            "recordingUrl": "https://storage.smallest.ai/rec/123.wav",
        },
    }

    mutation_types = [
        "corrupt_type",
        "delete_key",
        "null_val",
        "huge_string",
        "nested_dict",
        "non_ascii",
        "empty_list",
    ]

    for _ in range(500):
        # Deep copy
        payload = {
            "metadata": dict(base_envelope["metadata"]),
            "event": base_envelope["event"],
            "id": base_envelope["id"],
        }
        mutation = random.choice(mutation_types)
        key = random.choice(list(payload["metadata"].keys()))

        if mutation == "corrupt_type":
            payload["metadata"][key] = random.choice([True, 12345, [1, 2, 3], {"a": 1}])
        elif mutation == "delete_key":
            del payload["metadata"][key]
        elif mutation == "null_val":
            payload["metadata"][key] = None
        elif mutation == "huge_string":
            payload["metadata"][key] = "A" * 10000
        elif mutation == "nested_dict":
            payload["metadata"][key] = {"deep": {"level": {"data": None}}}
        elif mutation == "non_ascii":
            payload["metadata"][key] = "संस्कृतम् \U0001f916"
        elif mutation == "empty_list":
            payload["metadata"][key] = []

        # Must never raise an exception!
        res = normalize_event(payload)
        assert isinstance(res.flags, list)
        assert isinstance(res.transcript, list)
