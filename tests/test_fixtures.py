"""Tests for fixtures/generate.py — seeded deterministic payload generator."""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import sys

# Allow importing from fixtures/
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "fixtures"))

from generate import (  # noqa: E402
    SYNTHETIC_PHONE_PREFIX,
    generate_call_set,
)

from atoms_ledger.config import DEMO_WEBHOOK_SECRET  # noqa: E402

# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------


def test_same_seed_identical_output() -> None:
    """Same seed must produce byte-for-byte identical output."""
    scenarios_a = generate_call_set(n_calls=50, seed=42)
    scenarios_b = generate_call_set(n_calls=50, seed=42)
    assert len(scenarios_a) == len(scenarios_b)
    for a, b in zip(scenarios_a, scenarios_b):
        assert a.call_id == b.call_id
        assert a.faults_applied == b.faults_applied
        assert len(a.events) == len(b.events)
        for ea, eb in zip(a.events, b.events):
            assert ea["body"] == eb["body"], "Same seed must produce identical bytes"
            assert ea["headers"] == eb["headers"]


def test_different_seed_different_output() -> None:
    """Different seeds must produce different call_ids."""
    scenarios_a = generate_call_set(n_calls=20, seed=1)
    scenarios_b = generate_call_set(n_calls=20, seed=2)
    ids_a = {s.call_id for s in scenarios_a}
    ids_b = {s.call_id for s in scenarios_b}
    assert ids_a != ids_b


# ---------------------------------------------------------------------------
# Counts
# ---------------------------------------------------------------------------


def test_scenario_count_equals_n_calls() -> None:
    n = 100
    scenarios = generate_call_set(n_calls=n, seed=7)
    assert len(scenarios) == n


# ---------------------------------------------------------------------------
# Phone numbers
# ---------------------------------------------------------------------------


def test_all_phone_numbers_use_synthetic_prefix() -> None:
    """Every generated phone number must use the +91000000 prefix (SYNTHETIC)."""
    scenarios = generate_call_set(n_calls=200, seed=7)
    for scenario in scenarios:
        for ev in scenario.events:
            try:
                payload = json.loads(ev["body"])
            except Exception:
                continue  # malformed/garbage events — ok
            for phone_field in ("from", "toPhone", "to", "from_"):
                phone = payload.get(phone_field)
                if phone:
                    assert phone.startswith(SYNTHETIC_PHONE_PREFIX), (
                        f"Phone {phone!r} must start with {SYNTHETIC_PHONE_PREFIX!r}"
                    )


# ---------------------------------------------------------------------------
# Ground truth consistency
# ---------------------------------------------------------------------------


def test_ground_truth_consistent_with_events() -> None:
    """If post_sent=True, at least one event with label 'post' must exist (for relevant faults)."""
    scenarios = generate_call_set(n_calls=200, seed=7)
    for scenario in scenarios:
        fault = scenario.faults_applied[0] if scenario.faults_applied else "clean"
        labels = [ev["label"] for ev in scenario.events]

        if fault == "dropped_post":
            assert not scenario.ground_truth["post_sent"]
            assert "post" not in labels

        if fault == "dropped_analytics":
            assert not scenario.ground_truth["analytics_sent"]
            assert "analytics" not in labels

        if fault == "dropped_pre":
            assert not scenario.ground_truth["pre_sent"]
            assert "pre" not in labels

        if fault == "outage_window":
            assert not scenario.ground_truth["post_sent"]
            assert not scenario.ground_truth["analytics_sent"]
            assert len(scenario.events) == 0


# ---------------------------------------------------------------------------
# Signature validity for clean events
# ---------------------------------------------------------------------------


def test_clean_events_have_valid_signatures() -> None:
    """All events in clean scenarios must have valid HMAC-SHA256 signatures."""
    scenarios = generate_call_set(n_calls=50, seed=7)
    clean = [s for s in scenarios if s.faults_applied == ["clean"]]
    assert len(clean) > 0, "No clean scenarios in 50 calls (seed=7)"

    for scenario in clean:
        for ev in scenario.events:
            body = ev["body"]
            sig_header = ev["headers"].get("x-signature", "")
            expected = hmac.new(DEMO_WEBHOOK_SECRET.encode(), body, hashlib.sha256).hexdigest()
            assert sig_header == expected, f"Signature mismatch in {scenario.call_id}"


# ---------------------------------------------------------------------------
# Fault type coverage
# ---------------------------------------------------------------------------


def test_fault_types_represented() -> None:
    """With enough calls and seed=7, common fault types should appear."""
    scenarios = generate_call_set(n_calls=500, seed=7)
    found_faults = {s.faults_applied[0] for s in scenarios if s.faults_applied}
    # At minimum the high-probability faults should appear
    for expected in ["clean", "dropped_post", "dropped_analytics", "out_of_order"]:
        assert expected in found_faults, f"Fault {expected!r} not seen in 500 calls"
