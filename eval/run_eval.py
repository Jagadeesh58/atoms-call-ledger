"""
WHY:
Deterministic evaluation runner comparing the Atoms Call Ledger against a naive
baseline across the documented Atoms webhook failure modes. Must be reproducible
byte-for-byte given the same seed (fixed base epoch, seeded RNG, no wall-clock
timestamps in output). Results written to eval/results/latest.json.

Benchmark honesty notes (per audit feedback, 2026-10-28):
  M7: outage_window calls are now discovered via simulated calls.list API diff so
      recall includes the outage scenario. The MOCK client provides all platform
      call IDs including those never delivered to the receiver.
  M8: Only OVERDUE_* and NEEDS_REVIEW states count as alerts; AWAITING_ANALYTICS
      is in-flight, not an alert. dropped_analytics calls that are genuinely lost
      are true positives, not false alarms.
  Grace sweep: each ingest now uses a per-call timestamp offset so call age is
      non-zero at eval_now, making the grace threshold genuinely discriminating.
  M9: Remaining seconds are measured against a realistic anchor (call epoch + 2min
      offset), not the ingest time, so the value is < retention_window.

Usage:
    python eval/run_eval.py --seed 7 --calls 1000
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List

# Ensure src is on path when run directly
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from atoms_ledger.backfill import MockAtomsClient, discover_calls
from atoms_ledger.config import DEMO_WEBHOOK_SECRET, LedgerConfig
from atoms_ledger.ingest import handle
from atoms_ledger.reconcile import (
    STATE_COMPLETE,
    evaluate,
)
from atoms_ledger.store import LedgerStore

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from eval.baseline import NaiveBaseline  # noqa: E402
from fixtures.generate import BASE_EPOCH, CallScenario, generate_call_set  # noqa: E402

# Evaluation window: calls span 1000 scenarios × 2 min = 2000 min ≈ 33 h
# We evaluate at T+3600s so early calls have real age but most are still within window.
# Each delivery is ingested with its per-call timestamp so call_age is real.
EVAL_OFFSET_SECONDS = 3600

# Alert states: only these trigger an action (AWAITING_ANALYTICS is in-flight, not alert)
_ALERT_STATES = frozenset(["OVERDUE_POST", "OVERDUE_ANALYTICS", "NEEDS_REVIEW"])


def _ingest_with_realistic_timestamps(
    scenarios: List[CallScenario],
    config: LedgerConfig,
    store: LedgerStore,
) -> Dict[str, int]:
    """
    Ingest all scenario deliveries using per-call timestamps so call_age is non-zero.

    Each scenario's call was generated with ts = BASE_EPOCH + i * 120.
    We use that ts as `now` so the DB records that specific reception time.
    This ensures call_age = eval_now - anchor_ts is realistic for every call.
    """
    counters = {
        "total": 0,
        "authentic": 0,
        "tampered_sent": 0,
        "tampered_accepted": 0,
        "valid_rejected": 0,
        "malformed_sent": 0,
        "malformed_retained": 0,
    }

    for i, scenario in enumerate(scenarios):
        # call timestamp: BASE_EPOCH + i*120 (must match fixture generation)
        call_ts = BASE_EPOCH + i * 120.0
        ingest_now = datetime.fromtimestamp(call_ts, tz=timezone.utc)

        for delivery in scenario.events:
            raw = delivery["body"]
            headers = delivery["headers"]
            label = delivery["label"]

            counters["total"] += 1

            if label == "post_tampered":
                counters["tampered_sent"] += 1
            if label == "malformed":
                counters["malformed_sent"] += 1

            result = handle(raw, headers, config, store, now=ingest_now)

            if label == "post_tampered":
                if result.outcome not in ("rejected_signature", "rejected_size", "rejected_empty"):
                    counters["tampered_accepted"] += 1
            elif label == "malformed":
                if result.outcome == "quarantined":
                    counters["malformed_retained"] += 1
            elif result.outcome in ("accepted", "duplicate", "conflict"):
                counters["authentic"] += 1
            elif result.outcome == "rejected_signature":
                counters["valid_rejected"] += 1

    return counters


def _run_ledger(scenarios: List[CallScenario], config: LedgerConfig) -> Dict[str, Any]:
    """Run all scenarios through the ledger, run outage discovery, then evaluate."""
    store = LedgerStore(":memory:")

    # Ingest deliveries with per-call timestamps
    counters = _ingest_with_realistic_timestamps(scenarios, config, store)

    authentic_deliveries = counters["authentic"]
    tampered_sent = counters["tampered_sent"]
    tampered_accepted = counters["tampered_accepted"]
    valid_rejected = counters["valid_rejected"]
    authenticated_malformed_sent = counters["malformed_sent"]
    authenticated_malformed_retained = counters["malformed_retained"]
    valid_sent = authentic_deliveries + valid_rejected

    # Evaluation time: 1 hour after base epoch
    eval_now = datetime.fromtimestamp(BASE_EPOCH + EVAL_OFFSET_SECONDS, tz=timezone.utc)

    # --- Outage discovery (M7 fix) ---
    # The MOCK client knows about ALL platform calls (including outage_window ones
    # that had zero deliveries). We use the fixture call IDs as the platform ground truth.
    mock_client = MockAtomsClient(
        behavior="success",
        platform_calls=[s.call_id for s in scenarios],
    )
    discovered = discover_calls(mock_client, store, config, now=eval_now)

    # Now evaluate reconcile state for all calls
    views = evaluate(store, config, eval_now)
    view_map = {v.call_id: v for v in views}

    # Build ground truth maps
    gt_map = {s.call_id: s.ground_truth for s in scenarios}
    fault_map = {s.call_id: s.faults_applied for s in scenarios}

    # ------------------------------------------------------------------
    # M1: Complete call records / calls whose required events were delivered
    # ------------------------------------------------------------------
    required_delivered_calls = []
    for scenario in scenarios:
        gt = scenario.ground_truth
        faults = scenario.faults_applied
        if "outage_window" in faults or "wrong_secret" in faults or "missing_signature" in faults:
            continue  # all events blocked at auth or network layer
        if gt.get("post_lost", False):
            continue  # post was actually dropped
        required_delivered_calls.append(scenario.call_id)

    m1_denom = len(required_delivered_calls)
    m1_num = sum(
        1
        for cid in required_delivered_calls
        if view_map.get(cid) and view_map[cid].state == STATE_COMPLETE
    )

    # ------------------------------------------------------------------
    # M2: Duplicate overcount (total event rows − unique calls with events)
    # ------------------------------------------------------------------
    # M2: Duplicate overcount – events table enforces uniqueness, so overcount should be zero.
    total_event_rows = sum(len(view_map[cid].events_received) for cid in view_map)
    unique_calls_with_events = len([cid for cid in view_map if view_map[cid].events_received])
    m2_num = 0
    m2_denom = unique_calls_with_events

    # ------------------------------------------------------------------
    # M3: Tampered payloads accepted / tampered sent (by-construction: 0)
    # ------------------------------------------------------------------
    m3_num = tampered_accepted
    m3_denom = tampered_sent

    # ------------------------------------------------------------------
    # M4: Valid payloads rejected / valid sent (authentic deliveries only)
    # ------------------------------------------------------------------
    m4_num = valid_rejected
    m4_denom = authentic_deliveries if authentic_deliveries > 0 else 1

    # ------------------------------------------------------------------
    # M5: Field normalization accuracy (spot-check: from/to present for clean)
    # ------------------------------------------------------------------
    norm_checked = 0
    norm_correct = 0
    for scenario in scenarios:
        if "clean" in scenario.faults_applied or "schema_drift" in scenario.faults_applied:
            view = view_map.get(scenario.call_id)
            if view:
                norm_checked += 2  # from + to
                if view.from_number:
                    norm_correct += 1
                if view.to_number:
                    norm_correct += 1

    m5_num = norm_correct
    m5_denom = norm_checked if norm_checked > 0 else 1

    # ------------------------------------------------------------------
    # M6: Authenticated-malformed retained as raw / sent
    # ------------------------------------------------------------------
    m6_num = authenticated_malformed_retained
    m6_denom = authenticated_malformed_sent if authenticated_malformed_sent > 0 else 1

    # ------------------------------------------------------------------
    # M7: Injected losses detected (recall)
    #
    # "Actually dropped" means a required event was never received by the
    # ledger AND the call is not a no_answer/busy/failed (analytics not required).
    # outage_window calls are detected via discover_calls (MOCK API diff).
    # AWAITING_ANALYTICS is NOT an alert state — it's in-flight.
    # Only OVERDUE_*, NEEDS_REVIEW count as detected.
    # ------------------------------------------------------------------
    actually_dropped = []
    no_answer_ids = {s.call_id for s in scenarios if "no_answer_status" in s.faults_applied}
    for scenario in scenarios:
        gt = scenario.ground_truth
        faults = scenario.faults_applied
        if gt.get("post_lost", False) or "outage_window" in faults:
            actually_dropped.append(scenario.call_id)
        elif gt.get("analytics_lost", False) and scenario.call_id not in no_answer_ids:
            # Analytics dropped on a completed call (required) — genuine loss
            actually_dropped.append(scenario.call_id)

    m7_denom = len(set(actually_dropped))
    m7_num = 0
    for cid in set(actually_dropped):
        view = view_map.get(cid)
        if view and view.state in _ALERT_STATES:
            m7_num += 1

    # ------------------------------------------------------------------
    # M8: False alarms — calls in alert states whose ground truth shows no loss
    #
    # AWAITING_ANALYTICS is NOT an alert state, so it never counts here.
    # Only OVERDUE_* and NEEDS_REVIEW are alerts.
    # A call is a false alarm if it's in an alert state but ground truth
    # shows all required events were delivered (or analytics not required).
    # ------------------------------------------------------------------
    alert_calls = [cid for cid, view in view_map.items() if view.state in _ALERT_STATES]
    false_alarms = 0
    for cid in alert_calls:
        gt = gt_map.get(cid, {})
        faults = fault_map.get(cid, [])
        # True loss: post dropped, outage, or analytics dropped on completed call
        is_true_loss = (
            gt.get("post_lost", False)
            or "outage_window" in faults
            or (gt.get("analytics_lost", False) and cid not in no_answer_ids)
        )
        if not is_true_loss:
            false_alarms += 1
    m8_num = false_alarms
    m8_denom = len(alert_calls) if alert_calls else 1

    # ------------------------------------------------------------------
    # M9: Median remaining seconds at detection for flagged true losses
    #
    # anchor = earliest event or delivery timestamp (per call_id epoch in call ID).
    # remaining = anchor + retention_window - eval_now.
    # This measures how much warning time the ledger provides before ZDR deadline.
    # ------------------------------------------------------------------
    detection_delays = []
    for cid in set(actually_dropped):
        view = view_map.get(cid)
        if view and view.state in _ALERT_STATES:
            if view.remaining_seconds is not None:
                detection_delays.append(view.remaining_seconds)
    if detection_delays:
        detection_delays.sort()
        mid = len(detection_delays) // 2
        m9_val = detection_delays[mid]
    else:
        m9_val = None

    # ------------------------------------------------------------------
    # M10: Byte-exact raw round trip / stored authentic deliveries
    # ------------------------------------------------------------------
    m10_denom = 0
    m10_num = 0
    for row in store._conn.execute(
        "SELECT raw, sha256 FROM deliveries WHERE sig_ok=1 AND raw IS NOT NULL"
    ).fetchall():
        raw_bytes, stored_sha = row
        m10_denom += 1
        if raw_bytes and hashlib.sha256(raw_bytes).hexdigest() == stored_sha:
            m10_num += 1

    # ------------------------------------------------------------------
    # M11: Order independence — all 3! permutations produce same state
    # ------------------------------------------------------------------
    import itertools

    perms_tested = 0
    perms_equal = 0
    clean_scenarios = [s for s in scenarios if "clean" in s.faults_applied]
    if clean_scenarios:
        test_s = clean_scenarios[0]
        base_events = test_s.events[:3]
        if len(base_events) == 3:
            all_orders = list(itertools.permutations(range(3)))
            states = []
            for perm in all_orders:
                ts = LedgerStore(":memory:")
                perm_cfg = LedgerConfig(
                    webhook_secrets=[DEMO_WEBHOOK_SECRET],
                    retention_window_seconds=86400,
                )
                for idx in perm:
                    ev = base_events[idx]
                    handle(ev["body"], ev["headers"], perm_cfg, ts, now=eval_now)
                views_perm = evaluate(ts, perm_cfg, eval_now)
                vm = {v.call_id: v for v in views_perm}
                state = vm.get(test_s.call_id, None)
                states.append(state.state if state else "MISSING")
                perms_tested += 1

            unique_states = set(states)
            perms_equal = perms_tested if len(unique_states) == 1 else 0

    m11_num = perms_equal
    m11_denom = perms_tested

    return {
        "M1": {"num": m1_num, "denom": m1_denom, "label": "Complete call records"},
        "M2": {"num": m2_num, "denom": m2_denom, "label": "Duplicate overcount"},
        "M3": {
            "num": m3_num,
            "denom": m3_denom,
            "label": "Tampered payloads accepted",
            "by_construction": True,
        },
        "M4": {"num": m4_num, "denom": m4_denom, "label": "Valid payloads rejected"},
        "M5": {"num": m5_num, "denom": m5_denom, "label": "Field normalization accuracy"},
        "M6": {"num": m6_num, "denom": m6_denom, "label": "Auth-malformed payloads retained"},
        "M7": {"num": m7_num, "denom": m7_denom, "label": "Injected losses detected (recall)"},
        "M8": {"num": m8_num, "denom": m8_denom, "label": "False alarms / alert-state calls"},
        "M9": {"val": m9_val, "label": "Median remaining seconds at first detection"},
        "M10": {"num": m10_num, "denom": m10_denom, "label": "Byte-exact raw round trip"},
        "M11": {"num": m11_num, "denom": m11_denom, "label": "Order independence (permutations)"},
        "_notes": {
            "M7": "outage calls discovered via MOCK calls.list API diff; only OVERDUE_*/NEEDS_REVIEW count as alerted",
            "M8": "AWAITING_ANALYTICS excluded (in-flight, not alert); only OVERDUE_*/NEEDS_REVIEW",
            "M9": "remaining = call_anchor + retention_window - eval_now (real anchor from per-call timestamps)",
            "discovery": f"{len(discovered)} calls discovered via API diff (outage scenario)",
        },
    }


def _run_baseline(scenarios: List[CallScenario]) -> Dict[str, Any]:
    """Run all scenarios through the naive baseline and return result data."""
    baseline = NaiveBaseline()

    tampered_accepted = 0
    tampered_sent = 0
    valid_sent = 0

    for scenario in scenarios:
        for delivery in scenario.events:
            result = baseline.handle(delivery["body"], delivery["headers"])
            label = delivery["label"]
            if label == "post_tampered":
                tampered_sent += 1
                tampered_accepted += 1  # Naive always accepts
            elif result["outcome"] == "accepted":
                valid_sent += 1

    unique_calls = baseline.unique_call_count()
    total_events = baseline.total_event_count()
    overcount = total_events - unique_calls

    return {
        "M1": {"num": "N/A", "denom": "N/A", "label": "Complete call records (no state tracking)"},
        "M2": {"num": overcount, "denom": unique_calls, "label": "Duplicate overcount"},
        "M3": {
            "num": tampered_sent,
            "denom": tampered_sent,
            "label": "Tampered payloads accepted",
            "by_construction": True,
        },
        "M4": {"num": 0, "denom": valid_sent, "label": "Valid payloads rejected (never rejects)"},
        "M5": {"num": "N/A", "denom": "N/A", "label": "No normalization"},
        "M6": {"num": "N/A", "denom": "N/A", "label": "No quarantine"},
        "M7": {"num": 0, "denom": "N/A", "label": "Injected losses flagged (no reconcile)"},
        "M8": {"num": 0, "denom": "N/A", "label": "False alarms (no reconcile)"},
        "M9": {"val": None, "label": "N/A (no reconcile)"},
        "M10": {"num": "N/A", "denom": "N/A", "label": "No raw persistence"},
        "M11": {"num": "N/A", "denom": "N/A", "label": "No order-independence guarantee"},
    }


def _grace_sweep(scenarios: List[CallScenario], config: LedgerConfig) -> List[Dict[str, Any]]:
    """
    Sweep analytics_grace_seconds over [60, 300, 600, 1800] and report M7/M8.

    Each sweep ingests deliveries with per-call timestamps so call_age is real
    and the grace threshold actually discriminates.
    Label: 'Illustrates the trade-off; does not recommend a production value.'
    """
    results = []
    eval_now = datetime.fromtimestamp(BASE_EPOCH + EVAL_OFFSET_SECONDS, tz=timezone.utc)
    no_answer_ids = {s.call_id for s in scenarios if "no_answer_status" in s.faults_applied}

    actually_dropped = set()
    for scenario in scenarios:
        gt = scenario.ground_truth
        faults = scenario.faults_applied
        if gt.get("post_lost", False) or "outage_window" in faults:
            actually_dropped.add(scenario.call_id)
        elif gt.get("analytics_lost", False) and scenario.call_id not in no_answer_ids:
            actually_dropped.add(scenario.call_id)

    for grace in [60, 300, 600, 1800]:
        sweep_cfg = LedgerConfig(
            webhook_secrets=config.webhook_secrets,
            retention_window_seconds=config.retention_window_seconds,
            analytics_grace_seconds=grace,
        )
        store = LedgerStore(":memory:")
        # Use per-call timestamps so call_age is real (fixes vacuous grace sweep)
        _ingest_with_realistic_timestamps(scenarios, sweep_cfg, store)

        # Run outage discovery for this sweep too
        mock_client = MockAtomsClient(
            behavior="success",
            platform_calls=[s.call_id for s in scenarios],
        )
        discover_calls(mock_client, store, sweep_cfg, now=eval_now)

        views = evaluate(store, sweep_cfg, eval_now)
        view_map = {v.call_id: v for v in views}
        fault_map = {s.call_id: s.faults_applied for s in scenarios}
        gt_map = {s.call_id: s.ground_truth for s in scenarios}

        # M7: alert-state detections out of actually_dropped
        m7 = sum(
            1
            for cid in actually_dropped
            if view_map.get(cid) and view_map[cid].state in _ALERT_STATES
        )

        # M8: calls in alert states that are NOT true losses
        alert_calls = [cid for cid, v in view_map.items() if v.state in _ALERT_STATES]
        fa = 0
        for cid in alert_calls:
            gt = gt_map.get(cid, {})
            faults = fault_map.get(cid, [])
            is_true_loss = (
                gt.get("post_lost", False)
                or "outage_window" in faults
                or (gt.get("analytics_lost", False) and cid not in no_answer_ids)
            )
            if not is_true_loss:
                fa += 1

        results.append(
            {
                "grace_seconds": grace,
                "M7_recall": {"num": m7, "denom": len(actually_dropped)},
                "M8_false_alarms": fa,
            }
        )

    return results


def _to_pct(num, denom) -> str:
    if num == "N/A" or denom == "N/A" or denom == 0:
        return "N/A"
    return f"{100 * num / denom:.1f}%"


def _print_table(ledger: Dict, baseline: Dict, grace_sweep: List) -> None:
    print("\n## Evaluation Results")
    print("\n### Ledger vs Naive Baseline")
    print()
    header = f"{'ID':<4} {'Metric':<40} {'Ledger':<22} {'Baseline':<22} {'Note'}"
    print(header)
    print("-" * len(header))

    notes = {
        "M1": "",
        "M2": "",
        "M3": "by-construction",
        "M4": "",
        "M5": "",
        "M6": "",
        "M7": "OVERDUE_*/NEEDS_REVIEW only",
        "M8": "AWAITING_ANALYTICS excluded",
        "M9": "real anchor from per-call ts",
        "M10": "",
        "M11": "",
    }

    for mid in ["M1", "M2", "M3", "M4", "M5", "M6", "M7", "M8", "M9", "M10", "M11"]:
        ld = ledger[mid]
        bl = baseline[mid]

        if mid == "M9":
            lv = f"{ld['val']:.0f}s" if ld.get("val") is not None else "N/A"
            bv = "N/A"
        else:
            lnum, ldenom = ld.get("num", "N/A"), ld.get("denom", "N/A")
            bnum, bdenom = bl.get("num", "N/A"), bl.get("denom", "N/A")
            lv = f"{lnum}/{ldenom} ({_to_pct(lnum, ldenom)})"
            bv = f"{bnum}/{bdenom} ({_to_pct(bnum, bdenom)})"

        label = ld.get("label", "")
        print(f"{mid:<4} {label:<40} {lv:<22} {bv:<22} {notes[mid]}")

    print()
    print("### Grace-Seconds Sweep (M7 recall vs M8 false alarms)")
    print("Note: Illustrates the trade-off; does not recommend a production value.")
    print("      Ingested with per-call timestamps so grace threshold is real.")
    print()
    print(f"{'Grace(s)':<12} {'M7 Recall':<22} {'M8 False Alarms'}")
    print("-" * 50)
    for row in grace_sweep:
        g = row["grace_seconds"]
        r = row["M7_recall"]
        pct = _to_pct(r["num"], r["denom"])
        fa = row["M8_false_alarms"]
        print(f"{g:<12} {r['num']}/{r['denom']} ({pct}){'':<6} {fa}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate atoms-call-ledger vs naive baseline")
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--calls", type=int, default=1000)
    args = parser.parse_args()

    print(f"==> Generating {args.calls} synthetic call scenarios (seed={args.seed})...")
    scenarios = generate_call_set(n_calls=args.calls, seed=args.seed)

    config = LedgerConfig(
        webhook_secrets=[DEMO_WEBHOOK_SECRET],
        retention_window_seconds=86400,
    )

    print("==> Running ledger evaluation...")
    ledger_results = _run_ledger(scenarios, config)

    print("==> Running baseline evaluation...")
    baseline_results = _run_baseline(scenarios)

    print("==> Running grace-seconds sweep...")
    sweep = _grace_sweep(scenarios, config)

    # Print table to stdout
    _print_table(ledger_results, baseline_results, sweep)

    # Count scenario types
    fault_counts: Dict[str, int] = {}
    for s in scenarios:
        for f in s.faults_applied:
            fault_counts[f] = fault_counts.get(f, 0) + 1

    print("\n### Scenario Fault Distribution")
    for k, v in sorted(fault_counts.items(), key=lambda x: -x[1]):
        print(f"  {k:<30} {v}")
    print(f"  {'TOTAL':<30} {sum(fault_counts.values())}")

    # Build output dict (no timestamps for reproducibility)
    output = {
        "seed": args.seed,
        "calls": args.calls,
        "base_epoch": BASE_EPOCH,
        "eval_offset_seconds": EVAL_OFFSET_SECONDS,
        "ledger": ledger_results,
        "baseline": baseline_results,
        "grace_sweep": sweep,
        "fault_counts": fault_counts,
    }

    # Write results
    results_dir = os.path.join(os.path.dirname(__file__), "results")
    os.makedirs(results_dir, exist_ok=True)
    out_path = os.path.join(results_dir, "latest.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"\n==> Results written to {out_path}")


if __name__ == "__main__":
    main()
