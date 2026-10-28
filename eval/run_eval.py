"""
WHY:
Deterministic evaluation runner comparing the Atoms Call Ledger against a naive
baseline across the documented Atoms webhook failure modes. Must be reproducible
byte-for-byte given the same seed (fixed base epoch, seeded RNG, no wall-clock
timestamps in output). Results written to eval/results/latest.json.

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

from atoms_ledger.config import DEMO_WEBHOOK_SECRET, LedgerConfig
from atoms_ledger.ingest import handle
from atoms_ledger.reconcile import (
    STATE_COMPLETE,
    STATE_QUARANTINED_ONLY,
    evaluate,
)
from atoms_ledger.store import LedgerStore

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from eval.baseline import NaiveBaseline  # noqa: E402
from fixtures.generate import BASE_EPOCH, CallScenario, generate_call_set  # noqa: E402


def _run_ledger(scenarios: List[CallScenario], config: LedgerConfig) -> Dict[str, Any]:
    """Run all scenarios through the ledger and return raw result data."""
    store = LedgerStore(":memory:")

    # Fixed evaluation time: 1 hour after base epoch
    eval_now = datetime.fromtimestamp(BASE_EPOCH + 3600, tz=timezone.utc)

    total_deliveries = 0
    authentic_deliveries = 0
    tampered_sent = 0
    tampered_accepted = 0
    valid_sent = 0
    valid_rejected = 0
    authenticated_malformed_sent = 0
    authenticated_malformed_retained = 0

    for scenario in scenarios:
        for delivery in scenario.events:
            raw = delivery["body"]
            headers = delivery["headers"]
            label = delivery["label"]

            total_deliveries += 1

            # Track ground truth categories
            if label == "post_tampered":
                tampered_sent += 1
            if not headers.get("x-signature") and label not in ("malformed",):
                pass  # missing sig
            if label == "malformed":
                authenticated_malformed_sent += 1

            result = handle(raw, headers, config, store, now=eval_now)

            if label == "post_tampered":
                if result.outcome not in ("rejected_signature", "rejected_size", "rejected_empty"):
                    tampered_accepted += 1
            elif label == "malformed":
                if result.outcome == "quarantined":
                    authenticated_malformed_retained += 1
            elif result.outcome in ("accepted", "duplicate", "conflict"):
                authentic_deliveries += 1
            elif result.outcome == "rejected_signature":
                valid_rejected += 1

    # Valid = authenticated deliveries that should have passed
    valid_sent = authentic_deliveries + valid_rejected

    # Now evaluate reconcile
    views = evaluate(store, config, eval_now)
    view_map = {v.call_id: v for v in views}

    # Build ground truth maps
    gt_map = {s.call_id: s.ground_truth for s in scenarios}
    fault_map = {s.call_id: s.faults_applied for s in scenarios}

    # M1: Complete call records / calls whose required events were actually delivered
    required_delivered_calls = []
    for scenario in scenarios:
        gt = scenario.ground_truth
        # A call "had required events delivered" if post was not lost
        # (analytics loss only matters if status = completed)
        faults = scenario.faults_applied
        if "outage_window" in faults or "wrong_secret" in faults or "missing_signature" in faults:
            continue  # these calls had all events blocked at auth layer
        if gt.get("post_lost", False):
            continue  # post was actually dropped
        required_delivered_calls.append(scenario.call_id)

    m1_denom = len(required_delivered_calls)
    m1_num = sum(
        1
        for cid in required_delivered_calls
        if view_map.get(cid) and view_map[cid].state == STATE_COMPLETE
    )

    # M2: Duplicate overcount (total event rows - unique calls)
    all_events = []
    for cid in view_map:
        v = view_map[cid]
        all_events.extend(v.events_received)
    total_event_rows_in_events_table = sum(len(view_map[cid].events_received) for cid in view_map)
    unique_calls_with_events = len([cid for cid in view_map if view_map[cid].events_received])
    m2_num = total_event_rows_in_events_table - unique_calls_with_events
    m2_denom = unique_calls_with_events

    # M3: Tampered payloads accepted / tampered sent (by-construction: 0 accepted)
    m3_num = tampered_accepted
    m3_denom = tampered_sent

    # M4: Valid payloads rejected / valid sent
    m4_num = valid_rejected
    m4_denom = valid_sent if valid_sent > 0 else 1

    # M5: Field normalization accuracy (spot-check: from_number and to_number present for clean)
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

    # M6: Authenticated-malformed retained as raw / sent
    m6_num = authenticated_malformed_retained
    m6_denom = authenticated_malformed_sent if authenticated_malformed_sent > 0 else 1

    # M7: Injected losses flagged / calls with required event actually dropped
    actually_dropped = []
    for scenario in scenarios:
        gt = scenario.ground_truth
        faults = scenario.faults_applied
        # A "loss" is when a required event was expected but not sent to receiver
        if gt.get("post_lost", False) or "outage_window" in faults:
            actually_dropped.append(scenario.call_id)
        elif gt.get("analytics_lost", False) and scenario.call_id not in [
            s.call_id for s in scenarios if "no_answer_status" in s.faults_applied
        ]:
            # Analytics dropped on a completed call (required)
            actually_dropped.append(scenario.call_id)

    m7_denom = len(set(actually_dropped))
    m7_num = 0
    for cid in set(actually_dropped):
        view = view_map.get(cid)
        if view and view.state not in (STATE_COMPLETE,):
            m7_num += 1

    # M8: False alarms / calls flagged as incomplete (but ground truth shows all delivered)
    incomplete_calls = [
        cid
        for cid, view in view_map.items()
        if view.state not in (STATE_COMPLETE, STATE_QUARANTINED_ONLY)
    ]
    false_alarms = 0
    for cid in incomplete_calls:
        gt = gt_map.get(cid, {})
        faults = fault_map.get(cid, [])
        if not gt.get("post_lost", False) and "outage_window" not in faults:
            false_alarms += 1
    m8_num = false_alarms
    m8_denom = len(incomplete_calls) if incomplete_calls else 1

    # M9: Median detection delay vs deadline (flagged before EXPIRED)
    # remaining_seconds for flagged true losses
    detection_delays = []
    for cid in set(actually_dropped):
        view = view_map.get(cid)
        if view and view.state not in (STATE_COMPLETE,):
            if view.remaining_seconds is not None:
                detection_delays.append(view.remaining_seconds)
    if detection_delays:
        detection_delays.sort()
        mid = len(detection_delays) // 2
        m9_val = detection_delays[mid]
    else:
        m9_val = None

    # M10: Byte-exact raw round trip / stored authentic deliveries
    # Every accepted delivery should have its raw bytes intact in store
    m10_denom = 0
    m10_num = 0
    for row in store._conn.execute(
        "SELECT raw, sha256 FROM deliveries WHERE sig_ok=1 AND raw IS NOT NULL"
    ).fetchall():
        raw_bytes, stored_sha = row
        m10_denom += 1
        if raw_bytes and hashlib.sha256(raw_bytes).hexdigest() == stored_sha:
            m10_num += 1

    # M11: Order independence — test all 3! permutations produce same state
    import itertools

    perms_tested = 0
    perms_equal = 0
    # Take first clean scenario for order test
    clean_scenarios = [s for s in scenarios if "clean" in s.faults_applied]
    if clean_scenarios:
        test_s = clean_scenarios[0]
        base_events = test_s.events[:3]  # pre, post, analytics
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
        "M7": {"num": m7_num, "denom": m7_denom, "label": "Injected losses flagged (recall)"},
        "M8": {"num": m8_num, "denom": m8_denom, "label": "False alarms / flagged calls"},
        "M9": {"val": m9_val, "label": "Median remaining seconds at detection"},
        "M10": {"num": m10_num, "denom": m10_denom, "label": "Byte-exact raw round trip"},
        "M11": {"num": m11_num, "denom": m11_denom, "label": "Order independence (permutations)"},
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

    # Baseline M7: 0 (no reconcile, can never detect losses)
    # Baseline M3: always accepts tampered (by-construction for naive)
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
    Sweep analytics grace seconds [60, 300, 600, 1800] and report M7/M8.

    Label: 'Illustrates the trade-off; does not recommend a production value.'
    """
    results = []
    eval_now = datetime.fromtimestamp(BASE_EPOCH + 3600, tz=timezone.utc)

    for grace in [60, 300, 600, 1800]:
        sweep_cfg = LedgerConfig(
            webhook_secrets=config.webhook_secrets,
            retention_window_seconds=config.retention_window_seconds,
            analytics_grace_seconds=grace,
        )
        store = LedgerStore(":memory:")
        for scenario in scenarios:
            for delivery in scenario.events:
                handle(delivery["body"], delivery["headers"], sweep_cfg, store, now=eval_now)

        views = evaluate(store, sweep_cfg, eval_now)
        view_map = {v.call_id: v for v in views}
        fault_map = {s.call_id: s.faults_applied for s in scenarios}
        gt_map = {s.call_id: s.ground_truth for s in scenarios}

        actually_dropped = set()
        for scenario in scenarios:
            gt = scenario.ground_truth
            faults = scenario.faults_applied
            if gt.get("post_lost", False) or "outage_window" in faults:
                actually_dropped.add(scenario.call_id)
            elif gt.get("analytics_lost", False) and "no_answer_status" not in faults:
                actually_dropped.add(scenario.call_id)

        m7 = sum(
            1
            for cid in actually_dropped
            if view_map.get(cid) and view_map[cid].state != STATE_COMPLETE
        )
        incomplete = [
            cid
            for cid, v in view_map.items()
            if v.state not in (STATE_COMPLETE, STATE_QUARANTINED_ONLY)
        ]
        fa = sum(
            1
            for cid in incomplete
            if not gt_map.get(cid, {}).get("post_lost", False)
            and "outage_window" not in fault_map.get(cid, [])
        )
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
    header = f"{'ID':<4} {'Metric':<40} {'Ledger':<20} {'Baseline':<20} {'Note'}"
    print(header)
    print("-" * len(header))

    notes = {
        "M1": "",
        "M2": "",
        "M3": "by-construction",
        "M4": "",
        "M5": "",
        "M6": "",
        "M7": "",
        "M8": "",
        "M9": "",
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
        print(f"{mid:<4} {label:<40} {lv:<20} {bv:<20} {notes[mid]}")

    print()
    print("### Grace-Seconds Sweep (M7 recall vs M8 false alarms)")
    print("Note: Illustrates the trade-off; does not recommend a production value.")
    print()
    print(f"{'Grace(s)':<12} {'M7 Recall':<20} {'M8 False Alarms'}")
    print("-" * 44)
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
