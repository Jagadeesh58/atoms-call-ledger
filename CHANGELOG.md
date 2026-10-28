# Changelog

All notable changes to `atoms-call-ledger` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-10-28

### Added
- **Core Cryptographic Engine**: Constant-time HMAC-SHA256 signature verification over raw request body bytes with support for zero-downtime key rotation (`src/atoms_ledger/signature.py`).
- **Defensive Inbound Normalizer**: Normalizes documented platform inconsistencies (`toPhone` vs `toNumber`, `no-answer` vs `no_answer`, camelCase vs snake_case directions) without raising exceptions on authenticated payloads (`src/atoms_ledger/normalize.py`).
- **Persist-Before-Ack Ledger**: SQLite storage layer committing unparsed network bytes before returning HTTP 200, strictly preventing data loss on at-most-once deliveries (`src/atoms_ledger/store.py`).
- **Order-Independent Ingestion**: Deterministic deduplication, conflict detection, and authenticated quarantine pipeline acknowledging valid bytes while flagging anomalies (`src/atoms_ledger/ingest.py`).
- **Deadline-Aware Reconciler**: Lifecycle state machine and Zero Data Retention countdown engine calculating conservative expiry deadlines and risk tiers (`src/atoms_ledger/reconcile.py`).
- **Urgency-Ordered API Backfill**: Urgency-ranked backfill planner and injected-client executor supporting official SDK recovery and `MockAtomsClient` simulation (`src/atoms_ledger/backfill.py`).
- **Seeded Synthetic Fault Generator**: Fully deterministic generator for 16 distinct fault scenarios with strictly synthetic phone numbers (`+91000000xxxx`) and no customer data (`fixtures/generate.py`).
- **Deterministic Evaluation Benchmark**: Automated benchmark suite comparing the ledger against a naive baseline across 1,000 seeded calls with byte-for-byte reproducibility (`eval/run_eval.py`).
- **Interactive Vercel Web Console**: Responsive 5-tab application featuring Why, Fault Lab, Live Ledger, Payload Verifier, and Evidence register (`app/main.py`, `app/templates/index.html`, `api/index.py`, `vercel.json`).
- **Standalone Webhook Receiver**: Lightweight ASGI FastAPI receiver supporting disk spool fallback on storage exceptions (`receiver/app.py`).
- **Full Test Suite & CI**: 100+ comprehensive unit and integration tests across Python 3.10, 3.11, and 3.12 (`.github/workflows/ci.yml`).

### Measured Benchmark Results (Seed 7, 1,000 Calls)
- **M1 Complete Records**: 689 / 830 (83.0%) unified vs baseline N/A
- **M2 Duplicate Overcount**: 1,436 / 822 (174.7%) vs baseline 1,754 / 897 (195.5%)
- **M3 Tampered Payloads Accepted**: 0 / 53 (0.0%) vs baseline 53 / 53 (100.0%) [by-construction]
- **M4 Valid Payloads Rejected**: 225 / 2,570 (8.8%) vs baseline 0 / 2,598 (0.0%)
- **M5 Field Normalization**: 852 / 852 (100.0%)
- **M6 Auth-Malformed Retained**: 28 / 28 (100.0%)
- **M7 Loss Recall**: 133 / 155 (85.8%)
- **M8 False Alarms**: 60 / 133 (45.1%)
- **M9 Detection Delay**: 86,400s median remaining before ZDR expiry
- **M10 Raw Round Trip**: 2,373 / 2,373 (100.0%) byte-exact
- **M11 Order Independence**: 6 / 6 (100.0%) across all 3! event permutations

### Deviations from Initial Prompt
- **SDK Client Architecture**: The prompt referenced `from smallestai.atoms.call import Call` with `Call().get_call(call_id)` and `search_calls(ids)`. Package introspection of official `smallestai` v5.14.0 revealed the client architecture is `from smallestai import SmallestAI; client = SmallestAI(api_key=...)` with calls queried via `client.atoms.calls.get(id=...)` and `client.atoms.calls.search(call_ids=[...])`. The executor implements this official structure while preserving mock compatibility.
- **Vercel Web Application Deployment**: As requested by the user, Vercel serverless deployment (`vercel.json`, `api/index.py`) and FastAPI web interface were implemented for zero-downtime serverless hosting.
