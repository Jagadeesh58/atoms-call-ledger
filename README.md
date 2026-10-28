# Atoms Call Ledger

**Loss-proof webhook ingestion and deadline-aware reconciliation for Smallest.ai Atoms voice call events.**

> **Disclaimer**: This is an independent open-source project created by Jagadeesh and is **not affiliated with, endorsed by, or sponsored by Smallest.ai**.

🌐 **Live Interactive Console**: [LIVE_DEMO_URL](LIVE_DEMO_URL) *(Human operator populates after verifying deployment)*  
📦 **Repository**: [https://github.com/Jagadeesh58/atoms-call-ledger](https://github.com/Jagadeesh58/atoms-call-ledger)

---

## The Problem in 5 Confirmed Facts

1. **CONFIRMED FACT** — Atoms fires three distinct webhook events per call (`pre-conversation`, `post-conversation`, `analytics-completed`), joined asynchronously by `callId` ([Official Webhook Docs](https://docs.smallest.ai/voice-agents/integrate/webhooks.md)).
2. **CONFIRMED FACT** — Webhook delivery is at-most-once with a strict 30-second timeout. Atoms **does not retry** failed deliveries; non-2xx responses are logged `FAILED` and permanently dropped.
3. **CONFIRMED FACT** — No static egress IP ranges are published. Authenticity relies entirely on the `X-Signature` header computed as an HMAC-SHA256 hex digest over the **raw unparsed request body bytes**.
4. **CONFIRMED FACT** — Under Enterprise Zero Data Retention (ZDR), call recordings, transcripts, and analytics are permanently purged once retention expires (1 hour to 30 days; 24h default) ([Official ZDR Docs](https://docs.smallest.ai/overview/administration/zero-data-retention.md)).
5. **REASONABLE INFERENCE** — A receiver outage or transient network timeout results in permanent, unrecoverable data loss under ZDR unless calls are reconciled against the retention deadline and retrieved via API backfill before the window closes.

---

## What It Does & Does Not Do

### What It Does
- **Persist-Before-Ack Ingestion**: Commits raw unparsed delivery bytes to an immutable delivery log before returning HTTP 200, ensuring authenticated events are never dropped.
- **Raw-Byte HMAC Verification**: Validates `X-Signature` in constant time (`hmac.compare_digest`) directly over wire bytes, supporting zero-downtime secret rotation.
- **Authenticated Quarantine**: Replays or malformed authenticated JSON payloads are safely quarantined and acknowledged with 200, rather than rejected (which would cause Atoms to drop them permanently).
- **Defensive Normalization**: Resolves documented platform schema inconsistencies (`toPhone` vs `toNumber`, `no-answer` vs `no_answer`, camelCase vs snake_case direction classifiers).
- **ZDR Deadline Reconciliation**: Evaluates lifecycle states (`COMPLETE`, `AWAITING_ANALYTICS`, `OVERDUE_ANALYTICS`, `OVERDUE_POST`) and tracks real-time time-to-deadline countdowns.
- **Urgency-Ordered API Backfill**: Batches missing calls ($\le 100$ IDs) and interfaces with the official `smallestai` Python SDK to recover lost data before the retention window expires.

### What It Does NOT Do
- Does not implement React, TypeScript, heavy JavaScript frameworks, or custom CSS bundles.
- Does not perform LLM inference or agentic orchestration in the ingestion loop (deterministic data integrity only).
- Does not manage live telephony SIP trunks or PSTN carrier connections.
- Does not replace enterprise streaming queues (SQS/Kafka) or distributed relational stores (PostgreSQL) at scale.

---

## 60-Second Quickstart

### 1. Local Run (FastAPI + Vercel Entrypoint)
```bash
git clone https://github.com/Jagadeesh58/atoms-call-ledger.git
cd atoms-call-ledger
python -m pip install -e ".[dev]"

# Launch the interactive web console
uvicorn app.main:app --reload --port 8000
# Open http://localhost:8000 in your browser
```

### 2. Deploy to Vercel
```bash
# Using Vercel CLI or by importing Jagadeesh58/atoms-call-ledger into vercel.com
vercel deploy
```

### 3. Run Deterministic Benchmark Evaluation
```bash
python eval/run_eval.py --seed 7 --calls 1000
```

---

## Empirical Benchmark Results (1,000 Seeded Calls)

Pasted verbatim from `eval/results/latest.json` (Seed `7`, 1,000 synthetic calls, base epoch `1729000000.0`):

| ID | Metric | Ledger | Naive Baseline | Denominator / Evaluation Context |
|---|---|---|---|---|
| **M1** | Complete call records | **689 / 830 (83.0%)** | N/A / N/A (N/A) | Calls whose required events were actually delivered |
| **M2** | Duplicate overcount | **1,436 / 822 (174.7%)** | 1,754 / 897 (195.5%) | Total event rows minus unique calls (deduplication effectiveness) |
| **M3** | Tampered payloads accepted | **0 / 53 (0.0%)** | 53 / 53 (100.0%) | Valid-looking JSON with tampered body or bad signature (`by-construction`) |
| **M4** | Valid payloads rejected | **225 / 2,570 (8.8%)** | 0 / 2,598 (0.0%) | Payloads with unconfigured secret or missing signature |
| **M5** | Field normalization accuracy | **852 / 852 (100.0%)** | N/A / N/A (N/A) | Ground truth telephone & enum mappings verified |
| **M6** | Auth-malformed payloads retained | **28 / 28 (100.0%)** | N/A / N/A (N/A) | Authenticated-but-corrupt JSON quarantined with raw bytes intact |
| **M7** | Injected losses flagged (recall) | **133 / 155 (85.8%)** | 0 / N/A (N/A) | Calls with a dropped required event surfaced by reconcile |
| **M8** | False alarms / flagged calls | **60 / 133 (45.1%)** | 0 / N/A (N/A) | Calls in-flight flagged as incomplete due to analytics grace |
| **M9** | Median detection delay vs deadline | **86,400s** | N/A | Time remaining when loss was first surfaced (conservative anchor) |
| **M10**| Byte-exact raw round trip | **2,373 / 2,373 (100.0%)** | N/A / N/A (N/A) | Stored raw BLOBs matching network bytes byte-for-byte |
| **M11**| Order independence | **6 / 6 (100.0%)** | N/A / N/A (N/A) | All $3! = 6$ arrival permutations produce identical final state |

---

## Threats to Validity

1. **Synthetic Fault Suite**: Fault scenarios are programmatically generated from the documented API contract, not sampled from private production telemetry.
2. **Naive Baseline Construction**: The comparison baseline is an illustrative reference constructed for this project; it does not reflect any real customer's proprietary code.
3. **Retention Anchor Estimation**: Because official docs state *"never later than your retention window"* without defining the exact starting trigger, the ledger uses a conservative estimate: $\min(\text{timestamps})$.
4. **Analytics Status Hypothesis**: Assumes calls ending in `no-answer` do not dispatch `analytics-completed`. If Smallest.ai dispatches analytics for unanswered calls, detection recall will vary.

---

## Architecture & System Design

For comprehensive component descriptions, dataflow diagrams, and scale failure analysis, see:  
📖 [Architecture Document (`docs/ARCHITECTURE.md`)](docs/ARCHITECTURE.md)

---

## Honest Limitations

- **Storage Concurrency**: Built with SQLite stdlib. Under production concurrency (>150 req/sec), SQLite database-level write locks will saturate. Swap to PostgreSQL with identical schemas for high load.
- **Single-Region Exposure**: If an entire cloud region suffers an outage longer than your ZDR window (e.g. 1 hour), backfill cannot rescue purged data. Deploy multi-region active-active ingress.
- **Presigned URLs**: Recording URLs returned by Atoms expire in 15 minutes and are never cached by the ledger.

---

## Development & Integrity Disclosure

Developed with AI pair-programming assistance (Antigravity/Gemini); all domain invariants, SQLite schemas, mathematical metrics, and verification tests were designed, audited, and benchmarked by the author.

---

## License

MIT License — Copyright (c) 2026 Jagadeesh.
