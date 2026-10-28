# Forward Deployed Engineering Technical Interview Q&A

**Candidate:** Jagadeesh  
**Project:** `atoms-call-ledger`  
**Focus:** Architectural decisions, tradeoffs, and failure recovery for Smallest.ai Atoms voice webhooks.

---

### Question 1: Why return HTTP 200 for authenticated payloads you cannot parse? Isn't that hiding errors from the upstream platform?

**Answer Outline:**
- **The Core Constraint:** Atoms documentation explicitly states that delivery is best-effort and the platform **does not retry** failed webhook deliveries. If a receiver returns 4xx or 5xx, Atoms simply marks the delivery as `FAILED` in its internal log and moves on.
- **The ZDR Hazard:** Under Enterprise Zero Data Retention (ZDR), voice recordings, transcripts, and variables are deleted permanently once retention expires. Webhooks are the customer's sole copy. Returning a 400 or 500 would cause Atoms to drop the event permanently.
- **Quarantine over Dropping:** When a payload passes cryptographic HMAC verification (`sig_ok = 1`), we know with cryptographic certainty that Smallest.ai sent it. Even if the JSON body is truncated or schema-drifted, that raw byte stream contains irreplaceable conversational data.
- **Operational Reality:** The ledger logs the raw unparsed bytes to the `deliveries` table, raises an anomaly flag (`quarantined`), and returns HTTP 200 to satisfy Atoms' delivery expectation. Internal observability alerts fire immediately on quarantine anomalies so engineering can inspect raw bytes without data loss.

---

### Question 2: Why do you verify HMAC-SHA256 signatures over raw unparsed bytes rather than parsed JSON objects? What are the implications for replay attacks?

**Answer Outline:**
- **Why Raw Bytes:** Atoms documentation explicitly warns against re-serializing JSON prior to signature verification. In JSON, key ordering is non-deterministic, whitespace formatting varies between parsers, and Unicode escapes (e.g., Devanagari characters in voice transcripts) produce completely different byte streams when serialized. Computing HMAC over raw unparsed network bytes is the only cryptographically stable approach.
- **The Replay Attack Vulnerability:** The Atoms signature algorithm is pure `HMAC-SHA256(raw_bytes, secret)` without a timestamp header (unlike Stripe, Slack, or ElevenLabs). Consequently, an authentic payload captured by an adversary remains mathematically valid indefinitely.
- **The Application Defense:** Because replay protection is absent from the cryptographic envelope, it must be enforced by the application ledger. We implement strict idempotency on `(call_id, event_type)`. If an identical payload is replayed, the ledger identifies it as a duplicate and returns 200 without duplicate processing. If a payload with the same key arrives with altered bytes, the ledger flags a `duplicate_conflict` anomaly.

---

### Question 3: How does the ledger distinguish between a webhook event that is truly missing (lost) versus one that is simply arriving late?

**Answer Outline:**
- **The Inherent Ambiguity:** From a single incoming HTTP request, it is mathematically impossible to know whether a subsequent event (`analytics-completed`) was dropped by network transit or is still being computed by Smallest.ai's LLM evaluation pipeline.
- **State Separation:** We solve this by decoupling the lifecycle state into two discrete stages:
  1. `AWAITING_ANALYTICS`: Post-conversation has arrived; elapsed duration is within the configured grace period (`ANALYTICS_GRACE_SECONDS`, default 600s). The call is considered in-flight.
  2. `OVERDUE_ANALYTICS`: Elapsed duration exceeds the grace window. The call is flagged for automated API recovery.
- **Empirical Tuning:** In `eval/run_eval.py`, we executed an analytics grace period sweep across 60s, 300s, 600s, and 1800s. A shorter grace window catches dropped events earlier (increasing recall M7) but increases transient false alarms (M8) during platform lag. In production, an FDE should instrument the p99 delivery latency of `analytics-completed` and set grace to $p99 + 2\sigma$.

---

### Question 4: What exact end-to-end delivery guarantees does the Atoms Call Ledger provide? Do you claim "exactly-once" delivery?

**Answer Outline:**
- **Honest System Invariant:** We do not claim true end-to-end "exactly-once" delivery across distributed boundaries. Atoms' native transport is documented as at-most-once (no retries).
- **Effectively-Once Processing:** The ledger converts this at-most-once transport into **effectively-once processing**:
  1. *Persist-before-ack*: Raw network bytes are committed to SQLite before any response is dispatched.
  2. *First-write-wins idempotency*: Composite primary key `(call_id, event_type)` ensures duplicate deliveries cannot corrupt call history.
  3. *Proactive API Backfill*: Calls missing required events are reconciled against the ZDR retention deadline and retrieved via `client.atoms.calls.get()` before data is purged.
- **Hard Limit:** If an infrastructure outage or receiver downtime exceeds the enterprise retention window (e.g. receiver down for 25 hours on a 24-hour ZDR tier), the data is permanently lost. The ledger makes this risk visible via the `EXPIRED` risk tier.

---

### Question 5: What components break when traffic scales to thousands of concurrent calls, and how would you evolve the architecture?

**Answer Outline:**
- **Where SQLite Breaks:** SQLite uses a single database-level write lock. At high ingestion concurrency (>150 requests/second), writes will encounter lock contention and timeouts.
- **Where Synchronous Ingestion Breaks:** In the current design, HMAC verification and DB insertion occur synchronously within the ASGI request loop. A database slow query would risk breaching Atoms' 30-second timeout.
- **Production Target Architecture:**
  1. **Stateless Edge Ingress**: Deploy edge workers (FastAPI on AWS ECS or Cloudflare Workers) that verify HMAC-SHA256 and stream raw bytes directly to Amazon SQS FIFO or Kafka.
  2. **Asynchronous Consumer Cluster**: Background workers consume batches from SQS, perform idempotency checks, and commit to Amazon Aurora PostgreSQL.
  3. **Multi-Region Availability**: Deploy active-active receivers across multiple cloud regions with DNS failover, ensuring that a single-region cloud outage does not breach customer ZDR retention windows.
- **Acknowledged Limit:** In this evaluation, synthetic tests benchmarked correctness and order-independence; load testing under sustained 10,000 req/sec was explicitly excluded from scope.
