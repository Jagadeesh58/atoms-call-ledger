# Contract Notes & Architecture Observations

**Target:** Smallest.ai Atoms Webhooks & REST API  
**Observations Date:** 2026-10-28  
**Context:** Prepared as constructive integration notes from an independent Forward Deployed Engineering perspective.

---

## 1. Documented Inconsistencies & Integration Friction

These observations are synthesized from the public documentation ([Atoms Webhooks](https://docs.smallest.ai/voice-agents/integrate/webhooks.md) and [Call Metrics](https://docs.smallest.ai/voice-agents/developer-guide/operate/analytics/call-metrics.md)) as of 2026-10-28. They represent areas where client-side defensive normalization is currently necessary:

### 1.1 Asymmetric Telephony Identifiers Across Event Types
- **Observation:** `pre-conversation` payloads identify call parties using `toPhone` and `fromPhone`. In contrast, `post-conversation` and `analytics-completed` payloads use `toNumber` and `fromNumber`.
- **Integration Impact:** Naive consumers binding schema definitions to fixed property names fail to capture phone numbers on initial call setup.
- **Defensive Mitigation:** Normalized internally to `to_number` and `from_number` across all events.

### 1.2 Status Enumeration Spelling Mismatch (`callStatus` vs REST `status`)
- **Observation:** Webhook documentation specifies the terminal status for unanswered calls as `no-answer` (hyphenated). The REST analytics call-metrics documentation specifies `no_answer` (snake_case with underscore).
- **Integration Impact:** Direct database foreign key constraints or strict enum validators reject call records when joining webhook streams with REST backfill data.
- **Defensive Mitigation:** Canonicalize all status variants (`no-answer`, `no_answer`, `noanswer`, `NO_ANSWER`) to `no_answer`.

### 1.3 Casing Divergence in Direction Classifiers (`conversationType` vs `callDirection`)
- **Observation:** Inbound webhook envelopes convey conversation type as camelCase strings (e.g. `telephonyOutbound`, `telephonyInbound`). Meanwhile, call metadata attributes use snake_case `callDirection` (`telephony_outbound`), and REST endpoints report `type: "telephony_outbound"`.
- **Integration Impact:** Case-sensitive switch statements or strict Pydantic models require custom union discriminators.
- **Defensive Mitigation:** Bidirectional normalization mapping into canonical `inbound`, `outbound`, `chat`, or `unknown`.

### 1.4 Optionality of Metadata on Terminal Analytics
- **Observation:** The documentation notes that `callDirection` "may be absent" on the `analytics-completed` event.
- **Integration Impact:** Consumers processing `analytics-completed` independently cannot ascertain call direction without cross-referencing previous event state.
- **Defensive Mitigation:** The ledger merges incoming events into an aggregated `CallView`, carrying forward `call_direction` established during `pre-conversation` or `post-conversation`.

### 1.5 Ephemeral Recording Resolution
- **Observation:** Webhook envelopes carry a `recordingUrl` which is either raw storage or an authenticated endpoint depending on organization creation date. The permanent mechanism requires calling `GET /atoms/v1/recordings/{callId}` to obtain a presigned URL expiring in 15 minutes.
- **Integration Impact:** Webhook receivers cannot persist the raw URL as a permanent playable link.
- **Defensive Mitigation:** Treat `recordingUrl` as an opaque reference; never cache presigned URLs beyond their 15-minute validity window.

---

## 2. Product Architecture Questions for Smallest.ai

The following inquiries represent open integration questions for the Smallest.ai platform engineering team to clarify edge-case guarantees:

### Question 1: Delivery Failure Visibility & Dead-Letter Queues under ZDR
> *When an enterprise runs under Zero Data Retention (ZDR) and a customer webhook endpoint returns 500 or times out, Atoms logs the delivery as `FAILED`. Is there any customer-facing dead-letter mechanism, delivery receipt API, or manual redelivery trigger available prior to the retention window cutoff?*
- **Why it matters:** In regulated BFSI collections, proving whether an undelivered record was dropped due to receiver downtime versus platform egress failure is critical for SLA accountability.

### Question 2: Analytics Event Dispatch for Non-Completed Calls
> *Does the `analytics-completed` webhook fire for calls ending in non-terminal or non-connected statuses (e.g., `no-answer`, `busy`, `failed`, `cancelled`), or is it strictly dispatched for calls reaching `completed`?*
- **Why it matters:** The ledger's reconciliation state machine must know whether an unanswered call is awaiting post-call analytics or is immediately complete. Currently, the ledger defaults to `ANALYTICS_EXPECTED_STATUSES = ["completed"]` as an unvalidated hypothesis.

### Question 3: Distribution of Analytics Delivery Latency
> *What are the typical p50, p95, and p99 delivery latencies for `analytics-completed` relative to `post-conversation`?*
- **Why it matters:** A deterministic ledger needs a rational grace period (`ANALYTICS_GRACE_SECONDS`) to distinguish an in-flight analytical calculation from a lost webhook event without raising false alarms.

### Question 4: Exact Retention Window Anchor
> *Is the Zero Data Retention countdown (`RETENTION_WINDOW_SECONDS`) anchored to the exact call end timestamp (`ended_at`), the arrival timestamp of the `post-conversation` event, or the generation timestamp of the recording?*
- **Why it matters:** To calculate the exact time-to-deadline countdown for backfill triggering, the ledger currently computes a conservative anchor: `min(received_at, answered_at, ended_at)`.

### Question 5: Web and In-App Call Webhook Parity
> *Do interactive browser-based test calls or WebRTC agent sessions emit the same tripartite webhook sequence (`pre`, `post`, `analytics`), or are webhooks exclusively dispatched for telephony trunk calls (`telephonyOutbound` / `telephonyInbound`)?*
- **Why it matters:** Integration testing in development environments frequently uses browser clients before provisioning dedicated SIP/PSTN numbers.

### Question 6: Custom Tool Write-Back Endpoint Contract
> *For real-time mid-call tool execution and post-call variable write-backs, what is the expected schema and retry guarantee between Atoms voice workers and customer webhooks?*
- **Why it matters:** FDE deployments often orchestrate bidirectional state updates with customer CRM backends during live calls.
