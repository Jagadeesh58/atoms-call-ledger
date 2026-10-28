# Evidence Register & Problem Grounding

**Project:** `atoms-call-ledger`  
**Author:** Jagadeesh  
**Audit Date:** 2026-10-28  
**Scope:** Public documentation, public job specifications, and public competitor forums.  
**Affiliation Disclaimer:** This is an independent open-source project and is not affiliated with, endorsed by, or sponsored by Smallest.ai.

---

## 1. Evidence Classification Standards

Every claim, constraint, and architectural assumption in this project is explicitly tagged with one of four labels:

- **`CONFIRMED FACT`**: Directly stated in official documentation or official source material. Accompanied by exact URL and quote paraphrase.
- **`CUSTOMER-REPORTED PROBLEM`**: Documented publicly in developer forums or community threads by real users encountering the issue in production or integration.
- **`REASONABLE INFERENCE`**: Logical engineering deduction derived directly from combining two or more confirmed facts.
- **`UNVALIDATED HYPOTHESIS`**: Engineering assumption or edge-case expectation requiring empirical verification via direct API testing or provider clarification.

---

## 2. Atoms Webhook Ingestion Evidence Base

Source: [Smallest.ai Atoms Webhooks Documentation](https://docs.smallest.ai/voice-agents/integrate/webhooks.md) *(Fetched 2026-10-28)*

### 2.1 Lifecycle Events & Correlation
- **CONFIRMED FACT** — Atoms emits three distinct webhook events per call, correlated by a common `callId`:
  1. `pre-conversation`: Dispatched at call initiation with initial telephony metadata.
  2. `post-conversation`: Dispatched immediately upon call termination containing full transcript, recording references, call metadata, and extracted conversation variables.
  3. `analytics-completed`: Dispatched "some time after" the `post-conversation` event, containing post-call evaluation summaries, disposition metrics, and structured analytics.
- **CONFIRMED FACT** — Delivery ordering across concurrent calls is best-effort and not guaranteed. Receivers must join and reconcile events asynchronously using the `callId`.

### 2.2 Delivery Semantics & Failure Modes
- **CONFIRMED FACT** — Webhook delivery timeout is strictly 30 seconds.
- **CONFIRMED FACT** — Atoms **does not retry** failed webhook deliveries. If a customer receiver returns a non-2xx HTTP status code, times out, or encounters a TCP/network reset, Atoms marks the delivery as `FAILED` in its delivery log and moves on. The documentation instructs customers to acknowledge deliveries fast with a 2xx status, process asynchronously, and manage any retry or recovery mechanisms on the customer side.
- **CONFIRMED FACT** — Atoms does not publish static egress IP ranges. Network-level IP allow-listing is explicitly unsupported; authenticity verification depends solely on the cryptographic `X-Signature` HTTP header.

### 2.3 Cryptographic Signature Scheme
- **CONFIRMED FACT** — The `X-Signature` header contains a hexadecimal HMAC-SHA256 signature calculated over the **raw request body bytes**.
- **CONFIRMED FACT** — Official documentation explicitly cautions against re-serializing parsed JSON payloads prior to signature validation because differences in key order, indentation, and unicode escape sequences alter the cryptographic hash.
- **CONFIRMED FACT** — The documented signature algorithm contains no timestamp component in the signed payload or signature header.
- **REASONABLE INFERENCE** — Because the signature contains no timestamp, a captured valid webhook payload will pass cryptographic verification indefinitely. Protection against replay attacks cannot be delegated to the signature layer and must be implemented at the application ledger level via deterministic idempotency keys `(callId, eventType)`.

### 2.4 Documented Schema & Enum Inconsistencies
- **CONFIRMED FACT** — Field names vary across events:
  - `pre-conversation` uses `toPhone` and `fromPhone`.
  - `post-conversation` and `analytics-completed` use `toNumber` and `fromNumber`.
- **CONFIRMED FACT** — `callDirection` is documented as "may be absent" on `analytics-completed`.
- **CONFIRMED FACT** — Conversation variables beyond the 11 standard built-in keys are fully dynamic and user-defined; custom keys may be added, altered, or omitted arbitrarily between agents and calls.
- **CONFIRMED FACT** — `callStatus` in webhook payloads lists `no-answer` (kebab-case with hyphen), whereas the REST Call Metrics documentation ([Smallest.ai Call Metrics](https://docs.smallest.ai/voice-agents/developer-guide/operate/analytics/call-metrics.md)) documents `no_answer` (snake_case with underscore).
- **CONFIRMED FACT** — `conversationType` uses camelCase (`telephonyOutbound`, `telephonyInbound`), whereas `callDirection` and the REST API `type` attribute use snake_case (`telephony_outbound`, `telephony_inbound`).
- **CONFIRMED FACT** — The `recordingUrl` field in the webhook payload is documented as "either a raw storage URL or the authenticated endpoint URL depending on when your organization was created". Secure playback requires resolving the recording via `GET /atoms/v1/recordings/{callId}?channel=mono|dual` into a presigned URL that expires within 15 minutes.
- **CONFIRMED FACT** — REST call search limits: Date-range filters in `search_calls` are marked "coming soon" (requiring client-side filtering); batch `search_calls` accepts at most 100 call IDs per request; latency metrics in call metrics responses represent per-call averages.

---

## 3. High-Stakes Impact: Zero Data Retention (ZDR)

Source: [Smallest.ai Zero Data Retention Documentation](https://docs.smallest.ai/overview/administration/zero-data-retention.md) *(Fetched 2026-10-28)*

- **CONFIRMED FACT** — When an enterprise customer enables Zero Data Retention (ZDR), voice-agent session content (including audio recordings, full conversational transcripts, post-call analytics, extracted variable dictionaries, and caller phone numbers) is purged from Smallest.ai infrastructure once webhook deliveries conclude, and "never later than your retention window".
- **CONFIRMED FACT** — The retention window is configurable between 1 hour (3,600 seconds) and 30 days (2,592,000 seconds), defaulting to 24 hours (86,400 seconds).
- **CONFIRMED FACT** — The documentation explicitly warns: Webhooks are "your copy of each call". Any call record or payload not successfully received and stored before the retention window expires **cannot be recovered**.
- **CONFIRMED FACT** — If webhook delivery fails or the receiver is unavailable, content is still irrevocably purged once the retention window elapses.
- **REASONABLE INFERENCE** — Synthesizing the no-retry delivery model with the ZDR purge policy: Any transient outage, deployment restart, or unhandled exception on the customer receiver creates an immediate risk of permanent, unrecoverable data loss for regulated call content (transcripts, compliance disclosures, consent captures, and payment confirmations).
- **UNVALIDATED HYPOTHESIS** — Prior to retention window expiry, querying the REST API (`client.atoms.calls.get(id=call_id)`) can retrieve call transcripts and analytics for calls whose webhook delivery failed or timed out. (Validated in code via `scripts/validate_backfill.py` when credentials are provided).
- **UNVALIDATED HYPOTHESIS** — Whether `analytics-completed` is dispatched for calls ending in non-`completed` terminal statuses (e.g. `no-answer`, `busy`, `failed`, `cancelled`). Configurable in `atoms-call-ledger`; default assumption is non-completed calls do not expect analytics.
- **UNVALIDATED HYPOTHESIS** — Whether browser-based web/chat test calls emit webhook events. Documented `conversationType` enumerations currently cite only telephony values (`telephonyOutbound`, `telephonyInbound`).

---

## 4. Role Alignment & Forward Deployed Engineering Context

Source: [Smallest.ai Forward Deployed Engineer Job Specification](https://www.voiceaispace.com/jobs/c9a493cb-e343-4948-bb6e-3416e41fecf7) *(Job board posting 2026-05-09)*

- **CONFIRMED FACT** — The Forward Deployed Engineer (FDE) role at Smallest.ai requires:
  - Building reliable production Python services, webhook receivers, and custom enterprise tool integrations.
  - Debugging live voice pipelines and telephony callbacks under enterprise production load.
  - Defining, instrumenting, and monitoring operational reliability metrics.
  - Codifying recurring enterprise integration patterns into reusable templates and feeding architectural insights back to core product engineering.
  - Target deployment sectors: Customer Support, Sales, Collections, and Recruitment in regulated verticals (BFSI, Healthcare).
- **CONFIRMED FACT** — The official Atoms webhook integration documentation uses a "Debt Collection Agent" callback service as its primary reference architecture. In debt collection and BFSI, missing call transcripts violate regulatory audit requirements (e.g., FDCPA, CFPB, HIPAA, TCPA).

---

## 5. Comparative Ecosystem Analysis: Distinct Failure Topologies

To understand the architectural hazard, consider how peer voice-AI platforms handle webhook delivery:

### 5.1 Retell AI
- Source: [Retell Webhook Overview](https://docs.retellai.com/features/webhook-overview.md)
- **CONFIRMED FACT** — Retell automatically retries webhook deliveries up to 3 times following a 10-second timeout.
- **CUSTOMER-REPORTED PROBLEM** — In the Retell community forum, users reported race conditions and duplicate webhook dispatches:
  - Missing `call_analyzed` events in Zapier workflows due to rapid sequential dispatch: [Retell Community #800](https://community.retellai.com/t/webhook-race-condition-call-ended-fires-immediately-before-call-analyzed-causing-zapier-to-miss-analyzed-event/800)
  - Duplicate `call_ended` events dispatched within 1 second of each other: [Retell Community #519](https://community.retellai.com/t/retell-sends-multiple-call-ended-events-within-1-sec/519)
- **REASONABLE INFERENCE** — Retell's webhook topology fails primarily through **duplication and out-of-order bursts**.

### 5.2 ElevenLabs Agents
- Source: [ElevenLabs Post-Call Webhooks](https://elevenlabs.io/docs/eleven-agents/workflows/post-call-webhooks.md)
- **CONFIRMED FACT** — ElevenLabs post-call webhooks sign requests using HMAC-SHA256 with an explicit timestamp header for replay protection, support automated retries with exponential backoff, and automatically disable endpoints after sustained delivery failures. For HIPAA-compliant workspaces, automatic retries are disabled.

### 5.3 Smallest.ai Atoms Ingestion Synthesis
- **REASONABLE INFERENCE** — Retell fails by *duplicating*; Atoms is documented to fail by *dropping* (no retries). Consequently, an enterprise ingestion layer for Atoms cannot simply be a generic webhook relay. It must combine:
  1. Instantaneous persist-before-ack ingestion.
  2. Strict raw-byte HMAC verification without re-serialization.
  3. Strict deduplication and conflict detection.
  4. Active reconciliation tracking against the ZDR retention countdown to trigger API backfill before data is permanently purged.

---

## 6. Deliberate Scope Boundaries & Non-Claims

1. **No Allegation of Unreported Outages**: No public customer complaints regarding Smallest.ai webhook drops were identified. G2 reviews for Smallest.ai mention product templates and voice quality only. This project solves a documented architectural risk inherent in the official contract, not an active incident.
2. **Rejection of Agent Regression Testing Alternative**: Smallest.ai pricing lists a native "Testing Suite" (Basic and Enterprise), and multiple third-party voice simulation tools exist. A webhook reliability ledger directly targets the unaddressed FDE integration gap.
3. **Provider Limits**: Webhook behavior for Twilio, Deepgram, and Vapi was not evaluated during this research phase.

---

## 7. Project Selection Ranking Appendix

During project conception on 2026-10-28, three candidate projects were evaluated against the Smallest.ai FDE role requirements:

| Candidate Architecture | Problem Addressed | Relevance to FDE | Technical Feasibility | Decision |
|---|---|---|---|---|
| **A. Atoms Call Ledger** | Webhook loss-proofing, raw HMAC verification, ZDR deadline reconciliation, SDK backfill | Very High (direct enterprise BFSI/Collections hazard) | High (deterministic, verifiable, zero synthetic claims) | **Selected** |
| **B. Voice Latency Benchmarker** | Per-turn chunk latency measurement across telephony gateways | High (core voice pipeline) | Medium (requires active telephony trunks and live billing credits) | Rejected (cost & simulation fragility) |
| **C. Agent Scenario Test Harness** | Automated prompt regression testing for Atoms conversational agents | Medium (already covered by Smallest.ai native Testing Suite) | High (pure SDK calls) | Rejected (redundant with core product roadmap) |
