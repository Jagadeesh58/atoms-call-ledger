# 2-Minute Technical Demonstration Script

**Audience:** Smallest.ai Technical Co-Founder / Engineering Leadership  
**Presenter:** Jagadeesh  
**Target Duration:** Exactly 120 seconds  

---

### [0:00 - 0:20] The Problem Grounding
> *"Hi. Today I want to demonstrate how we solve data durability when integrating Smallest.ai Atoms voice agents in regulated enterprise environments.  
> Looking at the official documentation, two facts stand out: First, Atoms webhook delivery is at-most-once—deliveries timeout at 30 seconds with zero platform retries. Second, under Enterprise Zero Data Retention, call recordings, transcripts, and variables are permanently purged when the retention window ends.  
> If an enterprise receiver suffers a 30-second network glitch or restart, that call content is gone forever. I built `atoms-call-ledger` to make ingestion loss-proof and deadline-aware."*

---

### [0:20 - 0:55] Fault Lab: Ledger vs Naive Baseline
*(Action: Open the **Fault Lab** tab, click **Run Simulation** with 1,000 calls)*
> *"Here in the Fault Lab, we simulate 1,000 synthetic voice calls subjected to the documented failure modes: dropped post events, out-of-order deliveries, malformed JSON, and signature tampering.  
> Comparing our Ledger against a standard naive handler:
> - Notice **M3**: Naive baseline blindly accepts 53 tampered payloads. The Ledger rejects 100% of them by verifying HMAC over raw network bytes.
> - Look at **M7**: Out of 155 dropped events, the Ledger's state machine automatically detects 133 missing calls (85.8% recall) while the retention window is still open.
> - Look at **M6**: When authenticated JSON arrives truncated or corrupt, the naive handler crashes or drops it; the Ledger quarantines raw bytes with a 200 ack so data is never permanently lost."*

---

### [0:55 - 1:20] Live Ledger & ZDR Retention Countdown
*(Action: Switch to the **Ledger** tab, filter by State: `OVERDUE_ANALYTICS`)*
> *"Under the Ledger tab, we aggregate incoming events per call. Notice this call: `pre` and `post` arrived, but `analytics-completed` never showed up.  
> Because the call is in an incomplete state, the ledger computes a real-time countdown against the customer's 24-hour ZDR retention window. When time drops below 10%, the call transitions to CRITICAL risk.  
> Clicking **Run Backfill (MOCK)** triggers our urgency-ordered planner, invoking Smallest.ai's official SDK client to retrieve the missing transcript before the platform purges it."*

---

### [1:20 - 1:40] Try a Payload: The Raw-Bytes Proof
*(Action: Switch to the **Try a Payload** tab, click **Re-serialize JSON & Break Signature**)*
> *"A subtle point in the Smallest documentation is that signatures must be checked on raw request bytes.  
> In this tab, we take a valid webhook payload, parse it into JSON, and simply re-serialize it. Notice what happens: the HMAC verification immediately fails because key re-ordering and whitespace alterations change the SHA256 digest. Our ingestion pipeline preserves raw bytes from the wire before any parsing occurs."*

---

### [1:40 - 2:00] Honest Limits & Strategic Questions
> *"To be completely transparent about what this is not: this test suite was run on seeded synthetic fault distributions, not live customer traffic. Furthermore, SQLite is single-writer and would be replaced with Postgres and SQS in production.  
> This prototype surfaces three key questions for the Smallest engineering team:
> 1. Is there a customer dead-letter queue or redelivery mechanism planned for ZDR webhook failures?
> 2. Does `analytics-completed` fire for non-completed calls like `no-answer`?
> 3. Does the retention window anchor to call completion or first webhook dispatch?  
> Thank you."*
