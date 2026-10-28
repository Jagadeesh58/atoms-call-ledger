# Note for Smallest.ai Co-Founders

I am Jagadeesh, a software engineer with forward-deployed engineering experience in voice AI and webhook infrastructure.

Smallest.ai Atoms documentation specifies that webhooks do not retry and delivery is best-effort. Under Enterprise Zero Data Retention (ZDR), call content is permanently deleted when the retention window expires (1h–30d). Receiver outages risk irreversible data loss for BFSI and collections customers.

I built `atoms-call-ledger`, an open-source reliability runtime. On a 1,000-call synthetic fault benchmark:
- 0/53 tampered payloads accepted (100% rejection via raw-byte HMAC).
- 28/28 authenticated malformed payloads quarantined without loss.
- 133/155 injected event drops flagged before retention deadlines (85.8% recall).
- 100% order-independence across all event permutations.

I did not verify behavior on live platform clusters or test multi-region throughput at 10,000 req/s.

Three product questions for Smallest:
1. Is an automated dead-letter queue planned for failed ZDR deliveries?
2. Does `analytics-completed` emit for non-completed calls?
3. Where is the retention countdown anchored?

- Repository: https://github.com/Jagadeesh58/atoms-call-ledger
- Live Demo: LIVE_DEMO_URL
