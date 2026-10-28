"""
WHY:
Empirical validation experiment for real Smallest.ai Atoms API credentials.
Tests whether client.atoms.calls.get retrieves call details and transcripts
when webhook delivery failed or pointed to an unreachable endpoint.
Requires SMALLEST_API_KEY. Never commits real call data or fabricates outcomes.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# Add src to path
repo_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(repo_root / "src"))


def run_experiment() -> None:
    api_key = os.getenv("SMALLEST_API_KEY")
    if not api_key:
        print("ERROR: SMALLEST_API_KEY environment variable is not set.")
        print("This experiment requires a real Smallest.ai API key with active credits.")
        print("Until executed with a live key, the recovery hypothesis remains UNVALIDATED.")
        sys.exit(1)

    try:
        from smallestai import SmallestAI
    except ImportError:
        print("ERROR: 'smallestai' package is not installed. Run 'pip install smallestai'.")
        sys.exit(1)

    print("==> Initializing SmallestAI client...")
    client = SmallestAI(api_key=api_key)

    # Note: To avoid unsolicited phone dials or costs, prompt user for callId or place synthetic test
    call_id = os.getenv("VALIDATE_CALL_ID")
    if not call_id:
        print("\n[STEP REQUIRED]")
        print(
            "Set VALIDATE_CALL_ID to an existing callId from your Atoms console to test retrieval:"
        )
        print("  export VALIDATE_CALL_ID=CALL-xxxxxxxxxxxxx-xxxxxx")
        print("  python scripts/validate_backfill.py")
        sys.exit(0)

    print(f"==> Fetching call details for callId: {call_id}...")
    try:
        resp = client.atoms.calls.get(id=call_id)
        data = resp.data
        if not data:
            print(f"Status: {resp.status}, but data was empty.")
            sys.exit(0)

        print("\n=== Observed API Response Fields ===")
        print(f"Call ID:           {data.call_id}")
        print(f"Status:            {data.status}")
        print(f"Duration:          {data.duration}")
        print(f"Has Transcript:    {bool(data.transcript)} ({len(data.transcript or [])} turns)")
        print(f"Has Post Analytics:{bool(data.post_call_analytics)}")
        print(f"Recording URL:     {'PRESENT (presigned)' if data.recording_url else 'NONE'}")

        print("\n==> Validation Result:")
        print("Observed that client.atoms.calls.get() returned conversation details.")
        print(
            "Update docs/EVIDENCE.md to record this hypothesis as CONFIRMED FACT with today's date."
        )

    except Exception as exc:
        print(f"\nAPI Error during backfill experiment: {exc}")
        print(
            "If 404/not-found or 401/unauthorized occurred, check your key permissions and retention window."
        )


if __name__ == "__main__":
    run_experiment()
