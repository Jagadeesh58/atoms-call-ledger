"""
WHY:
Plans and executes API recovery of calls at risk of ZDR deadline loss. Exists
because Atoms does not retry; pre-expiry API backfill is the only recovery
mechanism. Executor requires an injected client (real or MOCK) and must never
synthesize fake webhook events or cache recording URLs.
"""

from __future__ import annotations

import json
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

from atoms_ledger.config import LedgerConfig
from atoms_ledger.models import BackfillTask
from atoms_ledger.reconcile import STATE_COMPLETE, STATE_QUARANTINED_ONLY, evaluate
from atoms_ledger.store import LedgerStore

# Recording URL expiry note — must never be cached.
_RECORDING_URL_NOTE = "recording presigned URLs expire in 15 minutes; do not cache"

# Result codes
RESULT_RECOVERED = "RECOVERED"
RESULT_UNRECOVERABLE = "UNRECOVERABLE"
RESULT_RETRY_LATER = "RETRY_LATER"
RESULT_CONFIG_ERROR = "CONFIG_ERROR"
RESULT_SKIPPED = "SKIPPED"


# ---------------------------------------------------------------------------
# MOCK client for testing
# ---------------------------------------------------------------------------


class MockAtomsClient:
    """
    MOCK: Simulates Atoms API responses for testing.
    NOT a real integration.
    """

    # MOCK response data for a successful recovery
    _MOCK_RECOVERED_DATA = {
        "call_id": None,  # filled per call
        "status": "completed",
        "duration": 120,
        "from_": "+91000000001",
        "to": "+91000000002",
        "type": "outbound",
        "transcript": [],
        "variables": {},
        "post_call_analytics": None,
        # NOTE: recording presigned URLs expire in 15 minutes; do not cache
        "recording_url": None,
        "recording_dual_url": None,
    }

    def __init__(
        self,
        behavior: str = "success",
        fail_call_ids: Optional[list] = None,
        call_data_overrides: Optional[Dict[str, Any]] = None,
    ) -> None:
        # MOCK behaviors: success, not_found, timeout, rate_limit, auth_error
        self._behavior = behavior
        self._fail_call_ids = fail_call_ids or []
        self._call_data_overrides = call_data_overrides or {}
        self._call_count: int = 0
        # Expose atoms.calls interface identical to SmallestAI SDK shape
        self.atoms = _MockAtoms(self)


class _MockGetCallsResponse:
    """MOCK: Simulates GetCallsResponse from smallestai SDK."""

    def __init__(self, call_id: str, data: Dict[str, Any]) -> None:
        self.status = "success"
        self.data = _MockGetCallsResponseData(call_id, data)


class _MockGetCallsResponseData:
    """MOCK: Simulates GetCallsResponseData from smallestai SDK."""

    def __init__(self, call_id: str, data: Dict[str, Any]) -> None:
        self.call_id = call_id
        self.status = data.get("status", "completed")
        self.duration = data.get("duration", 120)
        self.from_ = data.get("from_", "+91000000001")
        self.to = data.get("to", "+91000000002")
        self.type = data.get("type", "outbound")
        self.transcript = data.get("transcript", [])
        self.variables = data.get("variables", {})
        self.post_call_analytics = data.get("post_call_analytics")
        # NOTE: recording presigned URLs expire in 15 minutes; do not cache
        self.recording_url = data.get("recording_url")
        self.recording_dual_url = data.get("recording_dual_url")


class _MockCalls:
    """MOCK: Simulates atoms.calls API namespace."""

    def __init__(self, client: MockAtomsClient) -> None:
        self._client = client

    def get(self, id: str) -> _MockGetCallsResponse:
        """MOCK: Returns simulated API response."""
        self._client._call_count += 1
        behavior = self._client._behavior

        if id in self._client._fail_call_ids:
            behavior = "not_found"

        if behavior == "success":
            data = dict(MockAtomsClient._MOCK_RECOVERED_DATA)
            data.update(self._client._call_data_overrides.get(id, {}))
            data["call_id"] = id
            return _MockGetCallsResponse(id, data)
        elif behavior == "not_found":
            raise _MockAPIError(404, "not found")
        elif behavior == "timeout":
            raise _MockAPIError(0, "timeout")
        elif behavior == "rate_limit":
            raise _MockAPIError(429, "rate limited")
        elif behavior == "auth_error":
            raise _MockAPIError(401, "unauthorized")
        else:
            raise ValueError(f"MOCK: unknown behavior {behavior!r}")


class _MockAtoms:
    """MOCK: Simulates atoms API namespace."""

    def __init__(self, client: MockAtomsClient) -> None:
        self.calls = _MockCalls(client)


class _MockAPIError(Exception):
    """MOCK: Simulates API error responses."""

    def __init__(self, status_code: int, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code


# ---------------------------------------------------------------------------
# Plan
# ---------------------------------------------------------------------------


def plan(
    store: LedgerStore,
    config: LedgerConfig,
    now: datetime,
    max_per_batch: int = 100,
) -> List[BackfillTask]:
    """
    Return urgency-ordered (least remaining first) tasks for overdue/at-risk
    calls, chunked to max_per_batch.
    """
    views = evaluate(store, config, now)

    tasks: List[BackfillTask] = []
    for view in views:
        # Only plan recovery for incomplete, non-quarantine-only states
        if view.state in (STATE_COMPLETE, STATE_QUARANTINED_ONLY):
            continue
        # Only if there are missing required events and deadline is known
        if not view.missing_required_events:
            continue
        remaining = view.remaining_seconds if view.remaining_seconds is not None else float("inf")
        tasks.append(
            BackfillTask(
                call_id=view.call_id,
                urgency_rank=0,  # set below after sort
                remaining_seconds=remaining,
                missing_events=list(view.missing_required_events),
                agent_id=view.agent_id,
            )
        )

    # Sort by least remaining first (most urgent)
    tasks.sort(key=lambda t: t.remaining_seconds)
    for rank, task in enumerate(tasks):
        task.urgency_rank = rank

    return tasks[:max_per_batch]


# ---------------------------------------------------------------------------
# Execute
# ---------------------------------------------------------------------------


def execute(
    client: Any,  # SmallestAI instance or MockAtomsClient
    tasks: List[BackfillTask],
    store: LedgerStore,
    config: LedgerConfig,
) -> Dict[str, str]:
    """
    Execute backfill tasks by querying the Atoms API.

    Returns a dict mapping call_id → result_code.
    One failing task does NOT stop others, except auth_error which halts immediately.
    """
    results: Dict[str, str] = {}

    for task in tasks:
        if task.call_id in results:
            continue  # already processed (shouldn't happen but guard)

        result = _fetch_one(client, task, store)
        results[task.call_id] = result

        if result == RESULT_CONFIG_ERROR:
            # Auth/config errors: mark remaining tasks skipped and stop
            for remaining_task in tasks:
                if remaining_task.call_id not in results:
                    results[remaining_task.call_id] = RESULT_SKIPPED
            break

    return results


def _fetch_one(
    client: Any,
    task: BackfillTask,
    store: LedgerStore,
    max_attempts: int = 3,
) -> str:
    """Fetch a single call with bounded exponential backoff. Returns result code."""
    last_error: Optional[str] = None
    attempt = 0

    while attempt < max_attempts:
        attempt += 1
        try:
            response = client.atoms.calls.get(id=task.call_id)
            # Success
            recovered_data = _response_to_dict(response)
            recovered_json = json.dumps(recovered_data)

            now_iso = datetime.utcnow().isoformat()
            backfill_row = store.get_backfill(task.call_id)
            prior_attempts = backfill_row["attempts"] if backfill_row else 0

            store.upsert_backfill(
                call_id=task.call_id,
                state=RESULT_RECOVERED,
                attempts=prior_attempts + attempt,
                last_error=None,
                last_attempt_at=now_iso,
                # NOTE: recording presigned URLs expire in 15 minutes; do not cache
                recovered_json=recovered_json,
            )
            return RESULT_RECOVERED

        except Exception as exc:
            last_error = str(exc)
            status_code = getattr(exc, "status_code", 0)

            if status_code in (401, 403):
                # Auth/config error — do not retry
                store.upsert_backfill(
                    call_id=task.call_id,
                    state=RESULT_CONFIG_ERROR,
                    attempts=attempt,
                    last_error=last_error,
                    last_attempt_at=datetime.utcnow().isoformat(),
                    recovered_json=None,
                )
                return RESULT_CONFIG_ERROR

            if status_code == 404:
                # Not found — permanently unrecoverable
                store.upsert_backfill(
                    call_id=task.call_id,
                    state=RESULT_UNRECOVERABLE,
                    attempts=attempt,
                    last_error=last_error,
                    last_attempt_at=datetime.utcnow().isoformat(),
                    recovered_json=None,
                )
                return RESULT_UNRECOVERABLE

            # Transient error (timeout, 429, 5xx) → backoff and retry
            if attempt < max_attempts:
                backoff = min(2 ** (attempt - 1), 8)
                time.sleep(backoff)

    # Exhausted retries
    store.upsert_backfill(
        call_id=task.call_id,
        state=RESULT_RETRY_LATER,
        attempts=attempt,
        last_error=last_error,
        last_attempt_at=datetime.utcnow().isoformat(),
        recovered_json=None,
    )
    return RESULT_RETRY_LATER


def _response_to_dict(response: Any) -> Dict[str, Any]:
    """
    Convert API response to serializable dict.
    NOTE: recording presigned URLs expire in 15 minutes; do not cache.
    The recording_url is stored only in recovered_json; never cached or logged separately.
    """
    data = response.data
    result: Dict[str, Any] = {
        "call_id": getattr(data, "call_id", None),
        "status": getattr(data, "status", None),
        "duration": getattr(data, "duration", None),
        "from_": getattr(data, "from_", None),
        "to": getattr(data, "to", None),
        "type": getattr(data, "type", None),
        "transcript": getattr(data, "transcript", []),
        "variables": getattr(data, "variables", {}),
        "post_call_analytics": getattr(data, "post_call_analytics", None),
        # NOTE: recording presigned URLs expire in 15 minutes; do not cache
        "_recording_note": _RECORDING_URL_NOTE,
        "recording_url": getattr(data, "recording_url", None),
        "recording_dual_url": getattr(data, "recording_dual_url", None),
    }
    return result
