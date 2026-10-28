"""
WHY:
Centralizes runtime configuration and validation for atoms-call-ledger.
Exists because Atoms webhook retention windows (1h to 30d) and security secrets
must be strictly validated at startup to prevent silent data loss or buffer overflows.
It must never log secrets or expose unvalidated bounds.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Dict, List, Set

DEMO_WEBHOOK_SECRET = "demo-secret-not-a-real-secret"
MIN_RETENTION_WINDOW_SECONDS = 3600  # Documented 1 hour
MAX_RETENTION_WINDOW_SECONDS = 2592000  # Documented 30 days


@dataclass
class LedgerConfig:
    """Configuration container for Atoms Call Ledger."""

    webhook_secrets: List[str] = field(default_factory=lambda: [DEMO_WEBHOOK_SECRET])
    smallest_api_key: str | None = None
    ledger_db_path: str = ":memory:"
    retention_window_seconds: int = 86400
    required_events: Set[str] = field(
        default_factory=lambda: {"post-conversation", "analytics-completed"}
    )
    analytics_expected_statuses: Set[str] = field(default_factory=lambda: {"completed"})
    analytics_grace_seconds: int = 600
    post_reorder_grace_seconds: int = 60
    max_call_seconds: int = 3600
    max_body_bytes: int = 10_485_760  # 10 MB
    agent_required_events: Dict[str, Set[str]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if (
            self.retention_window_seconds < MIN_RETENTION_WINDOW_SECONDS
            or self.retention_window_seconds > MAX_RETENTION_WINDOW_SECONDS
        ):
            raise ValueError(
                f"retention_window_seconds must be between {MIN_RETENTION_WINDOW_SECONDS} and "
                f"{MAX_RETENTION_WINDOW_SECONDS} seconds (1 hour to 30 days). "
                f"Got: {self.retention_window_seconds}"
            )
        if self.max_body_bytes <= 0:
            raise ValueError(f"max_body_bytes must be positive. Got: {self.max_body_bytes}")
        if not self.webhook_secrets:
            self.webhook_secrets = [DEMO_WEBHOOK_SECRET]

    @classmethod
    def from_env(cls) -> LedgerConfig:
        """Create configuration loaded from environment variables with safe defaults."""
        raw_secrets = os.getenv("ATOMS_WEBHOOK_SECRETS")
        if raw_secrets:
            secrets = [s.strip() for s in raw_secrets.split(",") if s.strip()]
        else:
            secrets = [DEMO_WEBHOOK_SECRET]

        raw_retention = os.getenv("RETENTION_WINDOW_SECONDS")
        retention = int(raw_retention) if raw_retention else 86400

        raw_req = os.getenv("REQUIRED_EVENTS")
        if raw_req:
            req_events = {e.strip() for e in raw_req.split(",") if e.strip()}
        else:
            req_events = {"post-conversation", "analytics-completed"}

        raw_statuses = os.getenv("ANALYTICS_EXPECTED_STATUSES")
        if raw_statuses:
            exp_statuses = {s.strip() for s in raw_statuses.split(",") if s.strip()}
        else:
            exp_statuses = {"completed"}

        raw_grace = os.getenv("ANALYTICS_GRACE_SECONDS")
        grace = int(raw_grace) if raw_grace else 600

        raw_reorder = os.getenv("POST_REORDER_GRACE_SECONDS")
        reorder_grace = int(raw_reorder) if raw_reorder else 60

        raw_max_call = os.getenv("MAX_CALL_SECONDS")
        max_call = int(raw_max_call) if raw_max_call else 3600

        raw_max_body = os.getenv("MAX_BODY_BYTES")
        max_body = int(raw_max_body) if raw_max_body else 10_485_760

        return cls(
            webhook_secrets=secrets,
            smallest_api_key=os.getenv("SMALLEST_API_KEY"),
            ledger_db_path=os.getenv("LEDGER_DB_PATH", ":memory:"),
            retention_window_seconds=retention,
            required_events=req_events,
            analytics_expected_statuses=exp_statuses,
            analytics_grace_seconds=grace,
            post_reorder_grace_seconds=reorder_grace,
            max_call_seconds=max_call,
            max_body_bytes=max_body,
        )

    def get_required_events_for_agent(self, agent_id: str | None) -> Set[str]:
        """Return required events for a specific agent, falling back to global config."""
        if agent_id and agent_id in self.agent_required_events:
            return set(self.agent_required_events[agent_id])
        return set(self.required_events)
