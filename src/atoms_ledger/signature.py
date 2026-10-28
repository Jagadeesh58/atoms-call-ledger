"""
WHY:
Verifies HMAC-SHA256 signatures over raw request body bytes for Atoms webhooks.
Exists because Atoms documentation explicitly specifies signatures are computed over
raw bytes (not re-serialized JSON) and relies on X-Signature for all authentication.
It must never re-serialize JSON, leak secrets, or use timing-vulnerable comparisons.
"""

from __future__ import annotations

import hashlib
import hmac
import re
from typing import Sequence

HEX_PATTERN = re.compile(r"^[0-9a-fA-F]{64}$")


def compute_signature(raw_bytes: bytes, secret: str) -> str:
    """Compute HMAC-SHA256 hex digest over raw request body bytes.

    Args:
        raw_bytes: The exact unparsed byte sequence received from the network.
        secret: Webhook signing secret string.

    Returns:
        Hexadecimal HMAC-SHA256 signature in lowercase.
    """
    return hmac.new(secret.encode("utf-8"), raw_bytes, hashlib.sha256).hexdigest()


def verify_signature(
    raw_bytes: bytes,
    signature_header: str | None,
    secrets: Sequence[str],
) -> tuple[bool, str]:
    """Verify raw request body bytes against an X-Signature header using constant-time comparison.

    Supports key rotation by validating against each secret in `secrets` in order.

    Args:
        raw_bytes: Exact bytes of the HTTP request payload.
        signature_header: Raw string from the X-Signature HTTP header.
        secrets: Sequence of configured signing secrets.

    Returns:
        tuple[bool, str]: (is_valid, reason_code)
            Reasons: 'verified', 'missing_signature', 'no_configured_secrets',
                     'invalid_hex', 'signature_mismatch'
    """
    if not signature_header or not signature_header.strip():
        return False, "missing_signature"

    clean_header = signature_header.strip()

    # Atoms signature is a 64-character hex string (SHA256 hex digest)
    if not HEX_PATTERN.match(clean_header):
        return False, "invalid_hex"

    if not secrets:
        return False, "no_configured_secrets"

    header_lower = clean_header.lower()

    for secret in secrets:
        if not secret:
            continue
        expected = compute_signature(raw_bytes, secret).lower()
        if hmac.compare_digest(header_lower, expected):
            return True, "verified"

    return False, "signature_mismatch"
