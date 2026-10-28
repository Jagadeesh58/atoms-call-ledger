"""Unit tests for HMAC-SHA256 signature verification over raw body bytes."""

import hmac
import json

from atoms_ledger.signature import compute_signature, verify_signature


def test_valid_signature():
    secret = "test-secret-key-1"
    body = b'{"event":"test","callId":"CALL-123"}'
    sig = compute_signature(body, secret)
    ok, reason = verify_signature(body, sig, [secret])
    assert ok is True
    assert reason == "verified"


def test_wrong_secret():
    secret = "correct-secret"
    wrong_secret = "wrong-secret"
    body = b'{"hello":"world"}'
    sig = compute_signature(body, secret)
    ok, reason = verify_signature(body, sig, [wrong_secret])
    assert ok is False
    assert reason == "signature_mismatch"


def test_flipped_bit_in_body():
    secret = "test-secret"
    body = b'{"val": 1}'
    tampered_body = b'{"val": 2}'
    sig = compute_signature(body, secret)
    ok, reason = verify_signature(tampered_body, sig, [secret])
    assert ok is False
    assert reason == "signature_mismatch"


def test_flipped_bit_in_signature():
    secret = "test-secret"
    body = b'{"callId":"CALL-123"}'
    sig = compute_signature(body, secret)
    # Flip the last character
    flipped_char = "0" if sig[-1] != "0" else "1"
    tampered_sig = sig[:-1] + flipped_char
    ok, reason = verify_signature(body, tampered_sig, [secret])
    assert ok is False
    assert reason == "signature_mismatch"


def test_uppercase_hex():
    secret = "test-secret"
    body = b'{"status":"completed"}'
    sig = compute_signature(body, secret).upper()
    ok, reason = verify_signature(body, sig, [secret])
    assert ok is True
    assert reason == "verified"


def test_non_hex():
    secret = "test-secret"
    body = b'{"status":"completed"}'
    # Non-hex characters
    bad_sig = "z" * 64
    ok, reason = verify_signature(body, bad_sig, [secret])
    assert ok is False
    assert reason == "invalid_hex"

    # Too short
    ok, reason = verify_signature(body, "abcdef12345", [secret])
    assert ok is False
    assert reason == "invalid_hex"


def test_empty_header():
    secret = "test-secret"
    body = b'{"status":"completed"}'
    ok, reason = verify_signature(body, None, [secret])
    assert ok is False
    assert reason == "missing_signature"

    ok, reason = verify_signature(body, "", [secret])
    assert ok is False
    assert reason == "missing_signature"

    ok, reason = verify_signature(body, "   ", [secret])
    assert ok is False
    assert reason == "missing_signature"


def test_no_configured_secrets():
    body = b'{"status":"completed"}'
    sig = compute_signature(body, "any-secret")
    ok, reason = verify_signature(body, sig, [])
    assert ok is False
    assert reason == "no_configured_secrets"


def test_reserialized_json_fails():
    """Documented fact: Re-serializing JSON changes whitespace/keys and breaks signature."""
    secret = "test-secret"
    original_raw = b'{"b": 2, "a": 1, "msg": "hello"}'
    sig = compute_signature(original_raw, secret)

    # Parse and re-serialize with standard python dump (sorted keys or different whitespace)
    parsed = json.loads(original_raw.decode("utf-8"))
    reserialized = json.dumps(parsed, sort_keys=True).encode("utf-8")

    assert original_raw != reserialized
    ok, reason = verify_signature(reserialized, sig, [secret])
    assert ok is False
    assert reason == "signature_mismatch"


def test_unicode_and_emojis():
    secret = "test-secret"
    body = '{"transcript": "नमस्ते world \U0001f600"}'.encode("utf-8")
    sig = compute_signature(body, secret)
    ok, reason = verify_signature(body, sig, [secret])
    assert ok is True
    assert reason == "verified"


def test_secret_rotation():
    secret_old = "old-secret-1"
    secret_new = "active-secret-2"
    body = b'{"event":"post-conversation"}'

    # Signature signed with old secret should pass if old secret is in the list
    sig_old = compute_signature(body, secret_old)
    ok, reason = verify_signature(body, sig_old, [secret_new, secret_old])
    assert ok is True
    assert reason == "verified"

    # Signature signed with new secret also passes
    sig_new = compute_signature(body, secret_new)
    ok, reason = verify_signature(body, sig_new, [secret_new, secret_old])
    assert ok is True
    assert reason == "verified"


def test_constant_time_compare_called(monkeypatch):
    """Verify that hmac.compare_digest is strictly invoked for verification."""
    compare_digest_called = False
    original_compare = hmac.compare_digest

    def spy_compare(a, b):
        nonlocal compare_digest_called
        compare_digest_called = True
        return original_compare(a, b)

    monkeypatch.setattr(hmac, "compare_digest", spy_compare)

    secret = "test-secret"
    body = b'{"test":"constant_time"}'
    sig = compute_signature(body, secret)
    ok, _ = verify_signature(body, sig, [secret])
    assert ok is True
    assert compare_digest_called is True
