"""``generate_nonce`` / ``generate_state`` helper tests."""

from __future__ import annotations

import re

import pytest

from forze_identity.oauth import generate_nonce, generate_state

pytestmark = pytest.mark.unit

_URLSAFE = re.compile(r"^[A-Za-z0-9_-]+$")


def test_generate_nonce_is_urlsafe_with_sane_entropy() -> None:
    value = generate_nonce()

    assert _URLSAFE.match(value)
    assert len(value) >= 43  # 32 bytes -> 43 base64url chars


def test_generate_state_is_urlsafe_with_sane_entropy() -> None:
    value = generate_state()

    assert _URLSAFE.match(value)
    assert len(value) >= 43


def test_values_are_distinct_across_calls() -> None:
    values = {generate_nonce() for _ in range(64)}
    values |= {generate_state() for _ in range(64)}

    assert len(values) == 128
