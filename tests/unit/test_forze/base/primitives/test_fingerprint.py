"""Tests for :func:`~forze.base.primitives.fingerprint.stable_fingerprint`."""

from forze.base.primitives.fingerprint import stable_fingerprint


def test_stable_fingerprint_is_deterministic() -> None:
    assert stable_fingerprint("a", "b") == stable_fingerprint("a", "b")


def test_stable_fingerprint_differs_for_different_parts() -> None:
    assert stable_fingerprint("a") != stable_fingerprint("b")
