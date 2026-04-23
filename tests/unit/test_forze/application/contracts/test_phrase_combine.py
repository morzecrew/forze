"""Tests for :func:`~forze.application.contracts.search.effective_phrase_combine`."""

from forze.application.contracts.search import effective_phrase_combine


def test_effective_phrase_combine_defaults_to_any() -> None:
    assert effective_phrase_combine(None) == "any"
    assert effective_phrase_combine({}) == "any"


def test_effective_phrase_combine_respects_key() -> None:
    assert effective_phrase_combine({"phrase_combine": "all"}) == "all"
    assert effective_phrase_combine({"phrase_combine": "any"}) == "any"


def test_effective_phrase_combine_falls_back_for_unknown() -> None:
    unknown: dict = {"phrase_combine": "invalid"}
    assert effective_phrase_combine(unknown) == "any"
