"""Unit tests for text pattern validation helpers."""

from forze.base.exceptions import CoreException
import pytest

from forze.application.contracts.querying.internal.parse import QueryFilterLimits
from forze.application.contracts.querying.internal.text_pattern import (
    like_pattern_to_regex,
    validate_text_pattern,
)

def _limits(**kwargs: int) -> tuple[int, int]:
    lim = QueryFilterLimits(**kwargs)
    return lim.max_pattern_length, lim.max_pattern_or_branches

class TestLikePatternToRegex:
    def test_wildcards(self) -> None:
        assert like_pattern_to_regex("%road%") == "^.*road.*$"
        assert like_pattern_to_regex("a_b") == "^a.b$"

    def test_escapes_literal_percent(self) -> None:
        assert like_pattern_to_regex(r"100\%") == "^100%$"

    def test_case_insensitive_prefix(self) -> None:
        assert like_pattern_to_regex("abc", case_insensitive=True) == "(?i)^abc$"

class TestValidateTextPattern:
    def test_single_pattern(self) -> None:
        max_len, max_br = _limits()
        out = validate_text_pattern(
            "$ilike",
            "%x%",
            max_pattern_length=max_len,
            max_pattern_or_branches=max_br,
        )
        assert out == ("%x%",)

    def test_sequence_patterns(self) -> None:
        max_len, max_br = _limits()
        out = validate_text_pattern(
            "$ilike",
            ["%a%", "%b%"],
            max_pattern_length=max_len,
            max_pattern_or_branches=max_br,
        )
        assert out == ("%a%", "%b%")

    def test_rejects_empty_sequence(self) -> None:
        max_len, max_br = _limits()
        with pytest.raises(CoreException, match="at least one pattern"):
            validate_text_pattern(
                "$ilike",
                [],
                max_pattern_length=max_len,
                max_pattern_or_branches=max_br,
            )

    def test_rejects_whitespace_only(self) -> None:
        max_len, max_br = _limits()
        with pytest.raises(CoreException, match="non-empty"):
            validate_text_pattern(
                "$ilike",
                "   ",
                max_pattern_length=max_len,
                max_pattern_or_branches=max_br,
            )

    def test_rejects_nested_quantifier_regex(self) -> None:
        max_len, max_br = _limits()
        with pytest.raises(CoreException, match="nested quantifiers"):
            validate_text_pattern(
                "$regex",
                "(a+)+",
                max_pattern_length=max_len,
                max_pattern_or_branches=max_br,
            )

    def test_rejects_oversized_pattern(self) -> None:
        max_len, max_br = _limits(max_pattern_length=4)
        with pytest.raises(CoreException, match="maximum length"):
            validate_text_pattern(
                "$like",
                "12345",
                max_pattern_length=max_len,
                max_pattern_or_branches=max_br,
            )

    def test_rejects_too_many_branches(self) -> None:
        max_len, max_br = _limits(max_pattern_or_branches=2)
        with pytest.raises(CoreException, match="branch count"):
            validate_text_pattern(
                "$ilike",
                ["%a%", "%b%", "%c%"],
                max_pattern_length=max_len,
                max_pattern_or_branches=max_br,
            )
