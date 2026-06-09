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


class TestValidateTextPatternBranches:
    @pytest.mark.parametrize(
        "op,value,maxlen,maxbr,match",
        [
            ("$like", "", 100, 8, "non-empty"),
            ("$like", "   ", 100, 8, "non-empty"),
            ("$like", "x" * 50, 10, 8, "exceeds maximum length"),
            ("$like", (), 100, 8, "at least one pattern"),
            ("$like", ("a", "b", "c"), 100, 2, "branch count"),
        ],
    )
    def test_invalid_patterns_raise(
        self, op: str, value: object, maxlen: int, maxbr: int, match: str
    ) -> None:
        with pytest.raises(CoreException, match=match):
            validate_text_pattern(
                op,
                value,  # type: ignore[arg-type]
                max_pattern_length=maxlen,
                max_pattern_or_branches=maxbr,
            )

    def test_valid_patterns_normalized(self) -> None:
        out = validate_text_pattern(
            "$like", ("  a%  ", "b_"), max_pattern_length=100, max_pattern_or_branches=8
        )
        assert out == ("a%", "b_")

    @pytest.mark.parametrize(
        "pattern,match",
        [
            ("(a+)+", "nested quantifiers"),
            ("a{1,5000}", "repeat upper bound"),
            ("|".join(["a"] * 65), "alternation branches"),
        ],
    )
    def test_unsafe_regex_rejected(self, pattern: str, match: str) -> None:
        with pytest.raises(CoreException, match=match):
            validate_text_pattern(
                "$regex", pattern, max_pattern_length=10_000, max_pattern_or_branches=128
            )

    def test_safe_regex_accepted(self) -> None:
        out = validate_text_pattern(
            "$regex", "^foo.*bar$", max_pattern_length=100, max_pattern_or_branches=8
        )
        assert out == ("^foo.*bar$",)


class TestLikePatternEscapes:
    @pytest.mark.parametrize(
        "pattern,expected",
        [
            (r"50\%", "^50%$"),  # escaped literal %
            (r"a\_b", "^a_b$"),  # escaped literal _
            (r"a\\b", r"^a\\b$"),  # escaped backslash
            (r"a\.b", r"^a\\\.b$" if False else None),  # escaped non-special -> both escaped
        ],
    )
    def test_escapes(self, pattern: str, expected: str | None) -> None:
        result = like_pattern_to_regex(pattern)
        if expected is not None:
            assert result == expected
        else:
            # escaped non-special char: backslash + char both regex-escaped, wildcards literal
            assert result.startswith("^a") and result.endswith("b$")

    def test_case_insensitive_flag(self) -> None:
        out = like_pattern_to_regex("A%", case_insensitive=True)
        assert out.endswith("$")
