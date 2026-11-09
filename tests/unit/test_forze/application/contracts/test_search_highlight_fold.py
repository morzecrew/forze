"""The highlight fold equals the per-character fold it replaced, code point for code point.

Highlight offsets found in the folded text slice the original text, so the fold must keep one
code point per input character. It used to fold character by character; it now lowers the
whole string at once where that agrees, which this suite checks against the old fold.
"""

from __future__ import annotations

import sys

import hypothesis.strategies as st
import pytest
from hypothesis import given, settings

from forze.application.contracts.search import highlight_fragments, highlight_tokens
from forze.application.contracts.search.facet_highlight import (
    _casefold,  # pyright: ignore[reportPrivateUsage]
)

# ----------------------- #


def _per_character_fold(text: str) -> str:
    """The fold as it stood before the whole-string fast path."""

    return "".join(ch.lower()[:1] for ch in text)


# Code points where ``str.lower()`` departs from the per-character fold, plus their neighbours.
_TRICKY = "ΣσςİiIıΑαAa ʼ'̇ͅ"


class TestTheFoldMatchesThePerCharacterFold:
    @settings(max_examples=1000, deadline=None)
    @given(
        st.text(
            alphabet=st.one_of(
                st.characters(exclude_categories=()),  # every code point, surrogates included
                st.sampled_from(_TRICKY),
            )
        )
    )
    def test_on_arbitrary_text(self, text: str) -> None:
        assert _casefold(text) == _per_character_fold(text)

    def test_on_every_code_point_after_a_cased_letter(self) -> None:
        # One string, so a final sigma's context and every code point run through one fold.
        text = "".join(f"A{chr(cp)} " for cp in range(sys.maxunicode + 1))

        assert _casefold(text) == _per_character_fold(text)

    @pytest.mark.parametrize(
        ("text", "folded"),
        [
            ("ΟΔΥΣΣΕΥΣ", "οδυσσευσ"),  # ``str.lower`` would end it with ``ς``
            ("İstanbul", "istanbul"),  # ``str.lower`` would make ``İ`` two code points
            ("ООО «БетаМед»", "ооо «бетамед»"),
            ("Hello World", "hello world"),
            ("", ""),
        ],
    )
    def test_on_the_code_points_that_differ(self, text: str, folded: str) -> None:
        assert _casefold(text) == folded == _per_character_fold(text)


class TestHighlightOffsetsStayOnTheOriginalText:
    def test_a_match_after_a_dotted_capital_i_keeps_its_position(self) -> None:
        text = "İzmir ve İstanbul"

        assert highlight_fragments(
            text, highlight_tokens(["istanbul"]), pre_tag="[", post_tag="]"
        ) == ("İzmir ve [İstanbul]",)

    def test_a_final_sigma_still_matches_a_lowercase_query(self) -> None:
        assert highlight_fragments(
            "ΟΔΥΣΣΕΥΣ", highlight_tokens(["ευσ"]), pre_tag="[", post_tag="]"
        ) == ("ΟΔΥΣΣ[ΕΥΣ]",)
