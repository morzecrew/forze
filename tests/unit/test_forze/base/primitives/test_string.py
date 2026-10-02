
from enum import StrEnum

import hypothesis.strategies as st
import pytest
from hypothesis import given, settings

from forze.base.primitives import string as string_module
from forze.base.primitives.string import normalize_string


class TestNormalizeString:
    def test_none_passthrough(self) -> None:
        assert normalize_string(None) is None

    def test_collapses_whitespace_and_trims_lines(self) -> None:
        s = "  hello   world  \n  second\tline  "
        assert normalize_string(s) == "hello world\nsecond line"

    def test_removes_invisible_chars(self) -> None:
        s = "abc\u200b\u2060def"
        assert normalize_string(s) == "abcdef"

    def test_removes_bom(self) -> None:
        s = "\ufeffhello"
        assert normalize_string(s) == "hello"

    def test_replaces_crlf_with_lf(self) -> None:
        assert normalize_string("a\r\nb") == "a\nb"

    def test_replaces_cr_with_lf(self) -> None:
        assert normalize_string("a\rb") == "a\nb"

    def test_replaces_nbsp_with_space(self) -> None:
        assert normalize_string("a\u00a0b") == "a b"

    def test_preserves_newlines(self) -> None:
        assert normalize_string("a\nb\nc") == "a\nb\nc"

    def test_strips_private_use_category_chars(self) -> None:
        out = normalize_string("a\ue000b")
        assert out == "ab"

    def test_strips_format_chars_not_in_keep_list(self) -> None:
        s = "a\u200eb"  # LEFT-TO-RIGHT MARK (Cf, not in KEEP_CF)
        result = normalize_string(s)
        assert result == "ab"

    def test_preserves_zwj(self) -> None:
        s = "a\u200db"  # ZWJ (kept)
        result = normalize_string(s)
        assert "\u200d" in result

    def test_preserves_emoji_presentation_selector(self) -> None:
        s = "\u2764\ufe0f"  # heart + VS16
        result = normalize_string(s)
        assert "\ufe0f" in result

    def test_nfc_normalization(self) -> None:
        s = "e\u0301"  # decomposed é
        result = normalize_string(s)
        assert result == "\u00e9"  # NFC: precomposed é

    def test_empty_string(self) -> None:
        assert normalize_string("") == ""

    def test_whitespace_only_collapses_to_empty(self) -> None:
        assert normalize_string("   \t  ") == ""

    def test_multiline_trimming(self) -> None:
        s = "  line1  \n  line2  \n  line3  "
        assert normalize_string(s) == "line1\nline2\nline3"

    def test_multiple_invisible_chars_together(self) -> None:
        s = "\ufeff\u200b\u2060\u180e"
        assert normalize_string(s) == ""

    def test_preserves_zwnj(self) -> None:
        s = "a\u200cb"
        result = normalize_string(s)
        assert "\u200c" in result

    def test_mixed_whitespace_and_invisible(self) -> None:
        s = "  \u200b hello \u2060 world  "
        assert normalize_string(s) == "hello world"

    # ----------------------- #
    # ASCII fast path: pure-ASCII text skips NFC + the per-char scan, so it must
    # still collapse whitespace, preserve newlines, and return content unchanged.

    def test_ascii_text_passes_through_unchanged(self) -> None:
        s = "Hello World 123 - the quick brown fox."
        assert normalize_string(s) == s

    def test_ascii_fast_path_collapses_whitespace_keeps_newlines(self) -> None:
        s = "alpha\t\tbeta   gamma\nsecond   line"
        assert normalize_string(s) == "alpha beta gamma\nsecond line"

    def test_ascii_fast_path_keeps_all_printable_ascii(self) -> None:
        # Every printable-ASCII char survives (none fall in the stripped/format
        # categories the non-ASCII branch filters).
        s = "a1!?,.:;_-/=+()[]{}@#$%^&*"
        assert normalize_string(s) == s


# ----------------------- #
# Already-normalized fast path: text that the full pass would return unchanged is
# returned as-is. Every input must come out exactly as the full pass makes it.


def _full_pass(s: str) -> str:
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(string_module, "_is_normalized", lambda _: False)
        return normalize_string(s)


# One input per thing the full pass changes; each is also tried inside a multi-line text.
_CHANGED_BY_THE_FULL_PASS = [
    "a\tb",
    "a  b",
    " a",
    "a ",
    "e\u0301",  # decomposed é: printable, but not NFC
    "a\rb",
    "a\r\nb",
    "a\u00a0b",  # NBSP
    "a\ufeffb",  # BOM
    "a\u200bb",  # ZWSP
    "a\u2060b",  # word joiner
    "a\u180eb",  # Mongolian vowel separator
    "a\u200eb",  # LRM
    "a\u202eb",  # RLO
    "a\u00adb",  # soft hyphen
    "a\u2028b",  # line separator
    "a\u2029b",  # paragraph separator
    "a\u3000b",  # ideographic space
    "a\ue000b",  # private use
    "a\u0378b",  # unassigned
    "a\x00b",  # NUL
    "a\x1bb",  # ESC
    "a\x7fb",  # DEL
    "a\x85b",  # NEL, a C1 whitespace control
    "a\x9fb",  # a C1 control
    "a\u200db",  # ZWJ: kept, but takes the full pass
    "a\u200cb",  # ZWNJ: kept, but takes the full pass
]

_UNCHANGED_BY_THE_FULL_PASS = [
    "",
    "a",
    "Плата управления двигателем постоянного тока",
    "Motor control board, rev 4",
    "Используется в сборке шасси.\nПоставщик: ООО «Ромашка», партия 12.",
    "first\n\nthird",
    "\nleading newline",
    "trailing newline\n",
    "caf\u00e9 \u00e9t\u00e9",
    "emoji \U0001f600 ok",
    "中文 日本語 한국어",
    "\u2764\ufe0f \u2764\ufe0e",  # variation selectors are marks, not format characters
]


class TestTheAlreadyNormalizedFastPath:
    @pytest.mark.parametrize(
        "text",
        [
            *_CHANGED_BY_THE_FULL_PASS,
            *(f"line one\n{s}\nline three" for s in _CHANGED_BY_THE_FULL_PASS),
            *_UNCHANGED_BY_THE_FULL_PASS,
        ],
    )
    def test_the_output_is_the_full_pass_output(self, text: str) -> None:
        assert normalize_string(text) == _full_pass(text)

    @pytest.mark.parametrize("text", _CHANGED_BY_THE_FULL_PASS)
    def test_text_the_full_pass_changes_takes_it(self, text: str) -> None:
        assert not string_module._is_normalized(text)  # pyright: ignore[reportPrivateUsage]
        assert not string_module._is_normalized(f"x\n{text}\nx")  # pyright: ignore[reportPrivateUsage]

    @pytest.mark.parametrize("text", _UNCHANGED_BY_THE_FULL_PASS)
    def test_text_already_normalized_skips_it(self, text: str) -> None:
        assert string_module._is_normalized(text)  # pyright: ignore[reportPrivateUsage]

    @settings(max_examples=300, deadline=None)
    @given(
        st.text(
            alphabet=st.one_of(
                st.sampled_from(
                    [
                        " ",
                        "\n",
                        "\t",
                        "\r",
                        "a",
                        "\u0436",
                        "\u0301",
                        "\u00a0",
                        "\u200b",
                        "\u200d",
                        "\ufeff",
                        "\u2028",
                        "\u3000",
                        "\ue000",
                        "\ufe0f",
                        "e",
                        "\x00",
                        "\x1b",
                        "\x85",
                        "\u200e",
                        "\u0378",
                    ]
                ),
                st.characters(),
            ),
            max_size=12,
        )
    )
    def test_any_text_comes_out_as_the_full_pass_makes_it(self, text: str) -> None:
        # The full pass's own output is mostly normalized text, which takes the fast path.
        normalized = _full_pass(text)

        assert normalize_string(text) == normalized
        assert normalize_string(normalized) == _full_pass(normalized) == normalized


class TestIdempotence:
    """Normalizing normalized text changes nothing, so stored text takes the fast path."""

    @pytest.mark.parametrize(
        "text",
        [
            "\x9be\u200e\x1b\u200e\u0301",  # dropped characters between a letter and its accent
            "e\u200e\u0301",  # a format character
            "e\u0378\u0301",  # an unassigned code point
            "e\ue000\u0301",  # a private-use code point
        ],
    )
    def test_a_dropped_character_does_not_strand_an_accent(self, text: str) -> None:
        # Dropping it lets the letter and the accent compose, as NFC would have.
        normalized = normalize_string(text)

        assert normalized == "\u00e9"
        assert normalize_string(normalized) == normalized


_CONTROLS = [chr(c) for c in (*range(0x20), 0x7F, *range(0x80, 0xA0))]


class TestControlCharacters:
    """Control characters are dropped, except whitespace ones, which collapse like spaces.

    Postgres text columns refuse NUL, and none of them render.
    """

    @pytest.mark.parametrize("ch", [c for c in _CONTROLS if not c.isspace()], ids=ord)
    def test_one_that_is_not_whitespace_is_dropped(self, ch: str) -> None:
        assert normalize_string(f"a{ch}b") == "ab"
        assert normalize_string(f"\u0416{ch}b") == "\u0416b"

    @pytest.mark.parametrize(
        "ch", [c for c in _CONTROLS if c.isspace() and c not in "\n\r"], ids=ord
    )
    def test_one_that_is_whitespace_collapses_to_a_space(self, ch: str) -> None:
        assert normalize_string(f"a{ch}b") == "a b"
        assert normalize_string(f"\u0416{ch}b") == "\u0416 b"


class TestAStringSubclass:
    def test_comes_back_a_plain_string(self) -> None:
        class _Choice(StrEnum):
            ALPHA = "alpha"

        out = normalize_string(_Choice.ALPHA)

        assert (type(out), out) == (str, "alpha")
