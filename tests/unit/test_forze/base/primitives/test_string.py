
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
        assert normalize_string(normalized) == _full_pass(normalized)
