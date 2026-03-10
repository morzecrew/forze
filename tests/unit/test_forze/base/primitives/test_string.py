import pytest

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
