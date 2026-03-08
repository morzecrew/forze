from forze.base.primitives.string import normalize_string


def test_normalize_string_none_passthrough() -> None:
    assert normalize_string(None) is None


def test_normalize_string_collapses_whitespace_and_trims_lines() -> None:
    s = "  hello   world  \n  second\tline  "
    normalized = normalize_string(s)
    assert normalized == "hello world\nsecond line"


def test_normalize_string_removes_invisible_chars() -> None:
    s = "abc\u200b\u2060def"  # contains invisible chars that should be stripped
    normalized = normalize_string(s)
    assert normalized == "abcdef"
